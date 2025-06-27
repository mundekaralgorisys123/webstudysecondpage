import asyncio
import re
import os
import uuid
import logging
import base64
import random
import time
from datetime import datetime
from io import BytesIO
import httpx
from PIL import Image as PILImage
from openpyxl import Workbook
from openpyxl.drawing.image import Image as ExcelImage
from flask import Flask
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import json
import mimetypes
from proxysetup import get_browser_with_proxy_strategy
# Load environment
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

# Flask and paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')
# Resize image if needed
def resize_image(image_data, max_size=(100, 100)):
    try:
        img = PILImage.open(BytesIO(image_data))
        img.thumbnail(max_size, PILImage.LANCZOS)
        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=85)
        return buffer.getvalue()
    except Exception as e:
        log_event(f"Error resizing image: {e}")
        return image_data

mimetypes.add_type('image/webp', '.webp')


# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.png"
    image_full_path = os.path.join(image_folder, image_filename)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"

# Human-like delay
def random_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))

# Scroll to bottom of page to load all products
async def scroll_to_bottom(page):
    last_height = await page.evaluate("document.body.scrollHeight")
    while True:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(1, 3))  # Random delay between scrolls
        
        # Check if we've reached the bottom
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# Reliable page.goto wrapper
async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            await page.wait_for_selector(".sc-jkTopv", state="attached", timeout=30000)
            print("[Success] Product listing loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise
# Main scraper function
async def handle_jacobandco(url, max_pages=None):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_jacobandco_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    prev_prod_cout = 0
    load_more_clicks = 1
    
    while load_more_clicks <= max_pages:
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper =  ".pf-c"
                browser, page = await get_browser_with_proxy_strategy(p,url,product_wrapper)
                log_event(f"Successfully loaded: {url}")

                # Scroll to load all items
                await scroll_to_bottom(page)
                
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                # Get all product tiles
                product_wrapper = await page.query_selector("div.sc-jkTopv")
                products = await page.query_selector_all('[data-pf-type="ProductBox"]') if product_wrapper else []
                max_prod = len(products)
                products = products[prev_prod_cout: min(max_prod, prev_prod_cout + 30)]
                prev_prod_cout += len(products)

                if len(products) == 0:
                    log_event("No new products found, stopping the scraper.")
                    break

                logging.info(f"New products found: {len(products)}")
                print(f"New products found: {len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    
                    try:
                        name_tag = await product.query_selector("[data-product-type='title']")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    try:
                        # Handle price - check for both original and discounted price
                        price_tag = await product.query_selector("[data-product-type='price']")
                        compare_price_tag = await product.query_selector(".product-price__compare")
                        
                        current_price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                        compare_price = (await compare_price_tag.inner_text()).strip() if compare_price_tag else None
                        
                        if compare_price and compare_price != current_price:
                            price = f"{current_price}|{compare_price}"
                            additional_info.append(f"Discount: {compare_price} → {current_price}")
                        else:
                            price = current_price
                    except Exception:
                        price = "N/A"

                    try:
                        # Get the first/main image from the splide slider
                        main_image_slide = await product.query_selector(".splide__slide.is-active img")
                        if main_image_slide:
                            image_url = await main_image_slide.get_attribute("src")
                            # Clean up the image URL
                            if image_url and image_url != "N/A":
                                if image_url.startswith('//'):
                                    image_url = f"https:{image_url}"
                                image_url = image_url.split('?v=')[0]  # Remove version parameter
                        else:
                            image_url = "N/A"
                    except Exception:
                        image_url = "N/A"

                    try:
                        # Get product description/vendor info
                        vendor_tag = await product.query_selector("[data-product-type='vendor']")
                        product_description = (await vendor_tag.inner_text()).strip() if vendor_tag else ""
                    except Exception:
                        product_description = ""

                    # Extract gold type
                    gold_type_pattern = r"\b\d{1,2}(?:K|ct)?\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSterling Silver\b"
                    gold_type_match = re.search(gold_type_pattern, product_description or product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    # Extract diamond weight
                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, product_description or product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                    # Collect additional product information
                    try:
                        # Check for availability
                        availability_tag = await product.query_selector(".product-availability")
                        if availability_tag:
                            availability = (await availability_tag.inner_text()).strip()
                            additional_info.append(f"Availability: {availability}")
                    except Exception:
                        pass

                    try:
                        # Check for product options (like size, color)
                        options = await product.query_selector_all(".product-option-item")
                        if options:
                            option_texts = []
                            for option in options:
                                option_text = (await option.inner_text()).strip()
                                if option_text:
                                    option_texts.append(option_text)
                            if option_texts:
                                additional_info.append(f"Options: {'|'.join(option_texts)}")
                    except Exception:
                        pass

                    try:
                        # Check for product badges (like "New", "Sale")
                        badges = await product.query_selector_all(".product-badge")
                        if badges:
                            badge_texts = []
                            for badge in badges:
                                badge_text = (await badge.inner_text()).strip()
                                if badge_text:
                                    badge_texts.append(badge_text)
                            if badge_texts:
                                additional_info.append(f"Badges: {'|'.join(badge_texts)}")
                    except Exception:
                        pass

                    try:
                        # Check for rating information
                        rating_tag = await product.query_selector(".product-rating")
                        if rating_tag:
                            rating = (await rating_tag.inner_text()).strip()
                            additional_info.append(f"Rating: {rating}")
                    except Exception:
                        pass

                    try:
                        # Check for shipping information
                        shipping_tag = await product.query_selector(".shipping-info")
                        if shipping_tag:
                            shipping = (await shipping_tag.inner_text()).strip()
                            additional_info.append(f"Shipping: {shipping}")
                    except Exception:
                        pass

                    # Combine all additional info with pipe delimiter
                    additional_info_str = "|".join(additional_info) if additional_info else ""

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, product_description, kt, price, diamond_weight, additional_info_str))
                    sheet.append([current_date, page_title, product_name, product_description, kt, price, diamond_weight, time_only, image_url, additional_info_str])
                            
                # Process image downloads
                for row_num, unique_id, task in image_tasks:
                    try:
                        image_path = await asyncio.wait_for(task, timeout=60)
                        if image_path != "N/A":
                            try:
                                img = ExcelImage(image_path)
                                img.width, img.height = 100, 100
                                sheet.add_image(img, f"D{row_num}")
                            except Exception as e:
                                logging.error(f"Error embedding image: {e}")
                                image_path = "N/A"
                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Image download timed out for row {row_num}")
                
                load_more_clicks += 1
                all_records.extend(records)
                wb.save(file_path)
                
        except Exception as e:
            logging.error(f"Error during scraping: {str(e)}")
            wb.save(file_path)
        finally:
            if page: await page.close()
            if browser: await browser.close()

    if not all_records:
        return None, None, None

    # Save the workbook
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    # Encode the file in base64
    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    # Insert data into the database and update product count
    insert_into_db(all_records)
    update_product_count(len(all_records))

    # Return necessary information
    return base64_encoded, filename, file_path