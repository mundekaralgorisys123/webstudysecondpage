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
from proxysetup import get_browser_with_proxy_strategy
# Load environment
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

# Flask and paths
app = Flask(__name__)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(app.root_path, 'static', 'ExcelData')
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

# Transform URL to get high-res image
def modify_image_url(image_url):
    if not image_url or image_url == "N/A":
        return image_url
    
    modified_url = "https:"+image_url
    return modified_url 

# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    modified_url = modify_image_url(image_url)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(modified_url)
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

# Reliable page.goto wrapper
async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            await page.wait_for_selector(".collection__grid", state="attached", timeout=30000)
            print("[Success] Product cards loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function
async def handle_natasha(url, max_pages):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}, max_pages: {max_pages}")

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
    filename = f"handle_natasha_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    current_url = url
    while current_url and (page_count <= max_pages):
        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        page = None
        if page_count > 1:
            if '?' in current_url:
                current_url = f"{url}&page={page_count}"
            else:
                current_url = f"{url}?page={page_count}"
        try:
            async with async_playwright() as p:
                product_wrapper = ".collection__grid"
                browser, page = await get_browser_with_proxy_strategy(p, current_url,product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('.collection__grid').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_wrapper = await page.query_selector("div.collection__grid")
                products = await product_wrapper.query_selector_all("div.collection__grid-item") if product_wrapper else []
                logging.info(f"Total products scraped:{page_count} :{len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    print(f"Processing product {row_num} of {len(products)}")
                    additional_info = []
                    
                    try:
                        name_tag = await product.query_selector("h4.collection-product__title")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"
                
                    try:
                        price_tag = await product.query_selector("div.collection-product__price")
                        if price_tag:
                            # First try to get the price directly from the price container
                            price_text = (await price_tag.inner_text()).strip()
                            
                            # Clean up the price text
                            price_text = re.sub(r'\s+', ' ', price_text)  # Normalize whitespace
                            price_text = price_text.replace('from', '').replace('AUD', '').strip()
                            
                            # If we don't find a price in the main container, look for individual elements
                            if not any(c.isdigit() for c in price_text):
                                # Look for price elements within the container
                                price_elements = await price_tag.query_selector_all("span:not(.collection-product__price-from):not(.collection-product__price-currency)")
                                prices = []
                                for elem in price_elements:
                                    elem_text = (await elem.inner_text()).strip()
                                    if elem_text and any(c.isdigit() for c in elem_text):
                                        prices.append(elem_text)
                                
                                if prices:
                                    price = " | ".join(prices)
                                else:
                                    # Fallback to checking the entire price container again
                                    price_text = (await price_tag.inner_text()).strip()
                                    price = re.search(r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?', price_text)
                                    price = price.group() if price else "N/A"
                            else:
                                price = price_text
                        else:
                            price = "N/A"
                    except Exception as e:
                        logging.warning(f"Error extracting price: {str(e)}")
                        price = "N/A"

                    try:
                        image_tag = await product.query_selector("img.collection-product__img")
                        image_url = await image_tag.get_attribute("src") if image_tag else "N/A"
                    except Exception:
                        image_url = "N/A"
                    
                    subtitle_tag = await product.query_selector("div.collection-product__subtitle")
                    product_subtitle = (await subtitle_tag.inner_text()).strip() if subtitle_tag else "N/A"
                    metal_type = product_subtitle if product_subtitle != "N/A" else "N/A"

                    gold_type_pattern = r"\b\d{1,2}(?:K|ct)?\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b"
                    gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else metal_type  # Fall back to subtitle 

                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                    # Collect additional product information
                    try:
                        # Check for customisable message
                        customisable_tag = await product.query_selector(".customisable-message")
                        if customisable_tag:
                            customisable_text = (await customisable_tag.inner_text()).strip()
                            if customisable_text:
                                additional_info.append(f"Customisable: {customisable_text}")
                        
                        # Check for new season message
                        newseason_tag = await product.query_selector(".newseason-message")
                        if newseason_tag:
                            newseason_text = (await newseason_tag.inner_text()).strip()
                            if newseason_text:
                                additional_info.append(f"New Season: {newseason_text}")
                        
                        # Check for bestseller message
                        bestseller_tag = await product.query_selector(".bestseller-message")
                        if bestseller_tag:
                            bestseller_text = (await bestseller_tag.inner_text()).strip()
                            if bestseller_text:
                                additional_info.append(f"Bestseller: {bestseller_text}")
                        
                        # Check for color options
                        color_options = await product.query_selector_all(".color-swatch")
                        if color_options:
                            colors = []
                            for color in color_options:
                                color_name = await color.get_attribute("data-color-name")
                                if color_name:
                                    colors.append(color_name)
                            if colors:
                                additional_info.append(f"Colors: {', '.join(colors)}")
                        
                        # Check for availability
                        availability_tag = await product.query_selector(".availability-message")
                        if availability_tag:
                            availability_text = (await availability_tag.inner_text()).strip()
                            if availability_text:
                                additional_info.append(f"Availability: {availability_text}")
                        
                        # Check for any badges or labels
                        badge_tags = await product.query_selector_all(".product-badge, .label")
                        if badge_tags:
                            badges = []
                            for badge in badge_tags:
                                badge_text = (await badge.inner_text()).strip()
                                if badge_text and badge_text.lower() not in ['new', 'sale']:  # Skip common ones already captured
                                    badges.append(badge_text)
                            if badges:
                                additional_info.append(f"Badges: {', '.join(badges)}")
                    
                    except Exception as e:
                        logging.warning(f"Error collecting additional info: {str(e)}")

                    # Join all additional info with | delimiter
                    additional_info_text = " | ".join(additional_info) if additional_info else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, additional_info_text])

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

                all_records.extend(records)
                wb.save(file_path)
                
        except Exception as e:
            logging.error(f"Error on page {page_count}: {str(e)}")
            wb.save(file_path)
        finally:
            if page: await page.close()
            if browser: await browser.close()
            await asyncio.sleep(random.uniform(2, 5))

        page_count += 1

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
