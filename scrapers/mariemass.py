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
from proxysetup import get_browser_with_proxy_strategy
# Load environment
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

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
            await page.wait_for_selector(".collection__main", state="attached", timeout=30000)
            print("[Success] Product cards loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function
async def handle_mariemass(url, max_pages):
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
    filename = f"handle_mariemass_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    current_url = url
    while current_url and (page_count <= max_pages):
        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        context = None
        if page_count > 1:
            if '?' in current_url:
                current_url = f"{url}&page={page_count}"
            else:
                current_url = f"{url}?page={page_count}"
        try:
            async with async_playwright() as p:
                product_wrapper = ".collection__main"
                browser, page = await get_browser_with_proxy_strategy(p, current_url,product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('.collection__main').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_wrapper = await page.query_selector("div.collection__main")
                products = await product_wrapper.query_selector_all("product-card.product-card") if product_wrapper else []
                logging.info(f"Total products scraped:{page_count} :{len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    
                    try:
                        name_tag = await product.query_selector("a.product-title")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    # Enhanced price extraction
                    try:
                        price_tags = await product.query_selector_all("sale-price span.money")
                        prices = []
                        for price_tag in price_tags:
                            price_text = (await price_tag.inner_text()).strip()
                            if price_text:
                                # Clean and format price
                                clean_price = price_text.replace('€', '').replace('EUR', '').strip()
                                if clean_price:
                                    prices.append(f"€{clean_price}")
                        
                        if len(prices) > 1:
                            price = " | ".join(prices)
                            additional_info.append("Multiple prices available")
                        elif prices:
                            price = prices[0]
                        else:
                            price = "N/A"
                    except Exception:
                        price = "N/A"

                    # Enhanced image extraction
                    try:
                        # Try to get primary image first
                        image_container = await product.query_selector("img.product-card__image--primary")
                        if image_container:
                            # Get srcset to find highest resolution image
                            srcset = await image_container.get_attribute("srcset")
                            if srcset:
                                sources = [s.strip().split() for s in srcset.split(',') if s.strip()]
                                sources.sort(key=lambda x: int(x[1].replace('w', ''))) # Sort by width
                                image_url = sources[-1][0] if sources else await image_container.get_attribute("src")
                            else:
                                image_url = await image_container.get_attribute("src")
                        else:
                            # Fallback to secondary image
                            image_container = await product.query_selector("img.product-card__image--secondary")
                            image_url = await image_container.get_attribute("src") if image_container else "N/A"
                    except Exception:
                        image_url = "N/A"
                    
                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                    # Extract metal type and other product details
                    metal_type = "N/A"
                    try:
                        # Check for variant data in product-card element
                        variant_data = await product.get_attribute("data-current_variant")
                        if variant_data:
                            variant_json = json.loads(variant_data)
                            if "options" in variant_json and len(variant_json["options"]) >= 2:
                                metal_type = variant_json["options"][1]  # Assuming metal type is the second option
                                additional_info.append(f"Variant: {variant_json['options'][0]}")  # First option
                    except Exception:
                        pass

                    # If metal type not found in variant data, try to extract from product name
                    if metal_type == "N/A":
                        gold_type_pattern = r"\b\d{1,2}(?:K|ct)?\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b"
                        gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                        metal_type = gold_type_match.group() if gold_type_match else "N/A"

                    # Extract diamond weight and gemstone information
                    diamond_weight = "N/A"
                    try:
                        diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                        diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                        diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                        
                        # Extract gemstone information
                        gemstone_pattern = r"\b(?:Diamond|Ruby|Sapphire|Emerald|Topaz|Opal|Amethyst|Aquamarine)\b"
                        gemstones = re.findall(gemstone_pattern, product_name, re.IGNORECASE)
                        if gemstones:
                            additional_info.append(f"Gemstones: {', '.join(gemstones)}")
                    except Exception:
                        pass

                    # Check for sale or special tags
                    try:
                        sale_tag = await product.query_selector(".price--on-sale, .sale-badge")
                        if sale_tag:
                            sale_text = (await sale_tag.inner_text()).strip()
                            if sale_text:
                                additional_info.append(f"Sale: {sale_text}")
                    except Exception:
                        pass

                    # Check for product availability
                    try:
                        availability_tag = await product.query_selector(".product-availability")
                        if availability_tag:
                            availability_text = (await availability_tag.inner_text()).strip()
                            if availability_text:
                                additional_info.append(f"Availability: {availability_text}")
                    except Exception:
                        pass

                    # Join all additional info with | delimiter
                    additional_info_text = " | ".join(additional_info) if additional_info else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, metal_type, price, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, metal_type, price, diamond_weight, time_only, image_url, additional_info_text])

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
