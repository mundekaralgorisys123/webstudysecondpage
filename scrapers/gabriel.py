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
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"
    modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)
    return modified_url + query_params

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
            await page.wait_for_selector(".ProductCardWrapper", state="attached", timeout=30000)
            print("[Success] Product cards loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function
async def handle_gabriel(url, max_pages):
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
    filename = f"handle_gabriel_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    current_url = url

    while current_url and (page_count <= max_pages):
        if page_count > 1:
            if '?' in current_url:
                current_url = f"{url}&p={page_count}"
            else:
                current_url = f"{url}?p={page_count}"
        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = '.ProductCardWrapper'
                browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)
                # browser, page = await get_browser_with_proxy_strategy(p, current_url,".ProductCardWrapper")
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('.ProductCardWrapper').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                wrapper = page.locator("div.qd-product-list").first
                products = await wrapper.locator("div.ProductCard").all() if await wrapper.count() > 0 else []

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    product_name = "N/A"
                    price = "N/A"
                    image_url = "N/A"
                    kt = "N/A"
                    diamond_weight = "N/A"
                    unique_id = str(uuid.uuid4())

                    try:
                        # Product Name (using locator API)
                        name_locator = product.locator("h2.ProductName a.ProductCTA")
                        if await name_locator.count() > 0:
                            # Try to get full name from data-pname attribute first
                            full_name = await name_locator.get_attribute("data-pname")
                            displayed_text = (await name_locator.inner_text()).strip()
                            
                            product_name = full_name.strip() if full_name else displayed_text.replace("...", "").strip()
                    except Exception as e:
                        logging.error(f"Error getting product name: {e}")

                    try:
                        # Price handling (using locator API)
                        price_locator = product.locator('.price-wrapper .price')
                        if await price_locator.count() > 0:
                            price = (await price_locator.inner_text()).strip()
                            
                            # Check for original price if on sale
                            original_price_locator = product.locator('.old-price .price')
                            if await original_price_locator.count() > 0:
                                original_price = (await original_price_locator.inner_text()).strip()
                                price = f"{original_price} | Sale: {price}"
                                additional_info.append("On Sale")
                    except Exception as e:
                        logging.error(f"Error getting price: {e}")

                    try:
                        # Image URL (using locator API)
                        image_locator = product.locator("img.image-entity")
                        if await image_locator.count() > 0:
                            # Get first image's data-src or src
                            image_url = await image_locator.first.get_attribute("data-src") or \
                                    await image_locator.first.get_attribute("src")
                            
                            # Count total available images
                            image_count = await image_locator.count()
                            if image_count > 1:
                                additional_info.append(f"{image_count} images available")
                    except Exception as e:
                        logging.error(f"Error getting image URL: {e}")
                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                    # Metal type (kt)
                    try:
                        # First try from product name
                        if product_name != "N/A":
                            gold_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                            if gold_match:
                                kt = gold_match.group()
                        
                        # If not found in name, try from metal swatches (using locator API)
                        if kt == "N/A":
                            metal_span_locator = product.locator(".metalIcon")
                            if await metal_span_locator.count() > 0:
                                metal_text = await metal_span_locator.inner_text()
                                gold_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", metal_text, re.IGNORECASE)
                                if gold_match:
                                    kt = gold_match.group()
                    except Exception as e:
                        logging.error(f"Error extracting metal type: {e}")

                    # Diamond weight
                    try:
                        if product_name != "N/A":
                            dia_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", product_name, re.IGNORECASE)
                            diamond_weight = f"{dia_match.group(1)} ct" if dia_match else "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting diamond weight: {e}")

                    # Additional product info (using locator API)
                    try:
                        # Metal options - using more specific selector
                        metal_options_locator = product.locator(".metal-swatches li[data-product-id]")
                        metal_count = await metal_options_locator.count()
                        if metal_count > 0:
                            options = []
                            for i in range(metal_count):
                                option = metal_options_locator.nth(i)
                                title = await option.get_attribute("title")
                                if title and title != kt:
                                    options.append(title)
                            if options:
                                additional_info.append(f"Metal Options: {', '.join(options)}")
                        
                        # Product ID - using the most specific selector available
                        product_id_locator = product.locator(".price-box[data-product-id]")
                        if await product_id_locator.count() > 0:
                            product_id = await product_id_locator.get_attribute("data-product-id")
                            if product_id:
                                additional_info.append(f"Product ID: {product_id}")
                    except Exception as e:
                        logging.error(f"Error getting additional info: {e}")

                    # Prepare additional info string
                    additional_info_str = " | ".join(additional_info) if additional_info else "N/A"

                    # Schedule image download
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((
                        unique_id, current_date, page_title, product_name, None, 
                        kt, price, diamond_weight, time_only, image_url, additional_info_str
                    ))
                    sheet.append([
                        current_date, page_title, product_name, None, 
                        kt, price, diamond_weight, time_only, 
                        image_url, additional_info_str
                    ])

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
                                records[i] = (
                                    record[0], record[1], record[2], record[3], image_path, 
                                    record[5], record[6], record[7], record[8], record[9], record[10]
                                )
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
    
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")
    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path
