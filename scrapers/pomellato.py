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

mimetypes.add_type('image/webp', '.webp')

# Modified extract_best_image_url function
async def extract_best_image_url(picture_element):
    try:
        if not picture_element:
            return None
            
        # First try to get JPG sources
        sources = await picture_element.query_selector_all("source[type='image/jpg'], source[type='image/jpeg']")
        
        # If no JPG sources, try WEBP
        if not sources:
            sources = await picture_element.query_selector_all("source[type='image/webp']")
        
        # If still no sources, try the img tag directly
        if not sources:
            img_tag = await picture_element.query_selector("img")
            if img_tag:
                img_src = await img_tag.get_attribute("src")
                if img_src:
                    return img_src if img_src.startswith(("http:", "https:")) else f"https:{img_src}" if img_src.startswith("//") else f"https://www.pomellato.com{img_src}"
            return None
        
        # Find the highest resolution source
        best_url = None
        max_width = 0
        
        for source in sources:
            try:
                media = await source.get_attribute("media") or ""
                srcset = await source.get_attribute("srcset") or ""
                
                # Extract width from media query if available
                width_match = re.search(r"min-width:\s*(\d+)px", media)
                if width_match:
                    width = int(width_match.group(1))
                else:
                    # Or extract from srcset (e.g., "564_564/image.jpg")
                    size_match = re.search(r"/(\d+)_\d+/", srcset)
                    width = int(size_match.group(1)) if size_match else 0
                    
                if width > max_width:
                    max_width = width
                    best_url = srcset.split(" ")[0]  # Take the first URL in srcset
                    
            except Exception as e:
                log_event(f"Error processing image source: {e}")
                continue
                
        if best_url:
            return best_url 
        return None
        
    except Exception as e:
        log_event(f"Error extracting best image: {e}")
        return None
    
# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"  # Always save as JPG
    image_full_path = os.path.join(image_folder, image_filename)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                img_data = response.content
                
                # Convert WEBP to JPG if needed
                if image_url.lower().endswith('.webp'):
                    try:
                        img = PILImage.open(BytesIO(img_data))
                        if img.format == 'WEBP':
                            buffer = BytesIO()
                            img.convert('RGB').save(buffer, format="JPEG", quality=85)
                            img_data = buffer.getvalue()
                    except Exception as e:
                        log_event(f"Error converting WEBP to JPG: {e}")
                        continue
                
                with open(image_full_path, "wb") as f:
                    f.write(img_data)
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
            await page.wait_for_selector(".product-listing", state="attached", timeout=30000)
            print("[Success] Product listing loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function
async def handle_pomellato(url, max_pages=None):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_pomellato_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    browser = None
    page = None
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(PROXY_URL)
            context = await browser.new_context()
            page = await context.new_page()
            page.set_default_timeout(120000)

            await safe_goto_and_wait(page, url)
            log_event(f"Successfully loaded: {url}")

            # Scroll to load all items
            await scroll_to_bottom(page)
            
            page_title = await page.title()
            current_date = datetime.now().strftime("%Y-%m-%d")
            time_only = datetime.now().strftime("%H.%M")

            # Get all product tiles - updated selector
            product_wrapper = await page.query_selector("div.product-listing")
            product_tiles = await product_wrapper.query_selector_all("div.tile-options") if product_wrapper else []
            logging.info(f"Total products found: {len(product_tiles)}")
            print(f"Total products found: {len(product_tiles)}")
            records = []
            image_tasks = []
            
            for row_num, product in enumerate(product_tiles, start=len(sheet["A"]) + 1):
                try:
                    # Extract product name - updated selector
                    name_tag = await product.query_selector("h2[type='PLP']")
                    product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                except Exception:
                    product_name = "N/A"

                try:
                    # Extract price - updated selector
                    price_tag = await product.query_selector(".price")
                    price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                except Exception:
                    price = "N/A"

                image_url = "N/A"
                try:
                    # Find the picture element - updated selector
                    picture_element = await product.query_selector("picture.product-picture-content")
                    if picture_element:
                        # Get the img tag directly
                        img_tag = await picture_element.query_selector("img")
                        if img_tag:
                            img_src = await img_tag.get_attribute("src")
                            if img_src:
                                image_url = img_src if img_src.startswith(("http:", "https:")) else f"https:{img_src}" if img_src.startswith("//") else f"https://www.pomellato.com{img_src}"
                except Exception as e:
                    log_event(f"Error getting image URL: {e}")
                    image_url = "N/A"

                # Extract gold type (kt) from product name
                gold_type_pattern = r"\b\d{1,2}(?:K|kt|ct)\b|\bRose Gold\b|\bWhite Gold\b|\bYellow Gold\b|\bPlatinum\b|\bSilver\b"
                gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                kt = gold_type_match.group() if gold_type_match else "Not found"

                # Extract diamond weight from product name
                diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                unique_id = str(uuid.uuid4())
                if image_url and image_url != "N/A":
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight))
                sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url])
            
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
                            records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7])
                            break
                except asyncio.TimeoutError:
                    logging.warning(f"Image download timed out for row {row_num}")

            all_records.extend(records)
            wb.save(file_path)
            
    except Exception as e:
        logging.error(f"Error during scraping: {str(e)}")
        wb.save(file_path)
    finally:
        if page: await page.close()
        if browser: await browser.close()

    wb.save(file_path)
    log_event(f"Data saved to {file_path}")
    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path