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
import re
from urllib.parse import urljoin
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
            await page.wait_for_selector(".col-lg-12", state="attached", timeout=30000)
            print("[Success] Product listing loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function


def build_url_with_loadmore(base_url: str, load_more_clicks: int) -> str:
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}page={load_more_clicks}"   


async def handle_boochier(url, max_pages):
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
    filename = f"handle_boochier_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    prev_prod_cout = 0
    load_more_clicks = 1
    while load_more_clicks <= max_pages:
        current_url = build_url_with_loadmore(url, load_more_clicks)
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = '.col-lg-12'
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                # await scroll_to_bottom(page)
                
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                # Get all product tiles
                product_wrapper = await page.query_selector("div.col-lg-12")
                products = await page.query_selector_all("div.col-lg-3") if product_wrapper else []
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
                    try:
                        # Extract product name - from the product-title > a element
                        name_tag = await product.query_selector(".product-title a")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    try:
                        # 1️⃣ Grab the raw text
                        price_tag = await product.query_selector("span.price")
                        raw = (await price_tag.inner_text()).strip() if price_tag else ""

                        # 2️⃣ Normalize whitespace
                        raw = re.sub(r'\s+', ' ', raw)

                        # 3️⃣ Extract currency and number, preserving decimals
                        m = re.search(r'(Rs\.?)\s*([\d,]+(?:\.\d+)?)', raw, re.IGNORECASE)
                        if m:
                            currency = m.group(1)
                            number   = m.group(2).replace(",", "")   # remove thousands‐sep commas
                            price     = f"{currency} {number}"
                        else:
                            # Fallback to raw if it doesn’t match
                            price = raw or "N/A"

                    except Exception:
                        price = "N/A"
                        

                    try:
                        # Extract description - from the rte > p element (hidden in this HTML)
                        desc_tag = await product.query_selector(".rte p")
                        description = (await desc_tag.inner_text()).strip() if desc_tag else product_name
                    except Exception:
                        description = product_name
                        
                     
                    
                    image_url = "N/A"

                    try:
                        await product.scroll_into_view_if_needed()
                        await page.wait_for_timeout(1000)  # Ensure lazy loading happens

                        img_container = await product.query_selector(".pr_lazy_img")
                        image_options = []

                        if not img_container:
                            log_event(f"[Staging] No .pr_lazy_img found at {await product.inner_html()}")
                            
                        else:
                            tag_name = await img_container.evaluate("(el) => el.tagName.toLowerCase()")

                            if tag_name == "div":
                                bgset = await img_container.get_attribute("data-bgset")
                                if bgset:
                                    for img_info in bgset.split(','):
                                        parts = img_info.strip().split()
                                        if parts:
                                            url_part = parts[0]
                                            size = int(parts[1][:-1]) if len(parts) > 1 and parts[1].endswith('w') else 0
                                            full_url = urljoin(page.url, url_part)
                                            image_options.append((size, full_url))

                                # Try background-image style
                                if not image_options:
                                    style_attr = await img_container.get_attribute("style")
                                    if style_attr:
                                        match = re.search(r'background-image:\s*url\(["\']?(.*?)["\']?\)', style_attr)
                                        if match:
                                            full_url = urljoin(page.url, match.group(1))
                                            image_options.append((0, full_url))

                            elif tag_name == "img":
                                src = await img_container.get_attribute("src")
                                if src:
                                    full_url = urljoin(page.url, src)
                                    image_options.append((0, full_url))

                            if image_options:
                                image_options.sort(key=lambda x: x[0], reverse=True)
                                image_url = image_options[0][1]
                            else:
                                log_event(f"[Staging] No valid image URLs extracted for {page.url}")

                    except Exception as e:
        
                        log_event(f"[Staging] Exception while extracting image URL: {str(e)}")
                        image_url = "N/A"


                    
                    gold_type_pattern = r"\b(?:\d{1,2}\s*(?:k|kt|ct)\s*gold|platinum|silver|white gold|yellow gold|rose gold|black enamel|gold)\b"
                    gold_type_match = re.search(gold_type_pattern, description, re.IGNORECASE)
                    kt = gold_type_match.group().strip() if gold_type_match else "Not found"

                    # Diamond weight pattern - captures values like "1.8-carat", "0.2 ct", etc.
                    diamond_weight_pattern = r"\b\d+(?:\.\d+)?\s*[-]?\s*(?:ct|cts|tcw|carat|carats)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, description, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group().strip() if diamond_weight_match else "N/A"
                    
                    additional_info_str = description

                    unique_id = str(uuid.uuid4())
                    if image_url and image_url != "N/A":
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])
                # Process image downloads
                for row_num, unique_id, task in image_tasks:
                    try:
                        image_path = await asyncio.wait_for(task, timeout=30)
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
            load_more_clicks += 1
        except Exception as e:
            logging.error(f"Error during scraping: {str(e)}")
            wb.save(file_path)
        finally:
            if page: await page.close()
            if browser: await browser.close()

    if not all_records:
        return None, None, None    # Final save and database operations
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path