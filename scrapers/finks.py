import os
import re
import time
import logging
import random
import uuid
import asyncio
import base64
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError, Error
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from flask import Flask
from PIL import Image as PILImage
import requests
import concurrent.futures
from utils import get_public_ip, log_event, sanitize_filename
from dotenv import load_dotenv
from database import insert_into_db
from limit_checker import update_product_count
import aiohttp
from io import BytesIO
from openpyxl.drawing.image import Image as XLImage
import httpx
from proxysetup import get_browser_with_proxy_strategy
# Load environment variables from .env file
from functools import partial
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

app = Flask(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

async def download_and_resize_image(session, image_url):
    try:
        async with session.get(modify_image_url(image_url), timeout=10) as response:
            if response.status != 200:
                return None
            content = await response.read()
            image = PILImage.open(BytesIO(content))
            image.thumbnail((200, 200))
            img_byte_arr = BytesIO()
            image.save(img_byte_arr, format='JPEG', optimize=True, quality=85)
            return img_byte_arr.getvalue()
    except Exception as e:
        logging.warning(f"Error downloading/resizing image: {e}")
        return None

def modify_image_url(image_url):
    """Modify the image URL to replace '_260' with '_1200' while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    # Extract and preserve query parameters
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"

    # Replace '_260' with '_1200' while keeping the rest of the URL intact
    modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)

    return modified_url + query_params  # Append query parameters if they exist

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

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))


async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")


            # Wait for the selector with a longer timeout
            product_cards = await page.wait_for_selector(".product-card__img-wrapper", state="attached", timeout=30000)

            # Optionally validate at least 1 is visible (Playwright already does this)
            if product_cards:
                print("[Success] Product cards loaded.")
                return
        except Error as e:
            logging.error(f"Error navigating to {url} on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                logging.info("Retrying after waiting a bit...")
                random_delay(1, 3)  # Add a delay before retrying
            else:
                logging.error(f"Failed to navigate to {url} after {retries} attempts.")
                raise
        except TimeoutError as e:
            logging.warning(f"TimeoutError on attempt {attempt + 1} navigating to {url}: {e}")
            if attempt < retries - 1:
                logging.info("Retrying after waiting a bit...")
                random_delay(1, 3)  # Add a delay before retrying
            else:
                logging.error(f"Failed to navigate to {url} after {retries} attempts.")
                raise

            



async def handle_finks(url, max_pages):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}, max_pages: {max_pages}")

    # Prepare directories and files
    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    # Create workbook and setup
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", 
               "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"Finks_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    async with async_playwright() as p:
        while page_count <= max_pages:
            current_url = f"{url}?page={page_count}" if page_count > 1 else url
            logging.info(f"Processing page {page_count}: {current_url}")
            
            browser = None
            page = None
            try:
                product_wrapper = '.product-card__img-wrapper'
                browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    current_product_count = await page.locator('.product-card__img-wrapper').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count

                products = await page.query_selector_all("li.ss__result")
                if not products:
                    logging.warning(f"No products found on page {page_count}")
                    page_count += 1
                    continue

                logging.info(f"Total products found on page {page_count}: {len(products)}")
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    unique_id = str(uuid.uuid4())
                    
                    try:
                        # Product Name
                        product_name_elem = await product.query_selector("div.product-card__title a")
                        product_name = (await product_name_elem.inner_text()).strip() if product_name_elem else "N/A"

                        # Brand
                        brand_elem = await product.query_selector("span.product-card__brand")
                        brand = (await brand_elem.inner_text()).strip() if brand_elem else ""
                        if brand:
                            additional_info.append(f"Brand: {brand}")

                        # Price handling
                        price_info = []
                        regular_price_elem = await product.query_selector("span.price-item--regular")
                        if regular_price_elem:
                            regular_price = (await regular_price_elem.inner_text()).strip()
                            if regular_price:
                                price_info.append(f"Price: {regular_price}")
                        
                        sale_price_elem = await product.query_selector("span.price-item--sale")
                        if sale_price_elem:
                            sale_price = (await sale_price_elem.inner_text()).strip()
                            if sale_price:
                                price_info.append(f"Sale: {sale_price}")
                                if regular_price and sale_price:
                                    try:
                                        reg_num = float(re.sub(r'[^\d.]', '', regular_price))
                                        sale_num = float(re.sub(r'[^\d.]', '', sale_price))
                                        if reg_num > 0:
                                            discount_pct = round((1 - (sale_num / reg_num)) * 100)
                                            additional_info.append(f"Discount: {discount_pct}%")
                                    except Exception as e:
                                        logging.warning(f"Couldn't calculate discount: {e}")

                        price = " | ".join(price_info) if price_info else "N/A"

                        # Image URL
                        img_elem = await product.query_selector("img.product-card__img")
                        image_url = await img_elem.get_attribute("src") or await img_elem.get_attribute("data-src") if img_elem else "N/A"
                        if image_url and image_url.startswith("//"):
                            image_url = f"https:{image_url}"

                        # Metal Type
                        gold_type_match = re.findall(r"(\d{1,2}K\s*(?:Yellow|White|Rose)?\s*Gold|Platinum)", product_name, re.IGNORECASE)
                        kt = ", ".join(gold_type_match) if gold_type_match else "N/A"

                        # Diamond Weight
                        diamond_weight_match = re.findall(r"(\d+(?:\.\d+)?\s*ct(?:\w*))", product_name, re.IGNORECASE)
                        diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"

                        # Additional product info
                        rating_elem = await product.query_selector("div.product-card__rating")
                        if rating_elem:
                            rating_text = (await rating_elem.inner_text()).strip()
                            if rating_text:
                                additional_info.append(f"Rating: {rating_text}")

                        additional_info_str = " | ".join(additional_info) if additional_info else ""

                        # Schedule image download
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                        records.append((unique_id, current_date, page_title, product_name, None, 
                                      kt, price, diamond_weight, additional_info_str))
                        sheet.append([
                            current_date, 
                            page_title, 
                            product_name, 
                            None, 
                            kt, 
                            price, 
                            diamond_weight, 
                            time_only, 
                            image_url,
                            additional_info_str
                        ])

                    except Exception as e:
                        logging.error(f"Error processing product: {str(e)}")
                        continue

                # Process images and update records
                for row_num, unique_id, task in image_tasks:
                    try:
                        image_path = await asyncio.wait_for(task, timeout=60)
                        if image_path != "N/A":
                            try:
                                img = Image(image_path)
                                img.width, img.height = 100, 100
                                sheet.add_image(img, f"D{row_num}")
                            except Exception as img_error:
                                logging.error(f"Error adding image to Excel: {img_error}")
                                image_path = "N/A"
                        
                        # Update record with image path
                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (record[0], record[1], record[2], record[3], 
                                            image_path, record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Timeout downloading image for row {row_num}")

                all_records.extend(records)
                success_count += 1
                wb.save(file_path)
                logging.info(f"Saved {len(records)} products from page {page_count}")

            except Exception as e:
                logging.error(f"Error processing page {page_count}: {str(e)}", exc_info=True)
                if page:
                    await page.screenshot(path=f"error_page_{page_count}.png")
                wb.save(file_path)
            finally:
                if page:
                    await page.close()
                if browser:
                    await browser.close()
                
                await asyncio.sleep(random.uniform(2, 5))
            
            page_count += 1

    # Final operations
    wb.save(file_path)
    logging.info(f"Scraping completed. Total products: {len(all_records)}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path
