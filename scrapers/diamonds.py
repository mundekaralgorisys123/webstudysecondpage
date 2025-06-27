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
# Load environment variables from .env file
from functools import partial
from proxysetup import get_browser_with_proxy_strategy

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

async def handle_77diamonds(url, max_pages):
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_77diamonds_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    while page_count <= max_pages:
        current_url= url
        logging.info(f"Processing page {page_count}: {current_url}")

        # Create a new browser instance for each page
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = "div.prduct-holder"
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay between scrolls
                    current_product_count = await page.locator('div.prduct-holder').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count

                products = await page.query_selector_all("div.prduct-holder > a.product-item")
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []

                    try:
                        # Extract product name
                        product_name_element = await product.query_selector("p.product-name")
                        if product_name_element:
                            product_name_text = await product_name_element.inner_text()
                            product_name = product_name_text.strip()
                        else:
                            product_name = "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting product name: {e}")
                        product_name = "N/A"

                    price_parts = []
                    try:
                        # Extract product price
                        final_price_element = await product.query_selector("p.product-price2 span.final-price")
                        if final_price_element:
                            price_parts.append((await final_price_element.inner_text()).strip())
                        original_price_element = await product.query_selector("p.product-price2 span[discount]")
                        if original_price_element:
                            discount_price = await original_price_element.get_attribute("discount")
                            if discount_price:
                                price_parts.append(discount_price.strip())
                    except Exception as e:
                        logging.error(f"Error extracting product price: {e}")
                    price = "|".join(price_parts) if price_parts else "N/A"

                    try:
                        # Extract product image URL
                        img_elem = await product.query_selector("div.product-image img")
                        image_url = await img_elem.get_attribute("src") if img_elem else "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting product image URL: {e}")
                        image_url = "N/A"

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                    kt = "N/A"
                    try:
                        # Extract metal type from product info
                        product_info_element = await product.query_selector("p.product-desc")
                        if product_info_element:
                            product_info = await product_info_element.inner_text()
                            metal_match = re.search(r"(\d+k\s*(?:White|Yellow|Rose)\s*Gold)", product_info, re.IGNORECASE)
                            if metal_match:
                                kt = metal_match.group(1)
                            elif "Gold" in product_info:
                                kt = "Gold"
                            elif "Platinum" in product_info:
                                kt = "Platinum"
                            additional_info.append(f"Metal: {product_info.split('Engagement Rings')[0].strip()}")
                            category = "Engagement Rings" if "Engagement Rings" in product_info else product_info.split('(')[-1].replace(')', '').strip() if '(' in product_info else "N/A"
                            if category != "N/A":
                                additional_info.append(f"Category: {category}")
                    except Exception as e:
                        logging.error(f"Error extracting product info: {e}")

                    diamond_weight = "N/A"
                    try:
                        # Extract Diamond Weight from product name
                        diamond_weight_match = re.findall(r"(\d+(?:\.\d+)?\s*ct)", product_name, re.IGNORECASE)
                        diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting diamond weight from name: {e}")

                    try:
                        # Extract rating if available
                        rating_element = await product.query_selector("div.rating-holder span.rating-value")
                        if rating_element:
                            rating = await (rating_element.inner_text()).strip()
                            additional_info.append(f"Rating: {rating}")
                        reviews_count_element = await product.query_selector("div.rating-holder span.review-count")
                        if reviews_count_element:
                            reviews_count_text = await (reviews_count_element.inner_text()).strip()
                            additional_info.append(reviews_count_text)
                    except Exception as e:
                        logging.debug(f"Error extracting rating info: {e}")

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    additional_info_str = "|".join(additional_info)
                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight, additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, additional_info_str])

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

                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                updated_record = list(record)
                                updated_record[4] = image_path
                                records[i] = tuple(updated_record)
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Timeout downloading image for row {row_num}")

                all_records.extend(records)
                success_count += 1

                # Save progress after each page
                wb.save(file_path)
                logging.info(f"Progress saved after page {page_count}")

        except Exception as e:
            logging.error(f"Error processing page {page_count}: {str(e)}")
            # Save what we have so far
            wb.save(file_path)
        finally:
            # Clean up resources for this page
            if page:
                await page.close()
            if browser:
                await browser.close()

            # Add delay between pages
            await asyncio.sleep(random.uniform(2, 5))

        page_count += 1

    if not records:
        logging.warning("No records found. Exiting.")
        return None, None, None
    # Final save and database operations
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path