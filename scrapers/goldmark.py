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
    """Try to create a high-res version by updating width to 720 if width param exists."""
    if not image_url or image_url == "N/A":
        return image_url

    base_url, query = image_url.split("?", 1) if "?" in image_url else (image_url, "")

    if query:
        # Try to replace width parameter if exists
        if "width=" in query:
            query = re.sub(r'width=\d+', 'width=720', query)
        else:
            query += "&width=720"
    else:
        query = "width=720"

    return f"{base_url}?{query}"


async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    modified_url = modify_image_url(image_url)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                # Try high-resolution URL first
                response = await client.get(modified_url)
                if response.status_code == 200 and len(response.content) > 1024:
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    return image_full_path
                else:
                    logging.info(f"High-res not available, trying original for {product_name}")
                    response = await client.get(image_url)
                    response.raise_for_status()
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    return image_full_path
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")

    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"


def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}p={page_count}"   

async def handle_goldmark(url, max_pages):
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
    filename = f"handle_goldmark_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)


    page_count = 1
    success_count = 0
    current_url = url
    while page_count <= max_pages:
        current_url = build_url_with_loadmore(url, page_count)
        logging.info(f"Processing page {page_count}: {current_url}")
        
        # Create a new browser instance for each page
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = '.ps-category-items'
                browser , page = await get_browser_with_proxy_strategy(p, current_url,product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay between scrolls
                    current_product_count = await page.locator('.ps-category-items').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count


                product_wrapper = await page.query_selector("div.ps-category-items")
                print(f"Product wrapper found: {product_wrapper}")
                products = await product_wrapper.query_selector_all("div.ps-category-item") if product_wrapper else []
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product name using async query_selector and inner_text
                        product_name_tag = await product.query_selector("div.s-product__name")
                        product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"
                    except Exception as e:
                        logging.warning(f"⚠️ Error extracting product name: {e}")
                        product_name = "N/A"

                    try:
                        price = "N/A"

                        # Try discounted price with "now" and optionally "was"
                        price_now_tag = await product.query_selector("span.s-price__now")
                        price_was_tag = await product.query_selector("span.s-price__was")

                        if price_now_tag:
                            price_now = (await price_now_tag.inner_text()).strip()
                            price_was = (await price_was_tag.inner_text()).strip() if price_was_tag else None
                            price = f"{price_now} offer {price_was}" if price_was else price_now

                        else:
                            # Try getting regular price (in case no now/was tags)
                            price_container = await product.query_selector("div.s-product__price")
                            if price_container:
                                text = (await price_container.inner_text()).strip()
                                if text:
                                    price = text

                        # Final fallback: any span with price class
                        if price == "N/A":
                            fallback_span = await product.query_selector("span.s-price__now, span.s-price__was")
                            if fallback_span:
                                price = (await fallback_span.inner_text()).strip()

                    except Exception as e:
                        logging.warning(f"⚠️ Error extracting price: {e}")
                        price = "N/A"




                    try:
                        # Extract image element asynchronously
                        image_tag = await product.query_selector("img")

                        # Safely get src or data-src if image_tag is found
                        if image_tag:
                            image_url = await image_tag.get_attribute("src") or await image_tag.get_attribute("data-src")
                        else:
                            image_url = "N/A"
                    except Exception as e:
                        logging.warning(f"⚠️ Error extracting image URL: {e}")
                        image_url = "N/A"
                        
                    additional_info = []

                    try:
                        # Check for 'Sale' flag
                        sale_flag_el = await product.query_selector("div.s-product__flag.s-flag--sale")
                        if sale_flag_el:
                            sale_text = await sale_flag_el.inner_text()
                            if sale_text:
                                additional_info.append(sale_text.strip())

                        # Check for promotional offer (e.g., "Buy 2 Save 30%*")
                        offer_el = await product.query_selector("div.s-product__offer")
                        if offer_el:
                            offer_text = await offer_el.inner_text()
                            if offer_text:
                                additional_info.append(offer_text.strip())

                        if not additional_info:
                            additional_info.append("N/A")

                    except Exception as e:
                        logging.warning(f"⚠️ Error extracting additional info: {e}")
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)

    
                      
                    # Extract KT
                    gold_type_match = re.search(r"\b\d{1,2}CT(?:\s+(?:WHITE|YELLOW|ROSE|TWO TONE))?\s+GOLD\b", product_name, re.IGNORECASE)
                    kt = gold_type_match.group().upper() if gold_type_match else "Not found"

                    # Extract Diamond Weight
                    diamond_weight_match = re.search(r"(?:\b\d+/\d+|\d+\.\d+|\d+)\s*(?:CT|CARAT)\s*(?:TW)?", product_name.upper())
                    diamond_weight = diamond_weight_match.group().strip() if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])

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
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
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


    # # Final save and database operations
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