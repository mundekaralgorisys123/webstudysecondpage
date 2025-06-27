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
from proxysetup import get_browser_with_proxy_strategy
import httpx
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

def modify_image_url(image_url: str) -> str:
    """Modify the image URL to request high resolution by changing width=375 to width=720."""
    if not image_url or image_url == "N/A":
        return image_url

    # Ensure width=720 if a width param exists
    if "width=" in image_url:
        return re.sub(r'width=\d+', 'width=720', image_url)
    
    # If width param is missing, append it
    if "?" in image_url:
        return image_url + "&width=720"
    else:
        return image_url + "?width=720"

async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    high_res_url = modify_image_url(image_url)

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Try high-resolution first
        for attempt in range(retries):
            try:
                response = await client.get(high_res_url)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - High-res failed for {product_name}: {e}")
        
        # Fallback to original image
        try:
            response = await client.get(image_url)
            response.raise_for_status()
            with open(image_full_path, "wb") as f:
                f.write(response.content)
            return image_full_path
        except httpx.RequestError as e:
            logging.error(f"Fallback failed for {product_name}: {e}")
            return "N/A"


def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))



async def handle_prouds(url, max_pages):
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
    filename = f"handle_prouds_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0
    current_url = url
    while page_count <= max_pages:
        if page_count > 1:
            if "?" in url:
                current_url = f"{url}&p={page_count}"
            else:
                current_url = f"{url}?p={page_count}"
        logging.info(f"Processing page {page_count}: {current_url}")
        
        # Create a new browser instance for each page
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = ".ps-category-items"
                browser, page =  await get_browser_with_proxy_strategy(p, current_url,product_wrapper)
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
                products = await product_wrapper.query_selector_all("div.ps-category-item") if product_wrapper else []
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        product_name_tag = await product.query_selector("div.s-product__name")
                        product_name = (await product_name_tag.inner_text()).strip() if product_name_tag else "N/A"
                    except:
                        product_name = "N/A"

                    try:
                        price_now_tag = await product.query_selector("span.s-price__now")
                        price_was_tag = await product.query_selector("span.s-price__was")
                        full_price_tag = await product.query_selector("div.s-product__price.s-price")

                        price_now = (await price_now_tag.inner_text()).strip() if price_now_tag else ""
                        price_was = (await price_was_tag.inner_text()).strip() if price_was_tag else ""

                        if price_now and price_was:
                            price = f"{price_now} offer {price_was}"
                        elif price_now:
                            price = price_now
                        elif full_price_tag:
                            # Only use full_price_tag if both price_now and price_was are missing
                            # This covers fallback like: <div class="s-product__price s-price"> $2,299 </div>
                            price = (await full_price_tag.inner_text()).strip()
                        else:
                            price = "N/A"
                    except Exception as e:
                        price = "N/A"



                    try:
                        image_tag = await product.query_selector("img")
                        if image_tag:
                            # Prefer high-resolution images from srcset or data-srcset
                            srcset = await image_tag.get_attribute("data-srcset") or await image_tag.get_attribute("srcset")
                            if srcset:
                                image_url = srcset.strip().split(",")[-1].split()[0]  # Highest res
                            else:
                                image_url = await image_tag.get_attribute("data-src") or await image_tag.get_attribute("src")
                        else:
                            image_url = "N/A"
                    except:
                        image_url = "N/A"
                        
                        
                    additional_info = []

                    try:
                       
                        # New 'Sale' flag
                        flag_els = await product.query_selector_all("div.s-product__flag.s-flag")
                        if flag_els:
                            for flag_el in flag_els:
                                flag_text = await flag_el.inner_text()
                                if flag_text:
                                    additional_info.append(flag_text.strip())

                        if not additional_info:
                            additional_info.append("N/A")

                    except Exception:
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)
    
                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue  


                    gold_type_match = re.search(r"\b\d{1,2}CT(?:\s+(?:ROSE|YELLOW|WHITE))?\s+GOLD\b", product_name, re.IGNORECASE)
                    kt = gold_type_match.group().upper() if gold_type_match else "N/A"


                    diamond_weight_match = re.search(r"\d+(\.\d+)?\s*(CT|CARAT)\s+TW", product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group().upper() if diamond_weight_match else "N/A"



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


    # Final save and database operations
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
