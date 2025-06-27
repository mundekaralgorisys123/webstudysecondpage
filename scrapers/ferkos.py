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
import json
from limit_checker import update_product_count
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



# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
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

# Reliable page.goto wrapper
async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            await page.wait_for_selector(".itemlistbasildi", state="attached", timeout=30000)
            print("[Success] Product cards loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function
async def handle_ferkos(url, max_pages):
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
    filename = f"handle_ferkos_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    
    page_count = 1
    current_url = url
    while (page_count <= max_pages):
        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        page = None
        if page_count > 1:
            if '?' in url:
                current_url = f"{url}&page={page_count}"
            else:
                current_url = f"{url}?page={page_count}"
        try:
            async with async_playwright() as p:
                browser, page = await get_browser_with_proxy_strategy(p, current_url, ".itemlistbasildi")
                log_event(f"Successfully loaded: {current_url}")
            
                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('.itemlistbasildi').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_wrapper = await page.query_selector("div.itemlistbasildi")
                products = await product_wrapper.query_selector_all("div.boost-sd__product-item") if product_wrapper else []
                logging.info(f"Total products scraped:{page_count} :{len(products)}")
                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    
                    try:
                        # Improved product name extraction
                        name_tag = await product.query_selector(".boost-sd__product-title")
                        if name_tag:
                            product_name = (await name_tag.inner_text()).strip()
                            # Fallback to data-product attribute if name is empty
                            if not product_name:
                                product_data = await product.get_attribute("data-product")
                                if product_data:
                                    product_json = json.loads(product_data.replace("&quot;", '"'))
                                    product_name = product_json.get("handle", "").replace("-", " ").title()
                        else:
                            # Try alternative selectors if main selector fails
                            alt_name_tag = await product.query_selector(".product-title") or \
                                         await product.query_selector(".product-name") or \
                                         await product.query_selector("h3")
                            product_name = (await alt_name_tag.inner_text()).strip() if alt_name_tag else "N/A"
                            
                        # Clean up the product name
                        if product_name != "N/A":
                            product_name = ' '.join(product_name.split())  # Remove extra spaces
                            
                    except Exception as e:
                        logging.error(f"Error extracting product name: {e}")
                        product_name = "N/A"

                    try:
                        # Price handling - get both sale and compare prices
                        sale_price_tag = await product.query_selector(".boost-sd__product-price--sale .boost-sd__format-currency")
                        compare_price_tag = await product.query_selector(".boost-sd__product-price--compare .boost-sd__format-currency")
                        
                        sale_price = (await sale_price_tag.inner_text()).strip() if sale_price_tag else None
                        compare_price = (await compare_price_tag.inner_text()).strip() if compare_price_tag else None
                        
                        if sale_price and compare_price:
                            price = f"{sale_price}|{compare_price}"
                            # Calculate discount percentage
                            try:
                                sale_num = float(sale_price.replace('$', '').replace(',', ''))
                                compare_num = float(compare_price.replace('$', '').replace(',', ''))
                                discount_percent = int(round((1 - (sale_num / compare_num)) * 100))
                                additional_info.append(f"Discount: {discount_percent}%")
                            except:
                                pass
                        elif sale_price:
                            price = sale_price
                        else:
                            price = "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting price: {e}")
                        price = "N/A"

                    try:
                        # Image extraction - get main image
                        image_tag = await product.query_selector("img.boost-sd__product-image-img--main") or \
                                await product.query_selector("img.boost-sd__product-image-img")
                        image_url = await image_tag.get_attribute("src") if image_tag else "N/A"
                        if image_url and image_url.startswith("//"):
                            image_url = "https:" + image_url
                    except Exception as e:
                        logging.error(f"Error extracting image: {e}")
                        image_url = "N/A"

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                    # Gold type detection (from swatches or name)
                    gold_type = "Not found"
                    try:
                        swatches = await product.query_selector_all(".boost-sd__product-swatch-option")
                        for swatch in swatches:
                            label = await swatch.query_selector("label")
                            if label:
                                label_text = await label.get_attribute("aria-label") or ""
                                if "Gold" in label_text:
                                    gold_type = label_text.split(": ")[-1]
                                    break
                    except Exception as e:
                        logging.error(f"Error extracting gold type: {e}")

                    # Diamond weight from name
                    diamond_weight = "N/A"
                    try:
                        diamond_weight_match = re.search(r"\b\d+(\.\d+)?\s*(?:ct|tcw)\b", product_name, re.IGNORECASE)
                        if diamond_weight_match:
                            diamond_weight = diamond_weight_match.group()
                    except Exception as e:
                        logging.error(f"Error extracting diamond weight: {e}")

                    # Get product labels/tags
                    try:
                        labels = []
                        label_tags = await product.query_selector_all(".boost-sd__product-label-text")
                        for label in label_tags:
                            label_text = (await label.inner_text()).strip()
                            if label_text:
                                labels.append(label_text)
                        if labels:
                            additional_info.append(f"Labels: {'|'.join(labels)}")
                    except Exception as e:
                        logging.error(f"Error extracting labels: {e}")

                    # Combine all additional info with pipe delimiter
                    additional_info_text = "|".join(additional_info) if additional_info else ""

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, gold_type, price, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, gold_type, price, diamond_weight, time_only, image_url, additional_info_text])

                # Process images and update records
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

    wb.save(file_path)
    log_event(f"Data saved to {file_path}")
    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path