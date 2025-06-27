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
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event
from database import insert_into_db
from limit_checker import update_product_count
import json
from proxysetup import get_browser_with_proxy_strategy
# Load environment
from urllib.parse import urlparse, urlencode, parse_qs

load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

# Flask and paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')


def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    # Parse the base URL to separate the path and query parameters
    url_parts = urlparse(base_url)
    
    # Parse the query string into a dictionary
    query_params = parse_qs(url_parts.query)
    
    # Add or update the page count parameter
    query_params['page'] = page_count
    
    # Rebuild the query string with the updated page parameter
    new_query = urlencode(query_params, doseq=True)
    
    # Rebuild the full URL
    new_url = f"{url_parts.scheme}://{url_parts.netloc}{url_parts.path}?{new_query}"
    
    return new_url





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

# Main scraper function
async def handle_moissanite(url, max_pages):
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
    filename = f"handle_moissanite_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    
    while page_count <= max_pages:
        current_url = build_url_with_loadmore(url, page_count)
        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        page = None
        
        try:
            async with async_playwright() as p:
                product_wrapper = '.grid-area--collection'
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)

                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('.grid-area--collection').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_wrapper = await page.query_selector("div.grid-area--collection")
                products = await product_wrapper.query_selector_all("div.grid__item.large--one-quarter.medium--one-half.small--one-half") if product_wrapper else []
                logging.info(f"Total products scraped:{page_count} :{len(products)}")
                records = []
                image_tasks = []
                print(f"Total products found: {len(products)}")
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        name_tag = await product.query_selector("div.product-grid--title a")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    try:
                        price_tag = await product.query_selector("div.product-grid--price span.money")
                        price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                        # Clean price text
                        price = price.replace('Rs.', '').replace(',', '').strip() if price != "N/A" else "N/A"
                    except Exception:
                        price = "N/A"

                    try:
                        # Get the first image (primary image) and extract the highest resolution version
                        image_container = await product.query_selector("div.grid-view-item-image img.theme-img")
                        if image_container:
                            # Get the srcset attribute which contains multiple resolutions
                            srcset = await image_container.get_attribute("srcset")
                            if srcset:
                                # Extract all image URLs and their widths
                                image_options = [url.strip().split(' ') for url in srcset.split(',')]
                                # Sort by width (descending) and take the first one
                                image_options.sort(key=lambda x: int(x[1].replace('w', '')), reverse=True)
                                image_url = image_options[0][0] if image_options else "N/A"
                            else:
                                # Fallback to src attribute if srcset not available
                                image_url = await image_container.get_attribute("src") or "N/A"
                        else:
                            image_url = "N/A"
                    except Exception:
                        image_url = "N/A"

                    # Extract metal type from product name or options
                    metal_type = "N/A"
                    try:
                        # Check if there's a product-card element with variant data
                        product_card = await product.query_selector("product-card")
                        if product_card:
                            variant_data = await product_card.get_attribute("data-current_variant")
                            if variant_data:
                                variant_json = json.loads(variant_data)
                                # Extract metal type from options
                                if "options" in variant_json and len(variant_json["options"]) >= 2:
                                    metal_type = variant_json["options"][1]  # Assuming metal type is the second option
                    except Exception:
                        pass

                    # If metal type not found in variant data, try to extract from product name
                    gold_type_pattern = r"\b\d{1,2}(?:K|ct)?\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b"

                    gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                    if gold_type_match:
                        metal_type = gold_type_match.group().strip()
                    else:
                        metal_type = "N/A"

                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                    
                    additional_info_str = "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, metal_type, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, metal_type, price, diamond_weight, time_only, image_url,additional_info_str])
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