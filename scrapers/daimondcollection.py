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
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
from proxysetup import get_browser_with_proxy_strategy


# Flask and paths
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

def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    # Parse the base URL
    parsed_url = urlparse(base_url)
    query_params = parse_qs(parsed_url.query)

    # Update or add the `page` parameter
    query_params["page"] = [str(page_count)]

    # Build new query string
    new_query = urlencode(query_params, doseq=True)

    # Return the full new URL
    return urlunparse(parsed_url._replace(query=new_query))


async def handle_diamondcollection(url, max_pages):
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
    filename = f"handle_diamondcollection_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    current_url = url
    product_count = 0
    while page_count <= max_pages:
        current_url = build_url_with_loadmore(url, page_count)
        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        context = None
       
        try:
            async with async_playwright() as p:
                product_wrapper = '.collection__main'
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)

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
                products = products[product_count:]  # Limit to first 10 products
                product_count += len(products)
                print(f"Total products on page {page_count}: {len(products)}")
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    # print(f"Processing product {row_num-1} of {len(products)}")
                    # Extract product name
                    try:
                        name_tag = await product.query_selector("a.product-title")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    # Extract price
                    try:
                        price_tag = await product.query_selector("sale-price")
                        price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                        # Clean price text
                        price = price.replace('$', '').replace(',', '').strip() if price != "N/A" else "N/A"
                    except Exception:
                        price = "N/A"

                    # Extract image URL (highest resolution)
                    try:
                        # Get primary image
                        image_tag = await product.query_selector("img.product-card__image--primary")
                        if image_tag:
                            # Get srcset attribute which contains multiple resolutions
                            srcset = await image_tag.get_attribute("srcset")
                            if srcset:
                                # Extract all image URLs and their widths
                                image_options = [url.strip().split(' ') for url in srcset.split(',')]
                                # Sort by width (descending) and take the first one
                                image_options.sort(key=lambda x: int(x[1].replace('w', '')), reverse=True)
                                image_url = image_options[0][0] if image_options else "N/A"
                            else:
                                # Fallback to src attribute if srcset not available
                                image_url = await image_tag.get_attribute("src") or "N/A"
                        else:
                            image_url = "N/A"
                    except Exception:
                        image_url = "N/A"
                        
                    additional_info = []

                    try:
                        
                        # Extract from <p class="smallcaps text-subdued">
                        color_info_el = await product.query_selector("p.smallcaps.text-subdued")
                        if color_info_el:
                            color_info_text = await color_info_el.inner_text()
                            if color_info_text:
                                additional_info.append(color_info_text.strip())

                        if not additional_info:
                            additional_info.append("N/A")

                    except Exception as e:
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)
    

                    # Extract metal type from product name
                    gold_match = re.search(
                        r"\b(?:\d+K\s+)?(?:Rose|White|Yellow)\s+Gold\b",
                        product_name,
                        flags=re.IGNORECASE
                    )
                    metal_type = gold_match.group() if gold_match else "N/A"

                    # 2) Diamond total weight: look for patterns like "1.25 ct tw" or "1-1.5 ct tw"
                    weight_match = re.search(
                        r"\b\d+(?:[-/]\d+(?:\.\d+)?)?(?:\s*\d+/\d+)?\s*ct\s*tw\b",
                        product_name,
                        flags=re.IGNORECASE
                    )
                    diamond_weight = weight_match.group() if weight_match else "N/A"

                    # Generate unique ID and prepare image download
                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    # Append to records and spreadsheet
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