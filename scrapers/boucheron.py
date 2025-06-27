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
from PIL import Image as PILImage
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from collections import OrderedDict
from utils import get_public_ip, log_event
from dotenv import load_dotenv
from database import insert_into_db
from limit_checker import update_product_count
from io import BytesIO
from openpyxl.drawing.image import Image as XLImage
import httpx
from proxysetup import get_browser_with_proxy_strategy

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

async def download_and_resize_image(session, image_url):
    try:
        async with session.get(modify_image_url1(image_url), timeout=10) as response:
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


def modify_image_url1(image_url):
    """Modify the image URL to replace width=100 with width=2200 while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    # Check if the image URL already contains width=2200
    if "width=2200" in image_url:
        return image_url

    # Replace all occurrences of width=100 with width=2200 in the entire URL
    modified_url = re.sub(r'\bwidth=100\b', 'width=2200', image_url)

    return modified_url



async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.webp"
    image_full_path = os.path.join(image_folder, image_filename)

    async with httpx.AsyncClient(
        timeout=10.0,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "image/webp,image/apng,image/*,*/*;q=0.8"
        }
    ) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()

                # ✅ Check for image content-type
                content_type = response.headers.get("Content-Type", "")
                if not content_type.startswith("image/"):
                    logging.warning(f"Invalid content-type for {product_name}: {content_type}")
                    return "N/A"

                final_url = str(response.url)
                logging.info(f"Resolved redirect for {product_name}: {image_url} -> {final_url}")

                # Save the .webp file
                with open(image_full_path, "wb") as f:
                    f.write(response.content)

                # Convert .webp to .jpg
                if image_full_path.lower().endswith('.webp'):
                    with PILImage.open(image_full_path) as img:
                        new_image_path = image_full_path.rsplit('.', 1)[0] + '.jpg'
                        img.convert("RGB").save(new_image_path, 'JPEG')
                    image_full_path = new_image_path

                return image_full_path

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 302:
                    redirect_location = e.response.headers.get("location")
                    if redirect_location:
                        image_url = redirect_location
                        continue
                logging.warning(f"Retry {attempt + 1}/{retries} - HTTP error for {product_name}: {e}")
                await asyncio.sleep(1)

            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Connection error for {product_name}: {e}")
                await asyncio.sleep(1)

        logging.error(f"Failed to download image for {product_name} after {retries} attempts")
        return "N/A"


def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))


def build_url_with_loadmore(base_url: str, page_number: int) -> str:
    parsed_url = urlparse(base_url)
    existing_params = OrderedDict(parse_qsl(parsed_url.query))

    # Insert or move 'p' to the beginning
    existing_params.pop('p', None)
    new_params = OrderedDict()
    new_params['p'] = str(page_number)
    new_params.update(existing_params)

    # Reconstruct URL with new query string
    new_query = urlencode(new_params)
    new_url = urlunparse(parsed_url._replace(query=new_query))
    return new_url
            

async def handle_boucheron(url, max_pages):
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_boucheron_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    while page_count <= max_pages:
        current_url = build_url_with_loadmore(url, page_count)
        logging.info(f"Processing page {page_count}: {current_url}")
        
        # Create a new browser instance for each page
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                
                product_wrapper=".products.wrapper"
                browser, page = await get_browser_with_proxy_strategy(p, current_url ,product_wrapper)

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay between scrolls
                    current_product_count = await page.locator('.product-item').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count


                product_wrapper = await page.query_selector('#productSection')
                products = await product_wrapper.query_selector_all('.product-item') 
                logging.info(f"Total products found on page {page_count}: {len(products)}")


                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product name
                        name_tag = await product.query_selector('div.product-item-name')
                        product_name = await name_tag.inner_text() if name_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching product name: {e}")
                        product_name = "N/A"

                    try:
                        # Extract price
                        price_container = await product.query_selector('span.product-price')
                        if price_container:
                            price = await price_container.inner_text() if price_container else "N/A"
                        else:
                            price = "N/A"
                    except Exception as e:
                        print(f"Error fetching price: {e}")
                        price = "N/A"
                    try:
                        # Extract image URL
                        product_container = await product.query_selector('.product-item-photo')
                        image_tag = await product_container.query_selector('img') if product_container else None
                        if image_tag:
                            image_url = await image_tag.get_attribute("src")
                            # Remove any extra spaces that might be around the URL
                            image_url = image_url.strip() if image_url else "N/A"
                        else:
                            image_url = "N/A"
                    except Exception as e:
                        print(f"Error fetching image URL: {e}")
                        image_url = "N/A"




                    gold_type_match = re.search(r"\b\d+K\s+\w+\s+\w+\b", product_name)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    diamond_weight_match = re.search(r"\d+[-/]?\d*/?\d*\s*ct\s*tw", product_name)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url])

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
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7])
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
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path
