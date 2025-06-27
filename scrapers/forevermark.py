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
from openpyxl.drawing.image import Image as ExcelImage

from PIL import Image as PILImage
from proxysetup import get_browser_with_proxy_strategy
from utils import get_public_ip, log_event, sanitize_filename
from dotenv import load_dotenv
from database import insert_into_db
from limit_checker import update_product_count
from openpyxl.drawing.image import Image

import httpx
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')




async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.webp"
    image_full_path = os.path.join(image_folder, image_filename)

    # Rotating user agents and enhanced headers
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
    ]

    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.forevermark.com/",
        "Origin": "https://www.forevermark.com",
        "Sec-Fetch-Dest": "image",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-origin",
        "Priority": "u=1",
        "Connection": "keep-alive"
    }

    async with httpx.AsyncClient(
        timeout=20.0,
        follow_redirects=True,
        headers=headers,
        limits=httpx.Limits(max_keepalive_connections=10),
    ) as client:
        for attempt in range(retries):
            try:
                # Add jitter to request timing
                await asyncio.sleep(0.5 + random.uniform(0, 0.3))
                
                response = await client.get(image_url)
                
                # Handle forbidden errors specifically
                if response.status_code == 403:
                    raise httpx.HTTPStatusError(
                        message=f"403 Forbidden: Potential anti-bot protection triggered",
                        request=response.request,
                        response=response
                    )

                response.raise_for_status()

                # Save original webp file
                with open(image_full_path, "wb") as f:
                    f.write(response.content)

                # Convert to JPG if needed
                if image_full_path.lower().endswith('.webp'):
                    try:
                        with PILImage.open(image_full_path) as img:
                            new_path = image_full_path.rsplit('.', 1)[0] + '.jpg'
                            img.convert("RGB").save(new_path, 'JPEG', quality=95)
                            os.remove(image_full_path)  # Remove original webp
                            image_full_path = new_path
                    except Exception as e:
                        logging.error(f"Image conversion failed for {product_name}: {e}")
                        return "N/A"

                return image_full_path

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403:
                    logging.warning(f"Anti-bot detected ({attempt+1}/{retries}): Rotating headers...")
                    headers["User-Agent"] = random.choice(user_agents)
                    headers["Cache-Control"] = f"no-cache-{random.randint(1000,9999)}"
                
                backoff = 2 ** attempt + random.random()
                logging.warning(f"Retry {attempt+1}/{retries} in {backoff:.1f}s: {e}")
                await asyncio.sleep(backoff)

            except (httpx.RequestError, OSError) as e:
                backoff = 2 ** attempt + random.random()
                logging.warning(f"Retry {attempt+1}/{retries} in {backoff:.1f}s: {e}")
                await asyncio.sleep(backoff)

        logging.error(f"Permanent failure for {product_name} after {retries} attempts")
        return "N/A"

    

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))




def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '?' if '?' in base_url else '?'
    return f"{base_url}{separator}pageNo={page_count}"   

async def handle_forevermark(url, max_pages):
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
    now_date = datetime.now().strftime("%Y-%m-%d")
    now_time = datetime.now().strftime("%H.%M")
    filename = f"handle_forevermark_{now_date}_{now_time}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    while page_count <= max_pages:
        current_url = build_url_with_loadmore(url, page_count)
        logging.info(f"Processing page {page_count}: {url}")
       
        # Create a new browser instance for each page
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = '.container.finder-filters'
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {url}")

                # Use locator instead of query_selector/query_selector_all
                product_wrapper = page.locator("div.row")
                products = product_wrapper.locator("div.col-4_of_12")

                # Count products
                product_count = await products.count()
                logging.info(f"New products found: {product_count}")
                print(f"New products found: {product_count}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num in range(product_count):
                    product = products.nth(row_num)
                    try:
                        # Use locator to get the <h2> tag text
                        name_tag = product.locator("h2")
                        product_name = (await name_tag.text_content()).strip() if await name_tag.count() > 0 else "N/A"
                    except Exception:
                        product_name = "N/A"

                    price = "N/A"

                    try:
                        # Try to get from <source> with type="image/jpeg" using locator
                        source_tag = product.locator('picture source[type="image/jpeg"]')
                        image_url = await source_tag.get_attribute("srcset") if await source_tag.count() > 0 else "N/A"

                        # Fallback to <img> if <source> not found
                        if not image_url or image_url == "N/A":
                            img_tag = product.locator("picture img")
                            image_url = await img_tag.get_attribute("srcset") if await img_tag.count() > 0 else "N/A"

                        # Ensure the URL is complete
                        if image_url and image_url.startswith("/"):
                            image_url = "https://www.forevermark.com" + image_url.split("?")[0]
                    except Exception:
                        image_url = "N/A"

                    # print(product_name)
                    # print(image_url)




                    gold_type_match = re.findall(r"\b(\d{1,2}ct\s*(?:Yellow|White|Rose)?\s*Gold|Platinum)\b", product_name, re.IGNORECASE)
                    kt = ", ".join(gold_type_match) if gold_type_match else "N/A"

                    # Extract Diamond Weight (e.g., "1.85ct", "2ct")
                    diamond_weight_match = re.findall(r"\b(\d+(?:\.\d+)?\s*ct)\b", product_name, re.IGNORECASE)
                    diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"

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
