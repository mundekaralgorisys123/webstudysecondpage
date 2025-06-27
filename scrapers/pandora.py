import asyncio
import re
import os
import uuid
import logging
import base64
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from flask import Flask
from dotenv import load_dotenv
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import httpx
from playwright.async_api import async_playwright, TimeoutError,Error


import random
import time

# Load .env variables
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')


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


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
]

async def random_delay(min_sec=1, max_sec=3):
    """Asynchronous random delay with jitter"""
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    """Enhanced image downloader with anti-blocking features"""
    if not image_url or image_url == "N/A":
        return "N/A"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://us.pandora.net/",
        "Sec-Fetch-Dest": "image",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-origin",
        "DNT": "1"
    }

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_path = os.path.join(image_folder, image_filename)

    async with httpx.AsyncClient(
        headers=headers,
        timeout=30.0,
        follow_redirects=True,
        http2=True,
        limits=httpx.Limits(max_keepalive_connections=10)
    ) as client:
        for attempt in range(retries):
            try:
                target_url = image_url.split('?')[0] if random.random() > 0.5 else image_url
                response = await client.get(target_url)
                
                if response.status_code == 403:
                    raise httpx.HTTPStatusError("403 Forbidden", request=response.request, response=response)
                
                response.raise_for_status()

                if not response.headers.get("Content-Type", "").startswith("image/"):
                    raise ValueError("Non-image content received")

                os.makedirs(os.path.dirname(image_path), exist_ok=True)
                async with await asyncio.to_thread(open, image_path, "wb") as f:
                    await f.write(response.content)

                if os.path.getsize(image_path) == 0:
                    raise IOError("Empty file written")

                return image_path

            except (httpx.RequestError, httpx.HTTPStatusError, IOError, ValueError) as e:
                await asyncio.sleep(2 ** attempt + random.random())
                logging.warning(f"Retry {attempt+1}/{retries} for {product_name}: {str(e)}")
                headers["User-Agent"] = random.choice(USER_AGENTS)

    logging.error(f"Failed after {retries} attempts: {product_name}")
    return "N/A"


       
async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")

            # Corrected selector
            product_cards = await page.wait_for_selector(
                ".css-1youv1j",
                state="attached",
                timeout=30000
            )

            if product_cards:
                print("[Success] Product cards loaded.")
                return
        except (Error, TimeoutError) as e:
            logging.error(f"Error navigating to {url} on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                logging.info("Retrying after waiting a bit...")
                
            else:
                logging.error(f"Failed to navigate to {url} after {retries} attempts.")
                raise   

async def handle_pandora(url, max_pages):
    
    ip_address = get_public_ip()
    logging.info(f"Starting scrape for {url} from IP: {ip_address}")

    if not os.path.exists(EXCEL_DATA_PATH):
        os.makedirs(EXCEL_DATA_PATH)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath"]
    sheet.append(headers)

    current_date = datetime.now().strftime("%Y-%m-%d")
    time_only = datetime.now().strftime("%H.%M")

    seen_ids = set()
    records = []
    image_tasks = []


    load_more_clicks = 1
    previous_count = 0

    while load_more_clicks <= max_pages:
        async with async_playwright() as p:
            # Create a new browser instance for each page
            browser = await p.chromium.connect_over_cdp(PROXY_URL)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1"
                }
            )
            page = await context.new_page()
            page.set_default_timeout(120000)  # 2 minute timeout
            
            await safe_goto_and_wait(page, url)
            log_event(f"Successfully loaded: {url}")

            
            
            # Scroll and load handling
            scroll_attempts = 0
            while scroll_attempts < max_pages * 2:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(random.uniform(1.5, 3.0))
                scroll_attempts += 1
                try:
                    await page.wait_for_selector('button[data-auto="btnPLPShowMore"]:not(:disabled)', timeout=5000)
                    await page.click('button[data-auto="btnPLPShowMore"]')
                    await asyncio.sleep(2)
                except (TimeoutError, Error):
                    break


            # all_products = await page.query_selector_all("div.css-rklm6r")
            product_selector = 'div.css-rklm6r:visible'
            await page.wait_for_selector(product_selector)
            all_products = await page.query_selector_all(product_selector)

            total_products = len(all_products)
            new_products = all_products[previous_count:]
            logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
            previous_count = total_products

            print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
            page_title = await page.title()

            for row_num, product in enumerate(new_products, start=len(sheet["A"]) + 1):
                try:
                    product_name_tag = await product.query_selector('p[data-auto="btnPLPProductName"]')
                    product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"
                except Exception as e:
                    print(f"Error fetching product name: {e}")
                    product_name = "N/A"

                try:
                    price_tag = await product.query_selector('span[data-auto="lblRegularPrice"]')
                    price = await price_tag.inner_text() if price_tag else "N/A"
                except Exception as e:
                    print(f"Error fetching price: {e}")
                    price = "N/A"

                try:
                    image_tag = await product.query_selector('div[data-auto="imgPLPProductImage"] img')
                    if image_tag:

                        image_url = await image_tag.get_attribute('src')
                    else:
                        image_url = "N/A"
                except Exception as e:
                    print(f"Error fetching image URL: {e}")
                    image_url = "N/A"




                print("image_url:",image_url)


                kt_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                kt = kt_match.group() if kt_match else "Not found"

                diamond_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", product_name, re.IGNORECASE)
                diamond_weight = f"{diamond_match.group(1)} ct" if diamond_match else "N/A"

                unique_id = str(uuid.uuid4())
                image_tasks.append((row_num, unique_id, asyncio.create_task(
                    download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                )))

                records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight))
                sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url])

            # Process image downloads and attach them to Excel
            for row, unique_id, task in image_tasks:
                image_path = await task
                if image_path != "N/A":
                    img = Image(image_path)
                    img.width, img.height = 100, 100
                    sheet.add_image(img, f"D{row}")
                for i, record in enumerate(records):
                    if record[0] == unique_id:
                        records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7])
                        break

            await browser.close()
        load_more_clicks += 1

        # Save Excel
        filename = f'handle_pandora_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
        file_path = os.path.join(EXCEL_DATA_PATH, filename)
        wb.save(file_path)
        log_event(f"Data saved to {file_path} | IP: {ip_address}")

        if records:
            insert_into_db(records)
        else:
            logging.info("No data to insert into the database.")

        update_product_count(len(seen_ids))

        with open(file_path, "rb") as f:
            base64_encoded = base64.b64encode(f.read()).decode("utf-8")

        return base64_encoded, filename, file_path
