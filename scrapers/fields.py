import os
import time
import logging
import aiohttp
import asyncio
import concurrent.futures
from datetime import datetime
from io import BytesIO
from playwright.async_api import async_playwright, Page, TimeoutError, Error
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from flask import Flask
import uuid
import base64
from dotenv import load_dotenv
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db, create_table
from limit_checker import update_product_count
import random
import re
from proxysetup import get_browser_with_proxy_strategy
# Load environment variables
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

# Setup Flask
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def modify_image_url(image_url):
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

async def download_image(image_url, product_name, timestamp, image_folder, retries=3):
    """Download image with retries and return its local path."""
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{sanitize_filename(product_name)}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    
    modified_url = await modify_image_url(image_url)

    async with aiohttp.ClientSession() as session:
        for attempt in range(retries):
            try:
                async with session.get(modified_url, timeout=10) as response:
                    response.raise_for_status()
                    image_data = await response.read()
                    with open(image_full_path, "wb") as f:
                        f.write(image_data)
                    return image_full_path
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
                await asyncio.sleep(1)  # Add small delay between retries

    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"

async def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def scroll_and_wait(page: Page):
    """Scroll down to load lazy-loaded products."""
    previous_height = await page.evaluate("document.body.scrollHeight")
    await page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
    await asyncio.sleep(2)  # Allow time for content to load
    new_height = await page.evaluate("document.body.scrollHeight")
    return new_height > previous_height  # Returns True if more content is loaded

async def scroll_and_wait_advanced(page: Page, max_attempts=10, wait_time=1):
    """Scroll down and wait for new content to load dynamically."""
    last_height = await page.evaluate("document.body.scrollHeight")

    for attempt in range(max_attempts):
        logging.info(f"Scroll attempt {attempt + 1}/{max_attempts}")
        
        # Scroll down
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")

        # Wait for either new content to load or a timeout
        try:
            await page.wait_for_selector(".product-item", state="attached", timeout=3000)
        except:
            logging.info("No new content detected.")
        
        # Check if new content has loaded
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            logging.info("No more new content. Stopping scroll.")
            break  # Stop if the page height hasn't changed
        
        last_height = new_height
        await asyncio.sleep(wait_time)  # Optional short delay to avoid rapid requests

    logging.info("Finished scrolling.")
    return True  # Indicate successful scrolling

async def handle_fields(initial_url, max_pages):
    """Scrape product data by following each “View more” URL in a fresh browser,
    collect records for DB insertion, download images, write to Excel, then batch‐insert."""
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {initial_url} | IP: {ip_address} | Max pages: {max_pages}")

    # Prepare folders & Excel workbook
    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = [
        "UUID", "Current Date", "Header", "Product Name",
        "Kt", "Price", "Total Dia wt", "Time", "ImageURL"
    ]
    sheet.append(headers)

    # This will collect tuples for insert_into_db
    # (uuid, date, header, name, image_path, kt, price, diamond_weight)
    records = []

    # Hold download tasks so we can await them after scraping all pages
    image_tasks: list[tuple[int, str, asyncio.Task]] = []

    current_url = initial_url
    page_num = 1

    while page_num <= max_pages and current_url:
        logging.info(f"--- Scraping page {page_num}: {current_url}")

        async with async_playwright() as p:
            product_wrapper = "div.tile-container"
            browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)

            # On first page only: accept cookies & remove overlays
            if page_num == 1:
                try:
                    consent = page.locator("#onetrust-accept-btn-handler")
                    if await consent.is_visible(timeout=5000):
                        await consent.click()
                        await page.wait_for_load_state("domcontentloaded")
                    await page.evaluate("""() => {
                        ['.onetrust-pc-dark-filter',
                         '#onetrust-button-group-parent',
                         'svg[role="img"]',
                         '.modal-backdrop']
                        .forEach(sel => document.querySelectorAll(sel).forEach(el => el.remove()));
                    }""")
                except Exception:
                    pass

            # Let any lazy‑loaded images settle
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(1)

            # Grab all products on this page
            products = await page.query_selector_all("div.tile-container")
            header_text = await page.title()
            current_date = datetime.now().strftime("%Y-%m-%d")
            time_only = datetime.now().strftime("%H.%M")

            for prod in products:
                # — extract product details —
                name_tag = await prod.query_selector("a.link")
                product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"

                price_tag = await prod.query_selector("span.sales span.value")
                price = (await price_tag.inner_text()).strip() if price_tag else "N/A"

                img_tag = await prod.query_selector("img.tile-image")
                img_url = await img_tag.get_attribute("src") if img_tag else "N/A"

                kt_match = re.findall(
                    r"(\d{1,2}ct\s*(?:Yellow|White|Rose)?\s*Gold|Platinum|Cubic Zirconia)",
                    product_name, re.IGNORECASE
                )
                kt = ", ".join(kt_match) if kt_match else "N/A"

                dw_match = re.findall(r"(\d+(?:\.\d+)?\s*ct)", product_name, re.IGNORECASE)
                diamond_weight = ", ".join(dw_match) if dw_match else "N/A"

                # — record + Excel row —
                unique_id = str(uuid.uuid4())
                records.append((
                    unique_id,
                    current_date,
                    header_text,
                    product_name,
                    None,              # placeholder for image_path
                    kt,
                    price,
                    diamond_weight
                ))
                sheet.append([
                    unique_id,
                    current_date,
                    header_text,
                    product_name,
                    kt,
                    price,
                    diamond_weight,
                    time_only,
                    img_url
                ])
                row_idx = sheet.max_row

                # — schedule image download —
                task = asyncio.create_task(
                    download_image(img_url, product_name, timestamp, image_folder)
                )
                image_tasks.append((row_idx, unique_id, task))

            # — find next page URL —
            btn = page.locator("button.js-show-more-btn")
            if await btn.count() and await btn.is_visible():
                next_url = await btn.get_attribute("data-url")
                current_url = next_url or ""
                logging.info(f"Next page URL: {current_url}")
            else:
                logging.info("No further pages detected. Ending pagination.")
                current_url = None

            await browser.close()

        page_num += 1

    # — await all image downloads, embed into Excel, update records —
    for row_idx, uid, task in image_tasks:
        image_path = await task
        # update in-memory record
        for i, rec in enumerate(records):
            if rec[0] == uid:
                records[i] = (
                    rec[0], rec[1], rec[2], rec[3],
                    image_path, rec[5], rec[6], rec[7]
                )
                break
        # embed image into Excel
        if image_path and image_path != "N/A":
            img = Image(image_path)
            img.width, img.height = 100, 100
            sheet.add_image(img, f"E{row_idx}")

    # — save workbook —
    now_date = datetime.now().strftime("%Y-%m-%d")
    now_time = datetime.now().strftime("%H.%M")
    filename = f"handle_fields_{now_date}_{now_time}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    if not records:
        return None, None, None

    # Save the workbook
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    # Encode the file in base64
    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    # Insert data into the database and update product count
    insert_into_db(records)
    update_product_count(len(records))

    # Return necessary information
    return base64_encoded, filename, file_path