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
from playwright.async_api import async_playwright, TimeoutError
from html import unescape

# Load .env variables
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')


def upgrade_to_high_res_url(image_url):
    if not image_url or image_url == "N/A":
        return image_url

    # Replace the cache key to always use high-resolution cache
    high_res_cache_key = "e30df37fe797367961e091f338eb1dfc"
    upgraded_url = re.sub(r'cache/[^/]+/', f'cache/{high_res_cache_key}/', image_url)
    return upgraded_url


async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A":
        return "N/A"
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    # image_url = upgrade_to_high_res_url(image_url)

    for attempt in range(3):
        try:
            resp = await session.get(image_url, timeout=10)
            resp.raise_for_status()
            with open(image_full_path, "wb") as f:
                f.write(resp.content)
            return image_full_path
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/3 - Error downloading {product_name}: {e}")
    logging.error(f"Failed to download {product_name} after 3 attempts.")
    return "N/A"

async def handle_harrywinston(url, max_pages):
    
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

    async with httpx.AsyncClient() as session:
        load_more_clicks = 1
        previous_count = 0

        while load_more_clicks <= max_pages:
            async with async_playwright() as p:
                # Create a new browser instance for each page
                browser = await p.chromium.connect_over_cdp(PROXY_URL)
                page = await browser.new_page()

                try:
                    await page.goto(url, timeout=120000)
                except Exception as e:
                    logging.warning(f"Failed to load URL {url}: {e}")
                    await browser.close()
                    continue  # move to the next iteration

               
                for _ in range(load_more_clicks - 1):
                    try:
                        load_more_button = page.locator('div.component-content input[value="Load More"]')
                        await load_more_button.wait_for(state='visible', timeout=10000)
                        
                        if not await load_more_button.is_enabled():
                            logging.info("Load More button disabled - no more content")
                            break

                        # Get current product count before clicking
                        product_items = page.locator('div.product-item')
                        initial_count = await product_items.count()

                        # Human-like interaction
                        await load_more_button.scroll_into_view_if_needed()
                        await asyncio.sleep(1)
                        await load_more_button.click(delay=100)

                        # Wait for content to load using product count check
                        try:
                            await page.wait_for_function(
                                """([selector, initial]) => {
                                    const current = document.querySelectorAll(selector).length;
                                    return current > initial;
                                }""",
                                ["div.product-item", initial_count],
                                timeout=20000
                            )
                            logging.info(f"New products loaded successfully (count: {await product_items.count()})")
                        except Exception as e:
                            logging.info(f"No new products detected: {str(e)}")
                            break

                        # Additional safety wait for visual updates
                        await asyncio.sleep(1)

                    except Exception as e:
                        logging.info(f"Stopped clicking 'Load More': {str(e)}")
                        break

                all_products = await page.query_selector_all("ul.search-result-list li div.product__wrapper")

                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    try:
                        # Get product name from <h3> tag inside div.product__text
                        product_name_tag = await product.query_selector('div.product__text h3')
                        product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching product name: {e}")
                        product_name = "N/A"

                    try:
                        # There is no price in your HTML, so set "N/A"
                        price = "N/A"
                    except Exception as e:
                        print(f"Error fetching price: {e}")
                        price = "N/A"

                    image_url = "N/A"

                    try:
                        image_div = await product.query_selector('div.product__image.lazy-load')
                        
                        if image_div:
                            # First try data-src on the <div>
                            image_url = await image_div.get_attribute("data-src")

                            if not image_url:
                                print("No data-src found on div, checking style attribute...")

                                # Fallback to style if needed (for future-proofing)
                                style_attr = await image_div.get_attribute("style") or ""
                                cleaned_style = unescape(style_attr)
                                match = re.search(r'url\(["\']?(.*?)["\']?\)', cleaned_style)
                                if match:
                                    image_url = match.group(1).strip()
                        
                    except Exception as e:
                        print(f"Error extracting image: {e}")
                        image_url = "N/A"

                    # print("Final image URL:", image_url)




                    kt_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                    kt = kt_match.group() if kt_match else "Not found"

                    diamond_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_match.group(1)} ct" if diamond_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(download_image(session, image_url, product_name, timestamp, image_folder, unique_id))
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))

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
        filename = f'handle_harrywinston_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
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
