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
from proxysetup import get_browser_with_proxy_strategy
# Load .env variables
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A":
        return "N/A"
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

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

async def handle_fraserhart(url, max_pages):
    ip_address = "DUMMY_IP" # get_public_ip()
    logging.info(f"Starting scrape for {url} from IP: {ip_address}")

    if not os.path.exists(EXCEL_DATA_PATH):
        os.makedirs(EXCEL_DATA_PATH)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    current_date = datetime.now().strftime("%Y-%m-%d")
    time_only = datetime.now().strftime("%H.%M")

    seen_ids = set()
    records = []
    image_tasks = []
    current_url = url
    async with httpx.AsyncClient() as session:
        load_more_clicks = 1
        previous_count = 0

        while load_more_clicks <= max_pages:
            async with async_playwright() as p:
                # Create a new browser instance for each page
                product_wrapper = ".tile-container"
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)

                try:
                    await page.goto(url, timeout=120000)
                except Exception as e:
                    logging.warning(f"Failed to load URL {url}: {e}")
                    await browser.close()
                    continue  # move to the next iteration

                # Simulate clicking 'Load More' number of times
                for _ in range(load_more_clicks - 1):
                    try:
                        # Always scroll down to try revealing the button
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                        await asyncio.sleep(1)

                        # Fresh button query every time
                        load_more_button = await page.query_selector("button.more")

                        if load_more_button and await load_more_button.is_visible():
                            current_count = await page.eval_on_selector_all('.tile-container', 'els => els.length')

                            # Scroll it into view and click via JS to reduce risk of detach errors
                            await load_more_button.scroll_into_view_if_needed()
                            await asyncio.sleep(0.5)

                            await page.evaluate("(btn) => btn.click()", load_more_button)

                            # Wait until more products are visible
                            await page.wait_for_function(
                                """prev => document.querySelectorAll('.tile-container').length > prev""",
                                arg=current_count,
                                timeout=10000
                            )
                        else:
                            print("No more 'Load More' button visible.")
                            break

                    except Exception as e:
                        logging.warning(f"⚠️ Error clicking 'Load More': {e}")
                        break

                all_products = await page.query_selector_all(".tile-container")
                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    additional_info = []

                    try:
                        product_name_tag = await product.query_selector('a.link')
                        product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"
                    except Exception as e:
                        print(f"[Product Name] Error: {e}")
                        product_name = "N/A"

                    price_parts = []
                    try:
                        price_tag = await product.query_selector('span.sales span.value')
                        if price_tag:
                            price_parts.append(await price_tag.inner_text())
                        original_price_tag = await product.query_selector('del span.value')
                        if original_price_tag:
                            price_parts.append(f"Original: {await original_price_tag.inner_text()}")
                        price = " | ".join(price_parts) if price_parts else "N/A"
                    except Exception as e:
                        print(f"[Price] Error: {e}")
                        price = "N/A"

                    try:
                        image_tag = await product.query_selector('img.tile-image')
                        if image_tag:
                            image_url = await image_tag.get_attribute('src') \
                                        or await image_tag.get_attribute('data-src') \
                                        or "N/A"
                        else:
                            image_url = "N/A"
                    except Exception as e:
                        print(f"[Image URL] Error: {e}")
                        image_url = "N/A"

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                    # Extract Gold Type (e.g., "14K Yellow Gold").
                    gold_type_match = re.findall(r"(\d{1,2}ct\s*(?:Yellow|White|Rose)?\s*Gold|Platinum|Cubic Zirconia)", product_name, re.IGNORECASE)
                    kt = ", ".join(gold_type_match) if gold_type_match else "N/A"

                    # Extract Diamond Weight (supports "1.85ct", "2ct", "1.50ct", etc.)
                    diamond_weight_match = re.findall(r"(\d+(?:\.\d+)?\s*ct)", product_name, re.IGNORECASE)
                    diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"

                    # Extract additional info
                    try:
                        # Look for sale badge
                        sale_badge = await product.query_selector('.lozenges.offer.badge--sale')
                        if sale_badge:
                            additional_info.append(await sale_badge.inner_text())

                        # Look for any other relevant text elements within the product tile
                        other_info_selectors = [
                            '.tile-body .tile-brand',
                            '.tile-body .product-v12-finance .v12-finance-message'
                        ]
                        for selector in other_info_selectors:
                            elements = await product.query_selector_all(selector)
                            for el in elements:
                                text = await el.inner_text()
                                if text and text.strip():
                                    additional_info.append(text.strip())

                        # You might need to add more specific selectors based on the website structure
                        # Inspect the HTML for other relevant information you want to extract.

                    except Exception as e:
                        print(f"[Additional Info Extraction Error]: {e}")

                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(download_image(session, image_url, product_name, timestamp, image_folder, unique_id))
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))

                    additional_info_str = " | ".join(additional_info) if additional_info else ""
                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight, additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, additional_info_str])

                # Process image downloads and attach them to Excel
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        try:
                            img = Image(image_path)
                            img.width, img.height = 100, 100
                            sheet.add_image(img, f"D{row}")
                        except Exception as e:
                            print(f"Error adding image to Excel: {e}")
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                            break

                btn = page.locator("button.js-show-more-btn")
                if await btn.count() and await btn.is_visible():
                    next_url = await btn.get_attribute("data-url")
                    current_url = next_url or ""
                    logging.info(f"Next page URL: {current_url}")
                else:
                    logging.info("No further pages detected. Ending pagination.")
                    current_url = None

                await browser.close()
            load_more_clicks += 1

        if not all_products:
            return None, None, None
        # Save Excel
        filename = f'handle_fraserhart_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
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
