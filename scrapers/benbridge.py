import asyncio
import re
import os
import uuid
import logging
import base64
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image
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

async def handle_benbridge(url, max_pages):
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
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
                
                product_wrapper = '.col-sm-12.col-lg-9'
                browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)

                # Simulate clicking 'Load More' number of times
                for _ in range(load_more_clicks - 1):
                    try:
                        load_more_button = page.locator("button.more")
                        if await load_more_button.is_visible():
                            await load_more_button.click()
                            await page.wait_for_timeout(2000)  # Or use asyncio.sleep(2)
                        else:
                            break
                    except Exception as e:
                        logging.warning(f"Could not click 'Load More': {e}")
                        break

                product_wrapper = await page.wait_for_selector("div.product-grid", timeout=30000)
                all_products = await product_wrapper.query_selector_all("div.product-grid-tile")

                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    additional_info = []

                    try:
                        product_name_el = await product.query_selector("div.pdp-link a")
                        product_name = await product_name_el.inner_text() if product_name_el else "N/A"
                    except:
                        product_name = "N/A"

                    price_parts = []
                    try:
                        sales_price_el = await product.query_selector("span.sales .value")
                        if sales_price_el:
                            price_parts.append(await sales_price_el.inner_text())
                        original_price_el = await product.query_selector(".price span:not(.sales) .value")
                        if original_price_el and (not sales_price_el or (await original_price_el.inner_text()) != (await sales_price_el.inner_text())):
                            price_parts.append(f"Original: {await original_price_el.inner_text()}")
                    except:
                        pass
                    price = " | ".join(price_parts) if price_parts else "N/A"

                    try:
                        new_badge = await product.query_selector("span.badge-product.new")
                        if new_badge:
                            additional_info.append(await new_badge.inner_text())
                    except:
                        pass

                    try:
                        image_el = await product.query_selector("div.product-tile-image-container img.tile-image")
                        image_url = await image_el.get_attribute("src") if image_el else None
                        if image_url and image_url.startswith("//"):
                            image_url = "https:" + image_url
                        elif image_url and image_url.startswith("/"):
                            image_url = "https://www.benbridge.com" + image_url
                        elif not image_url:
                            image_url = "N/A"
                    except:
                        image_url = "N/A"

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                    gold_type_match = re.search(
                        r"(18K|14K|10K)?\s*(White Gold|Yellow Gold|Rose Gold|Gold|Platinum|Silver)",
                        product_name,
                        re.IGNORECASE
                    )
                    kt = gold_type_match.group(0).strip() if gold_type_match else "N/A"

                    diamond_weight_match = re.search(r"(\d+(\.\d+)?)\s*(ct|carat)", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_weight_match.group(1)} ct" if diamond_weight_match else "N/A"

                    # Extract available sizes
                    try:
                        size_items = await product.query_selector_all("div.sizes-display.product-option .size-item")
                        sizes = [await size.inner_text() for size in size_items]
                        if sizes:
                            additional_info.append(f"Available Sizes: {', '.join(sizes)}")
                    except:
                        pass

                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(download_image(session, image_url, product_name, timestamp, image_folder, unique_id))
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, time_only, image_url, " | ".join(additional_info)))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, " | ".join(additional_info)])

                # Process image downloads and attach them to Excel
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        img = Image(image_path)
                        img.width, img.height = 100, 100
                        sheet.add_image(img, f"D{row}")
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8], record[9])
                            break

                await browser.close()
            load_more_clicks += 1

        if not all_products:
            return None, None, None
        # Save Excel
        filename = f'handle_benbridge_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
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
