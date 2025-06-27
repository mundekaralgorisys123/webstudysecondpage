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
from playwright.async_api import async_playwright
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

async def handle_graff(url, max_pages):
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

    
    records = []
    image_tasks = []

    async with httpx.AsyncClient() as session:
        load_more_clicks = 1
        previous_count = 0

        while load_more_clicks <= max_pages:
            async with async_playwright() as p:
                # Create a new browser instance for each page
                product_wrapper = '.b-product-tile__inner.js-product-tile-inner'
                browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)

                # Simulate clicking 'Load More' number of times
                for _ in range(load_more_clicks - 1):
                    try:
                        load_more_button = page.locator("button.b-btn--type-1.m-btn-width-1")
                        if await load_more_button.is_visible():
                            await load_more_button.click()
                            await asyncio.sleep(2)
                    except Exception as e:
                        logging.warning(f"Could not click 'See more': {e}")
                        break


                all_products = await page.query_selector_all(".b-product-grid__item.js-product-grid__item ")
                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    try:
                        product_name_tag = await product.query_selector('a.b-product-tile__link.js-product-tile-link')
                        product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching product name: {e}")
                        product_name = "N/A"


                    try:
                        price_tag = await product.query_selector('span.b-price__value.value')
                        price = await price_tag.inner_text() if price_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching price: {e}")
                        price = "N/A"


                    try:
                        image_tag = await product.query_selector('img.b-picture__image.js-picture__image')
                        if image_tag:
                            image_src = await image_tag.get_attribute('src')
                            image_url = image_src if image_src else "N/A"
                        else:
                            image_url = "N/A"
                    except Exception as e:
                        print(f"Error fetching image URL: {e}")
                        image_url = "N/A"
                        
                    try:
                        material_tag = await product.query_selector('div.b-product-tile__short-description.h-text--h3')
                        kt = await material_tag.inner_text() if material_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching material: {e}")
                        kt = "N/A"
                        
                    additional_info = []

                    try:
                        tag_els = await product.query_selector_all(".b-product-tile__badge span")
                        if tag_els:
                            for tag_el in tag_els:
                                tag_text = await tag_el.inner_text()
                                if tag_text:
                                    additional_info.append(tag_text.strip())
                        else:
                            additional_info.append("N/A")

                    except Exception as e:
                        print(f"Error fetching additional info: {e}")
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)
                    diamond_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", kt, re.IGNORECASE)
                    diamond_weight = f"{diamond_match.group(1)} ct" if diamond_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(download_image(session, image_url, product_name, timestamp, image_folder, unique_id))
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])

                # Process image downloads and attach them to Excel
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        img = Image(image_path)
                        img.width, img.height = 100, 100
                        sheet.add_image(img, f"D{row}")
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                            break

                await browser.close()
            load_more_clicks += 1

        # Save Excel
        filename = f'handle_graff_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
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