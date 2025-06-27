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

async def handle_smilingrocks(url, max_pages):
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
                product_wrapper = "div.plp__grid"
                browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)

                try:
                    await page.goto(url, timeout=120000)
                except Exception as e:
                    logging.warning(f"Failed to load URL {url}: {e}")
                    await browser.close()
                    continue  # move to the next iteration

                # Simulate clicking 'Load More' number of times
                for _ in range(load_more_clicks - 1):
                    try:
                        load_more_button = page.locator("button.plp__view-all")
                        if await load_more_button.is_visible():
                            await load_more_button.click()
                            await page.wait_for_timeout(2000)  # Or use asyncio.sleep(2)
                        else:
                            break
                    except Exception as e:
                        logging.warning(f"Could not click 'Load More': {e}")
                        break

                product_wrapper = await page.wait_for_selector("div.plp__grid", timeout=30000)
                all_products = await product_wrapper.query_selector_all("div.product-card")
                
                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    additional_info = []
                    
                    try:
                        product_name = await (await product.query_selector("h3.product-card__title")).inner_text()
                        product_name = product_name.strip() if product_name else "N/A"
                    except:
                        product_name = "N/A"

                    # Price handling
                    price = "N/A"
                    try:
                        price_el = await product.query_selector("span.money")
                        if price_el:
                            price = await price_el.inner_text()
                            price = price.strip() if price else "N/A"
                            
                            # Check for discounted price (compare with original price if available)
                            original_price_el = await product.query_selector("span.money.compare-at-price")
                            if original_price_el:
                                original_price = await original_price_el.inner_text()
                                if original_price.strip():
                                    price = f"{price} | {original_price.strip()}"
                                    try:
                                        disc_num = float(price.split('$')[1].split('|')[0].strip())
                                        orig_num = float(original_price.replace('$', '').replace(',', '').strip())
                                        discount_pct = round((1 - (disc_num / orig_num)) * 100)
                                        additional_info.append(f"Discount: {discount_pct}%")
                                    except:
                                        pass
                    except:
                        price = "N/A"
                        
                    try:
                        kt_el = await product.query_selector("span.product-card__active-color")
                        kt = await kt_el.inner_text() if kt_el else "N/A"
                        kt = kt.strip() if kt else "N/A"
                    except:
                        kt = "N/A"   

                    # Get all available color options
                    try:
                        color_options = await product.query_selector_all(".color-selector__item")
                        if len(color_options) > 1:  # More than just the active color
                            colors = []
                            for color in color_options:
                                color_text = await color.inner_text()
                                if color_text.strip():
                                    colors.append(color_text.strip())
                            if colors:
                                additional_info.append(f"Available colors: {', '.join(colors)}")
                    except:
                        pass

                    try:
                        # Primary image is inside <div class="product-card__images"> -> first <img>
                        image_el = await product.query_selector("div.product-card__images img")
                        image_url = await image_el.get_attribute("src")
                        # Normalize to full URL if it's relative
                        if image_url and image_url.startswith("//"):
                            image_url = "https:" + image_url
                    except:
                        image_url = "N/A"

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue


                    # Extract diamond weight from product name
                    diamond_weight_match = re.search(r"(\d+(\.\d+)?)\s*(ct|carat)", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_weight_match.group(1)} ct" if diamond_weight_match else "N/A"

                    # Combine all additional info with pipe delimiter
                    additional_info_str = " | ".join(additional_info) if additional_info else ""
                    
                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(download_image(session, image_url, product_name, timestamp, image_folder, unique_id))
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight, additional_info_str))
                    sheet.append([
                        current_date, 
                        page_title, 
                        product_name, 
                        None,  # Image placeholder
                        kt, 
                        price, 
                        diamond_weight, 
                        time_only, 
                        image_url,
                        additional_info_str
                    ])

                # Process image downloads and attach them to Excel
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        try:
                            img = Image(image_path)
                            img.width, img.height = 100, 100
                            sheet.add_image(img, f"D{row}")
                        except Exception as e:
                            logging.error(f"Error adding image to Excel: {e}")
                            image_path = "N/A"
                    
                    # Update records with image path
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (record[0], record[1], record[2], record[3], image_path, 
                                         record[5], record[6], record[7], record[8])
                            break

                await browser.close()
            load_more_clicks += 1

        if not records:
            return None, None, None
        # Save Excel
        filename = f'handle_smilingrocks_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
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
