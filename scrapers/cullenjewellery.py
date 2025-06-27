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
# load_dotenv()
# PROXY_URL = os.getenv("PROXY_URL")

# Flask and paths
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


def modify_image_url(image_url):
    """Enhance Shopify image URLs to get higher resolution versions"""
    if not image_url or image_url == "N/A":
        return image_url

    # Handle Shopify CDN URLs
    if 'cdn.shopify.com' in image_url:
        # Remove size constraints from filename (e.g., 500x500)
        modified_url = re.sub(r'_(\d+x\d+)\.(jpg|jpeg|png)', r'.\2', image_url)
        
        # Remove any quality parameters
        modified_url = re.sub(r'&?quality=\d+', '', modified_url)
        
        # Set maximum width/height parameters
        modified_url = modified_url.replace('width=500', 'width=2000')
        modified_url = modified_url.replace('height=500', 'height=2000')
        
        # Add lossless compression if not already present
        if 'format=' not in modified_url:
            modified_url += '&format=webp' if '?' in modified_url else '?format=webp'
        
        return modified_url

    # Original Macy's handling (keep existing functionality)
    modified_url = re.sub(r'wid=\d+', 'wid=1200', image_url)
    modified_url = re.sub(r'hei=\d+', 'hei=1200', modified_url)
    modified_url = re.sub(r'qlt=[^&]+', 'qlt=95', modified_url)
    
    return modified_url


async def handle_cullenjewellery(url, max_pages):
    
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

    
    records = []
    image_tasks = []

    async with httpx.AsyncClient() as session:
        load_more_clicks = 1
        previous_count = 0

        while load_more_clicks <= max_pages:
            async with async_playwright() as p:
                # Create a new browser instance for each page
                
                product_wrapper = '.root.svelte-19w1zzs'
                
                browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)

                 # Simulate clicking 'Load More' number of times
                for _ in range(load_more_clicks - 1):
                    try:
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1)

                        button = await page.query_selector("button.load-more")
                        if button and await button.is_visible():
                            await button.scroll_into_view_if_needed()
                            await asyncio.sleep(0.5)
                            await button.click()
                            await asyncio.sleep(2)  # Wait for new products to load
                        else:
                            print("No more 'Load More' button.")
                            break
                    except Exception as e:
                        print(f"Error: {e}")
                        break
                                    
                all_products = await page.query_selector_all(".root.svelte-t7drm4")

                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                   # --- Product Name ---
                    try:
                        # Try to get the detailed product name from <h3 class="hide_caption">
                        product_name_tag = await product.query_selector('h3.hide_caption')
                        
                        if product_name_tag:
                            product_name = await product_name_tag.inner_text()
                        else:
                            # Fallback to <h2> if <h3.hide_caption> is not available
                            product_name_tag = await product.query_selector('h2.svelte-1j4gv6v')
                            product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"

                        product_name = product_name.strip()
                    except Exception as e:
                        logging.error(f"[Product Name] Error: {e}")
                        product_name = "N/A"


                    # --- Price ---
                    try:
                        price_element = await product.query_selector('h3.price')
                        if price_element:
                            price_text = await price_element.inner_text()
                            # Extract numeric value from the price string
                            price_value = ''.join(filter(lambda x: x.isdigit() or x == '.', price_text))
                            price = f"${price_value}" if price_value else "N/A"
                        else:
                            price = "N/A"
                    except Exception as e:
                        logging.error(f"[Price] Error: {e}")
                        price = "N/A"



                    # --- Image URL ---
                    try:
                        # Scroll into view to ensure lazy-loaded image loads
                        await product.scroll_into_view_if_needed()

                        # Find the image inside the hidden slider container
                        img_element = await product.query_selector('div.slider img.fillimage')

                        if img_element:
                            image_url = await img_element.get_attribute('src')
                            
                            # Optional: Check if it's a valid image (not a placeholder)
                            if not image_url or 'placeholder' in image_url:
                                image_url = "N/A"
                        else:
                            image_url = "N/A"

                    except Exception as e:
                        logging.error(f"[Image URL] Error: {e}")
                        image_url = "N/A"


                        
                    print(product_name)
                    print(price)
                    print(image_url)    



                    if product_name == "N/A" and price == "N/A" and image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue  


                    # Extract Gold Type (e.g., "14K Yellow Gold").
                    gold_type_match = re.findall(r"(\d{1,2}ct\s*(?:Yellow|White|Rose)?\s*Gold|Platinum|Cubic Zirconia)", product_name, re.IGNORECASE)
                    kt = ", ".join(gold_type_match) if gold_type_match else "N/A"

                    # Extract Diamond Weight (supports "1.85ct", "2ct", "1.50ct", etc.)
                    diamond_weight_match = re.findall(r"(\d+(?:\.\d+)?\s*ct)", product_name, re.IGNORECASE)
                    diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"

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
        filename = f'handle_cullenjewellery_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
        file_path = os.path.join(EXCEL_DATA_PATH, filename)
        if not records:
            return None, None, None

        # Final save and database operations
        wb.save(file_path)
        log_event(f"Data saved to {file_path} | IP: {ip_address}")

        with open(file_path, "rb") as file:
            base64_encoded = base64.b64encode(file.read()).decode("utf-8")

        insert_into_db(records)
        update_product_count(len(records))

        return base64_encoded, filename, file_path
