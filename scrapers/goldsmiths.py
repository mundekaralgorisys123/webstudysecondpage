import os
import time
import logging
import re
import uuid
import base64
import asyncio
from datetime import datetime
from proxysetup import get_browser_with_proxy_strategy
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.drawing.image import Image as ExcelImage
import httpx
from playwright.async_api import async_playwright
import random
from openpyxl.drawing.image import Image
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


# Setup paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def modify_image_url(image_url):
    if not image_url or image_url == "N/A":
        return image_url
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"
    modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)
    return modified_url + query_params


async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    modified_url = modify_image_url(image_url)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(modified_url)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
                await asyncio.sleep(2)
    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"



def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    # Remove existing 'page=' parameter if present
    base_url = re.sub(r'([?&])page=\d+', '', base_url)

    # Add the page parameter
    separator = '&' if '?' in base_url else '?'

    # Ensure no trailing ampersand
    base_url = base_url.rstrip('&')

    return f"{base_url}{separator}page={page_count}&sort="



async def handle_goldsmiths(url, max_pages):
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_goldsmiths_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 0
    success_count = 0

    async with async_playwright() as p:
        while page_count <= max_pages:
            
            current_url= build_url_with_loadmore(url, page_count)
           
            browser = None
            page = None
            try:
                # Use the new proxy strategy function
                product_wrapper=".gridBlock.row"
                browser, page = await get_browser_with_proxy_strategy(p, current_url,product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay between scrolls
                    current_product_count = await page.locator('.productTile').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count


                wrapper = await page.query_selector("div.gridBlock.row")
                products = await wrapper.query_selector_all("div.productTile") if wrapper else []
                logging.info(f"Total products found: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        product_name_el = await product.query_selector("div.productTileName")
                        product_name = (await product_name_el.inner_text()).strip()
                    except:
                        product_name = "N/A"

                    try:
                        price_el = await product.query_selector("div.productTilePrice")
                        price_text = (await price_el.inner_text()).strip() if price_el else None

                        if price_text:
                            # Extract main price and check if there's a nested original price span
                            was_price_el = await price_el.query_selector("span.productTileWasPrice")
                            if was_price_el:
                                was_price = (await was_price_el.inner_text()).strip()
                                # Remove the was_price from the main text to avoid duplication
                                sale_price = price_text.replace(was_price, "").strip()
                                price = f"{sale_price} offer {was_price}"
                            else:
                                price = price_text
                        else:
                            price = "N/A"

                    except Exception as e:
                        print(f"Error extracting price: {e}")
                        price = "N/A"


                    try:
                        await product.scroll_into_view_if_needed()
                        image_elements = await product.query_selector_all("img")
                        urls = []
                        for img in image_elements:
                            srcset = await img.get_attribute("srcset") or await img.get_attribute("data-srcset")
                            if srcset:
                                # Split by comma, take the last entry (largest image), and extract the URL
                                last_entry = srcset.split(",")[-1].strip().split(" ")[0]
                                if last_entry.startswith("//"):
                                    last_entry = "https:" + last_entry
                                urls.append(last_entry)
                            else:
                                # fallback to src or data-src
                                fallback = await img.get_attribute("src") or await img.get_attribute("data-src")
                                if fallback and fallback.startswith("http"):
                                    urls.append(fallback)

                        image_url = urls[0] if urls else "N/A"
                    except Exception as e:
                        image_url = "N/A"
                        
                        
                    additional_info = []

                    try:
                        brand_el = await product.query_selector("div.productTileBrand")
                        brand = (await brand_el.inner_text()).strip() if brand_el else None
                        if brand:
                            additional_info.append(brand)

                        name_el = await product.query_selector("div.productTileName")
                        name = (await name_el.inner_text()).strip() if name_el else None
                        if name:
                            additional_info.append(name)

                        finance_el = await product.query_selector("div.productTileIfc b")
                        finance = (await finance_el.inner_text()).strip() if finance_el else None
                        if finance:
                            additional_info.append(f"Finance from £{finance} per month")
                            
                    
                        discount_el = await product.query_selector("div.product-flag.sale-percentage-flag")
                        discount = (await discount_el.inner_text()).strip() if discount_el else None
                        if discount:
                            additional_info.append(discount)
                       

                        if not additional_info:
                            additional_info.append("N/A")

                    except Exception as e:
                        print(f"Error fetching additional info: {e}")
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)
                        


                    gold_type_match = re.search(r"\b(?:9|14|18|22|24)\s*(?:Carat|ct)\s*(?:White|Yellow|Rose)?\s*Gold\b", product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"


                    diamond_weight_match = re.search(r"\b\d+(?:\.\d+)?\s*(?:Carat|ct(?:tw)?)\b", product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"


                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])

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
                if page:
                    await page.close()
                if browser:
                    await browser.close()
                
                page_count += 1
                await asyncio.sleep(random.uniform(2, 5))
                
            except Exception as e:
                logging.error(f"Error processing page {page_count}: {str(e)}")
                if page:
                    await page.close()
                if browser:
                    await browser.close()
                wb.save(file_path)
                continue
            
            # Add delay between pages
            await asyncio.sleep(random.uniform(2, 5))
            
        page_count += 1

    # # Final save and database operations
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
