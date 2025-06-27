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
from openpyxl.drawing.image import Image
from PIL import Image as PILImage
from utils import get_public_ip, log_event, sanitize_filename
from dotenv import load_dotenv
from database import insert_into_db
from limit_checker import update_product_count
from io import BytesIO
import httpx
from proxysetup import get_browser_with_proxy_strategy
from typing import List, Tuple
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")


PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

async def download_and_resize_image(session, image_url):
    try:
        async with session.get(modify_image_url(image_url), timeout=10) as response:
            if response.status != 200:
                return None
            content = await response.read()
            image = PILImage.open(BytesIO(content))
            image.thumbnail((200, 200))
            img_byte_arr = BytesIO()
            image.save(img_byte_arr, format='JPEG', optimize=True, quality=85)
            return img_byte_arr.getvalue()
    except Exception as e:
        logging.warning(f"Error downloading/resizing image: {e}")
        return None

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
    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))


########################################  safe_goto_and_wait ####################################################################



def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '?' if '?' in base_url else '?'
    return f"{base_url}{separator}page={page_count}"   
         
########################################  Main Function Call ####################################################################
async def handle_tiffany(url, max_pages):
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
    filename = f"handle_tiffany_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    async with async_playwright() as p:
        while page_count <= max_pages:
            current_url = build_url_with_loadmore(url, page_count)
            # logging.info(f"Processing page {page_count}: {current_url}")
            browser = None
            page = None
            
            try:
                
                product_wrapper = ".wrapper"
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")
                


                product_wrapper = await page.query_selector("div.browse-grid")
                products = await product_wrapper.query_selector_all("div.layout_1x1") if product_wrapper else []
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        name_container = await product.query_selector("p.product-tile__details_name")
                        if name_container:
                            span_elements = await name_container.query_selector_all("span.product-tile__details_name__split")
                            parts = [await span.text_content() for span in span_elements]
                            product_name = " ".join(part.strip() for part in parts if part).strip()
                        else:
                            product_name = "N/A"
                    except:
                        product_name = "N/A"


                    try:
                        price_tag = await product.query_selector('p.product-tile__details_price')
                        price = await price_tag.text_content() if price_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching price: {e}")
                        price = "N/A"


                    try:
                        img_el = await product.query_selector("div.tiffany-picture img")
                        image_url = "N/A"

                        if img_el:
                            # Prefer data-srcset (higher-res, often used in lazy loading)
                            data_srcset = await img_el.get_attribute("data-srcset")
                            if data_srcset:
                                options = [url.strip().split()[0] for url in data_srcset.split(",")]
                                if options:
                                    image_url = options[0]
                            else:
                                # Fallback to srcset
                                srcset = await img_el.get_attribute("srcset")
                                if srcset:
                                    options = [url.strip().split()[0] for url in srcset.split(",")]
                                    if options:
                                        image_url = options[0]
                                else:
                                    # Fallback to src
                                    image_url = await img_el.get_attribute("src")

                        # Ensure image_url has a proper scheme
                        if image_url and image_url.startswith("//"):
                            image_url = "https:" + image_url
                        elif image_url and image_url.startswith("/"):
                            image_url = "https://www.tiffany.com" + image_url
                        elif image_url and not image_url.startswith("http"):
                            image_url = "https://www.tiffany.com/" + image_url.lstrip("/")

                    except Exception as e:
                        print(f"Image extraction error: {e}")
                        image_url = "N/A"
  
                    # print(image_url)
   
                    additional_info = []

                    try:
                        # Select both "New" tags and any future promotional tags
                        tag_els = await product.query_selector_all("div.tile-buttons span")
                        
                        if tag_els:
                            for tag_el in tag_els:
                                tag_text = await tag_el.inner_text()
                                if tag_text and tag_text.strip():
                                    additional_info.append(tag_text.strip())
                        else:
                            additional_info.append("N/A")

                    except Exception as e:
                        print(f"Tag extraction error: {e}")
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)


                    
                    
                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue    
                    
                    

                    gold_type_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"


                    diamond_weight_match = re.search(r"\d+(?:[-/]\d+)?(?:\s+\d+/\d+)?\s*ct\s+tw", product_name, re.IGNORECASE)
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
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
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