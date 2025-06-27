import asyncio
import re
import os
import uuid
import logging
import base64
import random
import time
from datetime import datetime
from io import BytesIO
import httpx
from PIL import Image as PILImage
from openpyxl import Workbook
from openpyxl.drawing.image import Image as ExcelImage
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from utils import get_public_ip, log_event
from database import insert_into_db
from limit_checker import update_product_count
from proxysetup import get_browser_with_proxy_strategy
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
# Load environment
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

# Flask and paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

# Resize image if needed
def resize_image(image_data, max_size=(100, 100)):
    try:
        img = PILImage.open(BytesIO(image_data))
        img.thumbnail(max_size, PILImage.LANCZOS)
        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=85)
        return buffer.getvalue()
    except Exception as e:
        log_event(f"Error resizing image: {e}")
        return image_data


# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.png"
    image_full_path = os.path.join(image_folder, image_filename)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Referer": "https://www.cartier.com/",  # Important to simulate a browser visit
    }

    async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"

# Human-like delay
def random_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))

# Scroll to bottom of page to load all products
async def scroll_to_bottom(page):
    last_height = await page.evaluate("document.body.scrollHeight")
    while True:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(1, 3))  # Random delay between scrolls
        
        # Check if we've reached the bottom
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    """
    Builds paginated URL while preserving existing parameters
    Handles both filtered and non-filtered URLs:
    - Without filters: https://domain.com/path?page=2
    - With filters: https://domain.com/path?filter=val&page=2
    """
    
    # Parse existing URL components
    parsed = urlparse(base_url)
    query_params = parse_qs(parsed.query)
    
    # Update page parameter (replace if exists)
    query_params['page'] = [str(page_count)]
    
    # Rebuild query string with proper encoding
    new_query = []
    for key in sorted(query_params.keys()):
        values = query_params[key]
        for value in values:
            new_query.append(f"{key}={value}")
    
    # Construct new URL
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        '&'.join(new_query),
        parsed.fragment
    ))     

# Main scraper function
async def handle_cartier(url, max_pages=None):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_cartier_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    prev_prod_cout = 0
    load_more_clicks = 1
    page_count = 1
    while page_count <= max_pages:
        browser = None
        page = None
        # if load_more_clicks > 1:
        #     url = f"{url}?page={load_more_clicks}"
        current_url = build_url_with_loadmore(url, page_count)
        try:
            async with async_playwright() as p:
                product_wrapper = '.product-grid'
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                await scroll_to_bottom(page)
                
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                # Get all product tiles
                product_wrapper = await page.query_selector("div.product-grid")
                products = await product_wrapper.query_selector_all("div.product-grid__item") if product_wrapper else []
                products = products[prev_prod_cout:] 
                prev_prod_cout += len(products)
                logging.info(f"New products found: {len(products)}")
                print(f"New products found: {len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product name - from product-tile__name
                        name_tag = await product.query_selector(".product-tile__name")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    try:
                        material_tag = await product.query_selector(".product-tile__material")
                        kt = (await material_tag.inner_text()).strip() if material_tag else "N/A"
                    except Exception as e:
                        logging.warning(f"Failed to extract material info: {e}")
                        kt = "N/A"


                    try:
                        # Extract price - from price__sales
                        price_tag = await product.query_selector(".price__sales .value")
                        price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                        # Clean up price string
                        price = re.sub(r'\s+', ' ', price).strip()
                    except Exception:
                        price = "N/A"

                    

                    image_url = "N/A"
                    try:
                        # Get the first product image
                        img_tag = await product.query_selector(".product-tile__packshots-img")
                        if img_tag:
                            image_url = await img_tag.get_attribute("src") or await img_tag.get_attribute("data-src")
                            if image_url:
                                # Replace dimensions to get higher quality (350 -> 1080)
                                image_url = image_url.replace("sw=350", "sw=1080").replace("sh=350", "sh=1080")
                                # Ensure URL is complete
                                if image_url.startswith("//"):
                                    image_url = f"https:{image_url}"
                                # Remove any query parameters if needed
                                image_url = image_url.split('?')[0]
                                
                    except Exception as e:
                        log_event(f"Error getting image URL: {e}")
                        image_url = "N/A"
                        
                    # print(product_name) 
                    # print(image_url)    
                    if product_name == "N/A" and price == "N/A" and image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue 

                    # Extract diamond weight from description
                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                    
                    additional_info = []

                    try:
                        material_el = await product.query_selector("p.product-tile__body-section.product-tile__material")
                        if material_el:
                            material_text = await material_el.inner_text()
                            if material_text:
                                additional_info.append(material_text.strip())
                        else:
                            additional_info.append("N/A")
                    except Exception as e:
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)


                    unique_id = str(uuid.uuid4())
                    if image_url and image_url != "N/A":
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])
                            
                # Process image downloads
                for row_num, unique_id, task in image_tasks:
                    try:
                        image_path = await asyncio.wait_for(task, timeout=60)
                        if image_path != "N/A":
                            try:
                                img = ExcelImage(image_path)
                                img.width, img.height = 100, 100
                                sheet.add_image(img, f"D{row_num}")
                            except Exception as e:
                                logging.error(f"Error embedding image: {e}")
                                image_path = "N/A"
                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Image download timed out for row {row_num}")
                load_more_clicks += 1
                page_count += 1
                all_records.extend(records)
                wb.save(file_path)
                
                  
        except Exception as e:
            logging.error(f"Error during scraping: {str(e)}")
            wb.save(file_path)
        finally:
            if page: await page.close()
            if browser: await browser.close()
            
    page_count += 1        

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