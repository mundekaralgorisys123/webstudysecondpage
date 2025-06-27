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
from urllib.parse import urlparse, parse_qs, urlunparse, quote_plus
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
from proxysetup import get_browser_with_proxy_strategy

# Load environment
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

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

    async with httpx.AsyncClient(timeout=10.0) as client:
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

# Reliable page.goto wrapper
async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            await page.wait_for_selector(".ProductListWrapper", state="attached", timeout=30000)
            print("[Success] Product listing loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise
            

def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    """
    Builds a paginated URL with 'page' parameter appearing first in the query string.
    Preserves existing parameters and handles multi-valued keys.
    """
    parsed = urlparse(base_url)
    query_params = parse_qs(parsed.query)

    # Prepare new query string with 'page' first
    new_query_parts = [f"page={page_count}"]

    for key, values in query_params.items():
        if key == "page":
            continue  # Skip old page value
        for value in values:
            new_query_parts.append(f"{quote_plus(key)}={quote_plus(value)}")

    # Reconstruct the full URL
    new_query = "&".join(new_query_parts)
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))            

# Main scraper function
async def handle_ringconcierge(url, max_pages):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_ringconcierge_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    load_more_clicks = 1
    current_url = url
    while load_more_clicks <= max_pages:
        current_url = build_url_with_loadmore(url, load_more_clicks)
        browser = None
        page = None
        # if load_more_clicks > 1:
        #     current_url = f"{url}?page={load_more_clicks}"
        try:
            async with async_playwright() as p:
    
                product_wrapper = '.ProductListWrapper'
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                await scroll_to_bottom(page)
                
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                # Get all product tiles
                product_wrapper = await page.query_selector("div.ProductListWrapper")
                products = await page.query_selector_all("div.Grid__Cell") if product_wrapper else []

                logging.info(f"New products found: {len(products)}")
                print(f"New products found: {len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product name from anchor inside h2 with both classes
                        name_tag = await product.query_selector(".ProductItem__Title.Heading a")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"



                    try:
                        # Locate the outer span containing the price phrase
                        price_container = product.locator(".ProductItem__PriceList .ProductItem__Price")
                        if await price_container.count() > 0:
                            full_price_text = await price_container.inner_text()
                            # full_price_text example: "Starting at $398"
                            # Extract the dollar amount (or just use full text)
                            price_match = re.search(r"\$\d+(?:\.\d{2})?", full_price_text)
                            price = price_match.group() if price_match else full_price_text.strip()
                        else:
                            price = "N/A"
                    except Exception:
                        price = "N/A"



                    

                    image_url = "N/A"
                    try:
                        # Get all product image elements
                        img_tags = await product.query_selector_all("img.ProductItem__Image")
                        
                        if img_tags:
                            # Use the first image tag
                            img_tag = img_tags[0]

                            # Try to get the 'srcset' attribute
                            srcset = await img_tag.get_attribute("srcset")

                            if srcset:
                                # Parse all entries in srcset
                                srcset_parts = [part.strip() for part in srcset.split(",")]
                                urls_resolutions = []

                                for part in srcset_parts:
                                    if " " in part:
                                        img_url, resolution = part.split(" ")
                                        resolution = int(resolution.replace("w", ""))
                                        urls_resolutions.append((resolution, img_url))

                                # Sort by highest resolution
                                urls_resolutions.sort(reverse=True)
                                if urls_resolutions:
                                    image_url = urls_resolutions[0][1]

                            # Fallback: use 'src' attribute if srcset not found or empty
                            if not image_url or image_url == "N/A":
                                image_url = await img_tag.get_attribute("src")

                            # Ensure URL is complete
                            if image_url and image_url.startswith("//"):
                                image_url = f"https:{image_url}"

                    except Exception as e:
                        log_event(f"Error getting image URL: {e}")
                        image_url = "N/A"
                        
                        
                    print(product_name) 
                    print(price) 
                    print(image_url)    


                    # Extract gold type (kt) from product name/description
                    gold_type_pattern = r"\b\d{1,2}(?:K|kt|ct|Kt)\b|\bPlatinum\b|\bSilver\b|\bWhite Gold\b|\bYellow Gold\b|\bRose Gold\b"
                    gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    # Extract diamond weight from description
                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                    
                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue  

                    unique_id = str(uuid.uuid4())
                    if image_url and image_url != "N/A":
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                    records.append((unique_id, current_date, page_title, product_name, product_name, kt, price, diamond_weight))
                    sheet.append([current_date, page_title, product_name, product_name, kt, price, diamond_weight, time_only, image_url])
                            
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
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Image download timed out for row {row_num}")
                load_more_clicks += 1
                all_records.extend(records)
                wb.save(file_path)
                
        except Exception as e:
            logging.error(f"Error during scraping: {str(e)}")
            wb.save(file_path)
        finally:
            if page: await page.close()
            if browser: await browser.close()

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