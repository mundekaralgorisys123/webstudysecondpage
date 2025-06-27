import time
import re
import os
import uuid
import asyncio
import base64
import logging
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image as ExcelImage
from dotenv import load_dotenv
from PIL import Image as PILImage
from io import BytesIO
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import random
import httpx
from playwright.async_api import async_playwright, TimeoutError, Error
from openpyxl.drawing.image import Image
import traceback
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
    """Modify the image URL to replace '_260' with '_1200' while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    # Extract and preserve query parameters
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"

    # Replace '_260' with '_1200' while keeping the rest of the URL intact
    modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)

    return modified_url + query_params  # Append query parameters if they exist

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


async def safe_goto_and_wait(page, url,isbri_data, retries=2):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            
            if isbri_data:
                await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            else:
                await page.goto(url, wait_until="domcontentloaded", timeout=180_000)

            # Wait for the selector with a longer timeout
            product_cards = await page.wait_for_selector(".list-group-horizontal", state="attached", timeout=30000)

            # Optionally validate at least 1 is visible (Playwright already does this)
            if product_cards:
                print("[Success] Product cards loaded.")
                return
        except Error as e:
            logging.error(f"Error navigating to {url} on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                logging.info("Retrying after waiting a bit...")
                random_delay(1, 3)  # Add a delay before retrying
            else:
                logging.error(f"Failed to navigate to {url} after {retries} attempts.")
                raise
        except TimeoutError as e:
            logging.warning(f"TimeoutError on attempt {attempt + 1} navigating to {url}: {e}")
            if attempt < retries - 1:
                logging.info("Retrying after waiting a bit...")
                random_delay(1, 3)  # Add a delay before retrying
            else:
                logging.error(f"Failed to navigate to {url} after {retries} attempts.")
                raise


########################################  get browser with proxy ####################################################################
      

async def get_browser_with_proxy_strategy(p, url: str):
    """
    Dynamically checks robots.txt and selects proxy accordingly
    Always uses proxies - never scrapes directly
    """
    parsed_url = httpx.URL(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.host}"
    
    # 1. Fetch and parse robots.txt
    disallowed_patterns = await get_robots_txt_rules(base_url)
    
    # 2. Check if URL matches any disallowed pattern
    is_disallowed = check_url_against_rules(str(parsed_url), disallowed_patterns)
    
    # 3. Try proxies in order (bri-data first if allowed, oxylabs if disallowed)
    proxies_to_try = [
        PROXY_URL if not is_disallowed else {
            "server": PROXY_SERVER,
            "username": PROXY_USERNAME,
            "password": PROXY_PASSWORD
        },
        {  # Fallback to the other proxy
            "server": PROXY_SERVER,
            "username": PROXY_USERNAME,
            "password": PROXY_PASSWORD
        } if not is_disallowed else PROXY_URL
    ]

    last_error = None
    for proxy_config in proxies_to_try:
        browser = None
        try:
            isbri_data = False
            if proxy_config == PROXY_URL:
                logging.info("Attempting with bri-data proxy (allowed by robots.txt)")
                browser = await p.chromium.connect_over_cdp(PROXY_URL)
                isbri_data = True
            else:
                logging.info("Attempting with oxylabs proxy (required by robots.txt)")
                browser = await p.chromium.launch(
                    proxy=proxy_config,
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-web-security'
                    ]
                )

            context = await browser.new_context()
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """)
            page = await context.new_page()
            
            await safe_goto_and_wait(page, url,isbri_data)
            return browser, page

        except Exception as e:
            last_error = e
            error_trace = traceback.format_exc()
            logging.error(f"Proxy attempt failed:\n{error_trace}")
            if browser:
                await browser.close()
            continue

    error_msg = (f"Failed to load {url} using all proxy options. "
                f"Last error: {str(last_error)}\n"
                f"URL may be disallowed by robots.txt or proxies failed.")
    logging.error(error_msg)
    raise RuntimeError(error_msg)




async def get_robots_txt_rules(base_url: str) -> List[str]:
    """Dynamically fetch and parse robots.txt rules"""
    robots_url = f"{base_url}/robots.txt"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(robots_url, timeout=10)
            if resp.status_code == 200:
                return [
                    line.split(":", 1)[1].strip()
                    for line in resp.text.splitlines()
                    if line.lower().startswith("disallow:")
                ]
    except Exception as e:
        logging.warning(f"Couldn't fetch robots.txt: {e}")
    return []


def check_url_against_rules(url: str, disallowed_patterns: List[str]) -> bool:
    """Check if URL matches any robots.txt disallowed pattern"""
    for pattern in disallowed_patterns:
        try:
            # Handle wildcard patterns
            if "*" in pattern:
                regex_pattern = pattern.replace("*", ".*")
                if re.search(regex_pattern, url):
                    return True
            # Handle path patterns
            elif url.startswith(f"{pattern}"):
                return True
            # Handle query parameters
            elif ("?" in url) and any(
                f"{param}=" in url 
                for param in pattern.split("=")[0].split("*")[-1:]
                if "=" in pattern
            ):
                return True
        except Exception as e:
            logging.warning(f"Error checking pattern {pattern}: {e}")
    return False


def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}page={page_count}"   


async def handle_apart(url, max_pages):
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
    filename = f"handle_apart_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    async with async_playwright() as p:
        while page_count <= max_pages:
            current_url = build_url_with_loadmore(url, page_count)
        
            logging.info(f"Processing page {page_count}: {current_url}")
            browser = None
            page = None
            try:
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('.ProductCardWrapper').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")
                
                products = await page.query_selector_all("li.item")
                logging.info(f"Total products scraped on page: {len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Product name
                        name_elem = await product.query_selector("div.product-name a.productListGTM")
                        product_name = await name_elem.inner_text() if name_elem else None
                        
                        if not product_name:
                            continue  # Skip if no product name
                    except Exception as e:
                        continue  # Skip if error getting product name

                    # Price handling - comprehensive approach
                    price_str = None
                    currency = None
                    try:
                        price_container = await product.query_selector("div.price-cnt")
                        if price_container:
                            # Get price value
                            price_elem = await price_container.query_selector("span.value")
                            price_value = await price_elem.inner_text() if price_elem else None
                            
                            # Get currency
                            currency_elem = await price_container.query_selector("span.currencyName")
                            currency = await currency_elem.inner_text() if currency_elem else None
                            
                            # Format price string
                            if price_value and currency:
                                price_str = f"{price_value} {currency}"
                            elif price_value:
                                price_str = price_value
                    except Exception as e:
                        print(f"[Price] Error: {e}")
                        price_str = None

                    if not price_str:
                        continue  # Skip if no price

                    try:
                        # Image URL
                        image_elem = await product.query_selector("img.group.list-group-image")
                        image_url = await image_elem.get_attribute("src") if image_elem else None
                        
                        if image_url:
                            if image_url.startswith("//"):
                                image_url = f"https:{image_url}"
                            elif image_url.startswith("/"):
                                image_url = f"https://www.apart.eu{image_url}"
                        else:
                            continue  # Skip if no image URL
                    except Exception as e:
                        print(f"[Image URL] Error: {e}")
                        continue  # Skip if error getting image URL
                    
                    if product_name == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Image: {image_url}")
                        continue

                    # Extract all labels/badges
                    additional_info = []
                    
                    try:
                        # Get all badge elements
                        badge_elements = await product.query_selector_all(".new-badge .new .label-master")
                        for badge in badge_elements:
                            badge_text = (await badge.inner_text()).strip()
                            if badge_text:
                                additional_info.append(badge_text)
                                
                        # Also check for group badges
                        group_badges = await product.query_selector_all(".new-badge .new .label-master.group")
                        for badge in group_badges:
                            badge_text = (await badge.inner_text()).strip()
                            if badge_text:
                                additional_info.append(badge_text)
                    except Exception as e:
                        print(f"[Badges] Error: {e}")

                    # Extract product details
                    gold_type_match = re.search(r"(\d{1,2}K|Platinum|Silver|Gold|White Gold|Yellow Gold|Rose Gold)", product_name, re.IGNORECASE)
                    kt = gold_type_match.group(0) if gold_type_match else "N/A"

                    diamond_weight_match = re.search(r"(\d+(\.\d+)?)\s*(ct|carat)", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_weight_match.group(1)} ct" if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))
                    
                    records.append((
                        unique_id, 
                        current_date, 
                        page_title, 
                        product_name, 
                        None,  # Placeholder for image path
                        kt, 
                        price_str, 
                        diamond_weight,
                        " | ".join(additional_info) if additional_info else "N/A"
                    ))
                    
                    sheet.append([
                        current_date, 
                        page_title, 
                        product_name, 
                        None,  # Placeholder for image
                        kt, 
                        price_str, 
                        diamond_weight, 
                        time_only, 
                        image_url,
                        " | ".join(additional_info) if additional_info else "N/A"
                    ])
                    
                # Process images and update records
                for row_num, unique_id, task in image_tasks:
                    try:
                        image_path = await asyncio.wait_for(task, timeout=60)
                        if not image_path or image_path == "N/A":
                            # Remove the record if image download failed
                            records = [r for r in records if r[0] != unique_id]
                            continue
                            
                        try:
                            img = Image(image_path)
                            img.width, img.height = 100, 100
                            sheet.add_image(img, f"D{row_num}")
                        except Exception as img_error:
                            logging.error(f"Error adding image to Excel: {img_error}")
                            # Remove the record if we can't add the image
                            records = [r for r in records if r[0] != unique_id]
                            continue
                        
                        # Update the record with image path
                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (
                                    record[0], 
                                    record[1], 
                                    record[2], 
                                    record[3], 
                                    image_path, 
                                    record[5], 
                                    record[6], 
                                    record[7],
                                    record[8]
                                )
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Timeout downloading image for row {row_num}")
                        # Remove the record if image download timed out
                        records = [r for r in records if r[0] != unique_id]

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
                
            finally:
                if browser:
                    try:
                        await browser.close()
                    except Exception as e:
                        logging.warning(f"Error closing browser: {e}")
            
            # Add delay between pages
            await asyncio.sleep(random.uniform(2, 5))

    # Final save and database operations
    if not all_records:
        return None, None, None
    
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    # Only insert complete records into database
    complete_records = [r for r in all_records if all(r[1:])]  # Skip if any field is None
    insert_into_db(complete_records)
    update_product_count(len(complete_records))

    return base64_encoded, filename, file_path