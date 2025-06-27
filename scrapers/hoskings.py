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
from openpyxl.drawing.image import Image as XLImage
import httpx
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
# Load environment variables from .env file
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
        async with session.get((image_url), timeout=10) as response:
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



# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
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
                await page.goto(url, wait_until="networkidle", timeout=180_000)

            # Wait for the selector with a longer timeout
            product_cards = await page.wait_for_selector("div.w-full.cursor-pointer.relative", state="attached", timeout=30000)

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




async def handle_hoskings(url, max_pages):
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath","AdditionalInfo"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_hoskings_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    while page_count <= max_pages:
        # current_url = f"{url}?page={page_count}"
        current_url= build_url_with_loadmore(url, page_count)
        logging.info(f"Processing page {page_count}: {current_url}")
        
        # Create a new browser instance for each page
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
            
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay

                    # Count products based on the container that holds individual product cards
                    current_product_count = await page.locator("div.w-full.cursor-pointer.relative").count()

                    if current_product_count == prev_product_count:
                        break  # No new products loaded
                    prev_product_count = current_product_count

                # Get all product elements
                products = await page.locator("div.w-full.cursor-pointer.relative").all()
                logging.info(f"✅ Total products found on page {page_count}: {len(products)}")



                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        product_name_locator = product.locator("a.product-item-meta__title, p.font-normal.text-text-subdued.text-label")
                        product_name = (await product_name_locator.inner_text()).strip() if await product_name_locator.count() > 0 else "N/A"
                    except Exception as e:
                        product_name = "N/A"

                    try:
                        price_container = product.locator('div.text-label.text-text-subdued.font-bold.mt-1')

                        sale_price_loc = price_container.locator('span.text-text-sale:visible')
                        original_price_loc = price_container.locator('span.line-through:visible')

                        if await sale_price_loc.count() > 0:
                            sale_price = (await sale_price_loc.first.inner_text()).strip().replace('$', '').replace(',', '')
                        else:
                            sale_price = "N/A"

                        if await original_price_loc.count() > 0:
                            original_price = (await original_price_loc.first.inner_text()).strip().replace('$', '').replace(',', '')
                        else:
                            original_price = "N/A"

                        if sale_price != "N/A" and original_price != "N/A":
                            price = f"{sale_price} offer {original_price}"
                        elif sale_price != "N/A":
                            price = sale_price
                        elif original_price != "N/A":
                            price = original_price
                        else:
                            # Fallback: try to get the only visible span inside price_container
                            fallback_price_loc = price_container.locator("span:visible")
                            if await fallback_price_loc.count() > 0:
                                price = (await fallback_price_loc.first.inner_text()).strip().replace('$', '').replace(',', '')
                            else:
                                price = "N/A"

                    except Exception as e:
                        price = "N/A"
                        logging.debug(f"Price extraction error: {str(e)}")

                        
                        


                    try:
                        await product.scroll_into_view_if_needed()
                        image_locator = product.locator("img[src]:not([src=''])")
                        await image_locator.first.wait_for(timeout=10000)
                        if await image_locator.count() > 0:
                            image_tag = image_locator.first
                            relative_url = await image_tag.get_attribute("data-src") or await image_tag.get_attribute("src")
                            image_url = f"https:{relative_url}" if relative_url and relative_url.startswith("//") else relative_url or "N/A"
                        else:
                            image_url = "N/A"
                    except Exception as e:
                        image_url = "N/A"
                        
                    try:
                        # Target the specific div containing "hoskings"
                        EXTRAlocator = product.locator(
                            "div.font-normal.text-text-subdued.uppercase.text-body3.mt-3"
                        )
                        # Fetch the text and strip any extra spaces
                        EXTRAlocator = (await EXTRAlocator.inner_text()).strip() if await EXTRAlocator.count() > 0 else "N/A"
                    except Exception:
                        EXTRAlocator = "N/A"




                    try:
                        Typesr = product.locator("div.text-text-sale")
                        Typesr = (await Typesr.inner_text()).strip() if await Typesr.count() > 0 else "N/A"
                    except Exception:
                        Typesr = "N/A"

                    AdditionalInfo = f"{EXTRAlocator}|{Typesr}"

                    
                    # print(AdditionalInfo)
                    # print(image_url)
                    # print(price)
                    # print(product_name)
                   
                        
                      
                    gold_type_match = re.search(r"(\d{1,2}(K|ct)\s*(Yellow|White|Rose)?\s*Gold|Platinum|Sterling Silver|Rhodium Plate)", product_name, re.IGNORECASE)
                    kt = gold_type_match.group(1) if gold_type_match else "Not found"

                    diamond_weight_match = re.search(r"(\d+(\.\d+)?)\s*(ct|carat)(\s*TW)?", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_weight_match.group(1)} ct{diamond_weight_match.group(4) if diamond_weight_match.group(4) else ''}" if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,AdditionalInfo))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,AdditionalInfo])

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

        except Exception as e:
            logging.error(f"Error processing page {page_count}: {str(e)}")
            # Save what we have so far
            wb.save(file_path)
        finally:
            # Clean up resources for this page
            if page:
                await page.close()
            if browser:
                await browser.close()
            
            # Add delay between pages
            await asyncio.sleep(random.uniform(2, 5))
            
        page_count += 1


    # Final save and database operations
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path
