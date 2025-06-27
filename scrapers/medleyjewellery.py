import os
import re
import time
import logging
import random
from typing import List
import uuid
import asyncio
import base64
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError, Error
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from PIL import Image as PILImage
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from utils import get_public_ip, log_event, sanitize_filename
from dotenv import load_dotenv
from database import insert_into_db
from limit_checker import update_product_count
from io import BytesIO
from openpyxl.drawing.image import Image as XLImage
import httpx
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
        async with session.get(build_high_res_url(image_url), timeout=10) as response:
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

def build_high_res_url(image_url, width="1500"):
    if not image_url or image_url == "N/A":
        return image_url

    # Add scheme if it's a protocol-relative URL (starts with //)
    if image_url.startswith("//"):
        image_url = "https:" + image_url

    # Parse the URL
    parsed = urlparse(image_url)
    query_dict = parse_qs(parsed.query)
    query_dict["width"] = [width]  # Overwrite or insert width

    # Rebuild the URL
    new_query = urlencode(query_dict, doseq=True)
    high_res_url = urlunparse(parsed._replace(query=new_query))

    return high_res_url

async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    high_res_url = build_high_res_url(image_url, width="1500")  # Make sure we're aiming for a high-res image.
    fallback_url = image_url  # In case the high-res image isn't found.

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Try HEAD request to check if high-res image exists
        try:
            head_response = await client.head(high_res_url)
            if head_response.status_code == 200:
                image_to_download = high_res_url  # High-res image found
            else:
                logging.warning(f"High-res image not found for {product_name}, using fallback.")
                image_to_download = fallback_url  # Fallback to original image
        except Exception as e:
            logging.warning(f"Could not check high-res image. Falling back. Reason: {e}")
            image_to_download = fallback_url  # Fallback if HEAD request fails

        # Attempt to download the image
        for attempt in range(retries):
            try:
                response = await client.get(image_to_download)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path  # Return the downloaded image path
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")

    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"


def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))

######################################################################################################

async def safe_goto_and_wait(page, url,isbri_data, retries=2):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            
            if isbri_data:
                await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            else:
                await page.goto(url, wait_until="domcontentloaded", timeout=180_000)

            # Wait for the selector with a longer timeout
            product_cards = await page.wait_for_selector(".snize-horizontal-wrapper", state="attached", timeout=30000)

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

     
async def handle_medleyjewellery(url, max_pages):
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
    headers = [
        "Current Date", "Header", "Product Name", "Image", "Kt", "Price", 
        "Total Dia wt", "Time", "ImagePath", "Additional Info"
    ]
    sheet.append(headers)

    all_records = []
    filename = f"handle_medleyjewellery_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    while page_count <= max_pages:
        current_url = build_url_with_loadmore(url, page_count)

        logging.info(f"Processing page {page_count}: {current_url}")
        try:
            async with async_playwright() as p:
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(3000)
                    current_product_count = await page.locator('li.snize-product[data-original-product-id]').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count

                # Final scroll
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(3000)

                products = await page.query_selector_all("li.snize-product[data-original-product-id]")
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract basic product info
                        product_name_element = await product.query_selector("span.snize-title")
                        product_name = await product_name_element.inner_text() if product_name_element else "N/A"

                        # Extract price information
                        price = "N/A"
                        try:
                            price_element = await product.query_selector("span.snize-price")
                            discounted_price = (await price_element.inner_text()).strip() if price_element else "N/A"
                            
                            original_price_element = await product.query_selector("span.snize-discounted-price")
                            original_price = (await original_price_element.inner_text()).strip() if original_price_element else None
                            
                            price = f"{discounted_price}|{original_price}" if original_price else discounted_price
                        except Exception as price_error:
                            logging.error(f"Error extracting price for product {row_num}: {str(price_error)}")

                        # Extract image URL
                        image_element = await product.query_selector("span.snize-thumbnail img")
                        image_url = await image_element.get_attribute("src") if image_element else "N/A"

                       
                        # Extract metal type
                        metal_element = await product.query_selector("span.snize-custom-swatch-title")
                        kt = await metal_element.inner_text() if metal_element else "N/A"
                        
                        
                        
                        
                        

                        # Extract Diamond Weight
                        diamond_weight = "N/A"
                        try:
                            diamond_weight_match = re.findall(r"(\d+(?:\.\d+)?\s*ct\w*)", product_name, re.IGNORECASE)
                            diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"
                        except re.error as regex_error:
                            logging.error(f"Regex error extracting diamond weight for product {row_num}: {str(regex_error)}")

                        # Extract additional information (without background/color info)
                        additional_info_parts = []

                        # 1. Product labels (like "Final Sale")
                        try:
                            label_element = await product.query_selector("div.snize-product-label")
                            if label_element:
                                label_text = (await label_element.inner_text()).strip()
                                additional_info_parts.append(f"Label:{label_text}")
                        except Exception as e:
                            logging.error(f"Error extracting label for product {row_num}: {str(e)}")

                        # 2. Size information
                        try:
                            size_element = await product.query_selector("div.snize-size-select-box .snize-size-active")
                            if size_element:
                                size_text = (await size_element.inner_text()).strip()
                                additional_info_parts.append(f"Size:{size_text}")
                        except Exception as e:
                            logging.error(f"Error extracting size for product {row_num}: {str(e)}")

                        # 3. Ratings information
                        try:
                            ratings_element = await product.query_selector("span.snize-reviews")
                            if ratings_element:
                                ratings_text = (await ratings_element.inner_text()).strip()
                                additional_info_parts.append(f"Ratings:{ratings_text}")
                        except Exception as e:
                            logging.error(f"Error extracting ratings for product {row_num}: {str(e)}")

                        # Combine all additional info with | separator
                        additional_info = " | ".join(additional_info_parts) if additional_info_parts else "N/A"

                        unique_id = str(uuid.uuid4())
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))
                        
                        product_name = f"{product_name} {kt}"

                        records.append((
                            unique_id, current_date, page_title, product_name, None, 
                            kt, price, diamond_weight, additional_info
                        ))
                        
                        sheet.append([
                            current_date, page_title, product_name, None, kt, 
                            price, diamond_weight, time_only, image_url, additional_info
                        ])

                    except Exception as e:
                        logging.error(f"Error processing entire product {row_num}: {str(e)}")
                        continue

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
                                records[i] = (
                                    record[0], record[1], record[2], record[3], 
                                    image_path, record[5], record[6], record[7], record[8]
                                )
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
