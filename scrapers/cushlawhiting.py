import asyncio
import re
import os
import uuid
import logging
import base64
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image
# from proxysetup import get_browser_with_proxy_strategy
from dotenv import load_dotenv
from utils import get_public_ip, log_event
from database import insert_into_db
from limit_checker import update_product_count
import httpx
from playwright.async_api import async_playwright, TimeoutError,Error
import traceback
from typing import List
import time
import random
# Load .env variables
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")


PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")



BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A" or image_url.startswith('data:image'):
        return "N/A"
    
    # Ensure proper URL format
    if image_url.startswith("//"):
        image_url = "https:" + image_url
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    
    for attempt in range(3):
        try:
            resp = await session.get(image_url, timeout=10)
            resp.raise_for_status()
            
            # Check if we got a real image (not a placeholder)
            content_type = resp.headers.get('content-type', '')
            if content_type.startswith('image/') and not content_type.startswith('image/gif'):
                with open(image_full_path, "wb") as f:
                    f.write(resp.content)
                return image_full_path
            else:
                logging.warning(f"Invalid image content type: {content_type}")
                return "N/A"
                
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/3 - Error downloading {product_name}: {e}")
            if attempt < 2:  # Don't sleep on last attempt
                await asyncio.sleep(1)
    
    logging.error(f"Failed to download {product_name} after 3 attempts.")
    return "N/A"

########################################  safe_goto_and_wait ####################################################################
def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))

async def safe_goto_and_wait(page, url,isbri_data, retries=2):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            
            if isbri_data:
                await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            else:
                await page.goto(url, wait_until="domcontentloaded", timeout=180_000)

            # Wait for the selector with a longer timeout
            product_cards = await page.wait_for_selector(".hide-page-dots", state="attached", timeout=30000)

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





async def handle_cushlawhiting(url_page, max_pages):
    ip_address = get_public_ip()
    logging.info(f"Starting scrape for {url_page} from IP: {ip_address}")  # Changed url to url_page

    if not os.path.exists(EXCEL_DATA_PATH):
        os.makedirs(EXCEL_DATA_PATH)

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
        current_page = 1
        all_products = []
        has_more_products = True

        while current_page <= max_pages and has_more_products:
            async with async_playwright() as p:
                # product_wrapper = '.hide-page-dots'
                browser, page = await get_browser_with_proxy_strategy(p, url_page)

                # Only click "Load More" after the first page
                # if current_page > 1:
                #     try:
                #         await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                #         await asyncio.sleep(1)

                #         button = await page.query_selector("button.load-more-btn.button-outlined")
                #         if button and await button.is_visible():
                #             await button.scroll_into_view_if_needed()
                #             await asyncio.sleep(0.5)
                #             await button.click()
                #             # Wait for new products to load
                #             try:
                #                 await page.wait_for_function(
                #                     "document.querySelectorAll('.grid__item').length > old_count",
                #                     timeout=10000,
                #                     poll=500
                #                 )
                #             except TimeoutError:
                #                 logging.info("No new products loaded after clicking 'Load More'")
                #                 has_more_products = False
                #         else:
                #             logging.info("No more 'Load More' button found")
                #             has_more_products = False
                #     except Exception as e:
                #         logging.warning(f"Error clicking 'Load More': {e}")
                #         has_more_products = False
                
                for _ in range(current_page - 1):
                    try:
                        # first try by ID, otherwise by the class name in your HTML
                        load_more_btn = await page.query_selector("#view-more-product")
                        if not load_more_btn:
                            load_more_btn = await page.query_selector("button.loadload-more-btn.button-outlined")

                        # if we found it and it’s visible, scroll, click, and wait a bit
                        if load_more_btn and await load_more_btn.is_visible():
                            await load_more_btn.scroll_into_view_if_needed()
                            await load_more_btn.click()
                            # give the next products a chance to render
                            await asyncio.sleep(2)
                        else:
                            # no more pages (or button not on this page)
                            break

                    except Exception as e:
                        logging.warning(f"Could not click 'View More Products': {e}")
                        break

                # Get all current products
                current_products = await page.query_selector_all("div.card-wrapper")
                logging.info(f"Page {current_page}: Found {len(current_products)} products")

                # Only process products we haven't seen before
                new_products = current_products[len(all_products):]
                all_products.extend(new_products)

                print(f"Page {current_page}: Scraping {len(new_products)} new products")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    print(f"Processing product {idx + 1}/{len(new_products)}")
                    try:
                        product_name_tag = await product.query_selector("span.card-information__text")
                        product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"
                    except Exception as e:
                        print(f"[Product Name] Error: {e}")
                        product_name = "N/A"

                    try:
                        price_tag = await product.query_selector("span.price-item--regular")
                        price = await price_tag.inner_text() if price_tag else "N/A"
                    except Exception as e:
                        print(f"[Price] Error: {e}")
                        price = "N/A"
                    image_url = "N/A"
                    try:
                        # Select the visible image container
                        media_container = await product.query_selector('div.card__inner')
                        if media_container:
                            # Get all images in the container
                            images = await media_container.query_selector_all('img')
                            
                            # Find the first visible image (not hidden)
                            visible_img = None
                            for img in images:
                                class_list = await img.get_attribute('class') or ''
                                if 'hide-image' not in class_list and 'motion-reduce' in class_list:
                                    visible_img = img
                                    break
                            
                            if visible_img:
                                # First try to get the highest resolution from data-srcset
                                data_srcset = await visible_img.get_attribute('data-srcset')
                                if data_srcset:
                                    # Extract all available sizes and pick the largest one
                                    srcset_parts = [part.strip() for part in data_srcset.split(",")]
                                    largest_url = ""
                                    largest_size = 0
                                    for part in srcset_parts:
                                        if not part:
                                            continue
                                        try:
                                            url, size = part.rsplit(" ", 1)  # Split on last space
                                            size = int(size.replace("w", ""))
                                            if size > largest_size:
                                                largest_size = size
                                                largest_url = url
                                        except Exception as e:
                                            logging.warning(f"Error parsing srcset part: {part} - {e}")
                                    
                                    if largest_url:
                                        image_url = largest_url
                                    else:
                                        # Fallback to data-src if available
                                        image_url = await visible_img.get_attribute('data-src') or await visible_img.get_attribute('src')
                                else:
                                    # No srcset, try regular attributes
                                    image_url = await visible_img.get_attribute('data-src') or await visible_img.get_attribute('src')
                                
                                # Ensure we have a proper URL
                                if image_url and image_url.startswith('//'):
                                    image_url = 'https:' + image_url
                                elif image_url and image_url.startswith('data:image'):
                                    image_url = "N/A"
                            else:
                                image_url = "N/A"
                        else:
                            image_url = "N/A"

                    except Exception as e:
                        print(f"[Image URL] Error: {e}")
                        image_url = "N/A"

                    gold_match = re.search(r"\b(?:\d{1,2}\s+KARAT\s+(?:Yellow|White|Rose)\s+Gold|Sterling Silver|Platinum|Cubic Zirconia)\b",
                        product_name,
                        flags=re.IGNORECASE
                    )
                    kt = gold_match.group(0) if gold_match else "N/A"

                    # Diamond weight: catch any "<number>.<digits>CT" or "<number>.<digits>CTW"
                    weights = re.findall(
                        r"(\d+(?:\.\d+)?\s*ctw?)",
                        product_name,
                        flags=re.IGNORECASE
                    )
                    # normalize to uppercase with no extra spaces
                    diamond_weight = ", ".join(w.upper().replace(" ", "") for w in weights) if weights else "N/A"
                    additional_info_str = "N/A"
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
            current_page += 1


        # Save Excel
        filename = f'handle_cushlawhiting_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
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
