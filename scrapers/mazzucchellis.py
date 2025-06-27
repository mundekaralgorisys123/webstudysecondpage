import asyncio
import re
import os
import uuid
import time
import random
import logging
import base64
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from dotenv import load_dotenv
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import httpx
from playwright.async_api import async_playwright, TimeoutError,Error
import traceback
from typing import List, Tuple
# Load .env variables
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")
PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

def modify_image_url(image_url):
    if not image_url or image_url == "N/A":
        return image_url
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"
    modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)
    return modified_url + query_params

async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A":
        return "N/A"
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    modified_url = modify_image_url(image_url)
    for attempt in range(3):
        try:
            resp = await session.get(modified_url, timeout=10)
            resp.raise_for_status()
            with open(image_full_path, "wb") as f:
                f.write(resp.content)
            return image_full_path
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/3 - Error downloading {product_name}: {e}")
    logging.error(f"Failed to download {product_name} after 3 attempts.")
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
            product_cards = await page.wait_for_selector(".nn-collection-main", state="attached", timeout=30000)

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



async def handle_mazzucchellis(url, max_pages):
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
        browser = None
        page = None

        while load_more_clicks <= max_pages:
            async with async_playwright() as p:
                # Create a new browser instance for each page
                browser, page = await get_browser_with_proxy_strategy(p, url)
                log_event(f"Successfully loaded: {url}")

                # Simulate clicking 'Load More' number of times
                load_more_selector = "button.btn.btn-outline-primary.btn-filter-load-more.btn-loader"

                while True:
                    try:
                        load_more_button = page.locator(load_more_selector)
                        
                        # Wait a bit for the button to become visible (optional safeguard)
                        await page.wait_for_timeout(1000)

                        if await load_more_button.is_visible():
                            prev_count = await page.locator("div.product-card").count()
                            await load_more_button.click()
                            logging.info("Clicked 'Load More' button.")
                            
                            # Wait for new products to load by checking count change
                            for _ in range(10):
                                await asyncio.sleep(1.5)
                                current_count = await page.locator("div.product-card").count()
                                if current_count > prev_count:
                                    logging.info(f"New products loaded: {current_count - prev_count}")
                                    break
                            else:
                                logging.warning("No new products loaded after clicking 'Load More'. Breaking.")
                                break
                        else:
                            logging.info("'Load More' button not visible anymore. Done loading.")
                            break

                    except Exception as e:
                        logging.warning(f"Could not click 'Load More': {e}")
                        break

                all_products = await page.query_selector_all(".nn-product-preview")
                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products
                
                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    additional_info = []
                    
                    try:
                        product_name_tag = await product.query_selector('h3 a')
                        product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"
                    except:
                        product_name = "N/A"

                    # Handle prices - check for both current and original prices
                    price = "N/A"
                    try:
                        price_tag = await product.query_selector('.nn-price-current')
                        if price_tag:
                            price = await price_tag.inner_text()
                            
                            # Check for original price (discounted items)
                            original_price_tag = await product.query_selector('.nn-price-original')
                            if original_price_tag:
                                original_price = await original_price_tag.inner_text()
                                price = f"{original_price} | {price}"
                                additional_info.append(f"Discount available")
                    except Exception as e:
                        print(f"Error getting price: {e}")
                        price = "N/A"

                    try:
                        image_tag = await product.query_selector("div.nn-product-preview-image img")
                        image_url = await image_tag.get_attribute("src") if image_tag else "N/A"

                        # Normalize protocol-relative URLs
                        if image_url and image_url.startswith("//"):
                            image_url = "https:" + image_url

                    except Exception as e:
                        print(f"Error getting image URL: {e}")
                        image_url = "N/A"

                    # Get product tags/labels
                    try:
                        tag_elements = await product.query_selector_all('.nn-product-preview-tag-item')
                        if tag_elements:
                            tags = [await tag.inner_text() for tag in tag_elements]
                            additional_info.append(f"Tags: {' | '.join(tags)}")
                    except Exception as e:
                        print(f"Error getting tags: {e}")

                    # Get availability information
                    try:
                        availability_tag = await product.query_selector('.stock-info, .availability')
                        if availability_tag:
                            availability = await availability_tag.inner_text()
                            additional_info.append(f"Availability: {availability.strip()}")
                    except Exception as e:
                        print(f"Error getting availability: {e}")

                    # Get product description if available
                    try:
                        desc_tag = await product.query_selector('.product-description, .short-description')
                        if desc_tag:
                            description = await desc_tag.inner_text()
                            if description.strip():
                                additional_info.append(f"Description: {description.strip()}")
                    except Exception as e:
                        print(f"Error getting description: {e}")

                    # Get product options (colors, sizes) if available
                    try:
                        options_tags = await product.query_selector_all('.product-option, .color-swatch')
                        if options_tags:
                            options = []
                            for option in options_tags:
                                option_text = await option.inner_text()
                                if option_text.strip():
                                    options.append(option_text.strip())
                            if options:
                                additional_info.append(f"Options: {' | '.join(options)}")
                    except Exception as e:
                        print(f"Error getting options: {e}")

                    # Get any other notable product features
                    try:
                        feature_tags = await product.query_selector_all('.product-feature, .spec-item')
                        if feature_tags:
                            features = []
                            for feature in feature_tags:
                                feature_text = await feature.inner_text()
                                if feature_text.strip():
                                    features.append(feature_text.strip())
                            if features:
                                additional_info.append(f"Features: {' | '.join(features)}")
                    except Exception as e:
                        print(f"Error getting features: {e}")

                    # Join all additional info with pipe delimiter
                    additional_info_text = " | ".join(additional_info) if additional_info else ""

                    kt_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                    kt = kt_match.group() if kt_match else "Not found"

                    diamond_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_match.group(1)} ct" if diamond_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(download_image(session, image_url, product_name, timestamp, image_folder, unique_id))
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))

                    # Updated record structure with Additional Info at the end
                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, additional_info_text])

                # Process image downloads and attach them to Excel
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        img = Image(image_path)
                        img.width, img.height = 100, 100
                        sheet.add_image(img, f"D{row}")
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8], record[9], record[10])
                            break

                await browser.close()
            load_more_clicks += 1

        # Save Excel
        filename = f'handle_mazzucchellis_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
        file_path = os.path.join(EXCEL_DATA_PATH, filename)
        wb.save(file_path)
        log_event(f"Data saved to {file_path} | IP: {ip_address}")

        if records:
            insert_into_db(records)
        else:
            logging.info("No data to insert into the database.")

        update_product_count(len(records))

        with open(file_path, "rb") as f:
            base64_encoded = base64.b64encode(f.read()).decode("utf-8")

        return base64_encoded, filename, file_path
