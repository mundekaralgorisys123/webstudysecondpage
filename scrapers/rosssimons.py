import os
import time
import logging
import aiohttp
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright, Page, TimeoutError, Error
from openpyxl import Workbook
from openpyxl.drawing.image import Image
import uuid
import base64
from dotenv import load_dotenv
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db, create_table
from limit_checker import update_product_count
import random
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import httpx
# Load environment variables
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

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def modify_image_url(image_url):
    """Update Helzberg image URL to use high resolution (800x800)."""
    if not image_url or image_url == "N/A":
        return image_url

    # Parse the URL
    parsed_url = urlparse(image_url)
    query = parse_qs(parsed_url.query)

    # Modify or add resolution parameters
    query["sw"] = ["800"]
    query["sh"] = ["800"]
    query["sm"] = ["fit"]

    # Rebuild the URL with updated query
    new_query = urlencode(query, doseq=True)
    high_res_url = urlunparse(parsed_url._replace(query=new_query))

    return high_res_url

async def download_image_with_httpx(session, image_url, product_name, timestamp, image_folder):
    if image_url == "N/A":
        return "N/A"
    
    image_filename = f"{sanitize_filename(product_name)}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    
    modified_url = modify_image_url(image_url)

    try:
        response = await session.get(modified_url)
        if response.status_code == 200:
            with open(image_full_path, 'wb') as f:
                f.write(response.content)
            return image_full_path
        else:
            logging.warning(f"Failed to download image {image_url}: HTTP {response.status_code}")
            return "N/A"
    except Exception as e:
        logging.error(f"Error downloading image {image_url}: {e}")
        return "N/A"


# async def download_image(session, image_url, product_name, timestamp, image_folder, retries=3):
#     """Download image with retries and return its local path."""
#     if not image_url or image_url == "N/A":
#         return "N/A"

#     image_filename = f"{sanitize_filename(product_name)}_{timestamp}.jpg"
#     image_full_path = os.path.join(image_folder, image_filename)
    
#     modified_url = modify_image_url(image_url)

#     for attempt in range(retries):
#         try:
#             async with session.get(modified_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
#                 response.raise_for_status()
#                 with open(image_full_path, "wb") as f:
#                     f.write(await response.read())
#                 return image_full_path
#         except Exception as e:
#             logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
#             await asyncio.sleep(1)  # Add small delay between retries

#     logging.error(f"Failed to download {product_name} after {retries} attempts.")
#     return "N/A"

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))

async def scroll_and_wait(page: Page, delay: float = 2.0) -> bool:
    """
    Scrolls down and waits to see if new content loads (based on page height change).

    Returns:
        bool: True if new content was loaded (height increased), False otherwise.
    """
    previous_height = await page.evaluate("() => document.body.scrollHeight")
    await page.evaluate("() => window.scrollBy(0, document.body.scrollHeight)")
    await asyncio.sleep(delay)
    new_height = await page.evaluate("() => document.body.scrollHeight")
    return new_height > previous_height


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
            product_cards = await page.wait_for_selector(".breadcrumb-wrapper, #maincontent, body", state="attached", timeout=30_000)


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
                        '--disable-web-security',
                        '--no-sandbox',
                        '--disable-dev-shm-usage'
                    ]
                )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                locale="en-US",
            )

            # Stealth: Hide navigator.webdriver
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
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


async def handle_rosssimons(url, max_pages):
    """Scrape product data from Ross Simons website using fresh browser instances for each page."""
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} | IP: {ip_address} | Max pages: {max_pages}")

    # Prepare folders
    if not os.path.exists(EXCEL_DATA_PATH):
        os.makedirs(EXCEL_DATA_PATH)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    # Prepare Excel
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    current_date = datetime.now().strftime("%Y-%m-%d")
    time_only = datetime.now().strftime("%H.%M")

    # Collect all data across pages
    all_records = []
    row_counter = 2

    current_url = url
    pages_processed = 0

    while current_url and pages_processed < max_pages:
        browser = None
        page = None
        try:
            # Create fresh browser instance for each page
            async with async_playwright() as p:
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")

                # # Scroll to load lazy-loaded content
                # scroll_attempts = 0
                # max_scroll_attempts = 5
                # while scroll_attempts < max_scroll_attempts and await scroll_and_wait(page):
                #     scroll_attempts += 1
                #     await random_delay(1, 3)

                # Get page title
                page_title = await page.title()

                # Extract products
                product_wrapper = await page.query_selector("div.row.product-grid")
                products = await product_wrapper.query_selector_all("div.product-tile") if product_wrapper else []
                logging.info(f"Found {len(products)} products on page {pages_processed}")

                # Use httpx.AsyncClient for downloading images
                async with httpx.AsyncClient() as session:
                    for product in products:
                        # Extract product name
                        product_name_tag = await product.query_selector('.product-tile-name .pdp-link a')
                        product_name = (await product_name_tag.text_content()).strip() if product_name_tag else "N/A"

                        # Extract price
                        try:
                            price_tag = await product.query_selector('.sales .z-price')
                            price_from = (await price_tag.text_content()).strip() if price_tag else "N/A"
                            compare_tag = await product.query_selector('.compare-at .z-price')
                            compare_at = (await compare_tag.text_content()).strip() if compare_tag else price_from
                            percent_saved_tag = await product.query_selector('.percent-saved')
                            percent_saved = (await percent_saved_tag.text_content()).strip() if percent_saved_tag else "N/A"
                            if percent_saved != "N/A":
                                price = f"From {price_from} Compare at {compare_at} Save {percent_saved}"
                            else:
                                price = f"Price: {price_from}"
                        except Exception as e:
                            price = "N/A"

                        # Extract image URL
                        image_tag = await product.query_selector('picture img')
                        image_url = await image_tag.get_attribute('src') if image_tag else "N/A"

                        additional_info = []
                        try:
                            promo_div = await product.query_selector('div.promotion span[style*="color:#c0392b"][style*="font-size:12px"]')
                            if promo_div:
                                promo_text = await promo_div.inner_text()
                                cleaned_text = promo_text.replace("\n", " ").replace("\r", "").strip()
                                additional_info.append(cleaned_text)
                            else:
                                additional_info.append("N/A")
                        except Exception as e:
                            logging.debug(f"Error processing promo banner: {str(e)}")
                            additional_info.append("N/A")

                        # Additional info extraction (tags and badges)
                        try:
                            tag_els = await product.query_selector_all("span.product-tag.groupby-tablet-product-tags")
                            if tag_els:
                                for tag_el in tag_els:
                                    tag_text = await tag_el.inner_text()
                                    if tag_text and tag_text.strip():
                                        additional_info.append(tag_text.strip())
                            else:
                                additional_info.append("N/A")
                        except Exception as e:
                            additional_info.append("N/A")

                        try:
                            badge_els = await product.query_selector_all("span.badge-product")
                            if badge_els:
                                for badge_el in badge_els:
                                    badge_text = await badge_el.inner_text()
                                    if badge_text and badge_text.strip():
                                        additional_info.append(badge_text.strip())
                            else:
                                additional_info.append("N/A")
                        except Exception as e:
                            additional_info.append("N/A")

                        additional_info_str = " | ".join(additional_info)

                        if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                            print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                            continue

                        # Extract metal type
                        gold_type_match = re.findall(r"(\d{1,2}K[t]?\s*(?:Yellow|White|Rose)?\s*Gold)", product_name, re.IGNORECASE)
                        kt = ", ".join(gold_type_match) if gold_type_match else "N/A"

                        # Extract Diamond Weight
                        diamond_weight_match = re.findall(r"(\d+(?:[-/]\d+)?(?:\.\d+)?\s*ct\.?\s*t\.?w\.?)", product_name, re.IGNORECASE)
                        diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"

                        # Download image using httpx
                        image_path = await download_image_with_httpx(session, image_url, product_name, timestamp, image_folder)

                        unique_id = str(uuid.uuid4())
                        all_records.append((unique_id, current_date, page_title, product_name, image_path, kt, price, diamond_weight, additional_info_str))

                        # Add to Excel
                        sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, additional_info_str])

                        # Add image to Excel if downloaded successfully
                        if image_path != "N/A":
                            try:
                                img = Image(image_path)
                                img.width, img.height = 100, 100
                                sheet.add_image(img, f"D{row_counter}")
                            except Exception as e:
                                logging.error(f"Error adding image to Excel: {e}")

                        row_counter += 1

                    # Check for the "More Results" button and extract the next URL
                    show_more_div = await page.query_selector('div.show-more')
                    if show_more_div:
                        more_button = await show_more_div.query_selector('button.more')
                        if more_button:
                            current_url = await more_button.get_attribute('data-url')
                            logging.info(f"Found next page URL: {current_url}")
                            # Navigate to the new page
                            await page.goto(current_url)
                        else:
                            current_url = None  # No next page, stop scraping
                    else:
                        current_url = None  # No "More Results" button, stop scraping

                # Close browser properly
                if browser:
                    await browser.close()
                    logging.debug(f"Closed browser for page {pages_processed + 1}")

                pages_processed += 1

        except Exception as e:
            logging.error(f"Error processing page {pages_processed}: {e}")
            if 'browser' in locals():
                await browser.close()
            break

    # Save Excel file
    filename = f"rosssimons_{current_date}_{time_only}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

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
