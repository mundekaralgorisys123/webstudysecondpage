import asyncio
import re
import os
import uuid
import logging
import base64
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image
import time
import random
from dotenv import load_dotenv
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import httpx
from playwright.async_api import async_playwright, TimeoutError,Error
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
# from proxysetup import get_browser_with_proxy_strategy
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

def get_high_res_image_url(image_url: str, desired_width: int = 2000) -> str:
    parsed = urlparse(image_url)
    query = parse_qs(parsed.query)

    # Update or insert the width
    query['width'] = [str(desired_width)]
    if 'height' in query:
        query.pop('height')  # Remove height to avoid distortion

    # Reconstruct the URL with new query params
    new_query = urlencode(query, doseq=True)
    new_url = urlunparse(parsed._replace(query=new_query))
    return new_url


async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A":
        return "N/A"
    
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    
    # Modify the URL to fetch high-resolution image if available
    modified_url = get_high_res_image_url(image_url)
    
    for attempt in range(3):
        try:
            # Download the image
            resp = await session.get(modified_url, timeout=10)
            resp.raise_for_status()
            with open(image_full_path, "wb") as f:
                f.write(resp.content)
            return image_full_path
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/3 - Error downloading {product_name}: {e}")
    
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
            product_cards = await page.wait_for_selector(".products-outer-wrapper", state="attached", timeout=30000)

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




async def handle_jcojewellery(url, max_pages):
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

 
    records = []
    image_tasks = []

    async with httpx.AsyncClient() as session:
        load_more_clicks = 1
        previous_count = 0

        while load_more_clicks <= max_pages:
            async with async_playwright() as p:
                # Create a new browser instance for each page
                # product_wrapper_selector = "div.products-outer-wrapper"
                browser , page = await get_browser_with_proxy_strategy(p, url)

               

                # Simulate clicking 'Load More' number of times
                for _ in range(load_more_clicks - 1):
                    try:
                        # More precise selection of the "Load More" button
                        load_more_button = page.get_by_role("button", name="Load More")

                        if await load_more_button.is_visible():
                            await load_more_button.click()
                            await page.wait_for_timeout(2000)
                        else:
                            break
                    except Exception as e:
                        logging.warning(f"Could not click 'Load More': {e}")
                        break

                # Wait for the product wrapper that contains all products
                try:
                    product_wrapper_selector = "div.products-outer-wrapper"
                    product_wrapper = await page.wait_for_selector(product_wrapper_selector, timeout=30000)
                except Exception as e:
                    logging.warning(f"Product wrapper not found on {url}: {e}")
                    await browser.close()
                    continue

                # Select all product cards inside the wrapper
                all_products = await product_wrapper.query_selector_all("div.product-card-wrapper")

                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    additional_info = []

                    try:
                        product_name_element = await product.query_selector("a.p4.width-100.regular-400.color-url")
                        product_name = await product_name_element.inner_text() if product_name_element else "N/A"
                    except:
                        product_name = "N/A"

                    price_parts = []
                    try:
                        price_element = await product.query_selector("div.price-container span.p5")
                        if price_element:
                            price_parts.append(await price_element.inner_text())
                        original_price_element = await product.query_selector("div.price-container span.money.sale-price") # Adjust selector if needed
                        if original_price_element:
                            price_parts.append(await original_price_element.inner_text())
                    except:
                        pass
                    price = "|".join(price_parts) if price_parts else "N/A"

                    try:
                        # Use a broader query selector to find the image
                        image_element = await product.query_selector("img")

                        image_url = None
                        if image_element:
                            # Try srcset/data-srcset first for high-res images
                            image_url = await image_element.get_attribute("data-srcset")
                            if not image_url:
                                image_url = await image_element.get_attribute("srcset")

                            # Fallback to data-src/src
                            if not image_url:
                                image_url = await image_element.get_attribute("data-src")
                            if not image_url:
                                image_url = await image_element.get_attribute("src")

                            # Parse srcset to get the highest resolution image
                            if image_url and " " in image_url:
                                image_url = image_url.split(",")[-1].split()[0]

                            # Ensure URL has https prefix
                            if image_url and image_url.startswith("//"):
                                image_url = "https:" + image_url

                        image_url = image_url if image_url else "N/A"

                    except Exception as e:
                        print(f"Error extracting product image URL: {e}")
                        image_url = "N/A"

                    gold_type_match = re.search(r"\b\d{1,2}K(?:\s+\w+){0,3}\s+Gold\b", product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    diamond_weight_match = re.search(r"(\d+(\.\d+)?)\s*(ct|carat)", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_weight_match.group(1)} ct" if diamond_weight_match else "N/A"

                    # Extract additional info
                    try:
                        # Check for color variants
                        color_swatches = await product.query_selector_all("div.color-variant-picker div.swatch-wrapper div[data-value]")
                        colors = [await swatch.get_attribute("data-value") for swatch in color_swatches]
                        if colors:
                            additional_info.append(f"Available Colors: {', '.join(colors)}")
                    except:
                        pass

                    try:
                        # Check for any labels or tags (e.g., "best seller")
                        tags_container = await product.query_selector("div.pro-tags-container")
                        if tags_container:
                            tags_elements = await tags_container.query_selector_all("div.tag-each")
                            tags = [await tag.inner_text() for tag in tags_elements]
                            if tags:
                                additional_info.append(f"Labels: {', '.join(tags)}")
                    except:
                        pass

                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(download_image(session, image_url, product_name, timestamp, image_folder, unique_id))
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))

                    additional_info_str = "|".join(additional_info)
                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight, additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, additional_info_str])

                # Process image downloads and attach them to Excel
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        try:
                            img = Image(image_path)
                            # Adjust image size as needed
                            img.width, img.height = 100, 100
                            sheet.add_image(img, f"D{row}")
                        except Exception as e:
                            logging.warning(f"Error adding image to Excel at row {row}: {e}")
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            updated_record = list(record)
                            updated_record[4] = image_path
                            records[i] = tuple(updated_record)
                            break

                await browser.close()
            load_more_clicks += 1

        # Save Excel
        filename = f'handle_jcojewellery_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
        file_path = os.path.join(EXCEL_DATA_PATH, filename)
        wb.save(file_path)
        log_event(f"Data saved to {file_path} | IP: {ip_address}")

        if records:
            # Prepare records for database insertion (including the additional info)
            db_records = [(r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]) for r in records]
            insert_into_db(db_records)
        else:
            logging.info("No data to insert into the database.")

        update_product_count(len(records))

        with open(file_path, "rb") as f:
            base64_encoded = base64.b64encode(f.read()).decode("utf-8")

        return base64_encoded, filename, file_path
