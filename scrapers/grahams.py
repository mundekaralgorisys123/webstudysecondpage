import asyncio
import re
import os
import time
import random
import uuid
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

async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A":
        return "N/A"
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    for attempt in range(3):
        try:
            resp = await session.get(image_url, timeout=10)
            resp.raise_for_status()
            with open(image_full_path, "wb") as f:
                f.write(resp.content)
            return image_full_path
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/3 - Error downloading {product_name}: {e}")
    logging.error(f"Failed to download {product_name} after 3 attempts.")
    return "N/A"

def modify_image_url(image_url):
    """Try to modify Shopify-style image URLs to use high resolution versions."""
    if not image_url or image_url == "N/A":
        return image_url

    # Extract and preserve query parameters
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"

    # Try common low-res suffix patterns and replace them
    replacements = [
        (r'(_\d+x\d+)(_crop_center)?(?=\.\w+$)', '_1220x1220_crop_center'),  # e.g., _600x600 or _600x600_crop_center
        (r'_260(?=\.\w+$)', '_1200'),  # specific pattern like _260
    ]

    modified_url = image_url
    for pattern, replacement in replacements:
        modified_url = re.sub(pattern, replacement, modified_url)

    return modified_url + query_params


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
            product_cards = await page.wait_for_selector(".ProductGridContainer", state="attached", timeout=30000)

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

async def handle_grahams(url, max_pages):
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]  # Moved Additional Info to last
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
                browser, page = await get_browser_with_proxy_strategy(p, url)
                log_event(f"Successfully loaded: {url}")

                # Simulate clicking 'Load More' number of times
                for i in range(load_more_clicks):
                    try:
                        # Scroll to bottom of the page to trigger lazy load
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1)

                        # Select and wait for the "Load More" button
                        button = await page.query_selector("button.load-more")
                        if button and await button.is_visible():
                            await button.scroll_into_view_if_needed()
                            await asyncio.sleep(0.5)
                            await button.click()
                            print(f"[Load More] Clicked load more button ({i + 1}/{load_more_clicks - 1})")

                            # Wait for more products to load
                            await page.wait_for_timeout(1500)

                            # Optional: Wait for new products to appear (ensures no stale state)
                            await page.wait_for_selector("li.column.ss__result--item", timeout=5000)

                        else:
                            print("[Load More] Button not found or not visible.")
                            break

                    except Exception as e:
                        print(f"[Load More Error] {e}")
                        break

                all_products = await page.query_selector_all("li.column.ss__result.ss__result--item")

                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count += len(new_products)

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    additional_info = []
                    
                    try:
                        name_tag = await product.query_selector("a.product-card-title")
                        product_name = await name_tag.inner_text() if name_tag else "N/A"
                    except Exception as e:
                        print(f"[Product Name] Error: {e}")
                        product_name = "N/A"

                    # Handle price (original and discounted)
                    price = "N/A"
                    try:
                        price_tag = await product.query_selector("span.price")
                        if price_tag:
                            original_price_tag = await price_tag.query_selector("del span.amount")
                            discounted_price_tag = await price_tag.query_selector("ins span.amount")
                            
                            original_price = await original_price_tag.inner_text() if original_price_tag else None
                            discounted_price = await discounted_price_tag.inner_text() if discounted_price_tag else None
                            
                            if original_price and discounted_price:
                                price = f"{original_price}|{discounted_price}"
                                additional_info.append(f"Discount: {original_price} → {discounted_price}")
                            elif discounted_price:
                                price = discounted_price
                            else:
                                price = await price_tag.inner_text()
                    except Exception as e:
                        print(f"[Price] Error: {e}")
                        price = "N/A"

                    
                    # Handle discount badges
                    try:
                        discount_badges = await product.query_selector_all(".badge.onsale")
                        for badge in discount_badges:
                            badge_text = await badge.inner_text()
                            if badge_text and "SAVE" in badge_text:
                                additional_info.append(f"Badge: {badge_text}")
                    except Exception as e:
                        print(f"[Discount Badge] Error: {e}")

                    # Handle product availability/stock status
                    try:
                        stock_status_tag = await product.query_selector(".stock-status")
                        if stock_status_tag:
                            stock_status = await stock_status_tag.inner_text()
                            additional_info.append(f"Stock: {stock_status}")
                    except Exception as e:
                        print(f"[Stock Status] Error: {e}")

                    # Handle color options if available
                    try:
                        color_options = await product.query_selector_all(".color-swatch")
                        if color_options:
                            colors = [await color.get_attribute("title") or await color.get_attribute("alt") for color in color_options]
                            colors = [c for c in colors if c]
                            if colors:
                                additional_info.append(f"Colors: {', '.join(colors)}")
                    except Exception as e:
                        print(f"[Color Options] Error: {e}")

                    # Handle any other product labels
                    try:
                        labels = await product.query_selector_all(".product-label")
                        if labels:
                            label_texts = [await label.inner_text() for label in labels]
                            additional_info.extend([f"Label: {text}" for text in label_texts if text])
                    except Exception as e:
                        print(f"[Product Labels] Error: {e}")

                    # Handle product rating if available
                    try:
                        rating_tag = await product.query_selector(".product-rating")
                        if rating_tag:
                            rating = await rating_tag.get_attribute("data-rating") or await rating_tag.inner_text()
                            additional_info.append(f"Rating: {rating}")
                    except Exception as e:
                        print(f"[Product Rating] Error: {e}")

                    # Handle image
                    try:
                        img_tag = await product.query_selector(".product-primary-image")
                        image_url = await img_tag.get_attribute("src") if img_tag else None

                        if not image_url:
                            img_tag = await product.query_selector(".product-secondary-image")
                            image_url = await img_tag.get_attribute("src") if img_tag else "N/A"

                        if image_url and image_url.startswith("//"):
                            image_url = "https:" + image_url
                        elif not image_url:
                            image_url = "N/A"
                    except Exception as e:
                        print(f"[Image URL] Error: {e}")
                        image_url = "N/A"

                    image_url = modify_image_url(image_url)
                    
                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue


                    # Extract Gold Type
                    gold_type_match = re.findall(r"(\d{1,2}ct\s*(?:Yellow|White|Rose)?\s*Gold|Platinum|Cubic Zirconia)", product_name, re.IGNORECASE)
                    kt = ", ".join(gold_type_match) if gold_type_match else "N/A"

                    # Extract Diamond Weight
                    diamond_weight_match = re.findall(r"(\d+(?:\.\d+)?\s*ct)", product_name, re.IGNORECASE)
                    diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"

                    # Join additional info with pipe delimiter
                    additional_info_text = " | ".join(additional_info) if additional_info else "N/A"

                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(download_image(session, image_url, product_name, timestamp, image_folder, unique_id))
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))

                    # Reordered to have Additional Info last
                    records.append((
                        unique_id, 
                        current_date, 
                        page_title, 
                        product_name, 
                        None, 
                        kt, 
                        price, 
                        diamond_weight,
                        time_only,
                        image_url,
                        additional_info_text  # Now last
                    ))
                    
                    sheet.append([
                        current_date, 
                        page_title, 
                        product_name, 
                        None, 
                        kt, 
                        price, 
                        diamond_weight,
                        time_only,
                        image_url,
                        additional_info_text  # Now last
                    ])

                # Process image downloads and attach them to Excel
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        img = Image(image_path)
                        img.width, img.height = 100, 100
                        sheet.add_image(img, f"D{row}")
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
                                record[8],
                                record[9],
                                record[10]  # Additional Info remains last
                            )
                            break

                await browser.close()
            load_more_clicks += 1

        # Save Excel

        if not all_products:
            return None, None, None
        
        filename = f'handle_grahams_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
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
