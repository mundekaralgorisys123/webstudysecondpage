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
from flask import Flask
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
# from proxysetup import get_browser_with_proxy_strategy
import traceback
from typing import List, Tuple
# Load environment
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")


PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")


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

# Human-like delay
def random_delay(min_sec=1, max_sec=3):
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
            product_cards = await page.wait_for_selector(".mixed-grid", state="attached", timeout=30000)

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
    Always use Oxylabs proxy (ignore robots.txt)
    """
    parsed_url = httpx.URL(url)
    # Oxylabs proxy config (replace with your actual Oxylabs proxy details)
    proxy_config = {
        "server": PROXY_SERVER,
        "username": PROXY_USERNAME,
        "password": PROXY_PASSWORD
    }

    try:
        logging.info("Using Oxylabs proxy for all requests")

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

        await safe_goto_and_wait(page, url, isbri_data=False)
        return browser, page

    except Exception as e:
        error_trace = traceback.format_exc()
        logging.error(f"Failed to launch browser with Oxylabs proxy:\n{error_trace}")
        raise RuntimeError(f"Oxylabs proxy failed for {url}: {e}")
      

# async def get_browser_with_proxy_strategy(p, url: str):
#     """
#     Dynamically checks robots.txt and selects proxy accordingly
#     Always uses proxies - never scrapes directly
#     """
#     parsed_url = httpx.URL(url)
#     base_url = f"{parsed_url.scheme}://{parsed_url.host}"
    
#     # 1. Fetch and parse robots.txt
#     disallowed_patterns = await get_robots_txt_rules(base_url)
    
#     # 2. Check if URL matches any disallowed pattern
#     is_disallowed = check_url_against_rules(str(parsed_url), disallowed_patterns)
    
#     # 3. Try proxies in order (bri-data first if allowed, oxylabs if disallowed)
#     proxies_to_try = [
#         PROXY_URL if not is_disallowed else {
#             "server": PROXY_SERVER,
#             "username": PROXY_USERNAME,
#             "password": PROXY_PASSWORD
#         },
#         {  # Fallback to the other proxy
#             "server": PROXY_SERVER,
#             "username": PROXY_USERNAME,
#             "password": PROXY_PASSWORD
#         } if not is_disallowed else PROXY_URL
#     ]

#     last_error = None
#     for proxy_config in proxies_to_try:
#         browser = None
#         try:
#             isbri_data = False
#             if proxy_config == PROXY_URL:
#                 logging.info("Attempting with bri-data proxy (allowed by robots.txt)")
#                 browser = await p.chromium.connect_over_cdp(PROXY_URL)
#                 isbri_data = True
#             else:
#                 logging.info("Attempting with oxylabs proxy (required by robots.txt)")
#                 browser = await p.chromium.launch(
#                     proxy=proxy_config,
#                     headless=True,
#                     args=[
#                         '--disable-blink-features=AutomationControlled',
#                         '--disable-web-security'
#                     ]
#                 )

#             context = await browser.new_context()
#             await context.add_init_script("""
#                 Object.defineProperty(navigator, 'webdriver', {
#                     get: () => undefined
#                 })
#             """)
#             page = await context.new_page()
            
#             await safe_goto_and_wait(page, url,isbri_data)
#             return browser, page

#         except Exception as e:
#             last_error = e
#             error_trace = traceback.format_exc()
#             logging.error(f"Proxy attempt failed:\n{error_trace}")
#             if browser:
#                 await browser.close()
#             continue

#     error_msg = (f"Failed to load {url} using all proxy options. "
#                 f"Last error: {str(last_error)}\n"
#                 f"URL may be disallowed by robots.txt or proxies failed.")
#     logging.error(error_msg)
#     raise RuntimeError(error_msg)




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



# Main scraper function
def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}page={page_count}"   

async def handle_piaget(url, max_pages=None):
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
    filename = f"handle_piaget_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    prev_prod_cout = 0
    load_more_clicks = 1
    while load_more_clicks <= max_pages:
        browser = None
        page = None
        current_url = build_url_with_loadmore(url, load_more_clicks) 
        
        try:
            async with async_playwright() as p:
                product_wrapper = '.mixed-grid'
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {url}")

                # Scroll to load all items
                # await scroll_to_bottom(page)
                
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                # Get all product tiles
                product_wrapper = await page.query_selector("div.mixed-grid")
                products = await product_wrapper.query_selector_all("div[class~='mixed-grid__item'][class~='mixed-grid__item--1/2']:not([class~='hidden'])") if product_wrapper else []

                # products = products[prev_prod_cout:]
                # prev_prod_cout += len(products)

                logging.info(f"New products found: {len(products)}")
                print(f"New products found: {len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        product_body = await product.query_selector(".product-card-body")
                        if product_body:
                            # Get all text content inside product body (includes title and "Rose Gold Bracelet" text)
                            full_text = (await product_body.inner_text()).strip()
                            
                            # Now remove the price text (if any)
                            # The price is inside `.product-card__price span`
                            price_tag = await product_body.query_selector(".product-card__price span")
                            if price_tag:
                                price_text = (await price_tag.inner_text()).strip()
                                # Remove price_text from full_text to isolate product name only
                                product_name = full_text.replace(price_text, "").strip()
                            else:
                                product_name = full_text
                        else:
                            product_name = "N/A"
                    except Exception:
                        product_name = "N/A"

                 

                    # try:
                    #     # Extract price - from the product-card__price span element
                    #     price_tag = await product.query_selector(".product-card__price span")
                    #     price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                    #     # Clean up price string
                    #     price = re.sub(r'\s+', ' ', price).strip()
                    # except Exception:
                    #     price = "N/A"
                    
                    price = "N/A"

                    image_url = "N/A"
                    try:
                        picture_tag = await product.query_selector("picture")
                        if picture_tag:
                            # Get all <source> tags inside <picture>
                            source_tags = await picture_tag.query_selector_all("source")
                            # Try to find the best image URL from all source tags by priority:
                            # (You can pick first source or pick the one with largest image, etc.)
                            for source_tag in source_tags:
                                srcset = await source_tag.get_attribute("srcset")
                                if srcset:
                                    # srcset string example: "https://example.com/image-2x.jpg 2x, https://example.com/image-1x.jpg 1x"
                                    # Parse srcset, split by commas, pick first URL
                                    candidates = [s.strip().split()[0] for s in srcset.split(",") if s.strip()]
                                    if candidates:
                                        image_url = candidates[0]
                                        # Make URL absolute if starts with "//"
                                        if image_url.startswith("//"):
                                            image_url = "https:" + image_url
                                        # Remove any query parameters
                                        image_url = image_url.split("?")[0]
                                        break  # found a valid image URL, break out of loop

                            # If no source tags had valid srcset, fallback to <img>
                            if image_url == "N/A":
                                img_tag = await picture_tag.query_selector("img")
                                if img_tag:
                                    # Prefer srcset from img if present
                                    img_srcset = await img_tag.get_attribute("srcset")
                                    if img_srcset:
                                        candidates = [s.strip().split()[0] for s in img_srcset.split(",") if s.strip()]
                                        if candidates:
                                            image_url = candidates[0]
                                    else:
                                        # fallback to src attribute
                                        img_src = await img_tag.get_attribute("src")
                                        if img_src:
                                            image_url = img_src
                                    if image_url.startswith("//"):
                                        image_url = "https:" + image_url
                                    image_url = image_url.split("?")[0]
                    except Exception as e:
                        log_event(f"Error getting image URL: {e}")
                        image_url = "N/A"


                    # Extract gold type (kt) from product name/description
                    gold_type_pattern = r"\b\d{1,2}(?:K|kt|ct|Kt)\b|\bPlatinum\b|\bSilver\b|\bWhite Gold\b|\bYellow Gold\b|\bRose Gold\b"
                    gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    # Extract diamond weight from description
                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                    
                    
                    if product_name == "N/A" or  image_url == "N/A":
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