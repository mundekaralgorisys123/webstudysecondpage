import os
import re
import uuid
import logging
import random
import asyncio
import base64
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError, Error
from openpyxl import Workbook
from openpyxl.drawing.image import Image as ExcelImage
from dotenv import load_dotenv
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
from urllib.parse import urljoin
import httpx
import time
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



def modify_image_url(image_url):
    """Modify the image URL to replace '_260' with '_1200' while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"

    modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)
    return modified_url + query_params

async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    """Download image with retries and return its local path."""
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



async def scroll_page(page):
    """Scroll down to load lazy-loaded products."""
    prev_product_count = 0
    for _ in range(50):  # Adjust scroll attempts as needed
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(2)  # Wait for lazy-loaded content to render

        # Wait for at least one product card to appear (if not already)
        await page.wait_for_selector('[data-testid="card"]', timeout=15000)

        # Count current number of product cards
        current_product_count = await page.locator('[data-testid="card"]').count()

        if current_product_count == prev_product_count:
            break  # Stop if no new products were loaded
        prev_product_count = current_product_count
        
        
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
            product_cards = await page.wait_for_selector("#product-cards", state="attached", timeout=30000)

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
         
         
async def handle_bash(url, max_pages):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}, max_pages: {max_pages}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    # Initialize Excel
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Gold Type", "Price", "Total Dia Wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)
    current_date = datetime.now().strftime("%Y-%m-%d")
    time_only = datetime.now().strftime("%H-%M-%S")

    all_records = []
    filename = f"handle_bash_{current_date}_{time_only}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0
    
    async with async_playwright() as p:
        while page_count <= max_pages:
            current_url = build_url_with_loadmore(url, page_count)
            logging.info(f"Processing page {page_count}: {current_url}")

            # Create a new browser instance for each page
            browser = None
            page = None
            try:
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")

                await scroll_page(page)

                page_title = await page.title()
                product_container = await page.query_selector("#product-cards")
                products = await product_container.query_selector_all("[data-testid='card']") if product_container else []
                # print(len(products))

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    
                    product_name_tag = await product.query_selector("h3.cursor-pointer.text-base.font-bold.leading-4.text-onyx-Black.line-clamp-2.h-8.z-5")
                    product_name = (await product_name_tag.inner_text()).strip() if product_name_tag else "N/A"

                    # Locate the price container
                    price_tag = await product.query_selector("div[data-testid='price']")
                    if price_tag:
                        # Try to get sale price
                        sale_price_el = await price_tag.query_selector("span[data-testid='sale-price']")
                        sale_price = (await sale_price_el.inner_text()).strip() if sale_price_el else ""

                        # Try to get original (strikethrough) price
                        original_price_el = await price_tag.query_selector("span.line-through")
                        original_price = (await original_price_el.inner_text()).strip() if original_price_el else ""

                        # Try to get discount percentage
                        discount_el = await price_tag.query_selector("span.text-sale")
                        discount = (await discount_el.inner_text()).strip() if discount_el else ""

                        # Handle case where the entire price is just plain text without span
                        if not (sale_price or original_price):
                            price_text = (await price_tag.inner_text()).strip()
                            price = price_text if price_text else "N/A"
                        else:
                            if sale_price and original_price and discount:
                                price = f"{sale_price} offer {original_price} ({discount})"
                            elif sale_price:
                                price = sale_price
                            elif original_price:
                                price = original_price
                            else:
                                price = "N/A"
                    else:
                        price = "N/A"

                    image_tag = await product.query_selector("img[data-testid='image']")
                    image_url = await image_tag.get_attribute("src") if image_tag else "N/A"
                    
                    additional_info = []

                    # Extract sale tag (e.g., "SALE", "NEW", etc.)
                    try:
                        sale_tag_el = await product.query_selector("div.text-widget-text span[data-testid='widget']")
                        if sale_tag_el:
                            sale_tag_text = (await sale_tag_el.inner_text()).strip()
                            if sale_tag_text:
                                additional_info.append(sale_tag_text)
                            else:
                                additional_info.append("sale: N/A")    
                    except Exception:
                        pass  # Ignore errors silently if sale tag is missing

                    # Extract brand name
                    try:
                        brand_el = await product.query_selector("h4 a[data-testid='link']")
                        if brand_el:
                            brand_text = (await brand_el.inner_text()).strip()
                            if brand_text:
                                additional_info.append(f"Brand: {brand_text}")
                            else:
                                additional_info.append("Brand: N/A")
                        else:
                            additional_info.append("Brand: N/A")
                    except Exception:
                        additional_info.append("Brand: N/A")

                    # Combine into a single string
                    additional_info_str = " | ".join(additional_info) if additional_info else "N/A"


                    

                    gold_type_match = re.search(r"(\d{1,2}K|Platinum|Silver|Gold|White Gold|Yellow Gold|Rose Gold)", product_name, re.IGNORECASE)
                    kt = gold_type_match.group(0) if gold_type_match else "N/A"

                    diamond_weight_match = re.search(r"(\d+(\.\d+)?)\s*(ct|carat)", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_weight_match.group(1)} ct" if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((
                        row_num,
                        unique_id,
                        asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )
                    ))

                    records.append((
                        unique_id,
                        current_date,
                        page_title,
                        product_name,
                        None,  # Placeholder for image path
                        kt,
                        price,
                        diamond_weight,
                        additional_info_str
                    ))

                    sheet.append([
                        current_date,
                        page_title,
                        product_name,
                        None,  # Placeholder for image
                        kt,
                        price,
                        diamond_weight,
                        time_only,
                        image_url,
                        additional_info_str
                    ])

                    

                # Process downloaded images
                for row_num, unique_id, task in image_tasks:
                    try:
                        image_path = await asyncio.wait_for(task, timeout=60)
                        if image_path != "N/A":
                            try:
                                img = ExcelImage(image_path)
                                img.width, img.height = 100, 100
                                sheet.add_image(img, f"D{row_num}")
                            except Exception as img_error:
                                logging.error(f"Error adding image to Excel: {img_error}")
                                image_path = "N/A"
                        
                        # Update record with actual image_path
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
                    
            except Exception as e:
                logging.error(f"Error processing page {page_count}: {str(e)}")
                if page:
                    await page.close()
                if browser:
                    await browser.close()
                wb.save(file_path)
                continue
            
            # Add delay between pages
            await asyncio.sleep(random.uniform(2, 5))
            
        page_count += 1

    # # Final save and database operations
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