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
from typing import List
# Load .env variables
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')


def modify_image_url(image_url, high_res=True):
    """Modify image URLs to use high-resolution if available."""
    if not image_url or image_url == "N/A":
        return image_url

    # Extract and preserve query parameters
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"

    if high_res:
        # Force high resolution by changing sw and sh to 1024
        query_params = re.sub(r"(\?|&)sw=\d+", r"\1sw=1024", query_params)
        query_params = re.sub(r"(\?|&)sh=\d+", r"\1sh=1024", query_params)

    return image_url + query_params


async def download_image(session: httpx.AsyncClient, image_url: str, product_name: str, timestamp: str, image_folder: str, unique_id: str):
    """Download an image using httpx, attempting high-res first."""
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    # Try high resolution first, then fallback to original
    high_res_url = modify_image_url(image_url, high_res=True)
    original_url = modify_image_url(image_url, high_res=False)

    for attempt in range(3):
        for url_to_try in [high_res_url, original_url]:
            try:
                response = await session.get(url_to_try, timeout=10)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1}/3 - Error downloading from {url_to_try} for {product_name}: {e}")
                if url_to_try == original_url:
                    break

    logging.error(f"Failed to download image for {product_name} after 3 attempts.")
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
            product_cards = await page.wait_for_selector(".l-grid", state="attached", timeout=30000)

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



async def handle_stroilioro(url, max_pages):
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
        current_page = 1
        previous_count = 0
        current_url = url
        while current_page <= max_pages:
            if current_page > 1:
                current_url = f"{url}?start={(current_page-1)*41}&sz=41"
                    
            browser = None
            page = None        
            async with async_playwright() as p:
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")
                
                try:
                    await page.wait_for_selector("#didomi-popup", timeout=5000)
                    accept_btn = await page.query_selector("button[aria-label='Accepter']")
                    if accept_btn:
                        await accept_btn.click()
                        print("✅ Cookie consent accepted.")
                        await asyncio.sleep(1)
                except:
                    print("ℹ️ No Didomi popup found or already dismissed.")
                    
                all_products = await page.query_selector_all("div.c-grid__item")
                total_products = len(all_products)
                new_products = all_products
                logging.info(f"Page {current_page}: Total = {total_products}, New = {len(new_products)}")

                print(f"Page {current_page}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    # Skip if product doesn't have basic required elements
                    if not await product.query_selector("a.c-product-tile__name-link"):
                        continue

                    try:
                        # Product name
                        name_tag = await product.query_selector("a.c-product-tile__name-link")
                        product_name = await name_tag.inner_text()
                        product_name = product_name.replace('\n', ' ').strip()
                        if not product_name:
                            continue
                    except Exception as e:
                        print(f"[Product Name] Error: {e}")
                        continue

                    try:
                        # Price
                        price_tag = await product.query_selector("span.c-price__standard")
                        price = await price_tag.inner_text() if price_tag else None
                        if not price:
                            continue
                    except Exception as e:
                        print(f"[Price] Error: {e}")
                        continue

                    try:
                        # Image URL
                        img_tag = await product.query_selector("div.c-product-tile__image-link picture img")
                        image_url = await img_tag.get_attribute("data-src") if img_tag else None
                        if not image_url:
                            image_url = await img_tag.get_attribute("src") if img_tag else None
                        if image_url and image_url.startswith("//"):
                            image_url = "https:" + image_url
                        if not image_url:
                            continue
                    except Exception as e:
                        print(f"[Image URL] Error: {e}")
                        continue

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                    # Extract Additional Info (labels, tags, etc.)
                    additional_info = []
                    
                    try:
                        # Get stickers/labels (like "Engraving" in the example)
                        stickers = await product.query_selector_all(".sticker .sticker-body")
                        for sticker in stickers:
                            label = await sticker.inner_text()
                            if label.strip():
                                additional_info.append(label.strip())
                    except Exception as e:
                        print(f"[Stickers] Error: {e}")

                    try:
                        # Get discount information if available
                        discount_tag = await product.query_selector(".c-product__discount-plp")
                        if discount_tag:
                            discount_text = await discount_tag.inner_text()
                            if discount_text.strip():
                                additional_info.append(discount_text.strip())
                    except Exception as e:
                        print(f"[Discount] Error: {e}")

                    try:
                        # Get ratings if available
                        rating_tag = await product.query_selector("[itemprop='ratingValue']")
                        if rating_tag:
                            rating = await rating_tag.get_attribute("content")
                            if rating:
                                additional_info.append(f"Rating: {rating}")
                    except Exception as e:
                        print(f"[Rating] Error: {e}")

                    # Format additional info
                    additional_info_str = " | ".join(additional_info) if additional_info else ""

                    # Extract product details
                    gold_type_match = re.findall(r"(\d{1,2}ct\s*(?:Yellow|White|Rose)?\s*Gold|Platinum|Cubic Zirconia)", 
                                              product_name, re.IGNORECASE)
                    kt = ", ".join(gold_type_match) if gold_type_match else "N/A"

                    diamond_weight_match = re.findall(r"(\d+(?:\.\d+)?\s*ct)", product_name, re.IGNORECASE)
                    diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(
                        download_image(session, image_url, product_name, timestamp, image_folder, unique_id)
                    )
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))
                    
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

                # Process image downloads and update records
                for row, unique_id, task in image_tasks:
                    try:
                        image_path = await asyncio.wait_for(task, timeout=60)
                        if not image_path or image_path == "N/A":
                            # Remove record if image download failed
                            records = [r for r in records if r[0] != unique_id]
                            continue
                            
                        try:
                            img = Image(image_path)
                            img.width, img.height = 100, 100
                            sheet.add_image(img, f"D{row}")
                        except Exception as img_error:
                            logging.error(f"Error adding image to Excel: {img_error}")
                            records = [r for r in records if r[0] != unique_id]
                            continue
                            
                        # Update record with image path
                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (
                                    record[0], 
                                    record[1], 
                                    record[2], 
                                    image_path, 
                                    record[4], 
                                    record[5], 
                                    record[6],
                                    record[7],
                                    record[8]
                                )
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Timeout downloading image for row {row}")
                        records = [r for r in records if r[0] != unique_id]

                await browser.close()
            current_page += 1

        if not all_products:
            return None, None, None
        
        # Save Excel
        filename = f'handle_stroilioro_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
        file_path = os.path.join(EXCEL_DATA_PATH, filename)
        wb.save(file_path)
        log_event(f"Data saved to {file_path} | IP: {ip_address}")

        # Only insert complete records into database
        complete_records = [r for r in records if all(r[1:])]
        if complete_records:
            insert_into_db(complete_records)
        else:
            logging.info("No complete records to insert into the database.")

        update_product_count(len(complete_records))

        with open(file_path, "rb") as f:
            base64_encoded = base64.b64encode(f.read()).decode("utf-8")

        return base64_encoded, filename, file_path
