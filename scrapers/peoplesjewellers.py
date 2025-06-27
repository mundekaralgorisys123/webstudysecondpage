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
        async with session.get(modify_image_url(image_url), timeout=10) as response:
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


def modify_image_url(image_url):
    """Modify the image URL to replace '_260' with '_1200' while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    # Extract and preserve query parameters
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"

    # Replace '_260' with '_1200' while keeping the rest of the URL intact
    modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)

    return modified_url + query_params  # Append query parameters if they exist


async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    modified_url = modify_image_url(image_url)

    async with httpx.AsyncClient(timeout=10.0) as client:
        # First try modified (processed) image
        for attempt in range(retries):
            try:
                response = await client.get(modified_url)
                if response.status_code == 200:
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    return image_full_path
                elif response.status_code == 404:
                    logging.info(
                        f"Processed image not found, trying original for {product_name}")
                    break  # Stop retrying if it’s a 404
            except httpx.RequestError as e:
                logging.warning(
                    f"Retry {attempt + 1}/{retries} - Error downloading processed image for {product_name}: {e}")

        # Fallback to original image URL
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
            except httpx.RequestError as e:
                logging.warning(
                    f"Retry {attempt + 1}/{retries} - Error downloading original image for {product_name}: {e}")

    logging.error(
        f"Failed to download any image for {product_name} after {retries} attempts.")
    return "N/A"


def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))


# async def safe_goto_and_wait(page, url, retries=3):
#     for attempt in range(retries):
#         try:
#             print(f"[Attempt {attempt + 1}] Navigating to: {url}")
#             await page.goto(url, timeout=180_000, wait_until="domcontentloaded")

#             # Corrected selector
#             product_cards = await page.wait_for_selector(
#                 ".product-listing.product-grid.products-list",
#                 state="attached",
#                 timeout=30000
#             )

#             if product_cards:
#                 print("[Success] Product cards loaded.")
#                 return
#         except (Error, TimeoutError) as e:
#             logging.error(f"Error navigating to {url} on attempt {attempt + 1}: {e}")
#             if attempt < retries - 1:
#                 logging.info("Retrying after waiting a bit...")
#                 await random_delay(1, 3)
#             else:
#                 logging.error(f"Failed to navigate to {url} after {retries} attempts.")
#                 raise

########################################  safe_goto_and_wait ####################################################################


async def safe_goto_and_wait(page, url, isbri_data, retries=2):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")

            if isbri_data:
                await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            else:
                await page.goto(url, wait_until="networkidle", timeout=180_000)

            # Wait for the selector with a longer timeout
            product_cards = await page.wait_for_selector(
                ".product-listing.product-grid.products-list",
                state="attached",
                timeout=30000
            )

            # Optionally validate at least 1 is visible (Playwright already does this)
            if product_cards:
                print("[Success] Product cards loaded.")
                return
        except Error as e:
            logging.error(
                f"Error navigating to {url} on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                logging.info("Retrying after waiting a bit...")
                random_delay(1, 3)  # Add a delay before retrying
            else:
                logging.error(
                    f"Failed to navigate to {url} after {retries} attempts.")
                raise
        except TimeoutError as e:
            logging.warning(
                f"TimeoutError on attempt {attempt + 1} navigating to {url}: {e}")
            if attempt < retries - 1:
                logging.info("Retrying after waiting a bit...")
                random_delay(1, 3)  # Add a delay before retrying
            else:
                logging.error(
                    f"Failed to navigate to {url} after {retries} attempts.")
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
    is_disallowed = check_url_against_rules(
        str(parsed_url), disallowed_patterns)

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
                logging.info(
                    "Attempting with bri-data proxy (allowed by robots.txt)")
                browser = await p.chromium.connect_over_cdp(PROXY_URL)
                isbri_data = True
            else:
                logging.info(
                    "Attempting with oxylabs proxy (required by robots.txt)")
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

            await safe_goto_and_wait(page, url, isbri_data)
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
    return f"{base_url}{separator}loadMore={page_count}"


async def handle_peoplesjewellers(url, max_pages):
    ip_address = get_public_ip()
    logging.info(
        f"Scraping started for: {url} from IP: {ip_address}, max_pages: {max_pages}")

    # Prepare directories and files
    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    # Create workbook and setup
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt",
               "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"peoplesjewellers_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 0
    success_count = 0

    while page_count < max_pages:
        current_url = build_url_with_loadmore(url, page_count)
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
                    # Random delay between scrolls
                    await asyncio.sleep(random.uniform(1, 2))
                    current_product_count = await page.locator('.product-item').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count

                product_wrapper = await page.query_selector("div.product-scroll-wrapper")
                products = await product_wrapper.query_selector_all("div.product-item") if product_wrapper else []
                logging.info(
                    f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        product_name = await (await product.query_selector("h2.name.product-tile-description")).inner_text()
                    except:
                        product_name = "N/A"

                    try:
                        # Extract current price (the offer price if available)
                        price_el = await product.query_selector("div.price")
                        current_price_text = await price_el.inner_text() if price_el else ""
                        # print(f"Current Price Text: {current_price_text}")  # Debugging
                        current_price = current_price_text.strip().split(
                        )[0] if current_price_text else ""  # ensures we get only "$1014.30"

                        # Extract discount if available (e.g., "30% off")
                        discount_el = await product.query_selector("span.tag-text")
                        discount_text = await discount_el.inner_text() if discount_el else ""
                        # print(f"Discount Text: {discount_text}")  # Debugging
                        discount = discount_text.replace(
                            " off", "").strip() if discount_text else ""  # just "30%"

                        # Extract original price with $ (if offer price is not available)
                        original_price_el = await product.query_selector("div.original-price")
                        original_price_text = await original_price_el.inner_text() if original_price_el else ""
                        # print(f"Original Price Text: {original_price_text}")  # Debugging
                        original_price = original_price_text.strip().replace("Was", "").strip().split()[
                            0] if original_price_text else ""  # "$1449.00"

                        # Build the final formatted price
                        if current_price:  # If there is a current price
                            if discount:
                                price = f"{current_price} offer of {discount} {original_price}"
                            else:
                                price = current_price  # No discount, just current price
                        elif original_price:  # If there is no current price but original price is available
                            price = original_price
                        else:
                            price = "N/A"  # If neither price is available

                    except Exception as e:
                        price = "N/A"
                        print(f"Error: {e}")  # Log the error for debugging

                    try:
                        image_url = await (await product.query_selector("img[itemprop='image']")).get_attribute("src")
                    except:
                        image_url = "N/A"

                    additional_info = []

                    try:
                        tag_els = await product.query_selector_all("span.product-tag.groupby-tablet-product-tags")
                        if tag_els:
                            for tag_el in tag_els:
                                tag_text = await tag_el.inner_text()
                                if tag_text:
                                    additional_info.append(tag_text.strip())
                        else:
                            additional_info.append("N/A")

                    except Exception as e:
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(
                            f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                   # Extract gold type
                    gold_type_match = re.search(
                        r"(Black\s+)?Sterling\s+Silver|Black\s+Rhodium", product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    # Extract diamond weight
                    diamond_weight_match = re.search(
                        r"\d+(?:[-/]\d+)?(?:\s+\d+/\d+)?\s*ct(?:\.|\s)*t\.?w\.?", product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(
                            image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name,
                                   None, kt, price, diamond_weight, additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt,
                                 price, diamond_weight, time_only, image_url, additional_info_str])

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
                                logging.error(
                                    f"Error adding image to Excel: {img_error}")
                                image_path = "N/A"

                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (record[0], record[1], record[2], record[3],
                                              image_path, record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(
                            f"Timeout downloading image for row {row_num}")

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
