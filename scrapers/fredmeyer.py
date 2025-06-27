import os
import time
import logging
import concurrent.futures
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError ,Error
from openpyxl import Workbook
from openpyxl.drawing.image import Image
import uuid
import base64
from database import insert_into_db
from limit_checker import update_product_count
import traceback
from typing import List, Tuple
import re
import random
from proxysetup import get_browser_with_proxy_strategy
import requests
from dotenv import load_dotenv
import httpx
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")


PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")
# Setup Flask
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def download_image(image_url, product_name, timestamp, image_folder, unique_id, retries=5, timeout=30):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    for attempt in range(1, retries + 1):
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(image_url, headers=headers, stream=True, timeout=timeout, allow_redirects=True)
            response.raise_for_status()

            with open(image_full_path, "wb") as f:
                f.write(response.content)

            return image_full_path

        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt}: Error downloading {image_url} - {e}")
            time.sleep(5)

    logging.error(f"Failed to download image after {retries} attempts: {image_url}")
    return None




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
            product_cards = await page.wait_for_selector(".empathyBrowse", state="attached", timeout=30000)

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

async def handle_fredmeyer(url, max_pages):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Gold Type", "Price", 
               "Total Dia Wt", "Time", "ImagePath"]
    sheet.append(headers)

    current_date = datetime.now().strftime("%Y-%m-%d")
    time_only = datetime.now().strftime("%H-%M-%S")
    records = []

    async with async_playwright() as p:
        for page_count in range(1, max_pages + 1):
            logging.info(f"Processing page {page_count}/{max_pages}")
            # product_wrapper=".x-layout-container"
            browser, page = await get_browser_with_proxy_strategy(p, url)

            try:
                # Simulate going to the correct page by clicking through pagination
                if page_count > 1:
                    for _ in range(1, page_count):
                        try:
                            next_button = page.locator("li.FMJ_page-item.next-page:not(.disabled) a")
                            if await next_button.count() > 0:
                                await next_button.scroll_into_view_if_needed()
                                current_page_element = page.locator("li.FMJ_Page-item.page-numb.activePage a")
                                current_page_number_text = await current_page_element.inner_text() if await current_page_element.count() > 0 else str(_)
                                current_page_number = int(current_page_number_text)

                                await next_button.click(force=True)
                                await page.wait_for_function(
                                    f"""() => {{
                                        const active = document.querySelector("li.FMJ_Page-item.page-numb.activePage a");
                                        return active && parseInt(active.innerText) > {current_page_number};
                                    }}""",
                                    timeout=15000
                                )
                                await asyncio.sleep(2)
                        except Exception as e:
                            logging.error(f"Error advancing to page {page_count}: {e}")
                            break

                prev_product_count = 0
                for _ in range(50):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_selector("div.grid-group-item", timeout=30000)
                    current_product_count = await page.locator('div.grid-group-item').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count

                products = await page.query_selector_all("div.grid-group-item")
                logging.info(f"Found {len(products)} products on page {page_count}")
                page_title = await page.title()

                if not products:
                    logging.warning("No products found, retrying...")
                    continue

                image_tasks = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    for row_num, product in enumerate(products, start=2 + len(records)):
                        try:
                            product_name_tag = await product.query_selector('div.prodtext a')
                            product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"
                        except Exception as e:
                            logging.warning(f"Error extracting product name: {e}")
                            product_name = "N/A"

                        try:
                            price_tag = await product.query_selector('.pricediv .curprice')
                            price = await price_tag.inner_text() if price_tag else "N/A"
                        except Exception as e:
                            logging.warning(f"Error extracting price: {e}")
                            price = "N/A"

                        try:
                            base_url = "https://www.fredmeyerjewelers.com"
                            image_tag = await product.query_selector('img.mainprodimage')
                            image_url = await page.evaluate(
                                '(el) => el.getAttribute("data-src") || el.getAttribute("src")', image_tag
                            ) if image_tag else "N/A"

                            if image_url and image_url.startswith("/"):
                                image_url = base_url + image_url
                        except Exception as e:
                            logging.error(f"Error extracting image: {e}")
                            image_url = "N/A"

                        gold_type_match = re.search(r"\b(\d{1,2}K\s*(?:Yellow|White|Rose)?\s*Gold)\b", product_name, re.IGNORECASE)
                        kt = gold_type_match.group(1) if gold_type_match else "N/A"

                        diamond_match = re.search(r"(\d+(?:[/\-]\d+)?(?:\.\d+)?\s*ct\.?\s*(?:t\.?w\.?)?)", product_name, re.IGNORECASE)
                        diamond_weight = diamond_match.group(1) if diamond_match else "N/A"

                        unique_id = str(uuid.uuid4())
                        image_future = executor.submit(download_image, image_url, product_name, timestamp, image_folder, unique_id)
                        image_tasks.append((row_num, unique_id, image_future))

                        records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight))
                        sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url])

                for row_num, unique_id, future in image_tasks:
                    image_path = future.result()
                    if image_path != "N/A":
                        img = Image(image_path)
                        img.width, img.height = 100, 100
                        sheet.add_image(img, f"D{row_num}")
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7])
                            break

            except Exception as e:
                logging.error(f"Error in page {page_count}: {e}")

            finally:
                await browser.close()
                logging.info(f"Browser closed for page {page_count}")

    filename = f"fredmeyer_products_{timestamp}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    wb.save(file_path)
    logging.info(f"Saved Excel file: {file_path}")

    with open(file_path, "rb") as f:
        base64_encoded = base64.b64encode(f.read()).decode("utf-8")

    insert_into_db(records)
    update_product_count(len(records))

    return base64_encoded, filename, file_path
