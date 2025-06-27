import asyncio
import random
import re
import os
import uuid
import logging
import base64
import time
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from dotenv import load_dotenv
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import httpx
from playwright.async_api import async_playwright, TimeoutError, Error
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



# async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3, proxy_url=None):
#     image_filename = f"{unique_id}_{timestamp}.jpg"
#     image_path = os.path.join(image_folder, image_filename)

#     headers = {
#         "User-Agent": (
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#             "AppleWebKit/537.36 (KHTML, like Gecko) "
#             "Chrome/124.0.0.0 Safari/537.36"
#         ),
#         "Referer": "https://www.vancleefarpels.com/",
#         "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
#         "Accept-Language": "en-US,en;q=0.9",
#         "Connection": "keep-alive",
#     }

#     for attempt in range(1, retries + 1):
#         try:
#             async with httpx.AsyncClient(
#                 timeout=httpx.Timeout(30.0, connect=10.0),
#                 headers=headers,
#             ) as client:
#                 response = await client.get(image_url)
#                 response.raise_for_status()

#                 with open(image_path, "wb") as f:
#                     f.write(response.content)

#                 print(f"[Downloaded] {image_path}")
#                 return image_path

#         except httpx.ReadTimeout:
#             print(f"[Timeout] Attempt {attempt}/{retries} - {image_url}")
#             await asyncio.sleep(1.5 * attempt)

#         except httpx.HTTPStatusError as e:
#             print(f"[HTTP Error] {e.response.status_code} - {image_url}")
#             return None

#         except Exception as e:
#             print(f"[Error] {e} while downloading {image_url}")
#             return None

#     print(f"[Failed] {image_url}")
#     return None

def modify_image_url(image_url):
    """
    Clean the image URL by removing `.transform.*.png` or redundant `.png.png` endings.
    """
    if not image_url or image_url == "N/A":
        return image_url

    # Handle `.transform.*.png`
    image_url = re.sub(r'\.transform\..*\.png$', '.png', image_url)

    # Handle `.png.png` → `.png`
    image_url = re.sub(r'\.png\.png$', '.png', image_url)

    return image_url


async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

   
    

    image_filename = f"{unique_id}_{timestamp}.png"
    image_full_path = os.path.join(image_folder, image_filename)

    async with httpx.AsyncClient(timeout=10.0) as client:
        # First try processed image
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                if response.status_code == 200:
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    return image_full_path
                elif response.status_code == 404:
                    logging.info(
                        f"Processed image not found (404), trying original for {product_name}")
                    break  # No point in retrying a 404
            except httpx.RequestError as e:
                logging.warning(
                    f"Retry {attempt + 1}/{retries} - Error downloading processed image for {product_name}: {str(e)}")

        # Fallback to original image
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                if response.status_code == 200:
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    return image_full_path
            except httpx.RequestError as e:
                logging.warning(
                    f"Retry {attempt + 1}/{retries} - Error downloading original image for {product_name}: {str(e)}")

    logging.error(
        f"Failed to download any image for {product_name} after {retries} attempts.")
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
            product_cards = await page.wait_for_selector("#search-engine", state="attached", timeout=30000)

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


async def handle_vancleefarpels(url, max_pages):
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath"]
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
                product_wrapper = "#search-engine"
                browser, page = await get_browser_with_proxy_strategy(p, url)
                # Simulate clicking 'Load More' number of times
                for _ in range(load_more_clicks - 1):
                    try:
                        # Locate the 'Load More' button using the correct selector
                        load_more_button = page.locator("button#loadMore.action-button.load-more.vca-underline")
                        
                        # Check if the button is visible and click it
                        if await load_more_button.is_visible():
                            await load_more_button.click()
                            await asyncio.sleep(2)  # Delay to allow new products to load
                    except Exception as e:
                        logging.warning(f"Could not click 'Load More': {e}")
                        break


                product_wrapper = await page.query_selector("ul.results-list")
                all_products = await product_wrapper.query_selector_all("li.vca-srl-product-tile") if product_wrapper else []

                print(f"Page {load_more_clicks}: Scraping {len(all_products)} new products.")
                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for row_num, product in enumerate(new_products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product name from the <h2> tag
                        product_name_tag = await product.query_selector('h2.product-name.vca-product-list-01')
                        product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"

                        # Extract product description from the <p> tag and clean up text
                        desc_tag = await product.query_selector('p.product-description.vca-body-02.vca-text-center')
                        if desc_tag:
                            desc_text = await desc_tag.inner_text()
                            product_name += f" - {desc_text.strip()}"
                    except Exception as e:
                        logging.error(f"Error fetching product name or description: {e}")
                        product_name = "N/A"


                    try:
                        # Extract price from the <span> tag with class 'vca-price'
                        price_tag = await product.query_selector('span.vca-price')
                        price = await price_tag.inner_text() if price_tag else "N/A"
                    except Exception as e:
                        logging.error(f"Error fetching price: {e}")
                        price = "N/A"


                    try:
                        image_url = "N/A"
                        
                        # Locate image inside the known container
                        image_element = await product.query_selector("div.image-container img")
                        
                        if image_element:
                            image_src = await image_element.get_attribute("src")
                            
                            if image_src:
                                if not image_src.startswith(("http", "//")):
                                    image_url = f"https://www.vancleefarpels.com{image_src}"
                                else:
                                    image_url = image_src
                    except Exception as e:
                        logging.error(f"Image extraction error: {e}")
                        image_url = "N/A"

                    # image_url should now contain the extracted URL or "N/A" if an error occurs
                    image_url = modify_image_url(image_url)
                    print(product_name)
                    print(price)
                    print(image_url)

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue 
                    
                    # print(image_url)
                    
                    kt_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                    kt = kt_match.group() if kt_match else "Not found"

                    diamond_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_match.group(1)} ct" if diamond_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url])

                # Process image downloads and attach them to Excel
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        img = Image(image_path)
                        img.width, img.height = 100, 100
                        sheet.add_image(img, f"D{row}")
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7])
                            break

                await browser.close()
            load_more_clicks += 1

        # Save Excel
        filename = f'handle_vancleefarpels_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
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

