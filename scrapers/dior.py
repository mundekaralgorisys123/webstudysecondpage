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
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import mimetypes
# from proxysetup import get_browser_with_proxy_strategy
from dotenv import load_dotenv
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

mimetypes.add_type('image/webp', '.webp')

async def extract_best_image_url(product_element):
    try:
        # Simply get the first img element and its src
        img_element = await product_element.query_selector("img")
        if img_element:
            img_src = await img_element.get_attribute("src")
            if img_src:
                return img_src 
        return None
    except Exception as e:
        log_event(f"Error extracting image URL: {e}")
        return None

# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"  # Always save as JPG
    image_full_path = os.path.join(image_folder, image_filename)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                img_data = response.content
                
                # Convert WEBP to JPG if needed
                if image_url.lower().endswith('.webp'):
                    try:
                        img = PILImage.open(BytesIO(img_data))
                        if img.format == 'WEBP':
                            buffer = BytesIO()
                            img.convert('RGB').save(buffer, format="JPEG", quality=85)
                            img_data = buffer.getvalue()
                    except Exception as e:
                        log_event(f"Error converting WEBP to JPG: {e}")
                        continue
                
                with open(image_full_path, "wb") as f:
                    f.write(img_data)
                return image_full_path
                
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
    
    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"

# Human-like delay
def random_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))

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
            product_cards = await page.wait_for_selector(".app-content", state="attached", timeout=30000)

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
                '--disable-http2',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security'
               
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
#                     headless=True,  # You can toggle to False for debugging
#                     args=[
#                         '--disable-blink-features=AutomationControlled',
#                         '--disable-web-security',
#                         '--no-sandbox',
#                         '--disable-dev-shm-usage'
#                     ]
#                 )

#             context = await browser.new_context(
#                 user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#                         "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
#                 viewport={"width": 1280, "height": 800},
#                 locale="en-US",
#             )

#             # Stealth: Hide navigator.webdriver
#             await context.add_init_script("""
#                 Object.defineProperty(navigator, 'webdriver', {
#                     get: () => undefined
#                 });
#             """)

#             page = await context.new_page()

#             await safe_goto_and_wait(page, url, isbri_data)
#             return browser, page

#         except Exception as e:
#             last_error = e
#             error_trace = traceback.format_exc()
#             logging.error(f"Proxy attempt failed:\n{error_trace}")
#             if browser:
#                 try:
#                     await browser.close()
#                 except Exception:
#                     pass  # Don't raise new exception during cleanup
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


# Main scraper function
async def handle_dior(url, max_pages=None):
    
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_dior_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    browser = None
    page = None
    
    try:
        async with async_playwright() as p:
           
            browser, page = await get_browser_with_proxy_strategy(p, url)
            log_event(f"Successfully loaded: {url}")

            # Scroll to load all items
            await scroll_to_bottom(page)
            
            page_title = await page.title()
            current_date = datetime.now().strftime("%Y-%m-%d")
            time_only = datetime.now().strftime("%H.%M")

            # Get all product tiles
            product_tiles = await page.query_selector_all("li.MuiGrid-item")
            logging.info(f"Total products found: {len(product_tiles)}")
            print(f"Total products found: {len(product_tiles)}")
            records = []
            image_tasks = []
            
            for row_num, product in enumerate(product_tiles, start=len(sheet["A"]) + 1):
                try:
                    name_tag = await product.query_selector(
                        ".MuiTypography-root.MuiTypography-label-m-medium.DS-Typography.mui-latin-sbe52t"
                    )
                    product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                except Exception:
                    product_name = "N/A"

                try:
                    price_tag = await product.query_selector(
                        ".MuiTypography-root.MuiTypography-label-m-regular.DS-Typography.card-legend-price.mui-latin-bmun1a"
                    )
                    price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                except Exception:
                    price = "N/A"


                


                image_url = "N/A"
                try:
                    image_url = await extract_best_image_url(product) or "N/A"
                except Exception as e:
                    log_event(f"Error getting image URL: {e}")
                    image_url = "N/A"
                    
                    
                try:
                    description_tag = await product.query_selector(
                        ".MuiTypography-root.MuiTypography-label-m-regular.DS-Typography.mui-latin-1btdzsw"
                    )
                    description = (await description_tag.inner_text()).strip() if description_tag else "N/A"
                except Exception:
                    description = "N/A"
 
                    
                print(description)  
                print(price)  
                print(image_url)   

                # Extract gold type (kt) from description
                finish_match = re.findall(r"(?:Matte\s+)?[A-Z][a-z]+-Finish\s+Metal", description)
                kt = ", ".join(finish_match) if finish_match else "N/A"
                    
               
                # Extract diamond weight from description
                diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw)\b"
                diamond_weight_match = re.search(diamond_weight_pattern, description, re.IGNORECASE)
                diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                
                additional_info_str = description
                
                if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                    print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                    continue      

                unique_id = str(uuid.uuid4())
                if image_url and image_url != "N/A":
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))
                    
                    

                product_name = f"{product_name} {description}" 
                records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])
            
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
                            records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                            break
                except asyncio.TimeoutError:
                    logging.warning(f"Image download timed out for row {row_num}")

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