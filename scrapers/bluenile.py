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
from utils import get_public_ip, log_event, sanitize_filename
from dotenv import load_dotenv
from database import insert_into_db
from limit_checker import update_product_count
import traceback
from typing import List
from openpyxl.drawing.image import Image as XLImage
import httpx
# Load environment variables from .env file
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")
PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")



BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

def upgrade_to_high_res_url(image_url):
    if not image_url or image_url == "N/A":
        return image_url

    base_url = image_url.split("?")[0]
    return re.sub(r'_\d+X\d+(?=\.jpg$)', '_1600X1600', base_url)


async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    high_res_url = upgrade_to_high_res_url(image_url)  # assume this transforms to higher quality version

    async with httpx.AsyncClient(timeout=10.0) as client:
        urls_to_try = [high_res_url, image_url]  # try high-res first, then fallback to original
        for url in urls_to_try:
            for attempt in range(retries):
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    return image_full_path
                except httpx.RequestError as e:
                    logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading from {url}: {e}")
                except httpx.HTTPStatusError as e:
                    logging.warning(f"Retry {attempt + 1}/{retries} - HTTP error from {url}: {e}")
            logging.info(f"Switching to fallback URL after {retries} failed attempts for {url}")
    
    logging.error(f"Failed to download image for {product_name} after trying both URLs.")
    return "N/A"

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
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
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)

            # Wait for the selector with a longer timeout
            product_cards = await page.wait_for_selector('.gallery-grid-container--vJWMdFUhYMhp1TP3jIfs', state="attached", timeout=60000) # 60 seconds


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


async def handle_bluenile(url, max_pages):
    
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}, max_pages: {max_pages}")

    # Prepare directories and files
    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    # Create workbook and setup
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_bluenile_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    prev_prod_cout = 0
    load_more_clicks = 1
    while load_more_clicks <= max_pages:
        
        logging.info(f"Processing page {load_more_clicks}: {url}")
        
        # Create a new browser instance for each page
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                # product_wrapper = ".gallery-grid-container--vJWMdFUhYMhp1TP3jIfs"
                # browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)
                
                browser, page = await get_browser_with_proxy_strategy(p, url)
                log_event(f"Successfully loaded: {url}")


                # Scroll to load all products
                await scroll_to_bottom(page)

                # Now query products using Blue Nile's actual DOM
                product_container = await page.wait_for_selector("#data-page-container", timeout=30000)
                products = await product_container.query_selector_all("div[class^='item--']")
                max_prod = len(products)
                logging.info(f"New products found: {max_prod}")
                print(f"New products found: {max_prod}")
                
                products = products[prev_prod_cout: min(max_prod, prev_prod_cout + 16)]
                prev_prod_cout += len(products)

                if len(products) == 0:
                    log_event("No new products found, stopping the scraper.")
                    break

                logging.info(f"New products found: {len(products)}")
                print(f"New products found: {len(products)}")
                # products = await page.query_selector_all("div.item--BtojO4WSSsxPN6lzc96B")

                # products =  await page.query_selector("div.item--BtojO4WSSsxPN6lzc96B").all()
                logging.info(f"Total products found on page {load_more_clicks}: {len(products)}")


                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        product_name_el = await product.query_selector("div.itemTitle--U5mJCpztfNqClWjA0gnb span")
                        product_name = await product_name_el.inner_text() if product_name_el else "N/A"
                    except:
                        product_name = "N/A"

                    try:
                        # Extract sale price and original price
                        sale_price_el = await product.query_selector('[data-qa="sale-price"]')
                        regular_price_el = await product.query_selector('[data-qa="price"]')

                        if sale_price_el and regular_price_el:
                            sale_price_text = await sale_price_el.inner_text()
                            regular_price_text = await regular_price_el.inner_text()

                            # Convert prices to float
                            sale_price = float(sale_price_text.replace('$', '').replace(',', ''))
                            regular_price = float(regular_price_text.replace('$', '').replace(',', ''))

                            # Calculate discount amount
                            discount_amount = regular_price - sale_price

                            # Format price string: "$1,065 off  $2,485"
                            price = f"${discount_amount:,.0f} off  ${sale_price:,.0f}"

                        elif regular_price_el:
                            regular_price_text = await regular_price_el.inner_text()
                            price = f"${float(regular_price_text.replace('$', '').replace(',', '')):,.0f}"
                        else:
                            price = "N/A"
                    except:
                        price = "N/A"



                    try:
                        # Lifestyle image usually looks more styled, prefer it if present
                        await product.scroll_into_view_if_needed()
                        image_el = await product.query_selector("div.imageContainer--UuMEUHM2d6Z6l3MEk8RD img")
                        if not image_el:
                            image_el = await product.query_selector("div.imageContainer--UuMEUHM2d6Z6l3MEk8RD img")
                        image_url = await image_el.get_attribute("src") if image_el else "N/A"
                    except:
                        image_url = "N/A"
                        
                    additional_info = []

                   

                    # Extract star rating
                    try:
                        star_els = await product.query_selector_all('div.star--vN1jYGuXy5fgfM_M2C7Z.full--eWF94JobeoCq1i40n_Tw')
                        if star_els:
                            rating = len(star_els)
                            additional_info.append(f"{rating} stars")
                    except:
                        pass

                    # Extract review count
                    try:
                        review_el = await product.query_selector('div.revText--Ouf1K_o8qVYxDgE516SF')
                        if review_el:
                            review_text = await review_el.inner_text()  # e.g., "( 52 )"
                            review_count = ''.join(filter(str.isdigit, review_text))  # get '52'
                            if review_count:
                                additional_info.append(f"{review_count} reviews")
                    except:
                        pass
                    
                    # Extract discount percentage
                    try:
                        discount_el = await product.query_selector('div[class^="discount--"]')
                        if discount_el:
                            discount_percent = await discount_el.inner_text()
                            additional_info.append(f"{discount_percent} off")
                    except:
                        pass

                    # Final string
                    additional_info_str = " | ".join(additional_info)

                    gold_type_match = re.findall(r"(\d{1,2}ct\s*(?:Yellow|White|Rose)?\s*Gold|Platinum)", product_name, re.IGNORECASE)
                    kt = ", ".join(gold_type_match) if gold_type_match else "N/A"

                    # Extract Diamond Weight (supports "1.85ct", "2ct", "1.50ct", etc.)
                    diamond_weight_match = re.findall(r"(\d+(?:\.\d+)?\s*ct)", product_name, re.IGNORECASE)
                    diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])

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
                                logging.error(f"Error adding image to Excel: {img_error}")
                                image_path = "N/A"
                        
                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Timeout downloading image for row {row_num}")

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