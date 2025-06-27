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
from PIL import Image
import httpx
from PIL import Image as PILImage
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from flask import Flask
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
# from proxysetup import get_browser_with_proxy_strategy
import traceback
from typing import List, Tuple
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")


PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# Load environment
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

# Transform URL to get high-res image
def modify_image_url(image_url):
    """Enhance Macy's image URL to get higher resolution version"""
    if not image_url or image_url == "N/A":
        return image_url

    # Replace dimensions in query parameters
    modified_url = re.sub(r'wid=\d+', 'wid=1200', image_url)
    modified_url = re.sub(r'hei=\d+', 'hei=1200', modified_url)
    
    # Replace image quality parameters
    modified_url = re.sub(r'qlt=[^&]+', 'qlt=95', modified_url)
    
    return modified_url


# Function to download image asynchronously
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
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
                
                # Detect WebP using content instead of URL
                image_data = BytesIO(response.content)
                try:
                    img = Image.open(image_data)
                    if img.format.lower() == 'webp':
                        # Convert WebP to JPEG
                        img = img.convert("RGB")
                        img.save(image_full_path, "JPEG", quality=85)
                    else:
                        # Save as JPEG regardless of original format to ensure compatibility
                        img.save(image_full_path, "JPEG", quality=85)
                except Exception as e:
                    logging.error(f"Error processing image for {product_name}: {e}")
                    raise
                
                return image_full_path
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(random.uniform(1, 3))  # Add delay before retry
            except Exception as e:
                logging.warning(f"Error processing image for {product_name}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(random.uniform(1, 3))
    
    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"


# Human-like delay
def random_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))

def build_macys_pagination_url(base_url: str, page_index: int) -> str:
    if page_index == 0:
        return base_url
    else:
        if base_url.endswith('/'):
            base_url = base_url.rstrip('/')
        parts = base_url.split('?')
        path = parts[0]
        query = f"?{parts[1]}" if len(parts) > 1 else ""
        return f"{path}/Pageindex/{page_index}{query}"



def convert_webp_to_jpg(image_path):
    if image_path.lower().endswith(".webp"):
        jpg_path = image_path.rsplit(".", 1)[0] + ".jpg"
        try:
            with Image.open(image_path).convert("RGB") as img:
                img.save(jpg_path, "JPEG")
            os.remove(image_path)  # Remove the original .webp
            return jpg_path
        except Exception as e:
            logging.error(f"Failed to convert WEBP to JPG: {e}")
            return "N/A"
    return image_path



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
            product_cards = await page.wait_for_selector(".product-thumbnail-container", state="attached", timeout=30000)

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
                    headless= True,
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


    
async def handle_macys(url, max_pages):
    ip_address = get_public_ip()
    
    logging.info(f"Scraping started for: {url} from IP: {ip_address}, max_pages: {max_pages}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Material", "Price", 
               "Size/Weight", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"Macy's_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    async with async_playwright() as p:
        while page_count <= max_pages:
            current_url = build_macys_pagination_url(url, page_count)
            browser = None
            page = None
            try:
                product_wrapper = ".product-thumbnail-container"
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_container = page.locator("ul.grid-x.small-up-2").first
                products = await product_container.locator("li.cell.sortablegrid-product").all()

                logging.info(f"Total products scraped: {len(products)}")
                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    
                    
                    try:
                        # Correct tag selector: h3 instead of div
                        product_name_tag = product.locator("h3.product-name.medium")
                        product_name = await product_name_tag.text_content() if await product_name_tag.count() > 0 else "N/A"
                        product_name = product_name.strip() if product_name else "N/A"
                    except Exception as e:
                        print(f"Product name extraction error: {e}")
                        product_name = "N/A"


                    price_info = []

                    try:
                        # Current price
                        current_price_tag = product.locator("span.discount.is-tier2")
                        if await current_price_tag.count() > 0:
                            current_price_text = await current_price_tag.first.text_content()
                            if current_price_text:
                                current_price = current_price_text.strip().split()[1]  # INR 433,003.00 → 433,003.00
                                price_info.append(f"Current price: INR {current_price}")

                        # Original price (strikethrough)
                        original_price_tag = product.locator("span.price-strike-sm")
                        if await original_price_tag.count() > 0:
                            original_price = await original_price_tag.first.text_content()
                            if original_price:
                                price_info.append(f"Original price: INR {original_price.strip()}")

                        # Fallback to regular price
                        if not price_info:
                            regular_price_tag = product.locator("span.price-reg.is-tier1")
                            if await regular_price_tag.count() > 0:
                                regular_price = await regular_price_tag.first.text_content()
                                if regular_price:
                                    price_info.append(f"Regular price: INR {regular_price.strip()}")

                    except Exception as e:
                        logging.warning(f"Error extracting price: {e}")
                        price_info = ["N/A"]

                    # Final output
                    price = " | ".join(price_info) if price_info else "N/A"



                    # Image extraction with fallbacks
                    try:
                        active_slideshow = product.locator('li.slideshow-item.active .picture-container source').first
                        if await active_slideshow.count() > 0:
                            image_url = await active_slideshow.get_attribute("srcset")
                        else:
                            image_url = "N/A"
                    except Exception as e:
                        image_url = "N/A"

                   

                    gold_type_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"


                    diamond_weight_match = re.search(r"\d+(?:[-/]\d+)?(?:\s+\d+/\d+)?\s*ct\s+tw", product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                    
                    
                    additional_info = []
                    
                    try:
                        # Extract discount tags like "(35% off)"
                        discount_locator = product.locator("span.sale-percent.percent-small")
                        discount_count = await discount_locator.count()

                        if discount_count > 0:
                            for i in range(discount_count):
                                discount_text = await discount_locator.nth(i).inner_text()
                                if discount_text and discount_text.strip():
                                    additional_info.append(discount_text.strip())
                        else:
                            additional_info.append("N/A")
                    except Exception as e:
                        print(f"Discount extraction error: {e}")
                        additional_info.append("N/A")


                    try:
                        # Extract promotional tags: "New", "Bonus Offer", etc.
                        tag_locator = product.locator("div.tile-buttons span, div.badge-wrapper span")
                        tag_count = await tag_locator.count()

                        if tag_count > 0:
                            for i in range(tag_count):
                                tag_text = await tag_locator.nth(i).inner_text()
                                if tag_text and tag_text.strip():
                                    additional_info.append(tag_text.strip())
                        else:
                            additional_info.append("N/A")
                    except Exception as e:
                        print(f"Tag extraction error: {e}")
                        additional_info.append("N/A")

                    # Extract Rating (e.g., "Rated 3.625 out of 5")
                    try:
                        rating_locator = product.locator("div.rating span[aria-label]")
                        if await rating_locator.count() > 0:
                            rating_text = await rating_locator.first.get_attribute("aria-label")
                            if rating_text:
                                additional_info.append(rating_text.strip())
                    except Exception as e:
                        print(f"Rating extraction error: {e}")

                    # Extract Review Count (e.g., "8 reviews")
                    try:
                        review_locator = product.locator("div.rating .rating-description span[aria-label]")
                        if await review_locator.count() > 0:
                            review_text = await review_locator.first.get_attribute("aria-label")
                            if review_text:
                                additional_info.append(review_text.strip())
                    except Exception as e:
                        print(f"Review count extraction error: {e}")

                    # Join all into a single string
                    additional_info_str = " | ".join(additional_info) if additional_info else "N/A"


                    
                    if product_name == "N/A" and price == "N/A" and image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])

                
                # Process images and update records
                for row_num, unique_id, task in image_tasks:
                    try:
                        # Wait for the image download task
                        image_path = await asyncio.wait_for(task, timeout=60)
                        
                        # image_path = convert_webp_to_jpg(image_path)
                        
                        # If image download is successful and not "N/A"
                        if image_path != "N/A":
                            try:
                                img = XLImage(image_path)
                                img.width, img.height = 100, 100
                                sheet.add_image(img, f"D{row_num}")
                            except Exception as e:
                                logging.error(f"Error embedding image: {e}")
                                image_path = "N/A"

                                                    
                           
                        
                        # Update the records with the image path
                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                                break

                    except asyncio.TimeoutError:
                        logging.warning(f"Image download timed out for row {row_num}")
                    except Exception as e:
                        logging.error(f"Error processing image task for row {row_num}, unique_id {unique_id}: {e}")

                all_records.extend(records)
                success_count += 1

                # Save progress after each page
                wb.save(file_path)
                logging.info(f"Progress saved after page {page_count}")

            except Exception as e:
                logging.error(f"Error processing page {page_count}: {str(e)}")
                wb.save(file_path)
            finally:
                if page:
                    await page.close()
                if browser:
                    await browser.close()
                
                # Add delay between pages
                await asyncio.sleep(random.uniform(2, 5))
            
            page_count += 1

    if not all_records:
        return None, None, None

    # Final save and database operations
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path


