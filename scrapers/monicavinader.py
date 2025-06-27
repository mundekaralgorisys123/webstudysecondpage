import os
import re
import time
import logging
import random
from typing import List
import uuid
import asyncio
import base64
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError, Error
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from flask import Flask
from PIL import Image as PILImage
import traceback
from utils import get_public_ip, log_event, sanitize_filename
from dotenv import load_dotenv
from database import insert_into_db
from limit_checker import update_product_count
from io import BytesIO
from openpyxl.drawing.image import Image as XLImage
import httpx
# from proxysetup import get_browser_with_proxy_strategy
# Load environment variables from .env file
from functools import partial
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
    modified_url = modify_image_url(image_url)  # High-res version

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                # Try high-res version first
                response = await client.get(modified_url)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
            except httpx.HTTPStatusError as e:
                # If high-res doesn't exist, fallback to original
                if e.response.status_code == 404 and modified_url != image_url:
                    logging.warning(f"High-res not found for {product_name}, trying original URL.")
                    try:
                        response = await client.get(image_url)
                        response.raise_for_status()
                        with open(image_full_path, "wb") as f:
                            f.write(response.content)
                        return image_full_path
                    except Exception as fallback_err:
                        logging.error(f"Fallback failed for {product_name}: {fallback_err}")
                        break
                else:
                    logging.warning(f"HTTP error on attempt {attempt+1} for {product_name}: {e}")
            except httpx.RequestError as e:
                logging.warning(f"Request error on attempt {attempt+1} for {product_name}: {e}")
    
    logging.error(f"Failed to download image for {product_name} after {retries} attempts.")
    return "N/A"


def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))

async def scroll_and_wait(page):
    """Scroll down to load lazy-loaded products."""
    previous_height = await page.evaluate("document.body.scrollHeight")
    await page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
    new_height = await page.evaluate("document.body.scrollHeight")
    return new_height > previous_height  # Returns True if more content is loaded
            

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
            product_cards = await page.wait_for_selector(".header-marker", state="attached", timeout=60000)

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
                    headless=True,  # You can toggle to False for debugging
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

            await safe_goto_and_wait(page, url, isbri_data)
            return browser, page

        except Exception as e:
            last_error = e
            error_trace = traceback.format_exc()
            logging.error(f"Proxy attempt failed:\n{error_trace}")
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass  # Don't raise new exception during cleanup
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
    separator = '?' if '?' in base_url else '?'
    return f"{base_url}{separator}page={page_count}"              

async def handle_monicavinader(url, max_pages):
    
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", 
               "Time", "ImagePath", "Additional Info"]  # Added Additional Info column
    sheet.append(headers)

    all_records = []
    filename = f"handle_monicavinader_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    while page_count <= max_pages:
        current_url = build_url_with_loadmore(url, page_count)
        logging.info(f"Processing page {page_count}: {current_url}")
        
        # Create a new browser instance for each page
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                
                # product_wrapper = '.product-catalogue'
                # browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                # log_event(f"Successfully loaded: {current_url}")
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.eval_on_selector(
                        ".product-catalogue-wrap",
                        "(el) => el.scrollTo(0, el.scrollHeight)"
                    )
                    await asyncio.sleep(random.uniform(1, 2))
                    current_product_count = await page.locator('article.product-preview').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count

                product_wrapper = await page.query_selector("div.product-catalogue-wrap") 
                products = await product_wrapper.query_selector_all("article.product-preview") if product_wrapper else []
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    
                    # Extract product name
                    try:
                        name_tag = await product.query_selector("h3.product-preview__title")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"
                        
                    try:
                        desc_tag = await product.query_selector("p.product-preview__description")
                        kt= (await desc_tag.inner_text()).strip() if desc_tag else "N/A"
                    except Exception:
                        kt = "N/A"    

                    # Extract price information
                    price = "N/A"
                    try:
                        price_tag = await product.query_selector("p.product-preview__price")
                        if price_tag:
                            price = (await price_tag.inner_text()).strip()
                            # Check for original price if available (not visible in sample HTML)
                    except Exception:
                        price = "N/A"

                    # Extract material/description
                    material = "N/A"
                    try:
                        desc_tag = await product.query_selector("p.product-preview__description")
                        if desc_tag:
                            material = (await desc_tag.inner_text()).strip()
                            additional_info.append(f"Material: {material}")
                    except Exception:
                        pass

                    # Extract product URL
                    try:
                        product_link = await product.query_selector("a.product-preview__link")
                        if product_link:
                            product_url = await product_link.get_attribute("href")
                            if product_url and product_url != "N/A":
                                additional_info.append(f"URL: {product_url}")
                    except Exception:
                        pass

                    # Extract data attributes for additional info
                    try:
                        product_link = await product.query_selector("a.product-preview__link")
                        if product_link:
                            # Get all data attributes
                            data_attrs = {
                                'brand': await product_link.get_attribute("data-brand"),
                                'product_id': await product_link.get_attribute("data-gaid"),
                                'price': await product_link.get_attribute("data-price"),
                                'gbp_price': await product_link.get_attribute("data-gbp-price"),
                                'variation_id': await product_link.get_attribute("data-cnstrc-item-variation-id")
                            }
                            
                            # Add non-empty data attributes to additional info
                            for key, value in data_attrs.items():
                                if value and value != "N/A":
                                    additional_info.append(f"{key.title()}: {value}")
                    except Exception:
                        pass

                    # Extract color/material options
                    try:
                        swatches = await product.query_selector_all("button.swatch")
                        if swatches:
                            colors = []
                            for swatch in swatches:
                                color_label = await swatch.get_attribute("aria-label")
                                if color_label and color_label != "N/A":
                                    colors.append(color_label)
                            if colors:
                                additional_info.append(f"Color Options: {'|'.join(colors)}")
                    except Exception:
                        pass

                    # Check for "New In" badge
                    try:
                        new_badge = await product.query_selector("div.flash-badge--listing")
                        if new_badge:
                            badge_text = (await new_badge.inner_text()).strip()
                            if badge_text and badge_text != "N/A":
                                additional_info.append(f"Badge: {badge_text}")
                    except Exception:
                        pass

                    # Extract image URLs (primary and hover)
                    image_url = "N/A"
                    hover_image_url = "N/A"
                    try:
                        # Primary image
                        primary_img = await product.query_selector("figure.product-preview__image--no-blend img.product-listing__image")
                        if primary_img:
                            image_url = await primary_img.get_attribute("src")
                            if image_url and image_url != "N/A":
                                if not image_url.startswith(('http://', 'https://')):
                                    image_url = f"https:{image_url}" if image_url.startswith('//') else f"https://www.monicavinader.com{image_url}"
                        
                        # Hover image
                        hover_img = await product.query_selector("figure.product-preview__image--hover img.product-listing__image")
                        if hover_img:
                            hover_image_url = await hover_img.get_attribute("src")
                            if hover_image_url and hover_image_url != "N/A":
                                if not hover_image_url.startswith(('http://', 'https://')):
                                    hover_image_url = f"https:{hover_image_url}" if hover_image_url.startswith('//') else f"https://www.monicavinader.com{hover_image_url}"
                                additional_info.append("Has hover image")
                    except Exception:
                        image_url = "N/A"

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue
                    
                    # Extract gold type from product name and material
                    

                    # Extract diamond weight from product name
                    diamond_weight = "N/A"
                    try:
                        diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                        diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                        diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                    except Exception:
                        diamond_weight = "N/A"

                    # Combine all additional info with | separator
                    additional_info_text = " | ".join(additional_info) if additional_info else ""

                    unique_id = str(uuid.uuid4())
                    if image_url and image_url != "N/A":
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))


                    product_name = f"{product_name} {kt}"
                    records.append((unique_id, current_date, page_title, product_name, None, kt, 
                                  price, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, kt, price, 
                                diamond_weight, time_only, image_url, additional_info_text])

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
                                records[i] = (record[0], record[1], record[2], record[3], image_path, 
                                             record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Timeout downloading image for row {row_num}")

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

    
    
    # Database operations
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