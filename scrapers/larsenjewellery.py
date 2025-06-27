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
# Load environment variables from .env file
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

async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    """Download and save an image with improved error handling and SVG detection"""
    if not image_url or image_url == "N/A":
        return "N/A"
    
    # Skip SVG placeholder images
    if image_url.startswith("data:image/svg+xml"):
        logging.info(f"Skipping SVG placeholder for {product_name}")
        return "N/A"
    
    # Fix malformed URLs (double slashes)
    if image_url.startswith("https://"):
        image_url = image_url.replace("https://", "https://", 1)
    
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                # Try original URL first
                response = await client.get(image_url)
                if response.status_code != 200:
                    # Try modifying the URL for higher resolution
                    modified_url = modify_image_url(image_url)
                    if modified_url != image_url:
                        response = await client.get(modified_url)
                        response.raise_for_status()
                
                # Check if we got an actual image
                content_type = response.headers.get('content-type', '')
                if not content_type.startswith('image/'):
                    logging.warning(f"Invalid content type {content_type} for {product_name}")
                    return "N/A"
                
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                
                # Verify the downloaded image
                try:
                    with PILImage.open(image_full_path) as img:
                        img.verify()
                    return image_full_path
                except Exception as verify_error:
                    logging.warning(f"Image verification failed for {product_name}: {verify_error}")
                    os.remove(image_full_path)
                    return "N/A"
                    
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(random.uniform(1, 3))
    
    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"

def modify_image_url(image_url):
    """Modify the image URL to get higher resolution while keeping query parameters"""
    if not image_url or image_url == "N/A":
        return image_url
    
    # Handle SVG placeholders
    if image_url.startswith("data:image/svg+xml"):
        return "N/A"
    
    # Fix common URL issues
    if image_url.startswith("//"):
        image_url = f"https:{image_url}"
    elif image_url.startswith("/"):
        # You might need to prepend the base URL here if needed
        pass
    
    # Replace common low-res patterns with high-res
    replacements = [
        ('_260.', '_1200.'),
        ('-300x300.', '-1200x1200.'),
        ('_small.', '_large.'),
        ('thumbnail', 'full'),
    ]
    
    for old, new in replacements:
        if old in image_url:
            image_url = image_url.replace(old, new)
            break
    
    return image_url

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))


# async def safe_goto_and_wait(page, url, retries=3):
#     for attempt in range(retries):
#         try:
#             print(f"[Attempt {attempt + 1}] Navigating to: {url}")
#             await page.goto(url, timeout=180_000, wait_until="domcontentloaded")


#             # Wait for the selector with a longer timeout
#             product_cards = await page.wait_for_selector(".products", state="attached", timeout=30000)

#             # Optionally validate at least 1 is visible (Playwright already does this)
#             if product_cards:
#                 print("[Success] Product cards loaded.")
#                 return
#         except Error as e:
#             logging.error(f"Error navigating to {url} on attempt {attempt + 1}: {e}")
#             if attempt < retries - 1:
#                 logging.info("Retrying after waiting a bit...")
#                 random_delay(1, 3)  # Add a delay before retrying
#             else:
#                 logging.error(f"Failed to navigate to {url} after {retries} attempts.")
#                 raise
#         except TimeoutError as e:
#             logging.warning(f"TimeoutError on attempt {attempt + 1} navigating to {url}: {e}")
#             if attempt < retries - 1:
#                 logging.info("Retrying after waiting a bit...")
#                 random_delay(1, 3)  # Add a delay before retrying
#             else:
#                 logging.error(f"Failed to navigate to {url} after {retries} attempts.")
#                 raise
########################################  safe_goto_and_wait ####################################################################


async def safe_goto_and_wait(page, url,isbri_data, retries=2):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            
            if isbri_data:
                await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            else:
                await page.goto(url, wait_until="networkidle", timeout=180_000)

            # Wait for the selector with a longer timeout
            product_cards = await page.wait_for_selector(".products", state="attached", timeout=30000)

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



async def handle_larsenjewellery(url, max_pages):
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
    filename = f"handle_larsenjewellery_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    while page_count <= max_pages:
        if page_count == 1:
            current_url = url
        else:
            current_url = f"{url}/page/{page_count}"

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
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay between scrolls
                    current_product_count = await page.locator('.col-lg-3.col-6.product').count()

                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count

                products = await page.query_selector_all("div.col-lg-3.col-6.product")
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    product_name = "N/A"
                    price = "N/A"
                    image_url = "N/A"
                    kt = "N/A"
                    diamond_weight = "N/A"
                    unique_id = str(uuid.uuid4())

                    try:
                        # Product Name
                        name_element = await product.query_selector("h2.name")
                        product_name = (await name_element.inner_text()).strip() if name_element else "N/A"
                        
                        # Product Description
                        desc_element = await product.query_selector("p.product-description")
                        if desc_element:
                            description = (await desc_element.inner_text()).strip()
                            if description and description.lower() != product_name.lower():
                                additional_info.append(f"Description: {description}")
                    except Exception as e:
                        logging.error(f"Error getting product name/description: {e}")

                    try:
                        # Price handling - check for multiple price elements
                        price_elements = await product.query_selector_all("p.price-from")
                        if price_elements:
                            prices = []
                            for elem in price_elements:
                                price_text = (await elem.inner_text()).strip()
                                if price_text:
                                    prices.append(price_text)
                            
                            if len(prices) > 1:
                                price = " | ".join(prices)
                                additional_info.append(f"Multiple price options")
                            else:
                                price = prices[0] if prices else "N/A"
                    except Exception as e:
                        logging.error(f"Error getting price: {e}")

                    try:
                        # Image URL
                        await product.scroll_into_view_if_needed()
                        img_tag = await product.query_selector("img.attachment-full.size-full:not([src^='data:image/svg+xml'])")
                        if not img_tag:
                            img_tag = await product.query_selector("img:not([src^='data:image/svg+xml'])")
                        
                        if img_tag:
                            # Prioritize data-src or data-lazy-src if available (common lazy loading pattern)
                            image_url = await img_tag.get_attribute("data-src") or \
                                    await img_tag.get_attribute("data-lazy-src") or \
                                    await img_tag.get_attribute("src")
                            
                            # Try srcset for higher resolution
                            srcset = await img_tag.get_attribute("srcset")
                            if srcset:
                                srcset_parts = [p.strip() for p in srcset.split(",") if p.strip()]
                                if srcset_parts:
                                    # Get the highest resolution image from srcset
                                    try:
                                        srcset_parts.sort(key=lambda x: int(x.split(" ")[1].replace("w", "")))
                                        image_url = srcset_parts[-1].split(" ")[0]
                                    except (IndexError, ValueError):
                                        pass
                            
                            # Ensure we have a valid URL
                            if image_url and not image_url.startswith("data:image/svg+xml"):
                                if image_url.startswith("//"):
                                    image_url = f"https:{image_url}"
                                elif image_url.startswith("/"):
                                    # You might need to prepend the base URL here
                                    pass
                            else:
                                image_url = "N/A"
                        else:
                            image_url = "N/A"
                    except Exception as e:
                        logging.error(f"Error getting image URL: {e}")
                        image_url = "N/A"

                    # Metal type (kt)
                    try:
                        gold_type_match = re.search(r"\b\d+K\s+\w+\s+\w+\b", product_name, re.IGNORECASE)
                        if not gold_type_match:
                            gold_type_match = re.search(r"\b(?:Yellow|White|Rose)\s+Gold\b", product_name, re.IGNORECASE)
                        if not gold_type_match:
                            gold_type_match = re.search(r"\b(?:Platinum|Silver)\b", product_name, re.IGNORECASE)
                        kt = gold_type_match.group() if gold_type_match else "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting metal type: {e}")

                    # Diamond weight
                    try:
                        diamond_weight_match = re.search(r"(\d+\.?\d*)\s*(?:ct|ctw|carat|carats)", product_name, re.IGNORECASE)
                        diamond_weight = f"{diamond_weight_match.group(1)} ct" if diamond_weight_match else "N/A"
                        
                        # If not found in name, check in description
                        if diamond_weight == "N/A" and "Description" in "|".join(additional_info):
                            desc_text = "|".join(additional_info)
                            diamond_weight_match = re.search(r"(\d+\.?\d*)\s*(?:ct|ctw|carat|carats)", desc_text, re.IGNORECASE)
                            diamond_weight = f"{diamond_weight_match.group(1)} ct" if diamond_weight_match else "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting diamond weight: {e}")

                    # Additional product info
                    try:
                        # Check for product ID
                        product_id = await product.get_attribute("data-id")
                        if product_id:
                            additional_info.append(f"Product ID: {product_id}")
                        
                        # Check for availability
                        availability_elem = await product.query_selector(".stock-status")
                        if availability_elem:
                            availability = (await availability_elem.inner_text()).strip()
                            if availability:
                                additional_info.append(f"Availability: {availability}")
                        
                        # Check for badges or special tags
                        badge_elements = await product.query_selector_all(".badge, .tag, .label")
                        if badge_elements:
                            badges = []
                            for badge in badge_elements:
                                badge_text = (await badge.inner_text()).strip()
                                if badge_text and badge_text.lower() not in ["new", "sale", "hot"]:
                                    badges.append(badge_text)
                            if badges:
                                additional_info.append(f"Tags: {', '.join(badges)}")
                        
                        # Check for color options
                        color_elements = await product.query_selector_all(".color-option, .swatch-color")
                        if color_elements:
                            colors = []
                            for color in color_elements:
                                color_name = await color.get_attribute("title") or await color.get_attribute("alt") or await color.get_attribute("data-color")
                                if color_name:
                                    colors.append(color_name.strip())
                            if colors:
                                additional_info.append(f"Color options: {', '.join(set(colors))}")
                    except Exception as e:
                        logging.error(f"Error getting additional info: {e}")

                    # Prepare additional info string
                    additional_info_str = " | ".join(additional_info) if additional_info else "N/A"

                    # Schedule image download
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    # Add to records
                    records.append((
                        unique_id, current_date, page_title, product_name, None, 
                        kt, price, diamond_weight, time_only, image_url, additional_info_str
                    ))
                    sheet.append([
                        current_date, page_title, product_name, None, 
                        kt, price, diamond_weight, time_only, 
                        image_url, additional_info_str
                    ])

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
                        
                        # Update the image path in records
                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (
                                    record[0], record[1], record[2], record[3], image_path, 
                                    record[5], record[6], record[7], record[8], record[9], record[10]
                                )
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

    # Final save and database operations
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path
