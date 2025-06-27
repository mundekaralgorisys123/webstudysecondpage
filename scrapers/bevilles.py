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
from database import insert_into_db, create_table
from limit_checker import update_product_count
import httpx
import traceback
from typing import List, Tuple
from urllib.parse import urlparse, urlunparse

load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")


PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

def modify_image_url(image_url):
    """Modify the image URL to replace any _### (e.g., _180, _260, _400, etc.) with _1200 while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"

    modified_url = re.sub(r'_(\d{2,4})(?=x?\.\w+$)', '_1200', image_url)
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

async def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def scroll_and_wait(page, max_attempts=10, wait_time=1):
    """Scroll down and wait for new content to load dynamically."""
    last_height = await page.evaluate("document.body.scrollHeight")

    for attempt in range(max_attempts):
        logging.info(f"Scroll attempt {attempt + 1}/{max_attempts}")
        
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")

        try:
            await page.wait_for_selector(".product-item", state="attached", timeout=3000)
        except:
            logging.info("No new content detected.")
        
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            logging.info("No more new content. Stopping scroll.")
            break
        
        last_height = new_height
        await asyncio.sleep(wait_time)

    logging.info("Finished scrolling.")
    return True

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
            product_cards = await page.wait_for_selector(".ss__has-results", state="attached", timeout=30000)

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
    parsed = urlparse(base_url)
    base = parsed._replace(query='', fragment='')  # Remove existing query/fragment temporarily
    page_part = f"?page={page_count}" if page_count > 1 else ""
    
    # Rebuild base URL
    url_with_page = urlunparse(base) + page_part
    
    # Add back fragment (filter)
    if parsed.fragment:
        url_with_page += f"#{parsed.fragment}"
    
    return url_with_page



async def handle_bevilles(url, max_pages):
    """Async version of Bevilles scraper"""
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} | IP: {ip_address} | Max pages: {max_pages}")

    # Prepare folders
    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    # Prepare Excel workbook
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    current_date = datetime.now().strftime("%Y-%m-%d")
    time_only = datetime.now().strftime("%H.%M")
    page_count = 1

    all_records = []
    filename = f"handle_bevilles_{current_date}_{time_only}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    current_url = url
    prev_prod_count = 0
    while current_url and page_count <= max_pages:
        logging.info(f"Processing page {page_count}: {current_url}")
        # Create a new browser instance for each page
        browser = None
        page = None
        
        current_url = build_url_with_loadmore(url, page_count)

        logging.info(f"Navigating to {current_url}")
        try:
            async with async_playwright() as p:
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")

                await scroll_and_wait(page, max_attempts=8)

                page_title = await page.title()
                products = await page.query_selector_all(".ss__result")
                logging.info(f"Total products scraped on page {page_count}: {len(products)}")
                products = products[prev_prod_count:]
                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product details
                        product_name_tag = await product.query_selector("a.boost-pfs-filter-product-item-title")
                        product_name = (await product_name_tag.inner_text()).strip() if product_name_tag else "N/A"

                        # Price handling - get both original and sale price if available
                        price = "N/A"
                        original_price = "N/A"
                        price_tag = await product.query_selector("span.boost-pfs-filter-product-item-sale-price")
                        if price_tag:
                            price = (await price_tag.inner_text()).strip()
                            
                            # Check for original price (strikethrough)
                            original_price_tag = await product.query_selector("p.boost-pfs-filter-product-item-price s")
                            if original_price_tag:
                                original_price = (await original_price_tag.inner_text()).strip()
                        
                        # Format price string to include both original and sale price if available
                        price_str = f"{original_price} | {price}" if original_price != "N/A" else price

                        image_tag = await product.query_selector("img.boost-pfs-filter-product-item-main-image")
                        if image_tag:
                            data_srcset = await image_tag.get_attribute("data-srcset") or ""
                            product_urls = [url.split(" ")[0] for url in data_srcset.split(",") if url.startswith("https://")]
                            image_url = product_urls[0] if product_urls else "N/A"
                        else:
                            image_url = "N/A"
                            
                        if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                            print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                            continue         

                        # Extract Kt
                        gold_type_pattern = r"\b\d{1,2}K\s+\w+(?:\s+\w+)?\b"
                        gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                        kt = gold_type_match.group() if gold_type_match else "Not found"

                        # Extract diamond weight
                        diamond_weight_pattern = r"(\d+(?:[./-]\d+)?(?:\s*/\s*\d+)?\s*ct(?:\s*tw)?)"
                        diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                        diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                        # Extract additional info
                        additional_info = []
                        
                        # Check for sale badge
                        try:
                            sale_badge = await product.query_selector(".ss__badge-rectangle--sale")
                            if sale_badge:
                                sale_text = (await sale_badge.inner_text()).strip()
                                if sale_text:
                                    additional_info.append(f"Label: {sale_text}")
                        except:
                            pass
                        
                        # Check for discount percentage
                        try:
                            discount_tag = await product.query_selector(".product-badge.on-sale")
                            if discount_tag:
                                discount_text = (await discount_tag.inner_text()).strip()
                                if discount_text:
                                    additional_info.append(discount_text)
                        except:
                            pass
                        
                        # Check for rating
                        try:
                            rating_container = await product.query_selector(".ruk_rating_snippet")
                            if rating_container:
                                # Count filled stars
                                filled_stars = await rating_container.query_selector_all(".ruk-icon-percentage-star--100")
                                rating = len(filled_stars)
                                
                                # Get review count
                                review_count_tag = await rating_container.query_selector(".ruk-rating-snippet-count")
                                review_count = (await review_count_tag.inner_text()).strip() if review_count_tag else ""
                                
                                if rating > 0:
                                    additional_info.append(f"Rating: {rating}/5 {review_count}")
                        except:
                            pass
                        
                        # Check for payment options
                        try:
                            payment_option = await product.query_selector(".installement-limespot")
                            if payment_option:
                                payment_text = (await payment_option.inner_text()).strip()
                                if payment_text:
                                    additional_info.append(f"Payment: {payment_text.split('with')[0].strip()}")
                        except:
                            pass
                        
                        # Join all additional info with pipe separator
                        additional_info_str = " | ".join(additional_info) if additional_info else "N/A"

                        # Schedule image download
                        unique_id = str(uuid.uuid4())
                        image_tasks.append((
                            row_num,
                            unique_id,
                            asyncio.create_task(
                                download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                        records.append((
                            unique_id,
                            current_date,
                            page_title,
                            product_name,
                            None,  # Placeholder for image path
                            kt,
                            price_str,  # Now includes both original and sale price
                            diamond_weight,
                            additional_info_str
                        ))

                        sheet.append([
                            current_date,
                            page_title,
                            product_name,
                            None,  # Placeholder for image
                            kt,
                            price_str,
                            diamond_weight,
                            time_only,
                            image_url,
                            additional_info_str
                        ])

                    except Exception as e:
                        logging.error(f"Error processing product {row_num}: {e}")
                        continue

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

                # Save progress after each page
                wb.save(file_path)
                logging.info(f"Progress saved after page {page_count}")
                page_count += 1
                prev_prod_count += len(products)
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

    if not all_records:
        return None, None, None
    # Final save and database operations
    wb.save(file_path)
    logging.info(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path