import os
import re
import logging
import random
import uuid
import asyncio
import base64
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError, Error
from openpyxl import Workbook
from openpyxl.drawing.image import Image
import httpx
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
from proxysetup import get_browser_with_proxy_strategy
# Load environment variables from .env file



BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

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

async def scroll_and_wait(page):
    """Scroll down to load lazy-loaded products."""
    previous_height = await page.evaluate("document.body.scrollHeight")
    await page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
    await asyncio.sleep(2)  # Allow time for content to load
    new_height = await page.evaluate("document.body.scrollHeight")
    return new_height > previous_height  # Returns True if more content is loaded
    

def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}p={page_count}"      


async def handle_anguscoote(url, max_pages):
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
    filename = f"anguscoote_data_{timestamp}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0
    current_url = url
    while page_count <= max_pages:
        # base_url = url.split('?')[0]
       
        current_url = build_url_with_loadmore(url, page_count)
        
        logging.info(f"Processing page {page_count}: {current_url}")
        
        # Create a new browser instance for each page
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = "div.ps-category-items"
                browser, page = await get_browser_with_proxy_strategy(p, current_url,product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load content
                scroll_attempts = 0
                while scroll_attempts < 3 and await scroll_and_wait(page):
                    scroll_attempts += 1
                    await random_delay(1, 2)

                # Process products on current page
                product_wrapper = await page.query_selector("div.ps-category-items")
                products = await product_wrapper.query_selector_all("div.ps-category-item") if product_wrapper else []
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product data
                        
                        try:
                            name_elem = await product.query_selector("div.s-product__name")
                            product_name = await name_elem.inner_text() if name_elem else "N/A"
                        except:
                            product_name = "N/A"
                            
                        try:
                            # Try to get both discounted and original prices
                            offer_elem = await product.query_selector("span.s-price__now")
                            was_elem = await product.query_selector("div.s-product__price")

                            offer_price = await offer_elem.inner_text() if offer_elem else ""
                            original_price = await was_elem.inner_text() if was_elem else ""

                            if offer_price and original_price:
                                price = f"{offer_price} offer {original_price}"
                            elif offer_price:
                                price = offer_price
                            elif original_price:
                                price = original_price
                            else:
                                price = "N/A"
                        except Exception:
                            price = "N/A"


                            
                        try:
                            img_elem = await product.query_selector("img")
                            image_url = await img_elem.get_attribute("src") if img_elem else "N/A"
                            if not image_url and img_elem:
                                image_url = await img_elem.get_attribute("data-src") or "N/A"
                        except:
                            image_url = "N/A"        
                        
                        
                        
                        additional_info = []

                        try:
                            # Extract "Sale" flag if present
                            sale_flag_elem = await product.query_selector("div.s-product__flag.s-flag.s-flag--sale")
                            if sale_flag_elem:
                                additional_info.append("Sale")
                            else:
                                additional_info.append("N/A")

                        

                        except Exception as e:
                            additional_info.append("N/A")

                        additional_info_str = " | ".join(additional_info)
                        
                        if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                            print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                            continue     

                        
                        # Extract gold and diamond info
                       # Extract gold karat and color (e.g., "9CT GOLD", "14CT WHITE GOLD")
                        kt_match = re.search(r"\b\d{1,2}CT(?:\s+(?:YELLOW|WHITE))?\s+GOLD\b", product_name, re.IGNORECASE)
                        kt = kt_match.group().upper() if kt_match else "N/A"

                        # Extract diamond weight (e.g., "1 CARAT TW", "1.20 CARATS TW", "1/2 CARAT TW", "TDW=.25CT")
                        diamond_match = re.search(r"(\d+(?:\.\d+)?|\d+/\d+)\s*(?:CARAT(?:S)?\s*TW|CT|TDW=\.?\d+CT)", product_name.upper())
                        diamond_weight = diamond_match.group() if diamond_match else "N/A"


                        unique_id = str(uuid.uuid4())
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                        records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                        sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])

                    except Exception as e:
                        logging.error(f"Error extracting product data: {e}")
                        continue

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
