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
from database import insert_into_db
from limit_checker import update_product_count
from io import BytesIO
import httpx
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
# Load environment variables from .env file
from proxysetup import get_browser_with_proxy_strategy


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

async def download_and_resize_image(session, image_url):
    try:
        async with session.get(upgrade_to_high_res_url(image_url), timeout=10) as response:
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

def upgrade_to_high_res_url(image_url):
    """Attempt to replace the cache directory with one that points to higher resolution."""
    if not image_url or image_url == "N/A":
        return image_url
    return re.sub(
        r"/cache/[^/]+/",  # Match cache folder
        "/cache/38c3c1b8e53ef11aa9803a5390245afc/",
        image_url
    )

async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    high_res_url = upgrade_to_high_res_url(image_url)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                # Try high-res first
                response = await client.get(high_res_url)
                if response.status_code == 200:
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    return image_full_path
                else:
                    logging.warning(f"High-res image not found, falling back to original for {product_name}")
                    break  # Exit and try original
            except httpx.RequestError as e:
                logging.warning(f"Attempt {attempt + 1}: Failed to get high-res for {product_name}: {e}")
                break  # On error, go to fallback

        # Fallback to original
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
    
    logging.error(f"Failed to download image for {product_name} after {retries} attempts.")
    return "N/A"

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))



# async def safe_goto_and_wait(page, url, retries=3):
#     for attempt in range(retries):
#         try:
#             print(f"[Attempt {attempt + 1}] Navigating to: {url}")
#             await page.goto(url, timeout=180_000, wait_until="domcontentloaded")


#             # Wait for the selector with a longer timeout
            
#             product_cards = await page.wait_for_selector(".products.wrapper.grid.products-grid", state="attached", timeout=30000)


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

        
def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    url_parts = urlparse(base_url)
    query_params = parse_qs(url_parts.query)

    # Remove rfk if it exists
    query_params.pop('rfk', None)

    # Set pagination
    query_params['p'] = [str(page_count)]

    new_query = urlencode(query_params, doseq=True)
    return urlunparse((url_parts.scheme, url_parts.netloc, url_parts.path, '', new_query, ''))

async def handle_reeds(url, max_pages):
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
    filename = f"handle_reeds_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
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
                product_wrapper=".products.wrapper.grid.products-grid"
                browser, page = await get_browser_with_proxy_strategy(p, current_url,product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay between scrolls
                    current_product_count = await page.locator('.product-item').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count


                product_wrapper = await page.wait_for_selector(".products.wrapper.grid.products-grid", timeout=30000)
                products = await product_wrapper.query_selector_all("li.product-item")
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    
                    try:
                        # Product Name
                        product_name = await (await product.query_selector("a.product-item-link")).inner_text()
                        product_name = product_name.strip() if product_name else "N/A"
                    except:
                        product_name = "N/A"

                    # Price handling with original and discounted prices
                    price_info = []
                    try:
                        # Get special/discounted price
                        special_price_el = await product.query_selector(".special-price .price")
                        if special_price_el:
                            discounted_price = await special_price_el.inner_text()
                            price_info.append(discounted_price.strip())
                            
                            # Get original price if available
                            original_price_el = await product.query_selector(".price-box .old-price .price")
                            if original_price_el:
                                original_price = await original_price_el.inner_text()
                                price_info.append(original_price.strip())
                                
                                # Calculate discount percentage if possible
                                try:
                                    disc_num = float(discounted_price.replace('$', '').replace(',', '').strip())
                                    orig_num = float(original_price.replace('$', '').replace(',', '').strip())
                                    discount_pct = round((1 - (disc_num / orig_num)) * 100)
                                    additional_info.append(f"Discount: {discount_pct}%")
                                except:
                                    pass
                        else:
                            # Regular price
                            regular_price_el = await product.query_selector(".price-box .price")
                            if regular_price_el:
                                price_info.append((await regular_price_el.inner_text()).strip())
                    except:
                        price_info = ["N/A"]
                    
                    price = " | ".join(price_info) if price_info else "N/A"

                    # Image URL
                    try:
                        image_el = await product.query_selector("span.product-image-wrapper img.product-image-photo")
                        image_url = await image_el.get_attribute("src")
                        if image_url and image_url.startswith("//"):
                            image_url = "https:" + image_url
                    except:
                        image_url = "N/A"

                    # Gold/Karat Type
                    gold_type_match = re.findall(
                        r"(\d{1,2}(?:/\d{1,2})?ctw?\s*(?:Yellow|White|Rose)?\s*Gold|Platinum|Sterling Silver|Gold-Plated|Rhodium-Plated)",
                        product_name,
                        re.IGNORECASE
                    )
                    kt = ", ".join(gold_type_match) if gold_type_match else "N/A"

                    # Diamond Weight
                    diamond_weight_match = re.findall(
                        r"(\d+(?:\.\d+)?(?:/\d+)?\s*ctw?)",
                        product_name,
                        re.IGNORECASE
                    )
                    diamond_weight = ", ".join(diamond_weight_match) if diamond_weight_match else "N/A"

                    # Additional Product Info
                    try:
                        # Stock status
                        stock_el = await product.query_selector(".rfk_oos")
                        if stock_el:
                            stock_status = await stock_el.inner_text()
                            if stock_status and stock_status.lower() != "out of stock":
                                additional_info.append(f"Stock: {stock_status.strip()}")
                        
                        # Brand
                        brand_el = await product.query_selector(".rfkx_brand")
                        if brand_el:
                            brand = await brand_el.inner_text()
                            if brand.strip():
                                additional_info.append(f"Brand: {brand.strip()}")
                        
                        # Product labels (sale, new, etc.)
                        label_el = await product.query_selector(".rfk_condition")
                        if label_el:
                            label = await label_el.inner_text()
                            if label.strip():
                                additional_info.append(f"Label: {label.strip()}")
                        
                        # Product category
                        category = await product.get_attribute("category")
                        if category:
                            additional_info.append(f"Category: {category}")
                        
                        # Product ID
                        product_id = await product.get_attribute("id")
                        if product_id:
                            additional_info.append(f"Product ID: {product_id}")
                        
                        # Color options (if available)
                        color_options = await product.query_selector_all(".rfk_alt-images img")
                        if color_options and len(color_options) > 1:
                            additional_info.append(f"Color options: {len(color_options)}")
                    
                    except Exception as e:
                        logging.warning(f"Error extracting additional info: {e}")

                    # Combine all additional info with pipe delimiter
                    additional_info_str = " | ".join(additional_info) if additional_info else ""

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight, additional_info_str))
                    sheet.append([
                        current_date, 
                        page_title, 
                        product_name, 
                        None,  # Image placeholder (will be added later)
                        kt, 
                        price, 
                        diamond_weight, 
                        time_only, 
                        image_url,
                        additional_info_str
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


    # Final save and database operations
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path