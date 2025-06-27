import os
import re
import time
import logging
import random
import uuid
import asyncio
import base64
from datetime import datetime
from playwright.async_api import async_playwright
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
from proxysetup import get_browser_with_proxy_strategy
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

async def download_and_resize_image(session, image_url):
    try:
        async with session.get(modify_image_url1(image_url), timeout=10) as response:
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


def modify_image_url1(image_url):
    """Modify the image URL to replace width=100 with width=2200 while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    # Check if the image URL already contains width=2200
    if "width=2200" in image_url:
        return image_url

    # Replace all occurrences of width=100 with width=2200 in the entire URL
    modified_url = re.sub(r'\bwidth=100\b', 'width=2200', image_url)

    return modified_url



async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    # Set up the image filename and the full image path
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    # print("image_url")
    # print(image_url)


    # Modify the image URL to get the high-resolution version
    high_res_url = modify_image_url1(image_url)
    # print("high_res_url")
    # print(high_res_url)

    # Try to download the high-resolution image first
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(high_res_url)
                response.raise_for_status()  # If status code is 200-299, proceed
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path  # Successfully downloaded the high-res image
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name} (High Res): {e}")
            
        # If high resolution fails, attempt to download the original resolution image
        logging.info(f"Falling back to original resolution for {product_name}.")
        try:
            # Attempt to download the original image (without modification)
            async with client:
                response = await client.get(image_url)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
        except httpx.RequestError as e:
            logging.error(f"Failed to download {product_name} after {retries} attempts.")
            return "N/A"
        

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))


def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    # If page_count is 1, return the base URL without appending page param
    if page_count == 1:
        return re.sub(r'([&?])page=\d+', '', base_url).rstrip('?&')
    
    # Remove existing page param if present
    base_url = re.sub(r'([&?])page=\d+', '', base_url).rstrip('?&')
    
    # Add the new page parameter
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}page={page_count}"           

async def handle_mejuri(url, max_pages):
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
    filename = f"handle_mejuri_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
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
                product_wrapper = 'ul[data-testid="products-list-page"]'
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay between scrolls
                    current_product_count = await page.locator('div[data-testid="product-card"]').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count


                product_wrapper = await page.query_selector('ul[data-testid="products-list-page"]')
                products = await product_wrapper.query_selector_all('div[data-testid="product-card"]') if product_wrapper else []
                logging.info(f"Total products found on page {page_count}: {len(products)}")


                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        name_tag = await product.query_selector('a[data-testid="internal-link"]')
                        if name_tag:
                            product_name = (await name_tag.inner_text()).strip()
                        else:
                            product_name = "N/A"
                    except Exception as e:
                        print(f"Error fetching product name: {e}")
                        product_name = "N/A"




                    try:
                        price_container = await product.query_selector('div.flex.gap-x-xs.flex-wrap')
                        if price_container:
                            original_price_tag = await price_container.query_selector('span.line-through')
                            if original_price_tag:
                                # If "original price" exists (on sale), take that
                                price = await original_price_tag.inner_text()
                            else:
                                # If no "original price", just grab the normal price
                                price_span = await price_container.query_selector('span')
                                price = await price_span.inner_text() if price_span else "N/A"
                        else:
                            price = "N/A"
                    except Exception as e:
                        print(f"Error fetching price: {e}")
                        price = "N/A"


                    try:
                        # Wait for the product container that contains the desired image
                        product_container = await product.query_selector(".relative.overflow-hidden.z-base.flex-shrink-0.snap-start.mx-px.md\\:mx-0.md\\:col-start-1.md\\:row-start-1.w-full.h-full.object-cover")
                        
                        # Locate the img element within that container
                        image_tag = await product_container.query_selector("img") if product_container else None
                        
                        # Get the image URL from the 'src' attribute
                        image_url = await image_tag.get_attribute("src") if image_tag else "N/A"
                       
                        
                    except Exception as e:
                        print(f"Error fetching image URL: {e}")
                        image_url = "N/A"


                    try:
                        # Extract the product variant info
                        variant_tag = await product.query_selector('span[data-testid="variant-selector-label"]')
                        if variant_tag:
                            kt = await variant_tag.inner_text()
                        else:
                            kt = "N/A"
                    except Exception as e:
                        print(f"Error fetching product variant: {e}")
                        kt = "N/A"
                        
                    additional_info = []

                    try:
                        tag_els = await product.query_selector_all("div.bg-content-inv.text-content.capitalize")
                        if tag_els:
                            for tag_el in tag_els:
                                tag_text = await tag_el.inner_text()
                                if tag_text:
                                    additional_info.append(tag_text.strip())
                        else:
                            additional_info.append("N/A")

                    except Exception as e:
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)    
                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue  
                    
                      
                    


                    diamond_weight_match = re.search(r"\d+[-/]?\d*/?\d*\s*ct\s*tw", kt)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))
                    product_name = f"{product_name} {kt}"
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
