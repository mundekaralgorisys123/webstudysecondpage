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
from proxysetup import get_browser_with_proxy_strategy
from urllib.parse import urlparse, urlunparse


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
    """Modify the image URL to replace w=440 with w=2200 and h=440 with h=2200 while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    # Check if the image URL already contains w=2200 and h=2200
    if "w=820" in image_url and "h=820" in image_url:
        return image_url

    # Replace all occurrences of w=440 and h=440 with w=2200 and h=2200 in the URL
    modified_url = re.sub(r'\bw=\d+\b', 'w=820', image_url)
    modified_url = re.sub(r'\bh=\d+\b', 'h=820', modified_url)

    return modified_url


async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    # Set up the image filename and path
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    
    # Prepare headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    
    image_url = modify_image_url(image_url)

    # Create cleaned URL without query parameters for fallback
    parsed_url = urlparse(image_url)
    clean_url = urlunparse(parsed_url._replace(query=""))

    async with httpx.AsyncClient(timeout=10.0, headers=headers, follow_redirects=True) as client:
        for attempt in range(retries):
            try:
                # Try high-res URL first (original URL)
                response = await client.get(clean_url)
                response.raise_for_status()
                
                # Save image if successful
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                logging.info(f"Successfully downloaded {product_name} image")
                return image_full_path
                
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                wait_time = 1 * (attempt + 1)
                logging.warning(f"Attempt {attempt + 1}/{retries} failed for {product_name}. Waiting {wait_time}s. Error: {str(e)}")
                await asyncio.sleep(wait_time)

        # If all retries fail, try clean URL without query parameters
        try:
            logging.info(f"Trying clean URL for {product_name}: {clean_url}")
            response = await client.get(clean_url)
            response.raise_for_status()
            
            with open(image_full_path, "wb") as f:
                f.write(response.content)
            logging.info(f"Successfully downloaded {product_name} using clean URL")
            return image_full_path
            
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logging.error(f"Final attempt failed for {product_name}: {str(e)}")
            return "N/A"
        

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))


            
def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}p={page_count}"   

async def handle_chaumet(url, max_pages):
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
    filename = f"handle_chaumet_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
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
                product_wrapper = '.products.wrapper.grid.products-grid'
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay between scrolls
                    current_product_count = await page.locator('li.item').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count


                product_container = await page.query_selector('ol.products.items.product-items.row')
                products = await product_container.query_selector_all('li.item') if product_container else []

                logging.info(f"Total products loaded: {len(products)}")


                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product name
                        name_tag = await product.query_selector('a.product__name')
                        product_name = await name_tag.inner_text() if name_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching product name: {e}")
                        product_name = "N/A"
                        
                    try:
                        # Extract price from the correct location
                        price_container = await product.query_selector('.price-wrapper .price')  # This is correct now
                        price = await price_container.inner_text() if price_container else "N/A"
                    except Exception as e:
                        print(f"Error fetching price: {e}")
                        price = "N/A"

                    
                    
                    try:
                        # Extract description (e.g., "White gold, 2.5 mm")
                        description_tag = await product.query_selector('.c-product-card__title-second')
                        kt = await description_tag.inner_text() if description_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching description: {e}")
                        kt = "N/A"    

                    try:
                        # Extract image URL from lazy-loaded image inside slick-slide
                        image_tag = await product.query_selector('.slick-slide img.lazyload')
                        if image_tag:
                            # Use data-src attribute for lazy-loaded images
                            image_url = await image_tag.get_attribute("data-src")
                            image_url = image_url.strip() if image_url else "N/A"
                        else:
                            image_url = "N/A"
                    except Exception as e:
                        print(f"Error fetching image URL: {e}")
                        image_url = "N/A"
                        
                    additional_info = []

                    try:
                        # Collect tags from both known tag classes
                        tag_els = await product.query_selector_all("span.u-gold-light")

                        if tag_els:
                            for tag_el in tag_els:
                                tag_text = await tag_el.inner_text()
                                if tag_text:
                                    additional_info.append(tag_text.strip())
                        else:
                            additional_info.append("N/A")

                    except Exception as e:
                        additional_info.append("N/A")
                        
                    try:
                        # Collect tags from both known tag classes
                        tag_els_demand = await product.query_selector_all("span.normal-price div")

                        if tag_els_demand:
                            for tag_ele in tag_els_demand:
                                tag_text1 = await tag_ele.inner_text()
                                if tag_text1:
                                    additional_info.append(tag_text1.strip())
                        else:
                            additional_info.append("N/A")

                    except Exception as e:
                        additional_info.append("N/A")
                        
                            

                    additional_info_str = " | ".join(additional_info)
                    
                    if product_name == "N/A" and price == "N/A" and image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue    
    

                    diamond_weight_match = re.search(r"\d+[-/]?\d*/?\d*\s*ct\s*tw", kt)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

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
        return None, None, None    # Final save and database operations
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path
