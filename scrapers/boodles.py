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
from flask import Flask
from PIL import Image as PILImage
from proxysetup import get_browser_with_proxy_strategy
from utils import get_public_ip, log_event, sanitize_filename
from dotenv import load_dotenv
from database import insert_into_db
from limit_checker import update_product_count
import aiohttp
from io import BytesIO
from openpyxl.drawing.image import Image as XLImage
import httpx

load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

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



async def handle_boodles(url, max_pages):
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Material", "Price", 
               "Diamond Weight", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_boodles_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0

    while page_count <= max_pages:
        current_url = f"{url}?page={page_count}" if page_count > 1 else url
        logging.info(f"Processing page {page_count}: {current_url}")
        
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                
                product_wrapper="#product-grid"
                browser, page = await get_browser_with_proxy_strategy(p, current_url ,product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    current_product_count = await page.locator('div[data-collection-item]').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count

                product_wrapper = await page.wait_for_selector("div#product-grid", timeout=5000)
                products = await product_wrapper.query_selector_all("div[data-collection-item]") if product_wrapper else []

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
                        name_element = await product.query_selector("div.text-lg.text-brand_dark_grey.pb-2")
                        product_name = await name_element.inner_text() if name_element else "N/A"
                    except Exception as e:
                        product_name = "N/A"
                        logging.error(f"Error extracting product name: {e}")

                    # Extract price
                    try:
                        price_element = await product.query_selector("span[data-price]")
                        price = await price_element.inner_text() if price_element else "N/A"
                        price_text = f"Price: {price}"
                    except Exception as e:
                        price_text = "N/A"
                        logging.error(f"Error extracting price: {e}")

                    # Extract material from product name
                    material = "N/A"
                    try:
                        material_pattern = r"\b(Platinum|Gold|Silver|Rose Gold|White Gold|Yellow Gold)\b"
                        material_match = re.search(material_pattern, product_name, re.IGNORECASE)
                        material = material_match.group() if material_match else "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting material: {e}")

                    # Extract diamond weight from product name
                    diamond_weight = "N/A"
                    try:
                        weight_pattern = r"\b(\d+(\.\d+)?\s*ct|carat)\b"
                        weight_match = re.search(weight_pattern, product_name, re.IGNORECASE)
                        diamond_weight = weight_match.group() if weight_match else "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting diamond weight: {e}")

                    # Extract product URL
                    product_url = "N/A"
                    try:
                        url_element = await product.query_selector("a[href^='/products/']")
                        if url_element:
                            product_url = await url_element.get_attribute("href")
                            if product_url and not product_url.startswith('http'):
                                product_url = f"https://www.boodles.com{product_url}"
                            additional_info.append(f"URL: {product_url}")
                    except Exception as e:
                        logging.error(f"Error extracting product URL: {e}")

                    # Extract variant options
                    try:
                        variant_elements = await product.query_selector_all("div[data-add-to-cart]")
                        if variant_elements:
                            variants = []
                            for variant in variant_elements:
                                variant_text = await variant.inner_text()
                                if variant_text and variant_text.strip():
                                    variants.append(variant_text.strip())
                            if variants:
                                additional_info.append(f"Variants: {'|'.join(variants)}")
                    except Exception as e:
                        logging.error(f"Error extracting variants: {e}")

                    # Improved image extraction
                    image_url = "N/A"
                    try:
                        # First try the main product image
                        img_element = await product.query_selector("picture img")
                        if img_element:
                            image_url = await img_element.get_attribute("src")
                        
                        # If no image found, try alternative selectors
                        if image_url == "N/A":
                            img_elements = await product.query_selector_all("img")
                            for img in img_elements:
                                src = await img.get_attribute("src")
                                if src and "boodles.com/cdn/shop/products/" in src:
                                    image_url = src
                                    break
                        
                        # Clean up image URL
                        if image_url and image_url != "N/A":
                            if image_url.startswith("//"):
                                image_url = f"https:{image_url}"
                            # Remove size parameters for higher quality image
                            image_url = image_url.split('?')[0] if '?' in image_url else image_url
                    except Exception as e:
                        logging.error(f"Error extracting image URL: {e}")
                        image_url = "N/A"
                    
                   

                    # if product_name == "N/A" or price_text == "N/A" or image_url == "N/A":
                    #     logging.warning(f"Skipping product due to missing data: Name: {product_name}, Price: {price_text}, Image: {image_url}")
                    #     continue
                    
                    # Combine all additional info
                    additional_info_text = " | ".join(additional_info) if additional_info else "N/A"

                    unique_id = str(uuid.uuid4())
                    if image_url and image_url != "N/A":
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                    records.append((unique_id, current_date, page_title, product_name, None, material, 
                                  price_text, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, material, price_text, 
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
            wb.save(file_path)
        finally:
            if page:
                await page.close()
            if browser:
                await browser.close()
            
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
