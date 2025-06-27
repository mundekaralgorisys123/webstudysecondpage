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
import httpx
from PIL import Image as PILImage
from openpyxl import Workbook
from openpyxl.drawing.image import Image as ExcelImage
from flask import Flask
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import json
import mimetypes
from proxysetup import get_browser_with_proxy_strategy
# Load environment
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

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

mimetypes.add_type('image/webp', '.webp')

# Modified extract_best_image_url function
async def extract_best_image_url(picture_element):
    try:
        if not picture_element:
            return None
            
        # First try to get JPG sources
        sources = await picture_element.query_selector_all("source[type='image/jpg'], source[type='image/jpeg']")
        
        # If no JPG sources, try WEBP
        if not sources:
            sources = await picture_element.query_selector_all("source[type='image/webp']")
        
        # If still no sources, try the img tag directly
        if not sources:
            img_tag = await picture_element.query_selector("img")
            if img_tag:
                img_src = await img_tag.get_attribute("src")
                if img_src:
                    return img_src if img_src.startswith(("http:", "https:")) else f"https:{img_src}" if img_src.startswith("//") else f"https://www.pomellato.com{img_src}"
            return None
        
        # Find the highest resolution source
        best_url = None
        max_width = 0
        
        for source in sources:
            try:
                media = await source.get_attribute("media") or ""
                srcset = await source.get_attribute("srcset") or ""
                
                # Extract width from media query if available
                width_match = re.search(r"min-width:\s*(\d+)px", media)
                if width_match:
                    width = int(width_match.group(1))
                else:
                    # Or extract from srcset (e.g., "564_564/image.jpg")
                    size_match = re.search(r"/(\d+)_\d+/", srcset)
                    width = int(size_match.group(1)) if size_match else 0
                    
                if width > max_width:
                    max_width = width
                    best_url = srcset.split(" ")[0]  # Take the first URL in srcset
                    
            except Exception as e:
                log_event(f"Error processing image source: {e}")
                continue
                
        if best_url:
            return best_url 
        return None
        
    except Exception as e:
        log_event(f"Error extracting best image: {e}")
        return None
    
# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"  # Always save as JPG
    image_full_path = os.path.join(image_folder, image_filename)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                img_data = response.content
                
                # Convert WEBP to JPG if needed
                if image_url.lower().endswith('.webp'):
                    try:
                        img = PILImage.open(BytesIO(img_data))
                        if img.format == 'WEBP':
                            buffer = BytesIO()
                            img.convert('RGB').save(buffer, format="JPEG", quality=85)
                            img_data = buffer.getvalue()
                    except Exception as e:
                        log_event(f"Error converting WEBP to JPG: {e}")
                        continue
                
                with open(image_full_path, "wb") as f:
                    f.write(img_data)
                return image_full_path
                
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
    
    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"

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

# Main scraper function
async def handle_davidmarshalllondon(url, max_pages=None):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", 
               "Time", "ImagePath", "Additional Info"]  # Added Additional Info column
    sheet.append(headers)

    all_records = []
    filename = f"handle_davidmarshalllondon_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    browser = None
    page = None
    
    try:
        async with async_playwright() as p:
            product_wrapper =  "li.product"
            browser, page = await get_browser_with_proxy_strategy(p, url,product_wrapper)
            log_event(f"Successfully loaded: {url}")

            # Scroll to load all items
            await scroll_to_bottom(page)
            
            page_title = await page.title()
            current_date = datetime.now().strftime("%Y-%m-%d")
            time_only = datetime.now().strftime("%H.%M")

            # Get all product tiles
            product_wrapper = await page.query_selector("ul.inner.clearfix")
            product_tiles = await product_wrapper.query_selector_all("li.product") if product_wrapper else []

            logging.info(f"Total products found: {len(product_tiles)}")
            records = []
            image_tasks = []
            
            for row_num, product in enumerate(product_tiles, start=len(sheet["A"]) + 1):
                additional_info = []
                
                try:
                    # Extract product name
                    name_tag = await product.query_selector("p.product-overlay span")
                    product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                except Exception:
                    product_name = "N/A"

                # Initialize price as N/A (price not visible in the HTML provided)
                price = "N/A"

                # Extract product metadata from class attributes
                try:
                    product_classes = await product.get_attribute("class")
                    if product_classes:
                        classes = product_classes.split()
                        metadata = {
                            'type': None,
                            'material': None,
                            'gemstones': []
                        }
                        
                        for cls in classes:
                            if cls.startswith('product-type_'):
                                metadata['type'] = cls.split('_')[1].replace('-', ' ')
                            elif cls.startswith('product-material_'):
                                metadata['material'] = cls.split('_')[1].replace('-', ' ')
                            elif cls.startswith('product-gemstone_'):
                                gemstone = cls.split('_')[1].replace('-', ' ')
                                metadata['gemstones'].append(gemstone)
                        
                        # Add metadata to additional info
                        if metadata['type']:
                            additional_info.append(f"Type: {metadata['type'].title()}")
                        if metadata['material']:
                            additional_info.append(f"Material: {metadata['material'].title()}")
                        if metadata['gemstones']:
                            additional_info.append(f"Gemstones: {', '.join([g.title() for g in metadata['gemstones']])}")
                except Exception as e:
                    logging.error(f"Error extracting product metadata: {e}")

                # Extract image URL
                image_url = "N/A"
                try:
                    image_div = await product.query_selector("div.product-image")
                    if image_div:
                        image_url_style = await image_div.get_attribute("style")
                        if "background-image: url(" in image_url_style:
                            # Extract the URL from the background-image style
                            image_url = image_url_style.split("url(")[1].split(")")[0].strip('"\'')
                            # Ensure the URL has the proper protocol
                            if not image_url.startswith(("http://", "https://")):
                                image_url = f"https://www.davidmarshalllondon.com{image_url}"
                            
                            # Check for image dimensions in URL and try to get highest quality
                            if '-w265' in image_url:
                                image_url = image_url.replace('-w265', '')  # Remove size parameter
                except Exception as e:
                    log_event(f"Error getting image URL: {e}")
                    image_url = "N/A"

                # Extract product URL
                try:
                    product_link = await product.query_selector("a")
                    product_url = await product_link.get_attribute("href") if product_link else "N/A"
                    if product_url and product_url != "N/A":
                        additional_info.append(f"Product URL: {product_url}")
                except Exception:
                    pass

                # Extract gold type (kt) from product name and metadata
                gold_type = "Not found"
                try:
                    # First try to get from product name
                    gold_type_pattern = r"\b\d{1,2}(?:K|kt|ct)\b|\bRose Gold\b|\bWhite Gold\b|\bYellow Gold\b|\bPlatinum\b|\bSilver\b"
                    gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                    gold_type = gold_type_match.group() if gold_type_match else "Not found"
                    
                    # If not found in name, check metadata
                    if gold_type == "Not found" and 'material' in locals():
                        material_match = re.search(gold_type_pattern, metadata.get('material', ''), re.IGNORECASE)
                        if material_match:
                            gold_type = material_match.group()
                except Exception:
                    gold_type = "Not found"

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

                records.append((unique_id, current_date, page_title, product_name, None, gold_type, 
                              price, diamond_weight, additional_info_text))
                sheet.append([current_date, page_title, product_name, None, gold_type, price, 
                            diamond_weight, time_only, image_url, additional_info_text])
            
            # Process image downloads
            for row_num, unique_id, task in image_tasks:
                try:
                    image_path = await asyncio.wait_for(task, timeout=60)
                    if image_path != "N/A":
                        try:
                            img = ExcelImage(image_path)
                            img.width, img.height = 100, 100
                            sheet.add_image(img, f"D{row_num}")
                        except Exception as e:
                            logging.error(f"Error embedding image: {e}")
                            image_path = "N/A"
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (record[0], record[1], record[2], record[3], image_path, 
                                         record[5], record[6], record[7], record[8])
                            break
                except asyncio.TimeoutError:
                    logging.warning(f"Image download timed out for row {row_num}")

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