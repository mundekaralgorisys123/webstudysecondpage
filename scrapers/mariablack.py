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

# Human-like delay
def random_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))

# Scroll to bottom of page to load all products
async def scroll_to_bottom(page, max_wait_time=60):
    """Scrolls to the bottom of the page until no more content is loaded or timeout."""
    import time
    start_time = time.time()
    
    last_height = await page.evaluate("() => document.body.scrollHeight")
    
    while True:
        # Scroll to the bottom of the page
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(1.5, 3.0))  # Slightly longer wait for dynamic content

        # Wait for any lazy-loaded content to appear
        try:
            await page.wait_for_timeout(1000)  # 1 second delay
        except:
            pass

        # Check scroll height again
        new_height = await page.evaluate("() => document.body.scrollHeight")

        if new_height == last_height:
            print("✅ Reached the bottom of the page.")
            break

        if time.time() - start_time > max_wait_time:
            print("⚠️ Scroll timed out after max_wait_time.")
            break

        last_height = new_height


# Main scraper function
async def handle_mariablack(url, max_pages=None):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Material", "Price", "Gemstone Info", 
               "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_mariablack_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    
    try:
        async with async_playwright() as p:
            browser, page = await get_browser_with_proxy_strategy(p, url, "div.listing-page")
            log_event(f"Successfully loaded: {url}")

            # Scroll to load all items
            await scroll_to_bottom(page)
            
            page_title = await page.title()
            current_date = datetime.now().strftime("%Y-%m-%d")
            time_only = datetime.now().strftime("%H.%M")

            # Get all product tiles
            product_tiles = await page.query_selector_all("div[data-product-listing-result-id]")
            logging.info(f"Total products found: {len(product_tiles)}")
            
            records = []
            image_tasks = []
            
            for row_num, product in enumerate(product_tiles, start=len(sheet["A"]) + 1):
                additional_info = []
                
                # Extract product name and material
                product_name = "N/A"
                material = "N/A"
                try:
                    name_tag = await product.query_selector("h2.css-1en0x6u")
                    material_tag = await product.query_selector("p.css-1w0gz9y")
                    
                    name_text = (await name_tag.inner_text()).strip() if name_tag else ""
                    material_text = (await material_tag.inner_text()).strip() if material_tag else ""
                    
                    if name_text and material_text:
                        product_name = f"{name_text} - {material_text}"
                    elif name_text:
                        product_name = name_text
                    else:
                        product_name = "N/A"
                        
                    material = material_text if material_text else "N/A"
                except Exception as e:
                    logging.error(f"Error getting product name/material: {str(e)}")
                    product_name = "N/A"
                    material = "N/A"

                # Extract price information
                price_text = "N/A"
                try:
                    price_tag = await product.query_selector("p.css-1p1d7hf")
                    if price_tag:
                        price = (await price_tag.inner_text()).strip()
                        price_text = f"Price: {price}"
                except Exception as e:
                    logging.error(f"Error getting price: {str(e)}")
                    price_text = "N/A"

                # Extract product URL
                product_url = "N/A"
                try:
                    product_link = await product.query_selector("a.css-1qj8w5r")
                    if product_link:
                        product_url = await product_link.get_attribute("href")
                        if product_url and product_url != "N/A":
                            if not product_url.startswith('http'):
                                product_url = f"https://www.maria-black.com{product_url}"
                            additional_info.append(f"URL: {product_url}")
                except Exception as e:
                    logging.error(f"Error getting product URL: {str(e)}")

                # Check for "New" badge
                try:
                    new_badge = await product.query_selector("div.css-1kc361l")
                    if new_badge:
                        badge_text = (await new_badge.inner_text()).strip()
                        if badge_text and badge_text != "N/A":
                            additional_info.append(f"Status: {badge_text}")
                except Exception as e:
                    logging.error(f"Error getting badge: {str(e)}")

                # Extract image URLs - improved extraction
                image_url = "N/A"
                try:
                    # First try the main product image container
                    image_container = await product.query_selector("a.css-rdseb6")
                    if image_container:
                        # Try getting the first img tag within the container
                        img_tag = await image_container.query_selector("img")
                        if img_tag:
                            image_url = await img_tag.get_attribute("src")
                            if image_url and image_url != "N/A":
                                if not image_url.startswith('http'):
                                    image_url = f"https:{image_url}" if image_url.startswith('//') else f"https://www.maria-black.com{image_url}"
                                # Clean up URL parameters if needed
                                image_url = image_url.split('?')[0] if '?' in image_url else image_url
                    
                    # If still no image, try alternative selectors
                    if image_url == "N/A":
                        img_tags = await product.query_selector_all("img")
                        for img in img_tags:
                            src = await img.get_attribute("src")
                            if src and "maria-black-products.imgix.net" in src:
                                image_url = src.split('?')[0] if '?' in src else src
                                break
                except Exception as e:
                    logging.error(f"Error getting image URL: {str(e)}")
                    image_url = "N/A"

                if product_name == "N/A" or price_text == "N/A" or image_url == "N/A":
                    logging.warning(f"Skipping product due to missing data: Name: {product_name}, Price: {price_text}, Image: {image_url}")
                    continue
                
                # Extract gemstone information from product name
                gemstone_info = "N/A"
                try:
                    gemstone_pattern = r"\b(Diamond|Ruby|Sapphire|Emerald|Aquamarine|Pearl|Onyx|Topaz|Opal|Amethyst|Citrine|Garnet|Peridot)\b"
                    gemstone_match = re.search(gemstone_pattern, product_name, re.IGNORECASE)
                    gemstone_info = gemstone_match.group() if gemstone_match else "N/A"
                except Exception as e:
                    logging.error(f"Error getting gemstone info: {str(e)}")
                    gemstone_info = "N/A"

                # Combine all additional info with | separator
                additional_info_text = " | ".join(additional_info) if additional_info else ""

                unique_id = str(uuid.uuid4())
                if image_url and image_url != "N/A":
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                records.append((unique_id, current_date, page_title, product_name, None, material, 
                              price_text, gemstone_info, additional_info_text))
                sheet.append([current_date, page_title, product_name, None, material, price_text, 
                            gemstone_info, time_only, image_url, additional_info_text])
            
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

    # Final save and database operations
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path