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

# Reliable page.goto wrapper
async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            await page.wait_for_selector("div.grid", state="attached", timeout=30000)
            print("[Success] Product listing loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function
async def handle_fernandojorge(url, max_pages=None):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

    # Initialize directories and files
    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_fernandojorge_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    try:
        async with async_playwright() as p:
            # Browser setup
            product_wrapper = "div.grid"
            browser, page = await get_browser_with_proxy_strategy(p, url,product_wrapper)
            await scroll_to_bottom(page)
            
            # Enhanced product data extraction with more fields
            product_data = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('.product-card-grid-item')).map(product => {
                    try {
                        const nameElem = product.querySelector('p.product-item__title');
                        const priceElem = product.querySelector('span.price span.new-price');
                        const comparePriceElem = product.querySelector('span.price span.old-price');
                        const imgElem = product.querySelector('figure.image-wrapper img');
                        const availabilityElem = product.querySelector('.product-availability');
                        const descriptionElem = product.querySelector('.product-item__description');
                        const variantElems = product.querySelectorAll('.product-option-item');
                        
                        // Collect all hover images
                        const hoverImages = Array.from(product.querySelectorAll('hover-images img')).map(img => img.src);
                        
                        // Collect variants if available
                        const variants = Array.from(variantElems).map(v => v.innerText.trim()).filter(Boolean);
                        
                        return {
                            name: nameElem?.innerText?.trim() || 'N/A',
                            price: priceElem?.innerText?.trim() || 'N/A',
                            comparePrice: comparePriceElem?.innerText?.trim() || null,
                            imageUrl: imgElem?.src || 'N/A',
                            hoverImages: hoverImages,
                            availability: availabilityElem?.innerText?.trim() || null,
                            description: descriptionElem?.innerText?.trim() || null,
                            variants: variants.length ? variants : null
                        };
                    } catch (e) {
                        console.error('Error parsing product:', e);
                        return {name: 'N/A', price: 'N/A', imageUrl: 'N/A'};
                    }
                });
            }""")

            current_date = datetime.now().strftime("%Y-%m-%d")
            time_only = datetime.now().strftime("%H.%M")
            page_title = await page.title()

            # Close browser early since we don't need it anymore
            await page.close()
            await browser.close()

            # Process collected data
            image_tasks = []
            for idx, data in enumerate(product_data, start=1):
                try:
                    product_name = data['name']
                    price = data['price']
                    compare_price = data['comparePrice']
                    image_url = data['imageUrl']
                    additional_info = []

                    # Handle price with discount if available
                    if compare_price and compare_price != price:
                        price = f"{price}|{compare_price}"
                        additional_info.append(f"Discount: {compare_price} → {price.split('|')[0]}")

                    # URL formatting
                    if image_url.startswith("//"):
                        image_url = f"https:{image_url}"
                    elif image_url.startswith("/"):
                        image_url = f"https://fernandojorge.co.uk{image_url}"

                    # Collect additional images
                    if data['hoverImages'] and len(data['hoverImages']) > 0:
                        additional_images = [img for img in data['hoverImages'] if img != image_url]
                        if additional_images:
                            additional_info.append(f"Additional Images: {'|'.join(additional_images[:3])}")

                    # Add availability if present
                    if data['availability']:
                        additional_info.append(f"Availability: {data['availability']}")

                    # Add variants if present
                    if data['variants']:
                        additional_info.append(f"Options: {'|'.join(data['variants'])}")

                    # Add description if present
                    if data['description']:
                        additional_info.append(f"Description: {data['description']}")

                    # Specifications extraction
                    gold_type_match = re.search(
                        r"\b\d{1,2}K\b|\bRose Gold\b|\bWhite Gold\b|\bYellow Gold\b|\bPlatinum\b", 
                        product_name, 
                        re.I
                    )
                    kt = gold_type_match.group() if gold_type_match else "N/A"

                    diamond_match = re.search(
                        r"\b\d+(?:\.\d+)?\s?(?:ct|tcw|carat|diamond)\b", 
                        product_name + (data['description'] or ''), 
                        re.I
                    )
                    diamond_weight = diamond_match.group() if diamond_match else "N/A"

                    # Combine all additional info
                    additional_info_str = "|".join(additional_info) if additional_info else ""

                    unique_id = str(uuid.uuid4())
                    row_num = idx + 1  # Account for header row

                    # Schedule image download
                    if image_url and image_url != "N/A":
                        task = asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )
                        image_tasks.append((row_num, unique_id, task))

                    # Add to records and sheet
                    all_records.append((
                        unique_id, current_date, 
                        page_title, product_name, 
                        None, kt, price, diamond_weight,
                        additional_info_str
                    ))
                    sheet.append([
                        current_date, page_title, product_name,
                        None, kt, price, diamond_weight,
                        time_only, image_url, additional_info_str
                    ])

                except Exception as e:
                    logging.error(f"Error processing product {idx}: {str(e)}")

            # Process images after browser closure
            for row_num, unique_id, task in image_tasks:
                try:
                    image_path = await asyncio.wait_for(task, timeout=60)
                    if image_path != "N/A":
                        img = ExcelImage(image_path)
                        img.width, img.height = 100, 100
                        sheet.add_image(img, f"D{row_num}")
                except Exception as e:
                    logging.error(f"Image processing error: {str(e)}")

            wb.save(file_path)
            log_event(f"Data saved to {file_path}")

    except Exception as e:
        logging.error(f"Critical error: {str(e)}")
        wb.save(file_path)
    
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