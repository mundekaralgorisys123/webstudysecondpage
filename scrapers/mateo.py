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

async def extract_best_image_url(product_element):
    try:
        # First try to get the img element inside product-item__image
        img_container = await product_element.query_selector(".product-item__image img")
        if img_container:
            # Check for data-srcset first
            srcset = await img_container.get_attribute("data-srcset")
            if srcset:
                # Parse srcset to get highest resolution image
                sources = []
                for source in srcset.split(','):
                    source = source.strip()
                    if not source:
                        continue
                    url, width = source.split(' ')
                    width = int(width.replace('w', ''))
                    sources.append((width, url))
                
                if sources:
                    # Sort by width descending and return the largest
                    sources.sort(reverse=True, key=lambda x: x[0])
                    return sources[0][1]
            
            # Fallback to src attribute if no srcset
            img_src = await img_container.get_attribute("src")
            if img_src:
                return img_src
        
        # Fallback to any img tag in the product element
        img_element = await product_element.query_selector("img")
        if img_element:
            # Check for data-srcset in fallback img
            srcset = await img_element.get_attribute("data-srcset")
            if srcset:
                sources = []
                for source in srcset.split(','):
                    source = source.strip()
                    if not source:
                        continue
                    url, width = source.split(' ')
                    width = int(width.replace('w', ''))
                    sources.append((width, url))
                
                if sources:
                    sources.sort(reverse=True, key=lambda x: x[0])
                    return sources[0][1]
            
            img_src = await img_element.get_attribute("src")
            if img_src:
                return img_src
        
        return None
    except Exception as e:
        log_event(f"Error extracting image URL: {e}")
        return None

# Async image downloader
def modify_image_url(image_url):
    if not image_url or image_url == "N/A":
        return image_url
    
    modified_url = "https:"+image_url
    return modified_url 


# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.png"
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
            await page.wait_for_selector(".collection__window", state="attached", timeout=30000)
            print("[Success] Product listing loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function
async def handle_mateo(url, max_pages=None):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

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
    filename = f"handle_mateo_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    prev_prod_count = 0
    load_more_clicks = 1
    
    while load_more_clicks <= max_pages if max_pages else True:
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = '.collection__window'
                browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)
                # browser, page = await get_browser_with_proxy_strategy(p, url, ".collection__window")
                log_event(f"Successfully loaded: {url}")

                # Scroll to load all items
                await scroll_to_bottom(page)
                
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                # Get all product tiles
                product_wrapper = await page.query_selector("div.collection__window")
                products = await page.query_selector_all("div.product-item") if product_wrapper else []
                max_prod = len(products)
                products = products[prev_prod_count: min(max_prod, prev_prod_count + 30)]
                prev_prod_count += len(products)

                if len(products) == 0:
                    log_event("No new products found, stopping the scraper.")
                    break

                logging.info(f"New products found: {len(products)}")
                print(f"New products found: {len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    try:
                        # Extract product name - from the anchor tag in product-item__details
                        name_tag = await product.query_selector(".product-item__details a")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    
                    # Handle prices - collect all price information
                    price_text = "N/A"
                    try:
                        # Get regular price
                        regular_price_tag = await product.query_selector(".price-item--regular")
                        if regular_price_tag:
                            price_text = (await regular_price_tag.inner_text()).strip()
                    except Exception as e:
                        logging.warning(f"Error getting regular price: {str(e)}")
                        price_text = "N/A"



                    # Get additional product information
                    try:
                        # Check for product type/category
                        category_element = await product.query_selector(".product-item__category")
                        if category_element:
                            category = await category_element.inner_text()
                            additional_info.append(f"Category: {category.strip()}")
                    except:
                        pass

                    try:
                        # Check for product materials
                        material_elements = await product.query_selector_all(".product-item__material")
                        if material_elements:
                            materials = [await el.inner_text() for el in material_elements]
                            additional_info.append(f"Materials: {', '.join(m.strip() for m in materials)}")
                    except:
                        pass

                    try:
                        # Check for product badges (like "New", "Bestseller")
                        badge_elements = await product.query_selector_all(".product-item__badge")
                        if badge_elements:
                            badges = [await el.inner_text() for el in badge_elements]
                            additional_info.append(f"Badges: {', '.join(b.strip() for b in badges)}")
                    except:
                        pass

                    try:
                        # Check for product variations (like sizes, colors)
                        variant_elements = await product.query_selector_all(".product-item__variant")
                        if variant_elements:
                            variants = [await el.inner_text() for el in variant_elements]
                            additional_info.append(f"Variants: {', '.join(v.strip() for v in variants)}")
                    except:
                        pass

                    # Join all additional info with pipe delimiter
                    additional_info_text = " | ".join(additional_info) if additional_info else "N/A"

                    image_url = "N/A"
                    try:
                        image_url = await extract_best_image_url(product) or "N/A"
                    except Exception as e:
                        log_event(f"Error getting image URL: {e}")
                        image_url = "N/A"

                    # Extract gold type (kt) from product name/description
                    gold_type_pattern = r"\b\d{1,2}(?:K|kt|ct|Kt)\b|\bPlatinum\b|\bSilver\b|\bWhite Gold\b|\bYellow Gold\b|\bRose Gold\b"
                    gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    # Extract diamond weight from description
                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    if image_url and image_url != "N/A":
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price_text, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, kt, price_text, diamond_weight, time_only, image_url, additional_info_text])
                            
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
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Image download timed out for row {row_num}")
                
                if max_pages:
                    load_more_clicks += 1
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