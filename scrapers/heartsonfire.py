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
from proxysetup import get_browser_with_proxy_strategy
# Load environment
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

# Flask and paths
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



# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
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

# Reliable page.goto wrapper
async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            await page.wait_for_selector(".product-grid", state="attached", timeout=30000)
            print("[Success] Product cards loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function
async def handle_heartsonfire(url, max_pages):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}, max_pages: {max_pages}")

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
    filename = f"handle_heartsonfire_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    current_url = url
    while (page_count <= max_pages):
        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        page = None
        if '?' in url:
            match = re.split(r"&sz=\d+", url)
            if len(match) == 2:
                part1, _ = match
            else:
                part1 = url
            current_url = f"{part1}&start={12*(page_count-1)}&sz=12"
        else:
            current_url = f"{url}?start={12*(page_count-1)}&sz=12"  # Fixed the URL parameter format
        try:
            async with async_playwright() as p:
                browser, page = await get_browser_with_proxy_strategy(p, current_url, "div.product-grid")
                log_event(f"Successfully loaded: {current_url}")
            
                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('.product-grid').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_wrapper = await page.query_selector("div.product-grid")
                products = await product_wrapper.query_selector_all("div.product-grid-tile") if product_wrapper else []
                logging.info(f"Total products scraped:{page_count} :{len(products)}")
                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    
                    try:
                        # Product name extraction with multiple fallbacks
                        name_tag = await product.query_selector(".pdp-link .animated-line") or \
                                  await product.query_selector(".product-tile .js-gtm-product-tile-name") or \
                                  await product.query_selector("h2.product-name") or \
                                  await product.query_selector(".product-name")
                        
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                        
                        # Clean up product name
                        if product_name != "N/A":
                            product_name = ' '.join(product_name.split())  # Remove extra whitespace
                            
                    except Exception as e:
                        logging.error(f"Error extracting product name: {e}")
                        product_name = "N/A"

                    try:
                        # Price handling
                        price_tag = await product.query_selector(".price .value")
                        price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                        
                        # Check for sale price vs regular price
                        sale_price_tag = await product.query_selector(".price .sales .value")
                        regular_price_tag = await product.query_selector(".price .strike-through")
                        
                        if sale_price_tag and regular_price_tag:
                            sale_price = (await sale_price_tag.inner_text()).strip()
                            regular_price = (await regular_price_tag.inner_text()).strip()
                            price = f"{sale_price}|{regular_price}"
                            
                            # Calculate discount percentage if possible
                            try:
                                sale_num = float(sale_price.replace('$', '').replace(',', ''))
                                regular_num = float(regular_price.replace('$', '').replace(',', ''))
                                discount_percent = int(round((1 - (sale_num / regular_num)) * 100))
                                additional_info.append(f"Discount: {discount_percent}%")
                            except:
                                pass
                        elif price_tag:
                            price = (await price_tag.inner_text()).strip()
                        else:
                            price = "N/A"
                            
                    except Exception as e:
                        logging.error(f"Error extracting price: {e}")
                        price = "N/A"

                    try:
                        # Improved image URL extraction
                        image_tag = await product.query_selector(".tile-image-primary source") or \
                                   await product.query_selector(".tile-image source") or \
                                   await product.query_selector("picture source")
                        
                        if image_tag:
                            srcset = await image_tag.get_attribute("srcset")
                            if srcset:
                                # Take the highest resolution image from srcset
                                urls = [url for url in srcset.split() if url.startswith('http')]
                                image_url = urls[-1] if urls else "N/A"
                            else:
                                image_url = await image_tag.get_attribute("src") or "N/A"
                        else:
                            # Fallback to img tag if source not found
                            img_tag = await product.query_selector(".tile-image-primary img") or \
                                      await product.query_selector(".tile-image img") or \
                                      await product.query_selector("picture img")
                            image_url = await img_tag.get_attribute("src") if img_tag else "N/A"
                            
                        # Ensure URL is complete
                        if image_url != "N/A" and image_url.startswith("//"):
                            image_url = "https:" + image_url
                            
                    except Exception as e:
                        logging.error(f"Error extracting image URL: {e}")
                        image_url = "N/A"

                    # Gold type detection from swatches
                    gold_type = "Not found"
                    try:
                        active_swatch = await product.query_selector(".swatch-button.active")
                        if active_swatch:
                            gold_type = await active_swatch.get_attribute("aria-label")
                            if gold_type:
                                # Extract just the metal type (e.g., "18K White Gold")
                                parts = gold_type.split(",")
                                if len(parts) > 1:
                                    gold_type = parts[1].strip()
                                else:
                                    gold_type = parts[0].strip()
                                    
                                # Add to additional info
                                additional_info.append(f"Metal: {gold_type}")
                    except Exception as e:
                        logging.error(f"Error extracting gold type: {e}")

                    # Diamond weight from name
                    diamond_weight = "N/A"
                    try:
                        diamond_weight_match = re.search(r"\b\d+(\.\d+)?\s*(?:ct|tcw)\b", product_name, re.IGNORECASE)
                        if diamond_weight_match:
                            diamond_weight = diamond_weight_match.group()
                            additional_info.append(f"Diamond Weight: {diamond_weight}")
                    except Exception as e:
                        logging.error(f"Error extracting diamond weight: {e}")

                    # Extract product badges/labels
                    try:
                        badges = []
                        badge_tags = await product.query_selector_all(".tile-badge div")
                        for badge in badge_tags:
                            badge_text = (await badge.inner_text()).strip()
                            if badge_text:
                                badges.append(badge_text)
                        if badges:
                            additional_info.append(f"Badges: {'|'.join(badges)}")
                    except Exception as e:
                        logging.error(f"Error extracting badges: {e}")

                    # Extract product ID if available
                    try:
                        product_id = await product.get_attribute("data-pid")
                        if product_id:
                            additional_info.append(f"Product ID: {product_id}")
                    except Exception as e:
                        logging.error(f"Error extracting product ID: {e}")

                    # Combine all additional info
                    additional_info_text = "|".join(additional_info) if additional_info else ""

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, gold_type, price, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, gold_type, price, diamond_weight, time_only, image_url, additional_info_text])
                    
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
                        # Update records with image path
                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Image download timed out for row {row_num}")

                all_records.extend(records)
                wb.save(file_path)
                
        except Exception as e:
            logging.error(f"Error on page {page_count}: {str(e)}")
            wb.save(file_path)
        finally:
            if page: await page.close()
            if browser: await browser.close()
            await asyncio.sleep(random.uniform(2, 5))

        page_count += 1

    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path