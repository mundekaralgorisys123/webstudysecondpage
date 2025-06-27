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
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import json
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
    if not image_url or image_url == "N/A" or not image_url.startswith(('http://', 'https://')):
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                
                # Convert WebP to JPEG if needed
                if image_url.lower().endswith('.webp'):
                    img = PILImage.open(BytesIO(response.content))
                    buffer = BytesIO()
                    img.convert("RGB").save(buffer, format="JPEG", quality=85)
                    content = buffer.getvalue()
                else:
                    content = response.content
                
                with open(image_full_path, "wb") as f:
                    f.write(content)
                return image_full_path
            except (httpx.HTTPError, httpx.InvalidURL) as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
                if attempt == retries - 1:
                    logging.error(f"Failed to download {product_name} after {retries} attempts.")
                    return "N/A"
                await asyncio.sleep(random.uniform(1, 3))
            except Exception as e:
                logging.error(f"Unexpected error downloading image: {e}")
                return "N/A"
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
            await page.wait_for_selector(".products", state="attached", timeout=30000)
            print("[Success] Product cards loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise


def get_next_page_url(current_url, next_page_number):
    parsed_url = urlparse(current_url)
    query_params = parse_qs(parsed_url.query)

    # Update the 'paged' parameter
    query_params['paged'] = [str(next_page_number)]

    # Reconstruct the query string
    new_query = urlencode(query_params, doseq=True)

    # Rebuild the final URL
    new_url = urlunparse(parsed_url._replace(query=new_query))
    return new_url

# Main scraper function
async def handle_cerrone(url, max_pages):
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
    filename = f"handle_cerrone_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    current_url = url
    while current_url and (page_count <= max_pages):
        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        page = None
        if page_count > 1:
            if '?' in current_url:
                current_url = get_next_page_url(current_url, page_count)
            else:
                current_url = f"{url}/page/{page_count}/"
        try:
            async with async_playwright() as p:
                
                product_wrapper = ".product-grid-container"
                browser, page = await get_browser_with_proxy_strategy(p, current_url,product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('.products').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_wrapper = await page.query_selector("ul#product-grid")
                products = await product_wrapper.query_selector_all("li.grid__item") if product_wrapper else []
                logging.info(f"Total products scraped:{page_count} :{len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    print(f"Processing product {row_num-1} of {len(products)}")
                    additional_info = []
                    
                    try:
                        name_tag = await product.query_selector("a.full-unstyled-link")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"


                    # Improved price extraction
                   

                    try:
                        price_tags = await product.query_selector_all(".price-item")
                        prices = []

                        for price_tag in price_tags:
                            price_text = (await price_tag.inner_text()).strip()
                            if price_text and any(c.isdigit() for c in price_text):
                                # Clean price text and format consistently (keep currency symbols like $ or A$)
                                clean_price = re.sub(r'[^A-Z$€£¥\d.,]', '', price_text)
                                if clean_price:
                                    prices.append(clean_price)

                        if len(prices) > 1:
                            price = " | ".join(prices)
                            additional_info.append("Multiple prices available")
                        elif prices:
                            price = prices[0]
                        else:
                            price = "N/A"
                    except Exception:
                        price = "N/A"

                    try:
                        image_tag = await product.query_selector("img")

                        if image_tag:
                            # First check for the highest resolution in srcset
                            srcset = await image_tag.get_attribute("srcset")
                            image_url = None

                            if srcset:
                                # Get the largest image URL from srcset (last one is usually the biggest)
                                sources = [s.strip().split() for s in srcset.split(',') if s.strip()]
                                sources.sort(key=lambda x: int(x[1].replace('w', '')) if len(x) > 1 else 0)
                                image_url = "https:" + sources[-1][0] if sources else None
                            else:
                                # Fallback to src
                                src = await image_tag.get_attribute("src")
                                if src and not src.startswith("data:image"):
                                    image_url = "https:" + src
                                else:
                                    image_url = "N/A"
                        else:
                            image_url = "N/A"

                    except Exception as e:
                        logging.warning(f"Error getting image URL: {e}")
                        image_url = "N/A"


                    # Extract product details from name and other elements
                    details_text = product_name

                    # Extract gold type
                    gold_type_pattern = r"\b\d{1,2}(?:K|ct)?\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b"
                    gold_type_match = re.search(gold_type_pattern, details_text, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    # Extract diamond weight and gemstone information
                    
                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, details_text, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                        

                    # Join all additional info with | delimiter
                    additional_info_text = "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, additional_info_text])

                for row_num, unique_id, task in image_tasks:
                    try:
                        image_path = await asyncio.wait_for(task, timeout=60)
                        if image_path != "N/A":
                            try:
                                # Open the image and convert to JPEG if needed
                                img = PILImage.open(image_path)
                                if image_path.lower().endswith('.webp'):
                                    jpeg_path = image_path.replace('.webp', '.jpg')
                                    img.convert("RGB").save(jpeg_path, format="JPEG", quality=85)
                                    image_path = jpeg_path
                                
                                # Create Excel image object
                                excel_img = ExcelImage(image_path)
                                excel_img.width, excel_img.height = 100, 100
                                sheet.add_image(excel_img, f"D{row_num}")
                                
                                # Update records
                                for i, record in enumerate(records):
                                    if record[0] == unique_id:
                                        records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                                        break
                            except Exception as e:
                                logging.error(f"Error embedding image: {e}")
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

