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

# Transform URL to get high-res image


def modify_image_url(image_url):
    if not image_url or image_url == "N/A":
        return image_url
    image_url = "https:" + image_url
    return image_url


# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A":
        return "N/A"
    
    try:
        # Ensure the URL is properly formatted
        if image_url.startswith('//'):
            image_url = f"https:{image_url}"
        elif image_url.startswith('/'):
            image_url = f"https://www.johnhardy.com{image_url}"
        
        # Clean up the URL by removing query parameters and fragments
        clean_url = image_url.split('?')[0].split('#')[0]
        
        # Create a safe filename
        
        extension = clean_url.split('.')[-1].lower()
        if extension not in ['jpg', 'jpeg', 'png', 'webp']:
            extension = 'jpg'  # default extension
            
        filename = f"{unique_id}_{timestamp}.jpg"
        filepath = os.path.join(image_folder, filename)
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(clean_url)
            response.raise_for_status()
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
                
            return filepath
            
    except httpx.RequestException as e:
        logging.warning(f"Failed to download image {image_url}: {str(e)}")
        return "N/A"
    except Exception as e:
        logging.warning(f"Unexpected error downloading image {image_url}: {str(e)}")
        return "N/A"


def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}page={page_count}"

# Main scraper function


async def handle_jonehardy(url, max_pages):
    ip_address = get_public_ip()
    logging.info(
        f"Scraping started for: {url} from IP: {ip_address}, max_pages: {max_pages}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt",
               "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_jonehardy_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1

    while (page_count <= max_pages):
        current_url = build_url_with_loadmore(url, page_count)

        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        page = None

        try:
            async with async_playwright() as p:
                product_wrapper = ".product-card__wrapper"
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('.main-collection__grid').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_wrapper = await page.query_selector("div.main-collection__grid")
                products = await page.query_selector_all("div.product-card")

                logging.info(
                    f"Total products scraped:{page_count} :{len(products)}")
                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    try:
                        # Extract product name
                        name_tag = await product.query_selector("p.product-card__title")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    try:
                        # Default price
                        price = "N/A"

                        # Try extracting sale price first
                        sale_price_tag = await product.query_selector("span.product-prices__price--sale")
                        if sale_price_tag:
                            sale_price_text = (await sale_price_tag.inner_text()).strip()
                            if sale_price_text:
                                price = sale_price_text
                                # Also get original price
                                original_price_tag = await product.query_selector("span.product-prices__price")
                                if original_price_tag:
                                    original_price_text = (await original_price_tag.inner_text()).strip()
                                    if original_price_text:
                                        additional_info.append(
                                            f"Original Price: {original_price_text}")
                        else:
                            # No sale price, use regular price
                            price_tag = await product.query_selector("span.product-prices__price")
                            if price_tag:
                                price_text = (await price_tag.inner_text()).strip()
                                price = price_text

                        # Clean price string
                        if price != "N/A":
                            price = price.replace(
                                '$', '').replace(',', '').strip()

                    except Exception:
                        price = "N/A"

                    try:
                        image_tag = await product.query_selector("img.images__image")
                        if image_tag:
                            srcset = await image_tag.get_attribute("srcset")
                            if srcset:
                                # Get last (highest resolution)
                                image_url = "https:" + \
                                    srcset.split(",")[-1].split()[0]
                            else:
                                image_url = await image_tag.get_attribute("src")
                                if image_url and image_url.startswith("//"):
                                    image_url = "https:" + image_url
                        else:
                            image_url = "N/A"
                    except:
                        image_url = "N/A"

                    # print(image_url)

                   # Check for product status (e.g., NEW, EXCLUSIVE, etc.)
                    try:
                        badge_tag = await product.query_selector("span.product-card__badge")
                        if badge_tag:
                            badge_text = (await badge_tag.inner_text()).strip()
                            if badge_text:
                                additional_info.append(f"Status: {badge_text}")
                    except Exception:
                        pass  # Silently ignore errors, or you can log if needed

                    # Extract metal type and gemstone information
                    gold_type_pattern = r"\b\d{1,2}(?:K|ct)?\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSterling Silver\b"
                    gold_type_match = re.search(
                        gold_type_pattern, product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                    diamond_weight_match = re.search(
                        diamond_weight_pattern, product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                    # Join all additional info with | delimiter
                    additional_info_text = " | ".join(
                        additional_info) if additional_info else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(
                            image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name,
                                   None, kt, price, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, kt, price,
                                 diamond_weight, time_only, image_url, additional_info_text])

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
                                records[i] = (record[0], record[1], record[2], record[3],
                                              image_path, record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(
                            f"Image download timed out for row {row_num}")

                all_records.extend(records)
                wb.save(file_path)

        except Exception as e:
            logging.error(f"Error on page {page_count}: {str(e)}")
            wb.save(file_path)
        finally:
            if page:
                await page.close()
            if browser:
                await browser.close()
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
