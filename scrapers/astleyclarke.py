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
    
    # Replace width and height parameters in the URL
    modified_url = re.sub(r'width=\d+', 'width=1080', image_url)
    modified_url = re.sub(r'height=\d+', 'height=1080', modified_url)
    
    # Remove any other size-related parameters that might affect quality
    modified_url = modified_url.replace('optimize=low', 'optimize=high')
    
    return modified_url

# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
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

# Main scraper function
async def handle_astleyclarke(url, max_pages):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}, max_pages: {max_pages}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Material", "Price", "Gemstone Info", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_astleyclarke_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    current_url = url
    while (page_count <= max_pages):
        logging.info(f"Processing page {page_count}: {current_url}")
        if page_count > 1:
            current_url = f"{url}?p={page_count}"
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                browser, page = await get_browser_with_proxy_strategy(p, current_url, "ol.products.list.items.product-items")
                log_event(f"Successfully loaded: {current_url}")
                
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_wrapper = await page.query_selector("ol.products.list.items.product-items")
                products = await product_wrapper.query_selector_all("li.item.product.product-item") if product_wrapper else []
                logging.info(f"Total products scraped:{page_count} :{len(products)}")
                records = []
                image_tasks = []
                print(f"Total products scraped: {len(products)}")
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    try:
                        name_tag = await product.query_selector("h2.product-item-name a")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    # Handle prices
                    price_text = "N/A"
                    try:
                        price_tag = await product.query_selector(".price-wrapper")
                        price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                        
                        # Check if there's a "As low as" label
                        price_label_tag = await product.query_selector(".price-label")
                        if price_label_tag:
                            price_label = (await price_label_tag.inner_text()).strip()
                            price_text = f"{price_label}: {price}"
                        else:
                            price_text = price
                    except Exception:
                        price_text = "N/A"

                    # Get additional product information
                    try:
                        # Check for gemstone information
                        gemstone_match = re.search(r"\b(Malachite|Mother of Pearl|Onyx|Lapis|Diamond|Ruby|Sapphire|Emerald|Aquamarine|Pearl)\b", 
                                                 product_name, re.IGNORECASE)
                        if gemstone_match:
                            additional_info.append(f"Gemstone: {gemstone_match.group()}")
                    except:
                        pass

                    try:
                        # Check for product type (necklace, earrings, etc.)
                        product_type_match = re.search(r"\b(Necklace|Earrings|Bracelet|Ring|Pendant|Locket|Choker)\b", 
                                                     product_name, re.IGNORECASE)
                        if product_type_match:
                            additional_info.append(f"Type: {product_type_match.group()}")
                    except:
                        pass

                    try:
                        # Check for product collection (Polaris, etc.)
                        collection_match = re.search(r"\b(Polaris|Dahlia|Millie|Icon)\b", 
                                                    product_name, re.IGNORECASE)
                        if collection_match:
                            additional_info.append(f"Collection: {collection_match.group()}")
                    except:
                        pass

                    try:
                        # Check for color variants
                        variant_wrapper = await product.query_selector(".swatch-link-products-wrap")
                        if variant_wrapper:
                            variant_count = len(await variant_wrapper.query_selector_all(".linked-product-list__item"))
                            if variant_count > 1:
                                additional_info.append(f"Color Variants: {variant_count}")
                    except:
                        pass

                    # Join all additional info with pipe delimiter
                    additional_info_text = " | ".join(additional_info) if additional_info else "N/A"

                    # Extract material from product name
                    material = "N/A"
                    try:
                        material_pattern = r"\b(Gold|Yellow Gold|Rose Gold|White Gold|Platinum|Silver|Vermeil)\b"
                        material_match = re.search(material_pattern, product_name, re.IGNORECASE)
                        material = material_match.group() if material_match else "Not found"
                    except:
                        pass

                    # Extract gemstone information
                    gemstone_info = "N/A"
                    try:
                        gemstone_matches = re.findall(r"\b(Malachite|Mother of Pearl|Onyx|Lapis|Diamond|Ruby|Sapphire|Emerald|Aquamarine|Pearl)\b", 
                                                     product_name, re.IGNORECASE)
                        if gemstone_matches:
                            gemstone_info = ", ".join(gemstone_matches)
                    except:
                        pass

                    # Get product image
                    image_url = "N/A"
                    try:
                        image_tag = await product.query_selector(".product-item-photo img")
                        if image_tag:
                            image_url = await image_tag.get_attribute("src")
                            # Try to get higher resolution image if possible
                            if "width=" in image_url:
                                image_url = image_url.split("width=")[0] + "width=800"
                    except Exception:
                        image_url = "N/A"

                    unique_id = str(uuid.uuid4())
                    if image_url and image_url != "N/A":
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                    records.append((unique_id, current_date, page_title, product_name, None, material, price_text, gemstone_info, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, material, price_text, gemstone_info, time_only, image_url, additional_info_text])

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

                all_records.extend(records)    
                wb.save(file_path)
                page_count += 1
        except Exception as e:
            logging.error(f"Error on page {page_count}: {str(e)}")
            wb.save(file_path)
        finally:
            if page: await page.close()
            if browser: await browser.close()
            await asyncio.sleep(random.uniform(2, 5))

    wb.save(file_path)
    log_event(f"Data saved to {file_path}")
    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path
