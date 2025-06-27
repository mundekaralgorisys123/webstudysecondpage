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
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event
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
            image_url = f"https://www.anitako.com{image_url}"
        
        # Clean up the URL by removing query parameters and fragments
        clean_url = image_url.split('?')[0].split('#')[0]
        
        # Create a safe filename
        safe_product_name = re.sub(r'[^\w\-_. ]', '', product_name)[:100]
        extension = clean_url.split('.')[-1].lower()
        if extension not in ['jpg', 'jpeg', 'png', 'webp']:
            extension = 'jpg'  # default extension
            
        filename = f"{safe_product_name}_{unique_id}.{extension}"
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

# Main scraper function
async def handle_anitako(url, max_pages):
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
    filename = f"handle_anitako_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    current_url = url
    while (page_count <= max_pages):
        logging.info(f"Processing page {page_count}: {current_url}")
        browser = None
        context = None
        if page_count > 1:
            if '?' in current_url:
                current_url = f"{url}&page={page_count}"
            else:
                current_url = f"{url}?page={page_count}"
        try:
            async with async_playwright() as p:
                product_wrapper = ".collection-grid__wrapper"
                browser, page = await get_browser_with_proxy_strategy(p, current_url,product_wrapper )
                log_event(f"Successfully loaded: {current_url}")
            
                # Scroll to load all items
                prev_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    count = await page.locator('div#CollectionSection').count()
                    if count == prev_count:
                        break
                    prev_count = count

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                product_wrapper = await page.query_selector("div#CollectionSection")
                products = await product_wrapper.query_selector_all("div.grid__item") if product_wrapper else []
                logging.info(f"Total products scraped:{page_count} :{len(products)}")
                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    
                    try:
                        name_tag = await product.query_selector("div.grid-product__title")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    # Enhanced price extraction
                    try:
                        price_tag = await product.query_selector("div.grid-product__price")
                        price_text = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                        
                        # Check for sale price
                        sale_price_tag = await product.query_selector("div.grid-product__price--sale")
                        if sale_price_tag:
                            sale_price_text = (await sale_price_tag.inner_text()).strip()
                            if sale_price_text:
                                additional_info.append(f"Sale Price: {sale_price_text}")
                                price = f"{price_text} | {sale_price_text}"
                            else:
                                price = price_text
                        else:
                            price = price_text
                            
                        # Clean price text
                        price = price.replace('Rs.', '').replace(',', '').strip()
                    except Exception:
                        price = "N/A"

                    # Enhanced image extraction
                    try:
                        image_container = await product.query_selector("div.grid__image-ratio")
                        if image_container:
                            bgset = await image_container.get_attribute("data-bgset")
                            if bgset:
                                # Extract all image URLs and get the highest resolution
                                image_urls = [url.strip().split(' ')[0] for url in bgset.split(',') if url.strip()]
                                if image_urls:
                                    highest_res_url = image_urls[-1]
                                    if highest_res_url.startswith('//'):
                                        highest_res_url = f"https:{highest_res_url}"
                                    elif highest_res_url.startswith('/'):
                                        highest_res_url = f"https://www.anitako.com{highest_res_url}"
                                    image_url = highest_res_url.split('?v=')[0]
                                else:
                                    image_url = "N/A"
                            else:
                                image_url = "N/A"
                        else:
                            image_url = "N/A"
                    except Exception:
                        image_url = "N/A"

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue
                    
                    # Check for product status (sold out, etc.)
                    try:
                        status_tag = await product.query_selector("div.grid-product__tag")
                        if status_tag:
                            status_text = (await status_tag.inner_text()).strip()
                            if status_text:
                                additional_info.append(f"Status: {status_text}")
                    except Exception:
                        pass

                    # Extract metal type and gemstone information
                    gold_type_pattern = r"\b\d{1,2}(?:K|ct)?\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSterling Silver\b"
                    gold_type_match = re.search(gold_type_pattern, product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"

                    # Extract diamond weight and gemstone details
                    diamond_weight = "N/A"
                    try:
                        diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                        diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                        diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                        
                        # Extract gemstone information
                        gemstone_pattern = r"\b(?:Diamond|Emerald|Ruby|Sapphire|Topaz|Amethyst|Aquamarine|Pearl)\b"
                        gemstones = re.findall(gemstone_pattern, product_name, re.IGNORECASE)
                        if gemstones:
                            additional_info.append(f"Gemstones: {', '.join(gemstones)}")
                            
                        # Extract special cuts (Marquis, Emerald Cut, etc.)
                        cut_pattern = r"\b(?:Marquis|Emerald Cut|Round Brilliant|Princess|Oval|Pear|Cushion|Radiant)\b"
                        cuts = re.findall(cut_pattern, product_name, re.IGNORECASE)
                        if cuts:
                            additional_info.append(f"Cut: {', '.join(cuts)}")
                    except Exception:
                        pass

                    # Check for product categories
                    try:
                        category_tag = await product.query_selector("a.grid-product__link")
                        if category_tag:
                            href = await category_tag.get_attribute("href")
                            if href:
                                categories = [part for part in href.split('/') if part and part not in ['products', 'collections']]
                                if categories:
                                    additional_info.append(f"Categories: {', '.join(categories)}")
                    except Exception:
                        pass

                    # Join all additional info with | delimiter
                    additional_info_text = " | ".join(additional_info) if additional_info else "N/A"

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
                
        except Exception as e:
            logging.error(f"Error on page {page_count}: {str(e)}")
            wb.save(file_path)
        finally:
            if page: await page.close()
            if browser: await browser.close()
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