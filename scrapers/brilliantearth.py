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
from playwright.async_api import async_playwright, TimeoutError, Error
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
from proxysetup import get_browser_with_proxy_strategy


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

def modify_image_url(image_url):
    """Modify the image URL to replace w=440 with w=2200 and h=440 with h=2200 while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    # Check if the image URL already contains high resolution (w=2200 and h=2200)
    if "w=2200" in image_url and "h=2200" in image_url:
        return image_url

    # Modify the image URL to use higher resolution (w=2200 and h=2200)
    modified_url = re.sub(r'\bw=\d+\b', 'w=2200', image_url)
    modified_url = re.sub(r'\bh=\d+\b', 'h=2200', modified_url)

    return modified_url

async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    # Final image filename (only JPG)
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    async with httpx.AsyncClient(
        timeout=10.0,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "image/webp,image/apng,image/*,*/*;q=0.8"
        }
    ) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
                response.raise_for_status()

                # Open image directly from memory
                img = PILImage.open(BytesIO(response.content))
                
                # Convert to RGB if necessary (e.g., if it's a webp with alpha channel)
                if img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")

                # Save directly as JPG
                img.save(image_full_path, "JPEG", quality=95)

                logging.info(f"Successfully downloaded and converted {product_name}: {image_full_path}")
                return image_full_path

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 302:
                    redirect_location = e.response.headers.get("location")
                    logging.info(f"Following redirect for {product_name}: {redirect_location}")
                    image_url = redirect_location
                    continue
                logging.warning(f"Retry {attempt + 1}/{retries} - HTTP error for {product_name}: {e}")
                await asyncio.sleep(1)

            except (httpx.RequestError, PILImage.UnidentifiedImageError) as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Connection/Image error for {product_name}: {e}")
                await asyncio.sleep(1)

        logging.error(f"Failed to download {product_name} after {retries} attempts")
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


            
# Main scraper function
async def handle_brilliantearth(url, max_pages=None):
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
    filename = f"handle_brilliantearth_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    prev_prod_cout = 0
    load_more_clicks = 1
    while load_more_clicks <= max_pages:
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = '.listing-grid'
                browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)
                log_event(f"Successfully loaded: {url}")

                # Scroll to load all items
                await scroll_to_bottom(page)
                
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                # Get all product tiles
                product_wrapper = await page.query_selector("div.listing-grid")
                # Ensure the correct class names are being used
                products = await page.query_selector_all("div.per-product.no-thumb.line-loaded") if product_wrapper else []

                max_prod = len(products)
                products = products[prev_prod_cout: min(max_prod, prev_prod_cout + 30)]
                prev_prod_cout += len(products)

                if len(products) == 0:
                    log_event("No new products found, stopping the scraper.")
                    break

                logging.info(f"New products found: {len(products)}")
                print(f"New products found: {len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product name
                        name_tag = await product.query_selector('a.clk_through .limit.new-product-title')
                        if name_tag:
                            # Extract the product name text (concatenate the spans)
                            product_name = (await name_tag.inner_text()).strip().replace("\n", " ") if name_tag else "N/A"
                        else:
                            product_name = "N/A"
                    except Exception as e:
                        print(f"Error fetching product name: {e}")
                        product_name = "N/A"


                    try:
                        # Extract price - from the span with class 'money'
                        price_tag = await product.query_selector(".money")
                        price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                        # Clean up price string (remove extra spaces)
                        price = re.sub(r'\s+', ' ', price).strip()
                    except Exception as e:
                        print(f"Error fetching price: {e}")
                        price = "N/A"


                    
                    image_url = "N/A"
                    try:
                        # Get the primary image URL from the img tag inside the anchor tag
                        img_tag = await product.query_selector("a img")
                        if img_tag:
                            image_url = await img_tag.get_attribute("src")
                            if image_url:
                                # Remove all query parameters to get original quality image
                                image_url = image_url.split('?')[0]
                                # Ensure URL is complete
                                if image_url.startswith("//"):
                                    image_url = f"https:{image_url}"
                    except Exception as e:
                        log_event(f"Error getting image URL: {e}")
                        image_url = "N/A"
                        
                    try:
                        material_el = await product.query_selector("small.firstline.hidden")
                        if material_el:
                            kt = (await material_el.inner_text()).strip()
                        else:
                            kt = "N/A"
                    except Exception as e:
                        kt = "N/A"

                        
                    print(kt)    


                    # Extract diamond weight from description
                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                    
                    additional_info = []

                    try:
                        # Correct selector for Best Seller badge
                        tag_els_demand = await product.query_selector_all("div.ir327-badge.invert span")

                        if tag_els_demand:
                            for tag_ele in tag_els_demand:
                                tag_text1 = await tag_ele.inner_text()
                                if tag_text1:
                                    additional_info.append(tag_text1.strip())
                        else:
                            additional_info.append("N/A")

                    except Exception as e:
                        additional_info.append("N/A")

                    additional_info_str = " | ".join(additional_info)


                    unique_id = str(uuid.uuid4())
                    if image_url and image_url != "N/A":
                        image_tasks.append((row_num, unique_id, asyncio.create_task(
                            download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                        )))

                
                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])
                            
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
        return None, None, None    # Final save and database operations
    wb.save(file_path)
    log_event(f"Data saved to {file_path}")

    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path

