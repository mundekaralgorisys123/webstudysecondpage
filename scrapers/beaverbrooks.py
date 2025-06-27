import os
import re
import time
import logging
import random
import uuid
import asyncio
import base64
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError, Error
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from PIL import Image as PILImage
from utils import get_public_ip, log_event, sanitize_filename
from dotenv import load_dotenv
from database import insert_into_db
from limit_checker import update_product_count
from proxysetup import get_browser_with_proxy_strategy
from openpyxl.drawing.image import Image as XLImage
import httpx

load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

def modify_image_url(image_url):
    if not image_url or image_url == "N/A":
        return image_url
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"
    modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)
    return modified_url + query_params

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

async def handle_beaverbrooks(url, max_pages):
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
    filename = f"handle_beaverbrooks_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 1
    success_count = 0
    current_url = url
    while page_count <= max_pages:
        if page_count > 1:
            if '?' in current_url:
                current_url = f"{url}&page={page_count}"
            else:
                current_url = f"{url}?page={page_count}"
        logging.info(f"Processing page {page_count}: {current_url}")

        browser = None
        page = None
        try:
            async with async_playwright() as p:
                product_wrapper = ".products--xdQkZ"
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                log_event(f"Successfully loaded: {current_url}")

                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    current_product_count = await page.locator('.product--AbtlR').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count

                product_wrapper = await page.query_selector("div.products--xdQkZ")
                products = await product_wrapper.query_selector_all("div.product--AbtlR") if product_wrapper else []
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    try:
                        product_name = await (await product.query_selector("h5.description__name--RGSu4")).inner_text()
                    except Exception as e:
                        logging.warning(f"⚠️ Error extracting product name: {e}")
                        product_name = "N/A"

                    try:
                        # Discounted price (current price after discount)
                        offer_price_tag = await product.query_selector("span.price-line__price--discounted--LmVtQ")
                        
                        # Original price before discount
                        was_price_tag = await product.query_selector("span.price-line__price--was--cpI48 s")

                        # Regular price (if no discount)
                        normal_price_tag = await product.query_selector("span.price-line__price--normal--x54ly")

                        if offer_price_tag:
                            current_price = (await offer_price_tag.inner_text()).strip()
                            if was_price_tag:
                                original_price = (await was_price_tag.inner_text()).strip()
                                price = f"{current_price} (was {original_price})"
                            else:
                                price = current_price
                        elif normal_price_tag:
                            price = (await normal_price_tag.inner_text()).strip()
                        else:
                            price = "N/A"
                    except Exception as e:
                        logging.warning(f"⚠️ Error extracting price: {e}")
                        price = "N/A"

                    try:
                        # # Ensure product is scrolled into view
                        await product.scroll_into_view_if_needed()

                        # Use query_selector to locate the first img element
                        image_element = await product.query_selector("img")

                        if image_element:
                            # Get the 'src' attribute of the img element
                            image_url = await image_element.get_attribute("src") if image_element else "N/A"

                            # Fallback to srcset if the 'src' attribute is not found
                            if not image_url or image_url == "N/A":
                                srcset = await image_element.get_attribute("srcset")
                                if srcset:
                                    image_url = srcset.split(',')[0].strip().split()[0]

                            # Remove query parameters if present
                            if image_url and "?" in image_url:
                                image_url = image_url.split("?")[0]

                            # Convert to full URL if it starts with "//"
                            if image_url and image_url.startswith("//"):
                                image_url = "https:" + image_url

                            # print("✅ Extracted Image URL:", image_url)
                        else:
                            image_url = "N/A"

                    except Exception as e:
                        logging.warning(f"⚠️ Error extracting image URL: {e}")
                        image_url = "N/A"
                        
                    # Initialize info parts
                    info_parts = []

                    # Financing Text
                    try:
                        financing_el = await product.query_selector("div.financing--vpMZt span")
                        if financing_el:
                            financing_text = (await financing_el.inner_text()).strip()
                            if financing_text:
                                info_parts.append(financing_text)
                    except Exception:
                        pass

                    # Brand Name
                    try:
                        brand_el = await product.query_selector("h4.description__brand--haZK5")
                        if brand_el:
                            brand = (await brand_el.inner_text()).strip()
                            if brand:
                                info_parts.append(brand)
                    except Exception:
                        pass

                    # Discount Calculation
                    try:
                        was_el = await product.query_selector("span.price-line__price--was--cpI48 s")
                        now_el = await product.query_selector("span.price-line__price--discounted--LmVtQ")

                        if was_el and now_el:
                            was_text = (await was_el.inner_text()).strip().replace("£", "")
                            now_text = (await now_el.inner_text()).strip().replace("£", "")
                            if was_text and now_text:
                                was_price = float(was_text.replace(",", ""))
                                now_price = float(now_text.replace(",", ""))
                                if was_price > now_price:
                                    discount_percent = round((was_price - now_price) / was_price * 100)
                                    info_parts.append(f"{discount_percent}% off")
                    except Exception:
                        pass

                    # Final combined string
                    additional_info_str = " | ".join(info_parts) if info_parts else "N/A"
                        
                        
                        
                        
                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue      



                    gold_type_match = re.search(r"(\d{1,2}K|Platinum|Silver|Gold|White Gold|Yellow Gold|Rose Gold)", product_name, re.IGNORECASE)
                    kt = gold_type_match.group(0) if gold_type_match else "N/A"

                    diamond_weight_match = re.search(r"(\d+(\.\d+)?)\s*(ct|carat)", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_weight_match.group(1)} ct" if diamond_weight_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])

                for row_num, unique_id, task in image_tasks:
                    try:
                        image_path = await asyncio.wait_for(task, timeout=60)
                        if image_path != "N/A":
                            try:
                                img = Image(image_path)
                                img.width, img.height = 100, 100
                                sheet.add_image(img, f"D{row_num}")
                            except Exception as img_error:
                                logging.error(f"Error adding image to Excel: {img_error}")
                                image_path = "N/A"

                        for i, record in enumerate(records):
                            if record[0] == unique_id:
                                records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                                break
                    except asyncio.TimeoutError:
                        logging.warning(f"Timeout downloading image for row {row_num}")

                all_records.extend(records)
                success_count += 1
                wb.save(file_path)
                logging.info(f"Progress saved after page {page_count}")

        except Exception as e:
            logging.error(f"Error processing page {page_count}: {str(e)}")
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