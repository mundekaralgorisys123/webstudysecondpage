import asyncio
import re
import os
import uuid
import logging
import base64
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from flask import Flask
from dotenv import load_dotenv
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import httpx
from playwright.async_api import async_playwright, TimeoutError
from proxysetup import get_browser_with_proxy_strategy
# Load .env variables
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')


def modify_image_url(image_url):
    """Modify the image URL to replace '_260' with '_1200' while keeping query parameters."""
    if not image_url or image_url == "N/A":
        return image_url

    # Extract and preserve query parameters
    query_params = ""
    if "?" in image_url:
        image_url, query_params = image_url.split("?", 1)
        query_params = f"?{query_params}"

    # Replace '_260' with '_1200' while keeping the rest of the URL intact
    modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)

    return modified_url + query_params  # Append query parameters if they exist

async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
 

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url, headers=headers)
                response.raise_for_status()
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                return image_full_path
            except httpx.RequestError as e:
                logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"

async def safe_click_with_retry(element, max_retries=3):
    for attempt in range(max_retries):
        try:
            await element.scroll_into_view_if_needed()
            await element.click(timeout=10000)
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(1 * (attempt + 1))
    return False

async def handle_missoma(url, max_pages):
    ip_address = get_public_ip()
    logging.info(f"Starting scrape for {url} from IP: {ip_address}")

    if not os.path.exists(EXCEL_DATA_PATH):
        os.makedirs(EXCEL_DATA_PATH)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    current_date = datetime.now().strftime("%Y-%m-%d")
    time_only = datetime.now().strftime("%H.%M")

    seen_ids = set()
    records = []
    image_tasks = []

    async with httpx.AsyncClient() as session:
        load_more_clicks = 1
        previous_count = 0

        while load_more_clicks <= max_pages:
            async with async_playwright() as p:
                # Create a new browser instance for each page
                browser, page = await get_browser_with_proxy_strategy(p, url, "ol.ais-InfiniteHits-list")

                # Simulate clicking 'Load More' number of times
                for _ in range(load_more_clicks - 1):
                    try:
                        load_more_button = page.locator('button.ais-InfiniteHits-loadMore')
                        
                        # Wait for both visibility and stable position
                        await load_more_button.wait_for(
                            state='visible',
                            timeout=15000,
                            position=(0.5, 0.5)  # Ensure element is in center viewport
                        )

                        # Get initial state
                        initial_count_text = await page.locator('.ais-sup-load-more-text').inner_text()
                        
                        # Custom click with viewport verification
                        if await safe_click_with_retry(load_more_button):
                            # Wait for either content update OR button disappearance
                            try:
                                await page.wait_for_function(
                                    f"""() => {{
                                        const textEl = document.querySelector('.ais-sup-load-more-text');
                                        return textEl.textContent !== '{initial_count_text}';
                                    }}""",
                                    timeout=25000
                                )
                            except:
                                # If button disappears after click (no more items)
                                if not await load_more_button.is_visible():
                                    logging.info("No more items to load")
                                    break

                        # Additional check for new items in DOM
                        await page.wait_for_load_state('networkidle', timeout=10000)
                        await page.wait_for_selector('.ais-InfiniteHits-item:last-child', timeout=15000)

                    except TimeoutError as e:
                        logging.warning(f"Timeout: {e}")
                        break
                    except Exception as e:
                        logging.warning(f"Critical error: {e}")
                        break

                all_products = await page.query_selector_all("ol.ais-InfiniteHits-list > li.ais-InfiniteHits-item")
                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for row_num, product in enumerate(new_products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    
                    try:
                        # Get product name
                        product_name_tag = await product.query_selector('p.ais-hit--title a')
                        product_name = await product_name_tag.inner_text() if product_name_tag else "N/A"
                        
                        # Get variant and append to product name
                        variant_tag = await product.query_selector('p.ais-hit--variant span')
                        variant_text = await variant_tag.inner_text() if variant_tag else None
                        if variant_text and variant_text != "N/A":
                            product_name = f"{product_name}, {variant_text}"
                            
                    except Exception as e:
                        print(f"Error fetching product name: {e}")
                        product_name = "N/A"

                    try:
                        # Handle price information
                        standard_price_tag = await product.query_selector('p.ais-hit--price b.standard-price')
                        standard_price = await standard_price_tag.inner_text() if standard_price_tag else None
                        
                        discounted_price_tag = await product.query_selector('p.ais-hit--price b.full-price')
                        discounted_price = await discounted_price_tag.inner_text() if discounted_price_tag else None
                        
                        if standard_price and discounted_price:
                            price = f"{discounted_price}|{standard_price}"
                        elif standard_price:
                            price = standard_price
                        else:
                            price = "N/A"
                            
                    except Exception as e:
                        print(f"Error fetching price: {e}")
                        price = "N/A"
                        
                    try:
                        # Get image URL
                        image_tag = await product.query_selector('img.ais-first-img')
                        image_url = await image_tag.get_attribute('src') if image_tag else None

                        if not image_url:
                            srcset = await image_tag.get_attribute('srcset')
                            if srcset:
                                image_url = srcset.split(',')[-1].split(' ')[0]

                        if image_url and image_url.startswith('//'):
                            image_url = 'https:' + image_url

                    except Exception as e:
                        print(f"Error fetching image URL: {e}")
                        image_url = "N/A"

                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue

                    try:
                        # Get karat information
                        variant_tag = await product.query_selector('p.ais-hit--variant span')
                        kt = await variant_tag.inner_text() if variant_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching product variant: {e}")
                        kt = "N/A"

                    # Extract diamond weight from product name
                    diamond_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_match.group(1)} ct" if diamond_match else "N/A"

                    try:
                        # Get availability information
                        availability_tag = await product.query_selector('span.ais-hit--cart-button__disabled')
                        if availability_tag:
                            availability_text = await availability_tag.inner_text()
                            additional_info.append(f"Availability: {availability_text}")
                    except:
                        pass

                    try:
                        # Get tags/labels
                        tags = []
                        tag_elements = await product.query_selector_all('span.ais-tag')
                        for tag in tag_elements:
                            tag_text = await tag.inner_text()
                            if tag_text.strip():
                                tags.append(tag_text)
                        if tags:
                            additional_info.append(f"Tags: {'|'.join(tags)}")
                    except:
                        pass

                    try:
                        # Get vendor information
                        vendor = await product.get_attribute('data-vendor')
                        if vendor:
                            additional_info.append(f"Vendor: {vendor}")
                    except:
                        pass

                    try:
                        # Get SKU information
                        sku = await product.get_attribute('data-sku')
                        if sku:
                            additional_info.append(f"SKU: {sku}")
                    except:
                        pass

                    # Combine all additional info with pipe delimiter
                    additional_info_text = "|".join(additional_info) if additional_info else ""

                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight, additional_info_text))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url, additional_info_text])

                # Process image downloads and attach them to Excel
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        img = Image(image_path)
                        img.width, img.height = 100, 100
                        sheet.add_image(img, f"D{row}")
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (record[0], record[1], record[2], record[3], image_path, record[5], record[6], record[7], record[8])
                            break

                await browser.close()
            load_more_clicks += 1

        # Save Excel
        filename = f'handle_missoma_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
        file_path = os.path.join(EXCEL_DATA_PATH, filename)
        wb.save(file_path)
        log_event(f"Data saved to {file_path} | IP: {ip_address}")

        if records:
            insert_into_db(records)
        else:
            logging.info("No data to insert into the database.")

        update_product_count(len(seen_ids))

        with open(file_path, "rb") as f:
            base64_encoded = base64.b64encode(f.read()).decode("utf-8")

        return base64_encoded, filename, file_path
