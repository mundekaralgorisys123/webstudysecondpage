import asyncio
import re
import os
import uuid
import logging
import base64
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from proxysetup import get_browser_with_proxy_strategy
from dotenv import load_dotenv
from utils import get_public_ip, log_event, sanitize_filename
from database import insert_into_db
from limit_checker import update_product_count
import httpx
from playwright.async_api import async_playwright, TimeoutError

# Load .env variables
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

def upgrade_to_high_res_url(image_url: str) -> str:
    """
    Replace the low-resolution cache key in a Buccellati image URL with the high-res version.
    """
    if not image_url or image_url == "N/A":
        return image_url

    high_res_cache_key = "e30df37fe797367961e091f338eb1dfc"
    return re.sub(r'cache/[^/]+/', f'cache/{high_res_cache_key}/', image_url)

async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A":
        return "N/A"
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    image_url = upgrade_to_high_res_url(image_url)

    for attempt in range(3):
        try:
            resp = await session.get(image_url, timeout=15)
            resp.raise_for_status()
            with open(image_full_path, "wb") as f:
                f.write(resp.content)
            return image_full_path
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/3 - Error downloading {product_name}: {e}")
    logging.error(f"Failed to download {product_name} after 3 attempts.")
    return "N/A"

async def handle_buccellati(url, max_pages):
    
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
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath"]
    sheet.append(headers)

    current_date = datetime.now().strftime("%Y-%m-%d")
    time_only = datetime.now().strftime("%H.%M")

    seen_ids = set()
    records = []
    image_tasks = []

    async with httpx.AsyncClient() as session:
        async with async_playwright() as p:
            product_wrapper = 'li.item.product.product-item'
            browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)
            
            try:
                # Initial page load
                await page.goto(url, timeout=120000)
                current_count = len(await page.query_selector_all('li.item.product.product-item'))

                # Pagination handling
                for page_num in range(1, max_pages):
                    try:
                        # Improved button handling with visibility checks
                        load_more_button = page.locator(
                            'button.action.next:has-text("LOAD MORE"):visible'
                        ).first
                        
                        # Wait for button to be fully interactive
                        await load_more_button.wait_for(
                            state='visible',
                            timeout=25000
                        )
                        await load_more_button.scroll_into_view_if_needed()
                        
                        # Store current count before interaction
                        pre_click_count = current_count
                        
                        # Click with multiple safeguards
                        async with page.expect_response(lambda r: "p=" in r.url and r.status == 200):
                            await load_more_button.click(delay=300)
                            
                        # Wait for product increase with fallback
                        try:
                            await page.wait_for_function(
                                f'''() => document.querySelectorAll('li.item.product.product-item').length > {pre_click_count}''',
                                timeout=60000
                            )
                            current_count = len(await page.query_selector_all('li.item.product.product-item'))
                        except TimeoutError:
                            new_count = len(await page.query_selector_all('li.item.product.product-item'))
                            if new_count <= pre_click_count:
                                logging.warning("No new products detected after click")
                                break
                            current_count = new_count

                        logging.info(f"Loaded page {page_num + 1} | Products: {current_count}")

                    except Exception as e:
                        logging.warning(f"Pagination error at page {page_num}: {str(e)}")
                        # Final verification
                        new_count = len(await page.query_selector_all('li.item.product.product-item'))
                        if new_count <= current_count:
                            break
                        current_count = new_count
                        continue
                    
                # Process all products
                all_products = await page.query_selector_all("li.item.product.product-item")
                logging.info(f"Total products found: {len(all_products)}")

                for product in all_products:
                    try:
                        # Product name
                        product_name_tag = await product.query_selector('strong.product.name.product-item-name a')
                        product_name = (await product_name_tag.inner_text()).strip() if product_name_tag else "N/A"

                        # Price
                        price_tag = await product.query_selector('div.price-box span.normal-price')
                        price = (await price_tag.inner_text()).strip() if price_tag else "N/A"

                        # Image
                        image_tag = await product.query_selector('img.product-image-photo')
                        image_src = await image_tag.get_attribute('src') if image_tag else None
                        if image_src:
                            image_url = image_src if image_src.startswith("http") else f"https://www.buccellati.com{image_src}"
                        else:
                            image_url = "N/A"
                            
                        if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                            print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                            continue     

                        # Metadata
                        kt_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                        kt = kt_match.group() if kt_match else "Not found"
                        
                        diamond_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", product_name, re.IGNORECASE)
                        diamond_weight = f"{diamond_match.group(1)} ct" if diamond_match else "N/A"

                        # Unique handling
                        unique_id = str(uuid.uuid4())
                        if unique_id in seen_ids:
                            continue
                        seen_ids.add(unique_id)

                        # Image download task
                        task = asyncio.create_task(
                            download_image(session, image_url, product_name, timestamp, image_folder, unique_id)
                        )
                        image_tasks.append((len(sheet['A']) + 1, unique_id, task))

                        # Record data
                        records.append((
                            unique_id, current_date, await page.title(),
                            product_name, None, kt, price, diamond_weight
                        ))
                        sheet.append([
                            current_date, await page.title(), product_name,
                            None, kt, price, diamond_weight, time_only, image_url
                        ])

                    except Exception as e:
                        logging.error(f"Error processing product: {str(e)}")
                        continue

                # Process images
                for row, unique_id, task in image_tasks:
                    image_path = await task
                    if image_path != "N/A":
                        try:
                            img = Image(image_path)
                            img.width, img.height = 100, 100
                            sheet.add_image(img, f"D{row}")
                            # Update record with image path
                            for i, record in enumerate(records):
                                if record[0] == unique_id:
                                    records[i] = (*record[:4], image_path, *record[5:])
                                    break
                        except Exception as e:
                            logging.error(f"Error inserting image: {str(e)}")

            except Exception as e:
                logging.error(f"Critical error: {str(e)}")
            finally:
                await browser.close()

        # Save Excel
        filename = f'buccellati_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
        file_path = os.path.join(EXCEL_DATA_PATH, filename)
        wb.save(file_path)
        log_event(f"Data saved to {file_path} | Products: {len(seen_ids)} | IP: {ip_address}")

        if records:
            insert_into_db(records)
        update_product_count(len(seen_ids))

        with open(file_path, "rb") as f:
            base64_encoded = base64.b64encode(f.read()).decode("utf-8")

        return base64_encoded, filename, file_path