import asyncio
import re
import os
import uuid
import logging
import base64
from datetime import datetime
from xml.dom.minidom import Document
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from utils import get_public_ip, log_event
from database import insert_into_db
from limit_checker import update_product_count
import httpx
from playwright.async_api import async_playwright, TimeoutError
from proxysetup import get_browser_with_proxy_strategy

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A":
        return "N/A"
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    for attempt in range(3):
        try:
            resp = await session.get(image_url, timeout=10)
            resp.raise_for_status()
            with open(image_full_path, "wb") as f:
                f.write(resp.content)
            return image_full_path
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/3 - Error downloading {product_name}: {e}")
    logging.error(f"Failed to download {product_name} after 3 attempts.")
    return "N/A"

async def handle_tacori(url, max_pages):
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

   
    records = []
    image_tasks = []

    async with httpx.AsyncClient() as session:
        load_more_clicks = 1
        previous_count = 0

        while load_more_clicks <= max_pages:
            async with async_playwright() as p:
                # Create a new browser instance for each page
                product_wrapper = '.LayoutDefault-children'
                browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper)
                
                for _ in range(load_more_clicks - 1):
                    try:
                        # Use more specific selector with escaped CSS colon
                        load_more_button = await page.wait_for_selector(
                            'button.button-white[id="\\:R4mt57ekkm\\:"]:has-text("View More"):not(:disabled)',
                            timeout=10000,
                            state="visible"
                        )
                        
                        if await load_more_button.is_visible():
                            # Scroll into view and click using JavaScript
                            await page.evaluate("""button => {
                                button.scrollIntoView({behavior: 'smooth', block: 'center'});
                                button.click();
                            }""", load_more_button)
                            
                            # Wait for either next load or button disable
                            await asyncio.sleep(1)
                            try:
                                await page.wait_for_load_state('networkidle', timeout=5000)
                            except TimeoutError:
                                pass
                            
                            # Check if button is still enabled
                            if await load_more_button.is_disabled():
                                break
                            
                            load_more_attempts += 1
                            logging.info(f"Clicked 'View More' ({load_more_attempts}/{max_pages})")
                            
                    except Exception as e:
                        logging.warning(f"Stopping pagination: {str(e)}")
                        break



               
                
    
                # all_products = await page.query_selector_all("div.plp-card.config")
                wrapper_selector = (
                    "div.MuiGrid-root"
                    ".grid"
                    ".grid-cols-2"
                    ".tb\\:grid-cols-4"
                    ".text-center"
                    ".mt-8"
                    ".gap-2"
                    ".mui-style-yc8scu"
                )

                # 2) Item selector: pick the key class(es) inside the grid
                item_selector = "div.simple.plp-card"

                # 3) Query once, then all items
                product_wrapper = await page.query_selector(wrapper_selector)
                if product_wrapper:
                    products = await product_wrapper.query_selector_all(item_selector)
                else:
                    products = []

                print(f"Found {len(products)} products")
                total_products = len(products)
                new_products = products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()

                for idx, product in enumerate(new_products):
                    try:
                        product_info_tag = await product.query_selector('p.MuiTypography-root')
                        if product_info_tag:
                            full_text = await product_info_tag.inner_text()
                            # Remove the price from the end (assuming it always starts with $)
                            product_lines = full_text.strip().split('\n')
                            product_line = product_lines[0] if product_lines else full_text.strip()
                            product_name = product_line.rsplit('$', 1)[0].strip()  # remove price if it's inline
                        else:
                            product_name = "N/A"
                    except Exception as e:
                        print(f"Error fetching product name: {e}")
                        product_name = "N/A"


                    try:
                        price_tag = await product.query_selector('span.pt-\\[5px\\].block')
                        price = await price_tag.inner_text() if price_tag else "N/A"
                    except Exception as e:
                        print(f"Error fetching price: {e}")
                        price = "N/A"

                    try:
                        image_elem = await product.query_selector("img")
                        image_url = "N/A"
                        
                        if image_elem:
                            # First try `src`
                            image_url = await image_elem.get_attribute("src")
                            
                            # If `src` is missing or invalid, fall back to `srcset`
                            if not image_url or image_url.strip() == "":
                                srcset = await image_elem.get_attribute("srcset")
                                if srcset:
                                    # srcset is like: "<url1> 1x, <url2> 2x"
                                    image_url = srcset.split(",")[0].split(" ")[0].strip()
                            
                            # Ensure absolute URL
                            if image_url and image_url.startswith("/"):
                                image_url = "https://www.tacori.com" + image_url
                    except Exception as e:
                        print(f"Error fetching image URL: {e}")
                        image_url = "N/A"




                    additional_info_str = "N/A"
                    kt_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                    kt = kt_match.group() if kt_match else "Not found"

                    diamond_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", product_name, re.IGNORECASE)
                    diamond_weight = f"{diamond_match.group(1)} ct" if diamond_match else "N/A"

                    unique_id = str(uuid.uuid4())
                    task = asyncio.create_task(download_image(session, image_url, product_name, timestamp, image_folder, unique_id))
                    image_tasks.append((len(sheet['A']) + 1, unique_id, task))

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])

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
        filename = f'handle_tacori_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
        file_path = os.path.join(EXCEL_DATA_PATH, filename)
        
    
        if not records:
            return None, None, None

        # Save the workbook
        wb.save(file_path)
        log_event(f"Data saved to {file_path}")

        # Encode the file in base64
        with open(file_path, "rb") as file:
            base64_encoded = base64.b64encode(file.read()).decode("utf-8")

        # Insert data into the database and update product count
        insert_into_db(records)
        update_product_count(len(records))

        # Return necessary information
        return base64_encoded, filename, file_path


       