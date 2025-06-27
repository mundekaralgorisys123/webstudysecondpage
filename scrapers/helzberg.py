import os
import logging
import aiohttp
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError
from openpyxl import Workbook
from openpyxl.drawing.image import Image
import uuid
import base64
from utils import get_public_ip
from database import insert_into_db, create_table
from limit_checker import update_product_count
import random
import re
from playwright.async_api import Page
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from proxysetup import get_browser_with_proxy_strategy



# Setup Flask
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def scroll_and_wait(page):
    """Scroll down to load lazy-loaded products."""
    previous_height = await page.evaluate("document.body.scrollHeight")
    await page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
    await asyncio.sleep(2)  # Allow time for content to load
    new_height = await page.evaluate("document.body.scrollHeight")
    return new_height > previous_height  # Returns True if more content is loaded

def modify_image_url(image_url):
    """Update Helzberg image URL to use high resolution (800x800)."""
    if not image_url or image_url == "N/A":
        return image_url

    # Parse the URL
    parsed_url = urlparse(image_url)
    query = parse_qs(parsed_url.query)

    # Modify or add resolution parameters
    query["sw"] = ["800"]
    query["sh"] = ["800"]
    query["sm"] = ["fit"]

    # Rebuild the URL with updated query
    new_query = urlencode(query, doseq=True)
    high_res_url = urlunparse(parsed_url._replace(query=new_query))

    return high_res_url

async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    """Download image with retries using aiohttp."""
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    modified_url = modify_image_url(image_url)

    for attempt in range(retries):
        try:
            async with session.get(modified_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                content = await response.read()
                with open(image_full_path, "wb") as f:
                    f.write(content)
                return image_full_path
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
            await asyncio.sleep(1)  # Add small delay between retries

    logging.error(f"Failed to download {product_name} after {retries} attempts.")
    return "N/A"

async def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def click_load_more(page, max_pages=5, delay_range=(3, 5)):
    """Click 'Load More' button dynamically until no more pages are available."""
    load_more_clicks = 0

    while load_more_clicks < (max_pages - 1):
        try:
            load_more_button = await page.query_selector(".show-more-btn")

            if load_more_button and await load_more_button.is_visible():
                await page.evaluate("(btn) => btn.click()", load_more_button)
                load_more_clicks += 1
                logging.info(f"✅ Clicked 'Load More' button {load_more_clicks} times.")
                
                # Wait for new content to load
                await random_delay(*delay_range)

                # Ensure new content loaded by checking page height change
                previous_height = await page.evaluate("document.body.scrollHeight")
                await asyncio.sleep(2)  # Short wait before rechecking height
                new_height = await page.evaluate("document.body.scrollHeight")

                if previous_height == new_height:
                    logging.info("⚠️ No new content loaded after clicking 'Load More'. Stopping.")
                    break
            else:
                logging.info("🔹 'Load More' button not found or not visible. Stopping.")
                break
        except Exception as e:
            logging.warning(f"⚠️ Error clicking 'Load More': {e}")
            break

async def handle_helzberg(url, max_pages):
    """Scrape product data from Helzberg website with enhanced product information."""
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} | IP: {ip_address} | Max pages: {max_pages}")

    # Prepare folders
    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    # Prepare Excel with Additional Info column
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    current_date = datetime.now().strftime("%Y-%m-%d")
    time_only = datetime.now().strftime("%H.%M")

    # Collect all data across pages
    all_records = []
    row_counter = 2
    current_url = url
    pages_processed = 0

    while current_url and pages_processed < max_pages:
        try:
            async with async_playwright() as p:
                product_wrapper = '.row.product-grid'
                browser, page = await get_browser_with_proxy_strategy(p, current_url, product_wrapper)
                
                pages_processed += 1

                # Scroll to load lazy content
                for _ in range(3):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(2)

                # Get page title
                page_title = await page.title()

                product_wrapper = await page.query_selector("div.row.product-grid")
                products = await product_wrapper.query_selector_all("div.col-6.col-sm-4") if product_wrapper else []
                if pages_processed > 1:
                    products = await page.query_selector_all("div.col-6.col-sm-4")
                logging.info(f"Found {len(products)} products on page {pages_processed}")

                # Process products on this page
                async with aiohttp.ClientSession() as session:
                    for product in products:
                        try:
                            additional_info = []
                            
                            # Product name
                            name_element = await product.query_selector("a.prodname-container__link")
                            product_name = await name_element.inner_text() if name_element else "N/A"
                            product_name = product_name.strip()

                            # Price handling - capture both current and original price
                            price_info = []
                            try:
                                # Current price
                                current_price_elem = await product.query_selector("span.sales.promo-price .value")
                                current_price = await current_price_elem.inner_text() if current_price_elem else "N/A"
                                current_price = current_price.strip()
                                if current_price != "N/A":
                                    price_info.append(current_price)
                                
                                # Original price
                                original_price_elem = await product.query_selector("span.strike-through.list .value")
                                if original_price_elem:
                                    original_price = await original_price_elem.inner_text()
                                    original_price = original_price.strip()
                                    if original_price and original_price != current_price:
                                        price_info.append(original_price)
                                        
                                        # Check for discount/sale badge
                                        sale_badge = await product.query_selector("div.plp-badge.badge-text-tile-sale")
                                        if sale_badge:
                                            sale_text = await sale_badge.inner_text()
                                            additional_info.append(f"Sale: {sale_text.strip()}")
                            except Exception as e:
                                logging.warning(f"Error getting price info: {str(e)}")
                                price_info = ["N/A"]
                            
                            price = " | ".join(price_info) if price_info else "N/A"

                            # Image URL
                            images = await product.query_selector_all("img.tile-image")
                            product_urls = []
                            for img in images:
                                src = await img.get_attribute("src")
                                if src:
                                    product_urls.append(src)
                            image_url = product_urls[0] if product_urls else "N/A"

                            if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                                print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                                continue


                            # Metal type
                            gold_type_match = re.search(r"\b\d+K\s+\w+\s+\w+\b", product_name)
                            kt = gold_type_match.group() if gold_type_match else "Not found"

                            # Diamond Weight
                            diamond_weight_match = re.search(r"(\d+(?:\.\d+)?(?:[-/]\d+(?:\.\d+)?)?\s*ct\.?\s*t\.?w\.?)", product_name)
                            diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"

                            # Additional product info
                            try:
                                # Check for available metals (colors)
                                metal_buttons = await product.query_selector_all("button.color-attribute")
                                if metal_buttons and len(metal_buttons) > 1:
                                    metals = []
                                    for button in metal_buttons:
                                        try:
                                            metal_label = await button.get_attribute("aria-label")
                                            if metal_label and "Metal" in metal_label:
                                                metals.append(metal_label.replace("Metal ", ""))
                                        except:
                                            continue
                                    if metals:
                                        additional_info.append(f"Metals: {', '.join(metals)}")
                            except:
                                pass

                            try:
                                # Check for diamond weight options
                                weight_options = await product.query_selector_all("li.custom-select-item.custom-select-item-pdp")
                                if weight_options and len(weight_options) > 1:
                                    weights = []
                                    for option in weight_options:
                                        try:
                                            weight_text = await option.inner_text()
                                            if weight_text.strip():
                                                weights.append(weight_text.strip())
                                        except:
                                            continue
                                    if weights:
                                        additional_info.append(f"Weights: {', '.join(weights)}")
                            except:
                                pass

                            try:
                                # Check for brand
                                brand_elem = await product.query_selector("span.brand-span")
                                if brand_elem:
                                    brand_text = await brand_elem.inner_text()
                                    if brand_text.strip():
                                        additional_info.append(f"Brand: {brand_text.strip()}")
                            except:
                                pass

                            # Combine all additional info with pipe delimiter
                            additional_info_str = " | ".join(additional_info) if additional_info else ""

                            unique_id = str(uuid.uuid4())
                            
                            # Download image immediately while browser is still open
                            image_path = await download_image(session, image_url, product_name, timestamp, image_folder, unique_id)
                            
                            # Add record
                            all_records.append((unique_id, current_date, page_title, product_name, image_path, kt, price, diamond_weight, additional_info_str))
                            
                            # Add to Excel
                            sheet.append([
                                current_date, 
                                page_title, 
                                product_name, 
                                None, 
                                kt, 
                                price, 
                                diamond_weight, 
                                time_only, 
                                image_url,
                                additional_info_str
                            ])
                            
                            # Add image to Excel if downloaded successfully
                            if image_path != "N/A":
                                try:
                                    img = Image(image_path)
                                    img.width, img.height = 100, 100
                                    sheet.add_image(img, f"D{row_counter}")
                                except Exception as e:
                                    logging.error(f"Error adding image to Excel: {e}")
                            
                            row_counter += 1

                        except Exception as e:
                            logging.error(f"Error processing product: {e}")
                            continue

                # Find next page URL from "Load More" button
                show_more_div = await page.query_selector('div.show-more')
                if show_more_div:
                    more_button = await show_more_div.query_selector('button.more.show-more-btn')
                    if more_button:
                        current_url = await more_button.get_attribute('data-url')
                        logging.info(f"Found next page URL: {current_url}")
                    else:
                        current_url = None
                else:
                    current_url = None

                await browser.close()
                await random_delay(3, 5)  # Increased delay between pages

        except Exception as e:
            logging.error(f"Error processing page {pages_processed + 1}: {e}")
            if 'browser' in locals():
                await browser.close()
            break
    
    if not all_records:
        return None, None, None

    # Save Excel file
    filename = f"Helzberg_{current_date}_{time_only}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    wb.save(file_path)
    logging.info(f"Data saved to {file_path}")

    # Database operations
    if all_records:
        insert_into_db(all_records)
    update_product_count(len(all_records))

    # Return results
    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    return base64_encoded, filename, file_path