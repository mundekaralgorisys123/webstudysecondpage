import asyncio
import re
import os
import uuid
import logging
import base64
import random
import time
from datetime import datetime
import httpx
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

# Flask and paths

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')


# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.png"
    image_full_path = os.path.join(image_folder, image_filename)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(image_url)
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

# Reliable page.goto wrapper
async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            await page.wait_for_selector(".collection-products", state="attached", timeout=30000)
            print("[Success] Product listing loaded.")
            return
        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                random_delay(1, 3)
            else:
                raise

# Main scraper function
async def handle_edgeofember(url, max_pages=None):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

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
    filename = f"handle_edgeofember_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    prev_prod_count = 0
    load_more_clicks = 1
    
    while load_more_clicks <= max_pages if max_pages else True:
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                browser, page = await get_browser_with_proxy_strategy(p, url, ".collection-products")
                log_event(f"Successfully loaded: {url}")

                # Handle overlay if it appears
                try:
                    await page.wait_for_selector(".klaviyo-popup", timeout=15000)
                    close_button = await page.query_selector(".klaviyo-popup .close")
                    if close_button:
                        await close_button.click()
                        log_event("Closed popup overlay")
                except Exception as e:
                    log_event(f"No overlay found or couldn't close it: {e}")

                # Scroll to load all items
                await scroll_to_bottom(page)
                
                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                # Get all product tiles
                products = await page.query_selector_all("div.product-grid-item")
                max_prod = len(products)
                products = products[prev_prod_count: min(max_prod, prev_prod_count + 30)]
                prev_prod_count += len(products)

                if len(products) == 0:
                    log_event("No new products found, stopping the scraper.")
                    break
                
                logging.info(f"New products found: {len(products)}")
                print(f"New products found: {len(products)}")
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):
                    additional_info = []
                    try:
                        # Extract product name
                        name_tag = await product.query_selector("a .eoe.line-small")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    try:
                        # Extract material (kt equivalent)
                        material_tag = await product.query_selector(".material")
                        material = (await material_tag.inner_text()).strip() if material_tag else "N/A"
                    except Exception:
                        material = "N/A"

                    # Handle prices - collect all price information
                    price_text = "N/A"
                    try:
                        # Get visible price (could be sale or regular)
                        visible_price_tag = await product.query_selector(".price-container .price:not(.price-hide)")
                        visible_price = (await visible_price_tag.inner_text()).strip() if visible_price_tag else None
                        
                        # Get hidden price (could be original price if on sale)
                        hidden_price_tag = await product.query_selector(".price-container .price.price-hide")
                        hidden_price = (await hidden_price_tag.inner_text()).strip() if hidden_price_tag else None
                        
                        # Format price text
                        if visible_price and hidden_price:
                            price_text = f"Sale: {visible_price} | Original: {hidden_price}"
                        elif visible_price:
                            price_text = f"Price: {visible_price}"
                            
                        # Clean up price string
                        price_text = re.sub(r'\s+', ' ', price_text).strip()
                    except Exception as e:
                        logging.warning(f"Error getting prices: {str(e)}")
                        price_text = "N/A"

                    # Get additional product information
                    try:
                        # Check for product badges (like "Bestseller", "New")
                        badge_elements = await product.query_selector_all(".product-badges span")
                        if badge_elements:
                            badges = [await el.inner_text() for el in badge_elements]
                            additional_info.append(f"Badges: {', '.join(b.strip() for b in badges if b.strip())}")
                    except:
                        pass

                    try:
                        # Check for gemstone/birthstone information
                        gemstone_match = re.search(r"\b(Aquamarine|Diamond|Ruby|Sapphire|Emerald|Topaz|Opal|Pearl|Amethyst|Citrine|Garnet|Peridot)\b", product_name, re.IGNORECASE)
                        if gemstone_match:
                            additional_info.append(f"Gemstone: {gemstone_match.group()}")
                    except:
                        pass

                    try:
                        # Check for birthstone month
                        birthstone_match = re.search(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b", product_name, re.IGNORECASE)
                        if birthstone_match:
                            additional_info.append(f"Birthstone Month: {birthstone_match.group()}")
                    except:
                        pass

                    try:
                        # Check for product type (earrings, necklace, etc.)
                        product_type_match = re.search(r"\b(Earrings|Necklace|Bracelet|Ring|Pendant|Choker|Band|Drop|Stud|Hoops)\b", product_name, re.IGNORECASE)
                        if product_type_match:
                            additional_info.append(f"Type: {product_type_match.group()}")
                    except:
                        pass

                    # Join all additional info with pipe delimiter
                    additional_info_text = " | ".join(additional_info) if additional_info else "N/A"

                    image_url = "N/A"
                    try:
                        # Get first image from the image container (prefer desktop version)
                        img_element = await product.query_selector(".image-container.desktop img") or await product.query_selector(".image-container.mobile img")
                        if img_element:
                            image_url = await img_element.get_attribute("src")
                            # Ensure we get the full resolution image if possible
                            if image_url and "width=" in image_url:
                                image_url = image_url.split("width=")[0] + "width=1000"
                    except Exception as e:
                        log_event(f"Error getting image URL: {e}")
                        image_url = "N/A"

                    # Extract gemstone/diamond information from description
                    gemstone_info = "N/A"
                    try:
                        gemstone_matches = re.findall(r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)?\s*(Aquamarine|Diamond|Ruby|Sapphire|Emerald|Topaz|Opal|Pearl|Amethyst|Citrine|Garnet|Peridot)\b", product_name, re.IGNORECASE)
                        if gemstone_matches:
                            gemstone_info = ", ".join([f"{size[0] if size[0] else ''} {stone}" for size, stone in gemstone_matches])
                    except:
                        pass

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

                if max_pages:
                    load_more_clicks += 1
                all_records.extend(records)
                wb.save(file_path)
                
        except Exception as e:
            logging.error(f"Error during scraping: {str(e)}")
            wb.save(file_path)
        finally:
            if page: await page.close()
            if browser: await browser.close()

    wb.save(file_path)
    log_event(f"Data saved to {file_path}")
    with open(file_path, "rb") as file:
        base64_encoded = base64.b64encode(file.read()).decode("utf-8")

    insert_into_db(all_records)
    update_product_count(len(all_records))

    return base64_encoded, filename, file_path