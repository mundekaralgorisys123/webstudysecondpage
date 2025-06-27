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
import json
import urllib

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


# Async image downloader
async def download_image_async(image_url, product_name, timestamp, image_folder, unique_id, retries=3):
    if not image_url or image_url == "N/A":
        return "N/A"

    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)

    # Enhanced headers with random user agents
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
    ]

    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://eu.louisvuitton.com/",
        "Origin": "https://eu.louisvuitton.com",
        "Sec-Fetch-Dest": "image",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-site",
    }

    async with httpx.AsyncClient(
        timeout=20.0,
        follow_redirects=True,
        headers=headers,
        limits=httpx.Limits(max_keepalive_connections=10),
    ) as client:
        for attempt in range(retries):
            try:
                # Add random delay between attempts
                await asyncio.sleep(random.uniform(0.1, 0.5))
                
                response = await client.get(image_url)
                
                # Check content type before processing
                content_type = response.headers.get("content-type", "")
                if "image" not in content_type:
                    raise ValueError(f"Unexpected content type: {content_type}")

                # Handle potential WebP images from .png URLs
                img = PILImage.open(BytesIO(response.content))
                if img.format not in ["JPEG", "PNG", "WEBP"]:
                    raise ValueError(f"Unsupported image format: {img.format}")

                # Convert and save as JPG
                if img.mode in ("RGBA", "LA", "P"):
                    background = PILImage.new("RGB", img.size, (255, 255, 255))  # white background
                    img = background.paste(img, mask=img.split()[-1]) if img.mode in ("RGBA", "LA") else background.paste(img)
                    img = background
                                
                img.save(image_full_path, "JPEG", quality=95, optimize=True)
                img.close()

                logging.info(f"Successfully processed {product_name}")
                return image_full_path

            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                logging.warning(f"Attempt {attempt+1}/{retries} - HTTP {status_code} for {product_name}")
                
                if status_code in [403, 429]:
                    # Rotate user agent for anti-bot protection
                    headers["User-Agent"] = random.choice(user_agents)
                    backoff = 2 ** attempt + random.random()
                    await asyncio.sleep(backoff)
                    continue
                    
                if 500 <= status_code < 600:
                    await asyncio.sleep(2 ** attempt)
                    continue

            except (PILImage.UnidentifiedImageError, ValueError) as e:
                logging.warning(f"Attempt {attempt+1}/{retries} - Image processing error: {str(e)}")
                if attempt == retries - 1 and response.content:
                    # Save problematic file for debugging
                    debug_path = os.path.join(image_folder, f"ERROR_{unique_id}.bin")
                    with open(debug_path, "wb") as f:
                        f.write(response.content)
                    logging.error(f"Saved problematic response to {debug_path}")

            except httpx.RequestError as e:
                logging.warning(f"Attempt {attempt+1}/{retries} - Network error: {str(e)}")
                await asyncio.sleep(1 + attempt * 2)

            except Exception as e:
                logging.error(f"Unexpected error processing {product_name}: {str(e)}")
                break

        logging.error(f"Permanent failure for {product_name} after {retries} attempts")
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

async def safe_goto_and_wait(page, url, retries=3):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            
            # Use networkidle to ensure page stability
            await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            
            # Ensure product cards are loaded
            product_cards = await page.wait_for_selector(".lv-paginated-list.lv-product-list", state="attached", timeout=30000)

            # Optionally validate at least 1 product card is visible
            if product_cards:
                print("[Success] Product cards loaded.")
                return
            
            # Handle UCM banner, ensuring no interference with the page load
            try:
                # Wait for and close the banner if it exists
                banner = await page.wait_for_selector("#ucm-banner", timeout=5000, state="attached")
                close_button = await banner.wait_for_selector("a.ucm-closeBanner", timeout=2000)
                
                # Close banner with JS to avoid layout shifts
                await close_button.evaluate("node => node.click()")
                
                # Wait for banner removal while preserving other elements
                await page.wait_for_selector("#ucm-banner", state="detached", timeout=3000)
                print("✔️  UCM banner closed gracefully")
            except Exception as banner_error:
                # If no banner or issue closing it
                print(f"ℹ️  No banner found or error closing: {str(banner_error)}")
            
            # Wait for product data with checks for at least one item and its visibility
            await page.wait_for_function(
                """() => {
                    const items = document.querySelectorAll('.lv-product-list__items > *');
                    return items.length > 0 && items[0].offsetHeight > 0;
                }""",
                timeout=45000
            )
            
            # Ensure images are loaded properly
            await page.wait_for_selector('.lv-product-list__items img[src]:not([src=""])', timeout=15000)
            print("[Success] Product data fully rendered and images loaded.")
            return

        except (Error, TimeoutError) as e:
            logging.warning(f"Attempt {attempt + 1} failed: {str(e)[:100]}...")
            if attempt < retries - 1:
                print(f"[Retrying] Attempt {attempt + 1} failed. Reloading page and trying again...")
                await page.reload()
                await asyncio.sleep(2 + attempt * 3)
            else:
                # Reraise the exception if we reach the last retry
                print(f"[Failed] Maximum retries reached. Exiting with failure.")
                raise


def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}page={page_count}"      
    
# Main scraper function
async def handle_louisvuitton(url, max_pages=None):
    ip_address = get_public_ip()
    logging.info(f"Scraping started for: {url} from IP: {ip_address}")

    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)

    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath"]
    sheet.append(headers)

    all_records = []
    filename = f"handle_louisvuitton_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)
    load_more_clicks = 1
    previous_count = 0
    while load_more_clicks <= max_pages:
        current_url = build_url_with_loadmore(url, load_more_clicks)
        browser = None
        page = None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp(PROXY_URL)
                context = await browser.new_context()
                page = await context.new_page()
                page.set_default_timeout(1200000)

                await safe_goto_and_wait(page, current_url)
                log_event(f"Successfully loaded: {current_url}")

                # Scroll to load all items
                # await scroll_to_bottom(page)
                
                 # Simulate clicking 'Load More' number of times
                # for _ in range(load_more_clicks - 1):
                #     try:
                #         load_more_button = page.locator("button.lv-paginated-list__button.lv-button.-secondary.lv-product-list__load-more")
                #         if await load_more_button.is_visible():
                #             await load_more_button.click()
                #             await asyncio.sleep(2)
                #         else:
                #             logging.info("'Load More' button not visible, stopping clicks.")
                #             break
                #     except Exception as e:
                #         logging.warning(f"Could not click 'Load More': {e}")
                #         break

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")


               
                product_wrapper = await page.query_selector("ul.lv-product-list__items")
                all_products = await product_wrapper.query_selector_all(
                    ":scope > li.lv-product-list__item"
                )
                    
               
               
                total_products = len(all_products)
                new_products = all_products[previous_count:]
                logging.info(f"Page {load_more_clicks}: Total = {total_products}, New = {len(new_products)}")
                previous_count = total_products

                print(f"Page {load_more_clicks}: Scraping {len(new_products)} new products.")
                page_title = await page.title()
                
                records = []
                image_tasks = []
                
                for row_num, product in enumerate(new_products, start=len(sheet["A"]) + 1):
                    try:
                        # Extract product name - from lv-product-card__name anchor
                        name_tag = await product.query_selector(".lv-product-card__name a")
                        product_name = (await name_tag.inner_text()).strip() if name_tag else "N/A"
                    except Exception:
                        product_name = "N/A"

                    try:
                        # Extract price - from .lv-price .notranslate
                        price_tag = await product.query_selector(".lv-price .notranslate")
                        price = (await price_tag.inner_text()).strip() if price_tag else "N/A"
                        # Clean up price string
                        price = re.sub(r'\s+', ' ', price).strip()
                    except Exception:
                        price = "N/A"

                    # Description is same as product name in this case
                    description = product_name
                    image_url = "N/A"
                    try:
                        # Use more specific selector for the image element
                        img_tag = await product.query_selector("img.lv-smart-picture__object")
                        
                        if img_tag:
                            # Get the data-srcset attribute instead of regular srcset
                            srcset = await img_tag.get_attribute("data-srcset")
                            
                            if srcset:
                                # Extract all available image URLs
                                sources = [url.split('?')[0] for url in srcset.split(', ')]
                                
                                # Select the highest resolution image (last in the list)
                                if sources:
                                    image_url = sources[-1].split()[0]
                            else:
                                # Fallback to data-src if needed
                                srcset = await img_tag.get_attribute("srcset")
                                if srcset:
                                    # Take first URL from srcset and remove query parameters
                                    first_image = srcset.split(',')[0].strip().split()[0]
                                    image_url = first_image.split('?')[0]

                    except Exception as e:
                        log_event(f"Error getting image URL: {e}")
                        image_url = "N/A"

                    # print(f"Final image URL: {image_url}")
            
                    # Extract gold type (kt) from product name/description
                    gold_type_pattern = r"\b\d{1,2}(?:K|kt|ct|Kt)\b|\bPlatinum\b|\bSilver\b|\bWhite Gold\b|\bYellow Gold\b|\bRose Gold\b"
                    gold_type_match = re.search(gold_type_pattern, description, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"


                    # Extract diamond weight from description
                    diamond_weight_pattern = r"\b\d+(\.\d+)?\s*(?:ct|tcw|carat)\b"
                    diamond_weight_match = re.search(diamond_weight_pattern, description, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"
                    
                    additional_info = []

                    try:
                        tag_els = await product.query_selector_all("div.lv-product-card-label span")
                        if tag_els:
                            for tag_el in tag_els:
                                tag_text = await tag_el.inner_text()
                                if tag_text:
                                    additional_info.append(tag_text.strip())
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