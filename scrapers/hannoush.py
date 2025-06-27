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
from playwright.async_api import async_playwright
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from proxysetup import get_browser_with_proxy_strategy
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")



BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

def get_high_res_image_url(image_url: str, desired_width: int = 640) -> str:
    parsed = urlparse(image_url)
    query = parse_qs(parsed.query)

    # Update or insert the width
    query['width'] = [str(desired_width)]
    if 'height' in query:
        query.pop('height')  # Remove height to avoid distortion

    # Reconstruct the URL with new query params
    new_query = urlencode(query, doseq=True)
    new_url = urlunparse(parsed._replace(query=new_query))
    return new_url


async def download_image(session, image_url, product_name, timestamp, image_folder, unique_id):
    if not image_url or image_url == "N/A":
        return "N/A"
    
    image_filename = f"{unique_id}_{timestamp}.jpg"
    image_full_path = os.path.join(image_folder, image_filename)
    
    # Modify the URL to fetch high-resolution image if available
    modified_url = get_high_res_image_url(image_url)
    
    for attempt in range(3):
        try:
            # Download the image
            resp = await session.get(modified_url, timeout=10)
            resp.raise_for_status()
            with open(image_full_path, "wb") as f:
                f.write(resp.content)
            return image_full_path
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/3 - Error downloading {product_name}: {e}")
    
    logging.error(f"Failed to download {product_name} after 3 attempts.")
    return "N/A"


async def handle_hannoush(url, max_pages):
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
    collected_products = []
    target_product_count = max_pages * 28  # Assuming approximately 28 products per scroll
    records = []
    image_tasks = []

    async with async_playwright() as p:
        product_wrapper_selector = "ul#product-grid"
        try:
            browser, page = await get_browser_with_proxy_strategy(p, url, product_wrapper_selector)
        except Exception:
            return None, None, None

        for scroll_index in range(max_pages):
            print(f"Scroll {scroll_index + 1}/{max_pages}")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)

            try:
                # Wait for the loading spinner to be hidden
                await page.wait_for_selector(".loading-overlay[style*='display: none']", state="hidden", timeout=10000)
            except Exception as e:
                print(f"Loader not found or hidden timeout: {e}")

            all_products = await page.locator('li.column').all()
            new_this_scroll = []

            for product in all_products:
                product_id = await product.get_attribute("data-productcode") or str(uuid.uuid4())
                if product_id not in seen_ids:
                    seen_ids.add(product_id)
                    new_this_scroll.append(product)

            print(f"New items this scroll: {len(new_this_scroll)}")
            collected_products.extend(new_this_scroll)

            if len(collected_products) >= target_product_count:
                break

        collected_products = collected_products[:target_product_count]
        print(f"Total products to process: {len(collected_products)}")
        page_title = await page.title()
        
        async with httpx.AsyncClient() as session:
            for idx, product in enumerate(collected_products):
                additional_info = []
                price_parts = []
                unique_id = str(uuid.uuid4())

                try:
                    # Extracting product name
                    product_name_locator = product.locator("a.product-card-title")
                    product_name = await product_name_locator.text_content()
                    product_name = product_name.strip() if product_name else "N/A"
                except Exception as e:
                    print(f"Error extracting product name: {e}")
                    product_name = "N/A"

                try:
                    # Extracting product prices - more comprehensive handling
                    price_container = product.locator("span.price").first
                    
                    # Check for sale price (ins tag)
                    sale_price_elem = price_container.locator("ins .amount").first
                    sale_price = await sale_price_elem.text_content() if await sale_price_elem.count() > 0 else None
                    
                    # Check for original price (del tag)
                    original_price_elem = price_container.locator("del .amount").first
                    original_price = await original_price_elem.text_content() if await original_price_elem.count() > 0 else None
                    
                    # Check for regular price (no sale)
                    regular_price_elem = price_container.locator(".amount").first
                    regular_price = await regular_price_elem.text_content() if await regular_price_elem.count() > 0 else None

                    # Format price string
                    if sale_price and original_price:
                        price = f"{original_price.strip()} | Sale: {sale_price.strip()}"
                        additional_info.append(f"Discount: {calculate_discount(original_price, sale_price)}")
                    elif sale_price:
                        price = sale_price.strip()
                    elif regular_price:
                        price = regular_price.strip()
                    else:
                        price = "N/A"

                except Exception as e:
                    print(f"Error extracting product price: {e}")
                    price = "N/A"

                try:
                    # Extracting product image URL - more reliable method
                    image_container = product.locator("a.product-featured-image-link").first
                    image_elem = image_container.locator("img.product-primary-image").first
                    
                    # Try src first, then srcset
                    image_url = await image_elem.get_attribute("src")
                    if not image_url or "crop=center" in image_url:  # Skip placeholder images
                        srcset = await image_elem.get_attribute("srcset")
                        if srcset:
                            # Get the highest resolution image from srcset
                            srcset_parts = [p.strip() for p in srcset.split(",")]
                            if srcset_parts:
                                # Sort by width and take the largest
                                srcset_parts.sort(key=lambda x: int(x.split(" ")[1].replace("w", "")))
                                image_url = srcset_parts[-1].split(" ")[0]

                    if image_url and image_url.startswith("//"):
                        image_url = "https:" + image_url
                    image_url = image_url if image_url else "N/A"
                except Exception as e:
                    print(f"Error extracting product image URL: {e}")
                    image_url = "N/A"

                # Extract metal type (kt) from product name
                kt_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                kt = kt_match.group() if kt_match else "N/A"

                # Extract diamond weight (if any)
                diamond_match = re.search(r"\b(\d+(\.\d+)?)\s*(?:ct|ctw|carat)\b", product_name, re.IGNORECASE)
                diamond_weight = f"{diamond_match.group(1)} ct" if diamond_match else "N/A"

                # Extract product dimensions/size if available
                size_match = re.search(r"\b(\d+(\.\d+)?)\s*x\s*(\d+(\.\d+)?)\s*mm\b", product_name, re.IGNORECASE)
                if size_match:
                    additional_info.append(f"Size: {size_match.group(1)}x{size_match.group(3)}mm")

                # Extract product code/sku
                try:
                    product_code = await product.get_attribute("data-productcode")
                    if product_code:
                        additional_info.append(f"Product Code: {product_code}")
                except:
                    pass

                # Extract badges (like 'Sale', 'New', etc.)
                try:
                    badges_locator = product.locator(".product-card--badges span")
                    badges = await badges_locator.all_text_contents()
                    if badges:
                        additional_info.append(f"Badges: {', '.join([b.strip() for b in badges if b.strip()])}")
                except:
                    pass

                # Schedule image download
                task = asyncio.create_task(
                    download_image(session, image_url, product_name, timestamp, image_folder, unique_id)
                )
                image_tasks.append((idx + 2, unique_id, task))

                # Join additional info with pipe delimiter
                additional_info_str = " | ".join(additional_info) if additional_info else "N/A"

                records.append((
                    unique_id, current_date, page_title, product_name, None, 
                    kt, price, diamond_weight, time_only, image_url, additional_info_str
                ))
                sheet.append([
                    current_date, page_title, product_name, None, 
                    kt, price, diamond_weight, time_only, 
                    image_url, additional_info_str
                ])

            # Process downloaded images
            for row, unique_id, task in image_tasks:
                try:
                    image_path = await task
                    if image_path != "N/A":
                        try:
                            img = Image(image_path)
                            img.width, img.height = 100, 100
                            sheet.add_image(img, f"D{row}")
                        except Exception as img_error:
                            logging.error(f"Error adding image to Excel: {img_error}")
                            image_path = "N/A"
                    
                    # Update record with image path
                    for i, record in enumerate(records):
                        if record[0] == unique_id:
                            records[i] = (
                                record[0], record[1], record[2], record[3], 
                                image_path, record[5], record[6], record[7], 
                                record[8], record[9], record[10]
                            )
                            break
                except Exception as e:
                    logging.error(f"Error processing image for row {row}: {e}")

        await browser.close()

        filename = f'handle_hannoush_{datetime.now().strftime("%Y-%m-%d_%H.%M")}.xlsx'
        file_path = os.path.join(EXCEL_DATA_PATH, filename)
        wb.save(file_path)
        log_event(f"Data saved to {file_path} | IP: {ip_address}")

        if records:
            insert_into_db(records)
        else:
            logging.info("No data to insert into the database.")

        update_product_count(len(collected_products))

        with open(file_path, "rb") as f:
            base64_encoded = base64.b64encode(f.read()).decode("utf-8")

        return base64_encoded, filename, file_path


def calculate_discount(original_price, sale_price):
    """Helper function to calculate discount percentage"""
    try:
        original = float(original_price.replace('$', '').replace(',', ''))
        sale = float(sale_price.replace('$', '').replace(',', ''))
        discount = ((original - sale) / original) * 100
        return f"{round(discount)}%"
    except:
        return "N/A"
