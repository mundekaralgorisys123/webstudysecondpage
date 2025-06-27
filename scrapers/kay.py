import json
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
from database import insert_into_db, insert_into_db_details
from limit_checker import update_product_count
from io import BytesIO
import httpx
from playwright.async_api import async_playwright, TimeoutError, Error as PlaywrightError
from playwright.async_api import Page
import traceback
from typing import List, Tuple
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
load_dotenv()
PROXY_URL = os.getenv("PROXY_URL")


PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')

async def download_and_resize_image(session, image_url):
    try:
        async with session.get(modify_image_url(image_url), timeout=10) as response:
            if response.status != 200:
                return None
            content = await response.read()
            image = PILImage.open(BytesIO(content))
            image.thumbnail((200, 200))
            img_byte_arr = BytesIO()
            image.save(img_byte_arr, format='JPEG', optimize=True, quality=85)
            return img_byte_arr.getvalue()
    except Exception as e:
        logging.warning(f"Error downloading/resizing image: {e}")
        return None

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

def random_delay(min_sec=1, max_sec=3):
    """Introduce a random delay to mimic human-like behavior."""
    time.sleep(random.uniform(min_sec, max_sec))


########################################  safe_goto_and_wait ####################################################################


async def safe_goto_and_wait(page, url,isbri_data, retries=2):
    for attempt in range(retries):
        try:
            print(f"[Attempt {attempt + 1}] Navigating to: {url}")
            
            if isbri_data:
                await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
            else:
                await page.goto(url, wait_until="networkidle", timeout=180_000)

            # Wait for the selector with a longer timeout
            product_cards = await page.wait_for_selector(".product-scroll-wrapper", state="attached", timeout=30000)

            # Optionally validate at least 1 is visible (Playwright already does this)
            if product_cards:
                print("[Success] Product cards loaded.")
                return
        except Error as e:
            logging.error(f"Error navigating to {url} on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                logging.info("Retrying after waiting a bit...")
                random_delay(1, 3)  # Add a delay before retrying
            else:
                logging.error(f"Failed to navigate to {url} after {retries} attempts.")
                raise
        except TimeoutError as e:
            logging.warning(f"TimeoutError on attempt {attempt + 1} navigating to {url}: {e}")
            if attempt < retries - 1:
                logging.info("Retrying after waiting a bit...")
                random_delay(1, 3)  # Add a delay before retrying
            else:
                logging.error(f"Failed to navigate to {url} after {retries} attempts.")
                raise


########################################  get browser with proxy ####################################################################
      

async def get_browser_with_proxy_strategy(p, url: str):
    """
    Dynamically checks robots.txt and selects proxy accordingly
    Always uses proxies - never scrapes directly
    """
    parsed_url = httpx.URL(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.host}"
    
    # 1. Fetch and parse robots.txt
    disallowed_patterns = await get_robots_txt_rules(base_url)
    
    # 2. Check if URL matches any disallowed pattern
    is_disallowed = check_url_against_rules(str(parsed_url), disallowed_patterns)
    
    # 3. Try proxies in order (bri-data first if allowed, oxylabs if disallowed)
    proxies_to_try = [
        PROXY_URL if not is_disallowed else {
            "server": PROXY_SERVER,
            "username": PROXY_USERNAME,
            "password": PROXY_PASSWORD
        },
        {  # Fallback to the other proxy
            "server": PROXY_SERVER,
            "username": PROXY_USERNAME,
            "password": PROXY_PASSWORD
        } if not is_disallowed else PROXY_URL
    ]

    last_error = None
    for proxy_config in proxies_to_try:
        browser = None
        try:
            isbri_data = False
            if proxy_config == PROXY_URL:
                logging.info("Attempting with bri-data proxy (allowed by robots.txt)")
                browser = await p.chromium.connect_over_cdp(PROXY_URL)
                isbri_data = True
            else:
                logging.info("Attempting with oxylabs proxy (required by robots.txt)")
                browser = await p.chromium.launch(
                    proxy=proxy_config,
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-web-security'
                    ]
                )

            context = await browser.new_context()
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """)
            page = await context.new_page()
            
            await safe_goto_and_wait(page, url,isbri_data)
            return browser, page

        except Exception as e:
            last_error = e
            error_trace = traceback.format_exc()
            logging.error(f"Proxy attempt failed:\n{error_trace}")
            if browser:
                await browser.close()
            continue

    error_msg = (f"Failed to load {url} using all proxy options. "
                f"Last error: {str(last_error)}\n"
                f"URL may be disallowed by robots.txt or proxies failed.")
    logging.error(error_msg)
    raise RuntimeError(error_msg)




async def get_robots_txt_rules(base_url: str) -> List[str]:
    """Dynamically fetch and parse robots.txt rules"""
    robots_url = f"{base_url}/robots.txt"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(robots_url, timeout=10)
            if resp.status_code == 200:
                return [
                    line.split(":", 1)[1].strip()
                    for line in resp.text.splitlines()
                    if line.lower().startswith("disallow:")
                ]
    except Exception as e:
        logging.warning(f"Couldn't fetch robots.txt: {e}")
    return []


def check_url_against_rules(url: str, disallowed_patterns: List[str]) -> bool:
    """Check if URL matches any robots.txt disallowed pattern"""
    for pattern in disallowed_patterns:
        try:
            # Handle wildcard patterns
            if "*" in pattern:
                regex_pattern = pattern.replace("*", ".*")
                if re.search(regex_pattern, url):
                    return True
            # Handle path patterns
            elif url.startswith(f"{pattern}"):
                return True
            # Handle query parameters
            elif ("?" in url) and any(
                f"{param}=" in url 
                for param in pattern.split("=")[0].split("*")[-1:]
                if "=" in pattern
            ):
                return True
        except Exception as e:
            logging.warning(f"Error checking pattern {pattern}: {e}")
    return False



def build_url_with_loadmore(base_url: str, page_count: int) -> str:
    separator = '&' if '?' in base_url else '?'
    return f"{base_url}{separator}loadMore={page_count}"   

#===========================================================================================================         

# async def await_inner_details(page, url, retries=4) -> bool:
#     """Attempt to load page and wait for critical selector. Returns True if successful, False otherwise."""
#     for attempt in range(retries):
#         try:
#             logging.info(f"[Attempt {attempt + 1}/{retries}] Navigating to: {url}")

#             await page.goto(url, timeout=180_000, wait_until="networkidle")

#             await page.wait_for_selector(".main-inner", state="visible", timeout=60000)

#             logging.info("[Success] Product page loaded and title found.")
#             return True

#         except TimeoutError as e:
#             logging.warning(f"[Attempt {attempt + 1}] TimeoutError: {e}")
#         except PlaywrightError as e:
#             logging.error(f"[Attempt {attempt + 1}] Playwright error: {e}")

#         if attempt < retries - 1:
#             wait_sec = random.uniform(1, 3)
#             logging.info(f"Retrying after {wait_sec:.2f}s...")
#             await asyncio.sleep(wait_sec)

#     logging.error(f"❌ Failed to load page after {retries} attempts: {url}")
#     return False


# async def await_inner_details(page, url, retries=4):
#     """Attempt to load page by trying multiple potential selectors, including generic fallbacks."""
#     MAIN_SELECTORS = [
#         ".main-inner",
#         ".product-detail__summary--name h1",
#         ".product-price__price",
#         ".product-detail__affirm-plcc",
#         ".pdp-spinner",
#         "div.product-detail__summary--price",
#     ]
#     for attempt in range(retries):
#         try:
#             logging.info(f"[Attempt {attempt + 1}/{retries}] Navigating to: {url}")

#             if attempt % 2 == 0:
#                 await page.goto(url, timeout=180_000, wait_until="networkidle")
#             else:
#                 await page.goto(url, timeout=180_000, wait_until="domcontentloaded")
#                 await page.wait_for_load_state("networkidle", timeout=60_000)

#             # Try Main Selectors
#             for selector in MAIN_SELECTORS:
#                 try:
#                     await page.wait_for_selector(selector, state="visible", timeout=30_000)
#                     logging.info(f"[Success] Page Loaded for {url} (found {selector})")
#                     return True
#                 except TimeoutError:
#                     continue

#             # Fallback: Try a Generic H1
#             try:
#                 await page.wait_for_selector("h1", state="visible", timeout=10_000)
#                 logging.warning(f"[Fallback] Loaded page for {url} using generic <h1> tag.")
#                 return True
#             except TimeoutError:
#                 logging.warning(f"[Attempt {attempt + 1}] No known selectors found for {url}")

#         except TimeoutError:
#             logging.warning(f"[Attempt {attempt + 1}] Timeout error. Retrying...")
#             await page.reload()
#             await asyncio.sleep(random.uniform(2, 3))
#         except Exception as e:
#             logging.error(f"[Attempt {attempt + 1}] Navigation error: {str(e)}")
#             await asyncio.sleep(random.uniform(2, 3))

#     logging.error(f"❌ Failed to load page after {retries} attempts: {url}")
#     return False


async def await_inner_details(page, url, retries=5):
    """Robust page loading with advanced detection and rendering fixes"""
    PRIORITY_SELECTORS = [
        ".pdp-title",
        "div.product-price__price",
        ".pdp-product-number",
        ".pdp-inventory-loading",
        "h1.product-name",
        ".product-image-gallery__image"
    ]
    CONTENT_MARKERS = [
        "Lab-Grown Diamond",
        "ct tw",
        "Gold",
        "Sterling Silver",
        "Add to Cart"
    ]
    for attempt in range(retries):
        try:
            logging.info(f"[Attempt {attempt + 1}/{retries}] Loading: {url}")

            cache_buster = random.randint(100000, 999999)
            await page.goto(f"{url}?cacheBust={cache_buster}",
                            timeout=120_000,
                            wait_until="commit",
                            referer="https://www.kay.com/")
            
            await page.wait_for_load_state("domcontentloaded", timeout=30_000)
            await asyncio.sleep(1.5)
            await page.wait_for_load_state("load", timeout=30_000)
            await asyncio.sleep(1)

            try:
                await page.wait_for_selector(".pdp-inventory-loading",
                                              state="visible",
                                              timeout=5_000)
                await page.wait_for_selector(".pdp-inventory-loading",
                                              state="hidden",
                                              timeout=20_000)
            except Exception:
                pass

            await page.evaluate("() => { document.documentElement.scrollTop += 10; }")
            await asyncio.sleep(0.5)

            # Check priority selectors
            for selector in PRIORITY_SELECTORS:
                try:
                    await page.wait_for_selector(selector, state="visible", timeout=15_000)
                    logging.info(f"[Priority] Found {selector} on {url}")
                    return True
                except TimeoutError:
                    continue

            # Check content markers
            for marker in CONTENT_MARKERS:
                try:
                    await page.wait_for_selector(f"text='{marker}'",
                                                  state="visible",
                                                  timeout=10_000)
                    logging.warning(f"[Content] Found '{marker}' on {url}")
                    return True
                except TimeoutError:
                    continue

            body_text = await page.evaluate("() => document.body.innerText") or ""
            if any(marker in body_text for marker in CONTENT_MARKERS):
                logging.warning("[Text Analysis] Found content markers in page text.")
                return True

            await page.screenshot(path=f"attempt_{attempt + 1}_{url.split('/')[-1]}.png", full_page=True)
            logging.warning(f"[Attempt {attempt + 1}] No detectable content.")

        except TimeoutError:
            logging.warning(f"[Attempt {attempt + 1}] Timeout.")
        except Exception as e:
            logging.error(f"[Attempt {attempt + 1}] Error: {str(e)}")
            await asyncio.sleep(random.uniform(3, 5))

        if attempt == retries - 2:
            logging.warning("Resetting browser context...")
            await page.context.clear_cookies()
            await page.evaluate("() => { localStorage.clear(); sessionStorage.clear(); }")

    # Final diagnostics
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        product_id = url.split("/")[-1] if "/p/" in url else "unknown"

        await page.screenshot(path=f"kay_failure_{product_id}_{timestamp}.png", full_page=True)

        content = await page.content()
        with open(f"kay_failure_{product_id}_{timestamp}.html", "w", encoding="utf-8") as f:
            f.write(content)

        user_agent = await page.evaluate("() => navigator.userAgent")
        viewport_size = await page.evaluate("() => ({width: window.innerWidth, height: window.innerHeight})")

        logging.error(f"Diagnostics saved for {product_id}")
        logging.error(f"User Agent: {user_agent}")
        logging.error(f"Viewport: {viewport_size}")
        logging.error(f"Content Length: {len(content)} characters")
    except Exception as e:
        logging.error(f"Diagnostic failure: {str(e)}")

    logging.error(f"❌ Critical failure after {retries} attempts: {url}")
    return False




async def scrape_product_detail(url):
    title = sku = final_output_price = protection_plan = monthly_payment = review_summary = "N/A"
    ring_sizes = []
    image_urls = []
    product_specifications = {}

   
    seen_sections = set()

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ]
    CAPTCHA_API_KEY = "7f23f281047d1036dcbb62bfa691218f"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",  # ✅ Will open a maximized window
                "--window-size=1920,1080"
            ]
            )
            # use without proxy
            # context = await browser.new_context()
            
            context = await browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/127.0.0.0 Safari/537.36"),
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                java_script_enabled=True,
                bypass_csp=True,
                extra_http_headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
                    "Accept-Language": "en-US,en;q=0.9",
                },
                # proxy={  
                #     "server": PROXY_SERVER,
                #     "username": PROXY_USERNAME,
                #     "password": PROXY_PASSWORD
                # }
            )


            
           

            page = await context.new_page()
            
            
            # After page is created
            await page.add_init_script("""
                // WebDriver detection
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                // Plugins spoof
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                // Languages spoof
                Object.defineProperty(navigator, 'languages', { get: () => ["en-US", "en"] });
                // GPU/Renderer spoof
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    if (parameter === 37445) return 'Intel Inc.';
                    if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                    return getParameter(parameter);
                };
                // Chrome Property
                window.chrome = { runtime: {} };
            """)

            
            logging.info(f"Opening page: {url}")

            page_loaded = await await_inner_details(page, url)

            if not page_loaded:
                return {
                    "error": "Page failed to load after multiple attempts.",
                    "url": url,
                    "diagnostics": {
                        "screenshot": f"kay_failure_{url.split('/')[-1]}.png",
                        "html_dump": f"kay_failure_{url.split('/')[-1]}.html"
                    }
                }

            await asyncio.sleep(3)  # Final wait
            logging.info(f"Successfully accessed {url}")
            
            
            
            
            # Wait for a critical element (product title) as an anchor
            # try:
            #     await page.wait_for_selector('.product-detail__summary--name h1', state='visible', timeout=30000)
            # except Exception as e:
            #     logging.warning(f"Title element didn’t appear within 30s: {e}")

            # Wait for critical elements to load
            try:
                logging.info("Scrolling from top to bottom to load all lazy-loaded content...")
                previous_height = await page.evaluate("document.body.scrollHeight")
            
                while True:
                    await page.evaluate("window.scrollBy(0, 1000)")  # scroll in steps
                    await asyncio.sleep(0.5)  # allow lazy elements to load
            
                    new_height = await page.evaluate("document.body.scrollHeight")
                    if new_height == previous_height:
                        break  # reached the bottom
                    previous_height = new_height
            
                logging.info("Scrolling completed.")
            except Exception as e:
                logging.warning(f"Scrolling failed: {e}")

            # # Wait and click the "No, thanks" button using aria-label
            try:
                await page.wait_for_selector('button[aria-label="No, thanks; close the dialog"]', timeout=5000)
                await page.click('button[aria-label="No, thanks; close the dialog"]')
                print("Clicked the 'No, thanks' button.")
            except:
                print("Button not found or already closed.")
                
          
            # SKU Extraction
            try:
                sku_el = await page.query_selector(".product-detail__intro--productcode")
                if sku_el:
                    text = (await sku_el.inner_text()).strip()
                    if "Item #:" in text:
                        sku = text.split("Item #:")[1].strip()
            except Exception as e:
                logging.warning(f"[SKU Extraction Error] {e}")

            # Images
            base_url = "https://www.kay.com"
            try:
                img_elements = await page.query_selector_all(".swiper-slide img")
                for img in img_elements:
                    src = await img.get_attribute("src")
                    if src and src.startswith("/productimages/processed"):
                        image_urls.append(base_url + src)
            except Exception as e:
                logging.warning(f"[Image Extraction Error] {e}")

            # Title
            # try:
            #     title = await page.locator("div.product-detail__summary--name h1").inner_text()
            # except:
            #     title = "N/A"
            
            title = "N/A"
            try:
                title_locator = page.locator("div.product-detail__summary--name h1")
                if await title_locator.count() > 0 and await title_locator.first.is_visible():
                    title_candidate = await title_locator.first.inner_text()
                    if title_candidate.strip():
                        title = title_candidate.strip()
            except Exception as e:
                logging.warning(f"[Title Extraction Error] {str(e)}")
                        
            



            # Prices and Discount
            try:
                discounted_price = await page.locator("span.product-price__price").inner_text()
            except:
                discounted_price = "N/A"
            
            try:
                original_price = await page.locator("span.product-price__striked").inner_text()
            except:
                original_price = None
            
            try:
                discount = await page.locator("span.tag-text").inner_text()
            except:
                discount = None
            
            # Construct final string smartly
            price_info = f"Discounted Price: {discounted_price.strip()}"
            
            price_info += f" | Original Price: {original_price.strip() if original_price else 'N/A'}"
            price_info += f" | Discount: {discount.strip() if discount else 'N/A'}"
            
            final_output_price = price_info


            try:
                # This gets the full visible text, including amount and total
                monthly_payment = await page.inner_text("div.affirm-plcc.ng-star-inserted")
            except Exception as e:
                logging.error(f"Error extracting monthly payment: {str(e)}")
                monthly_payment = "N/A"
            
            




            # Ring Sizes
            try:
                # Wait until ring size selector is loaded
                await page.wait_for_selector("div.ring-size-selector label.selector-label", timeout=10000)
                
                # Get all visible ring size texts
                size_elements = await page.locator("div.ring-size-selector label.selector-label").all_inner_texts()
            
                # Clean each size (remove asterisks and extra whitespace)
                ring_sizes = [s.strip().replace("*", "") for s in size_elements]
            
                # print("Extracted Ring Sizes:", ring_sizes)
            
            except Exception as e:
                logging.warning(f"Failed to extract ring sizes: {e}")
                ring_sizes = []


            
            # # Protection Plan

            protection_plan = "N/A"

            try:
                protection_plan_data = {"title": "", "subtitle": "", "services": []}
                # Wait for the protection area itself
                await page.wait_for_selector("app-product-protection-plan-details div.warranty_heading", state="visible", timeout=5000)

                warranty_heading_locator = page.locator("app-product-protection-plan-details div.warranty_heading span")
                if await warranty_heading_locator.count() >= 2:
                    title_block = await warranty_heading_locator.all_inner_texts()
                    protection_plan_data["title"] = title_block[0].strip() if title_block else ""
                    protection_plan_data["subtitle"] = title_block[1].strip() if len(title_block) > 1 else ""

                rows_locator = page.locator("app-product-protection-plan-details table.table tbody tr")
                row_count = await rows_locator.count()
                for i in range(row_count):
                    if await rows_locator.nth(i).is_visible():
                        cols = await rows_locator.nth(i).locator("td").all_inner_texts()
                        if len(cols) >= 3:
                            protection_plan_data["services"].append({
                                "service_needed": cols[0].strip(),
                                "typical_cost": cols[1].strip(),
                                "with_plan_cost": cols[2].strip()
                            })

                protection_plan = json.dumps(protection_plan_data, ensure_ascii=False)

            except Exception as e:
                logging.error(f"[Protection Plan Extraction Error] {str(e)}")
                protection_plan = json.dumps({"error": f"Error extracting protection plan: {str(e)}"})


            try:
                # Try Email Sign Up close button
                email_close = await page.query_selector('button[id^="bx-close-inside-"]')
                if email_close:
                    await email_close.click()
                    print("✅ Closed the email sign-up dialog.")
            except Exception as e:
                print(f"ℹ️ Email sign-up dialog error or not found: {e}")    
          
            
            # Product Specifications
            try:
                
                # 1. Expand 'Details' accordion if collapsed
                try:
                    details_button = await page.query_selector('button#Details[aria-expanded="false"]')
                    if details_button:
                        await details_button.click()
                        await page.wait_for_selector("div.accordion-body", state="visible", timeout=8000)
                        await asyncio.sleep(1)
                        logging.info("Expanded 'Details' accordion")
                except Exception as e:
                    logging.warning(f"Could not expand Details accordion: {e}")
            
                # 2. More resilient table handling
                tables = []
                try:
                    tables = await page.query_selector_all("table.specs-table")
                    if not tables:
                        await page.wait_for_selector("table.specs-table", state="attached", timeout=15000)
                        tables = await page.query_selector_all("table.specs-table")
                except Exception as e:
                    logging.warning(f"Table query error: {e}")
            
                if not tables:
                    product_specifications = {"error": "No specifications found"}
                    logging.info("No specification tables found")
                else:
                    logging.info(f"Found {len(tables)} specification tables")
            
                    for table in tables:
                        try:
                            # Default section title
                            section_title = "Specifications"
            
                            # Try different header locations
                            section_title_elem = await table.query_selector("thead th[colspan]") or \
                                                  await table.query_selector("caption") or \
                                                  await table.query_selector("thead tr:first-child th")
            
                            if section_title_elem:
                                section_title = (await section_title_elem.inner_text()).strip()
            
                            if section_title in seen_sections:
                                continue
                            seen_sections.add(section_title)
            
                            section_data = {}
            
                            rows = await table.query_selector_all("tbody tr")
                            for row in rows:
                                try:
                                    cells = await row.query_selector_all("td")
                                    if len(cells) >= 2:
                                        key = (await cells[0].inner_text()).strip()
                                        value = (await cells[1].inner_text()).strip()
                                        if key or value:
                                            section_data[key] = value
                                except Exception:
                                    continue  # Skip problematic rows
            
                            if section_data:
                                product_specifications[section_title] = section_data
            
                        except Exception as e:
                            logging.warning(f"Error processing table: {e}")
                            continue
            
                if not product_specifications:
                    product_specifications = {"message": "No specifications found"}
            
            except Exception as e:
                product_specifications = {"error": f"Specifications extraction failed: {str(e)}"}
                logging.error(f"Specifications extraction failed: {e}")
            


            
            # Reviews Extraction with \n in output
            
            try:
                # Expand REVIEWS accordion if collapsed
                review_data = {
                    "summary": {},
                    "overview": {},
                    "breakdown": {},
                    "reviews": []
                }
                try:
                    reviews_button = await page.query_selector('button#Reviews[aria-expanded="false"]')
                    if reviews_button:
                        await reviews_button.click()
                        await page.wait_for_selector("app-pdp-reviews-display", state="visible", timeout=8000)
                        await asyncio.sleep(1)
                        logging.info("Expanded 'Reviews' accordion successfully.")
                except Exception as e:
                    logging.warning(f"Failed to expand 'Reviews' accordion: {e}")
            
                # Extract review summary text and overall rating
                try:
                    review_text = await page.locator(".pdp-review-stars__desc").inner_text()
                    overall_rating = await page.locator(".pdp-review-stars-rating__count").inner_text()
                    review_data["summary"] = {
                        "text": review_text.strip(),
                        "overall_rating": overall_rating.strip()
                    }
                except Exception as e:
                    logging.warning(f"Review summary extraction failed: {e}")
                    review_data["summary"] = {
                        "text": "Review summary not available",
                        "overall_rating": "N/A"
                    }
            
                # Ratings Overview
                try:
                    star_rows = await page.locator(".pdp-review-breakdown__second-ratings").all()
                    for row in star_rows:
                        stars = await row.locator(".second-ratings-name").inner_text()
                        count = await row.locator(".second-rating-count").inner_text()
                        review_data["overview"][stars.strip()] = count.strip()
                except Exception as e:
                    logging.warning(f"Ratings overview extraction failed: {e}")
            
                # Ratings Breakdown
                try:
                    breakdown_rows = await page.locator("app-signet-pdp-rating-breakdown .pdp-review-breakdown__second-ratings").all()
                    for row in breakdown_rows[:3]:
                        category = await row.locator(".second-ratings-name").inner_text()
                        rating = await row.locator(".second-rating-count").inner_text()
                        review_data["breakdown"][category.strip()] = rating.strip()
                except Exception as e:
                    logging.warning(f"Ratings breakdown extraction failed: {e}")
            
                # Customer Reviews
                try:
                    reviews = await page.locator(".pdp-review-display__review").all()
                    for review in reviews[:5]:  # limit to 5
                        try:
                            reviewer = await review.locator(".pdp-review-display__name").inner_text()
                            date = await review.locator(".pdp-review-display__time").inner_text()
                            stars = len(await review.locator(".fa-Star-Rated").all())
                            
                            # Badges
                            badges = []
                            badge_elements = await review.locator(".pdp-review-display__review-badge-text").all()
                            for badge in badge_elements:
                                badge_text = await badge.inner_text()
                                if badge_text.strip():
                                    badges.append(badge_text.strip())
            
                            # Content
                            title = await review.locator(".pdp-review-display__title").inner_text() or "No Title"
                            content = await review.locator(".pdp-review-display__content").inner_text()
            
                            # Recommendation
                            recommend = bool(await review.locator(".pdp-review-display__recommend").count())
            
                            # Helpful counts
                            helpful_text = await review.locator(".pdp-review-display__helpful").inner_text()
                            yes_match = re.search(r'Yes \((\d+)\)', helpful_text)
                            no_match = re.search(r'No \((\d+)\)', helpful_text)
                            yes_count = int(yes_match.group(1)) if yes_match else 0
                            no_count = int(no_match.group(1)) if no_match else 0
            
                            # Image count
                            images = await review.locator(".pdp-review-display__images img").count()
            
                            review_data["reviews"].append({
                                "reviewer": reviewer.strip(),
                                "date": date.strip(),
                                "rating": stars,
                                "badges": badges,
                                "title": title.strip(),
                                "content": content.strip(),
                                "recommends": recommend,
                                "helpful": {"yes": yes_count, "no": no_count},
                                "images": images
                            })
            
                        except Exception as e:
                            logging.warning(f"Error processing individual review: {e}")
                except Exception as e:
                    logging.warning(f"Customer reviews extraction failed: {e}")
            
            except Exception as review_error:
                logging.error(f"[Review Extraction Error] {review_error}")
                review_data = {
                    "summary": {"text": "Review information not available", "overall_rating": "N/A"},
                    "overview": {},
                    "breakdown": {},
                    "reviews": []
                }
            
            # Convert to JSON string for DB
            review_summary = json.dumps(review_data, ensure_ascii=False)

            await browser.close()
            await context.close()
            await page.close()

            return {
                "title": title,
                "sku": sku,
                "final_output_price": final_output_price,
                "ring_sizes": ring_sizes,
                "protection_plan": protection_plan,
                "monthly_payment": monthly_payment,
                "product_details": product_specifications,
                "review_summary": review_summary,
                "image_urls": image_urls
            }

    except Exception as e:
        logging.error(f"[Detail Error] {url}: {e}")
        return {
            "title": title,
            "sku": sku,
            "final_output_price": final_output_price,
            "ring_sizes": ring_sizes,
            "protection_plan": protection_plan,
            "monthly_payment": monthly_payment,
            "product_details": product_specifications,
            "review_summary": review_summary,
            "image_urls": image_urls
        }
        
    finally:
        if page:
            await page.close()
        if context:
            await context.close()
        if browser:
            await browser.close()          
        
         
#===========================================================================================================         
########################################  Main Function Call ####################################################################
async def handle_kay(url, max_pages):
    print("============================")
    print(url)
    print("============================")
    # Prepare directories and files
    os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_folder = os.path.join(IMAGE_SAVE_PATH, timestamp)
    os.makedirs(image_folder, exist_ok=True)
    # Create workbook and setup
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Products"
    headers = ["Current Date", "Header", "Product Name", "Image", "Kt", "Price", "Total Dia wt", "Time", "ImagePath", "Additional Info"]
    sheet.append(headers)

    all_records = []
    records_details = []
    filename = f"Kay_{datetime.now().strftime('%Y-%m-%d_%H.%M')}.xlsx"
    file_path = os.path.join(EXCEL_DATA_PATH, filename)

    page_count = 0
    success_count = 0

    async with async_playwright() as p:
        while page_count < max_pages:
            current_url = build_url_with_loadmore(url, page_count)
            # logging.info(f"Processing page {page_count}: {current_url}")
            browser = None
            page = None
            
            try:
                # Use the new proxy strategy function
                browser, page = await get_browser_with_proxy_strategy(p, current_url)
                log_event(f"Successfully loaded: {current_url}")
                # Scroll to load all products
                prev_product_count = 0
                for _ in range(10):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))  # Random delay between scrolls
                    current_product_count = await page.locator('.product-item').count()
                    if current_product_count == prev_product_count:
                        break
                    prev_product_count = current_product_count


                product_wrapper = await page.query_selector("div.product-scroll-wrapper")
                products = await product_wrapper.query_selector_all("div.product-item") if product_wrapper else []
                logging.info(f"Total products found on page {page_count}: {len(products)}")

                page_title = await page.title()
                current_date = datetime.now().strftime("%Y-%m-%d")
                time_only = datetime.now().strftime("%H.%M")

                records = []
                image_tasks = []
                print("No of products in Portal",len(products))

                for row_num, product in enumerate(products, start=len(sheet["A"]) + 1):

                    print("Record Number :",row_num-1)

                    try:
                        base_url = "https://www.kay.com"
                        product_link_element = await product.query_selector("a.thumb.main-thumb")
                        product_href = await product_link_element.get_attribute("href")
                        if product_href:
                            product_url = base_url + product_href
                           
                        else:
                            product_url = "N/A"
                    except:
                        product_url = "N/A"
                    
                    try:
                        product_name = await (await product.query_selector("h2.name.product-tile-description")).inner_text()
                    except:
                        product_name = "N/A"

                    try:
                        # Extract current price (the offer price if available)
                        price_el = await product.query_selector("div.price")
                        current_price_text = await price_el.inner_text() if price_el else ""
                        #print(f"Current Price Text: {current_price_text}")  # Debugging
                        current_price = current_price_text.strip().split()[0] if current_price_text else ""  # ensures we get only "$1014.30"

                        # Extract discount if available (e.g., "30% off")
                        discount_el = await product.query_selector("span.tag-text")
                        discount_text = await discount_el.inner_text() if discount_el else ""
                        #print(f"Discount Text: {discount_text}")  # Debugging
                        discount = discount_text.replace(" off", "").strip() if discount_text else ""  # just "30%"

                        # Extract original price with $ (if offer price is not available)
                        original_price_el = await product.query_selector("div.original-price")
                        original_price_text = await original_price_el.inner_text() if original_price_el else ""
                        #print(f"Original Price Text: {original_price_text}")  # Debugging
                        original_price = original_price_text.strip().replace("Was", "").strip().split()[0] if original_price_text else ""  # "$1449.00"

                        # Build the final formatted price
                        if current_price:  # If there is a current price
                            if discount:
                                price = f"{current_price} offer of {discount} {original_price}"
                            else:
                                price = current_price  # No discount, just current price
                        elif original_price:  # If there is no current price but original price is available
                            price = original_price
                        else:
                            price = "N/A"  # If neither price is available

                    except Exception as e:
                        price = "N/A"
                        print(f"Error: {e}")  # Log the error for debugging

                    try:
                        image_url = await (await product.query_selector("img[itemprop='image']")).get_attribute("src")
                    except:
                        image_url = "N/A"

                    
                    
                        
                    additional_info = []

                    try:
                        tag_els = await product.query_selector_all("span.product-tag.groupby-tablet-product-tags")
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

                    print("=================== 1st phase data =============================")

                    print("Row Number:", row_num - 1)                          # int
                    print("Product URL:", product_url)                     # str
                    print("Product Name:", product_name)                   # str
                    print("Price:", price)                                 # str or float
                    print("Image URL:", image_url)                         # str or List[str]
                    print("Additional Info :", additional_info_str)  # str (or JSON if parsed)
                    
                    print("================================================")

                    
                    detail_data = await scrape_product_detail(product_url)
                    
                    print("==================== second phase data IN ===========")

                    print("title:", detail_data["title"])                      # str
                    print("sku:", detail_data["sku"])                          # str
                    print("ring_sizes:", detail_data["ring_sizes"])            # str or List[str]
                    print("final_output_price:", detail_data["final_output_price"])  # str or float
                    print("protection_plan:", detail_data["protection_plan"])  # str
                    print("monthly_payment:", detail_data["monthly_payment"])  # str
                    print("product_details:", detail_data["product_details"])  # str (OR JSON str if you convert it)
                    print("review_summary:", detail_data["review_summary"])    # str (OR JSON str if you convert it)
                    print("image_urls:", detail_data["image_urls"])            # List[str] or JSON str
                    
                    print("==================== second phase data OUT ===========")
 

                    
                    
                    if product_name == "N/A" or price == "N/A" or image_url == "N/A":
                        print(f"Skipping product due to missing data: Name: {product_name}, Price: {price}, Image: {image_url}")
                        continue    
                    
                    

                    gold_type_match = re.search(r"\b\d{1,2}K\s*(?:White|Yellow|Rose)?\s*Gold\b|\bPlatinum\b|\bSilver\b", product_name, re.IGNORECASE)
                    kt = gold_type_match.group() if gold_type_match else "Not found"


                    diamond_weight_match = re.search(r"\d+(?:[-/]\d+)?(?:\s+\d+/\d+)?\s*ct\s+tw", product_name, re.IGNORECASE)
                    diamond_weight = diamond_weight_match.group() if diamond_weight_match else "N/A"


                    unique_id = str(uuid.uuid4())
                    image_tasks.append((row_num, unique_id, asyncio.create_task(
                        download_image_async(image_url, product_name, timestamp, image_folder, unique_id)
                    )))
                    
                    portal_name ="kay"
                    
                    
                    


                   

                    # Append safely
                    records_details.append((
                        unique_id,
                        current_date,
                        portal_name,
                        page_title,
                        product_name,
                        detail_data.get("sku", "N/A"),
                        ", ".join(detail_data.get("ring_sizes", [])),  # join list to string
                        detail_data.get("final_output_price", "N/A"),
                        json.dumps(detail_data.get("protection_plan", {})),
                        detail_data.get("monthly_payment", "N/A"),
                        json.dumps(detail_data.get("product_details", {})),
                        json.dumps(detail_data.get("review_summary", {})),
                        json.dumps(detail_data.get("image_urls", [])),
                        product_url,
                        current_date
                    ))
                                                                                

                    records.append((unique_id, current_date, page_title, product_name, None, kt, price, diamond_weight,additional_info_str))
                    sheet.append([current_date, page_title, product_name, None, kt, price, diamond_weight, time_only, image_url,additional_info_str])

                # Process images and update records
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

                # Save progress after each page
                wb.save(file_path)
                logging.info(f"Progress saved after page {page_count}")
                if page:
                    await page.close()
                if browser:
                    await browser.close()
                
                page_count += 1
                await asyncio.sleep(random.uniform(2, 5))
                
            except Exception as e:
                logging.error(f"Error processing page {page_count}: {str(e)}")
                if page:
                    await page.close()
                if browser:
                    await browser.close()
                wb.save(file_path)
                continue
            
            # Add delay between pages
            await asyncio.sleep(random.uniform(2, 5))
            
        page_count += 1

    # # Final save and database operations
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
    insert_into_db_details(records_details)
    # Return necessary information
    return base64_encoded, filename, file_path