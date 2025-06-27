# import asyncio
# import logging
# import os
# from playwright.async_api import async_playwright

# # Load environment variables
# PROXY_URL = os.getenv("PROXY_URL")  # Bright Data CDP Proxy URL
# PROXY_SERVER = os.getenv("PROXY_SERVER")  # Oxylabs Proxy server
# PROXY_USERNAME = os.getenv("PROXY_USERNAME")
# PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")


# async def check_bri_data_proxy() -> bool:
#     """Check if Bright Data proxy is working via CDP."""
#     try:
#         async with async_playwright() as p:
#             browser = await p.chromium.connect_over_cdp(PROXY_URL)
#             context = await browser.new_context()
#             page = await context.new_page()
#             await page.goto("https://httpbin.org/ip", timeout=180_000,wait_until="domcontentloaded")
#             await browser.close()
#             return True
#     except Exception as e:
#         logging.error(f"Bright Data proxy failed: {e}")
#         return False


# async def check_oxylabs_proxy() -> bool:
#     """Check if Oxylabs proxy is working using standard Chromium proxy config."""
#     try:
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(
#                 proxy={
#                     "server": PROXY_SERVER,
#                     "username": PROXY_USERNAME,
#                     "password": PROXY_PASSWORD
#                 },
#                 headless=True
#             )
#             context = await browser.new_context()
#             page = await context.new_page()
#             await page.goto("https://httpbin.org/ip", timeout=180_000,wait_until="domcontentloaded")
#             await browser.close()
#             return True
#     except Exception as e:
#         logging.error(f"Oxylabs proxy failed: {e}")
#         return False

# async def _check_proxies_async():
#     """Check both proxies and return a unified status message."""
#     bri_ok, oxy_ok = await asyncio.gather(
#         check_bri_data_proxy(),
#         check_oxylabs_proxy()
#     )

#     if bri_ok and oxy_ok:
#         return True, "Both Bright Data and Oxylabs proxies are working."
#     elif bri_ok:
#         return False, "Oxylabs proxy failed, but Bright Data proxy is working."
#     elif oxy_ok:
#         return True, "Bright Data proxy failed, but Oxylabs proxy is working."
#     else:
#         return False, "Both Bright Data and Oxylabs proxies failed."


# def check_proxies():
#     """Synchronous wrapper to run proxy checks without URL input."""
#     print("IN CODE PROXY VALIDATION")
#     try:
#         result, message = asyncio.run(_check_proxies_async())
#         print(result)
#         print(message)
#         return result, message
#     except Exception as e:
#         logging.error(f"Proxy check error: {e}")
#         return False, f"Proxy check failed: {str(e)}"

import asyncio
import logging
import os
from playwright.async_api import async_playwright

# Load environment variables
PROXY_URL = os.getenv("PROXY_URL")  # Bright Data CDP Proxy URL
PROXY_SERVER = os.getenv("PROXY_SERVER")  # Oxylabs Proxy server
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# Timeout constants
NAV_TIMEOUT = 30_000  # 30 seconds for page navigation
TOTAL_TIMEOUT = 45_000  # 45 seconds per proxy check

async def check_bri_data_proxy() -> bool:
    """Check Bright Data proxy with tighter timeouts and resource cleanup."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(PROXY_URL, timeout=TOTAL_TIMEOUT)
            context = await browser.new_context()
            page = await context.new_page()
            try:
                await page.goto(
                    "https://httpbin.org/ip", 
                    timeout=NAV_TIMEOUT,
                    wait_until="domcontentloaded"
                )
                # Verify actual proxy response (optional)
                content = await page.content()
                if "origin" not in content:
                    raise ValueError("Invalid proxy response")
                return True
            finally:
                await page.close()
                await context.close()
                await browser.close()
    except Exception as e:
        logging.error(f"Bright Data proxy failed: {e}")
        return False

async def check_oxylabs_proxy() -> bool:
    """Check Oxylabs proxy with optimized settings."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                proxy={
                    "server": PROXY_SERVER,
                    "username": PROXY_USERNAME,
                    "password": PROXY_PASSWORD
                },
                headless=True,
                timeout=TOTAL_TIMEOUT
            )
            context = await browser.new_context()
            page = await context.new_page()
            try:
                await page.goto(
                    "https://httpbin.org/ip", 
                    timeout=NAV_TIMEOUT,
                    wait_until="domcontentloaded"
                )
                # Verify actual proxy response (optional)
                content = await page.content()
                if "origin" not in content:
                    raise ValueError("Invalid proxy response")
                return True
            finally:
                await page.close()
                await context.close()
                await browser.close()
    except Exception as e:
        logging.error(f"Oxylabs proxy failed: {e}")
        return False

# async def _check_proxies_async():
#     """Run proxy checks with fail-fast timeout."""
#     try:
#         results = await asyncio.wait_for(
#             asyncio.gather(
#                 check_bri_data_proxy(),
#                 check_oxylabs_proxy()
#             ),
#             timeout=60.0  # Max 60 seconds total for both
#         )
#         bri_ok, oxy_ok = results
#     except asyncio.TimeoutError:
#         logging.error("Proxy checks timed out")
#         return False, "Proxy validation timed out"

#     # Generate status message
#     status_msgs = []
#     if bri_ok: status_msgs.append("Bright Data working")
#     if oxy_ok: status_msgs.append("Oxylabs working")
    
#     msg = (
#         "Both proxies working" if bri_ok and oxy_ok else
#         " | ".join(status_msgs) if status_msgs else
#         "All proxies failed"
#     )
#     return all(results), msg+


async def _check_proxies_async():
    """Check both proxies and return a unified status message."""
    bri_ok, oxy_ok = await asyncio.gather(
        check_bri_data_proxy(),
        check_oxylabs_proxy()
    )

    if bri_ok and oxy_ok:
        return True, "Both Bright Data and Oxylabs proxies are working."
    elif bri_ok:
        return False, "Oxylabs proxy failed, but Bright Data proxy is working."
    elif oxy_ok:
        return True, "Bright Data proxy failed, but Oxylabs proxy is working."
    else:
        return False, "Both Bright Data and Oxylabs proxies failed."

def check_proxies():
    """Synchronous entry point."""
    print("Starting proxy validation")
    try:
        result, message = asyncio.run(_check_proxies_async())
        print(f"Result: {result}\nMessage: {message}")
        return result, message
    except Exception as e:
        logging.exception("Proxy check crashed")
        return False, f"Critical failure: {str(e)}"