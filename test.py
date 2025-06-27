# import pymssql
# from openpyxl import Workbook
# import os

# # Table name
# table_name = "IBM_Algo_Webstudy_Products_details"

# # Database connection details from environment variables
# DB_CONFIG = {
#     "server": os.getenv("DB_SERVER"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
#     "database": os.getenv("DB_NAME"),
# }

# # Ensure environment variables are set
# if not all(DB_CONFIG.values()):
#     raise EnvironmentError("One or more database environment variables are not set.")

# # Connect to the SQL Server database
# conn = pymssql.connect(
#     server=DB_CONFIG["server"],
#     user=DB_CONFIG["user"],
#     password=DB_CONFIG["password"],
#     database=DB_CONFIG["database"]
# )
# cursor = conn.cursor()

# # Execute query to fetch all data
# query = f"SELECT * FROM {table_name}"
# cursor.execute(query)

# # Fetch column names
# columns = [desc[0] for desc in cursor.description]

# # Fetch all rows
# rows = cursor.fetchall()

# # Create Excel workbook and worksheet
# wb = Workbook()
# ws = wb.active
# ws.title = table_name[:31]  # Excel sheet title must be <= 31 chars

# # Write column headers
# ws.append(columns)

# # Write table rows
# for row in rows:
#     ws.append(row)

# # Save to Excel file
# excel_filename = f"{table_name}_data.xlsx"
# wb.save(excel_filename)

# # Clean up
# cursor.close()
# conn.close()

# print(f"✅ Data exported successfully to '{excel_filename}'")



# import requests

# url = "https://www.kay.com/studio-by-kay-baguettecut-diamond-deconstructed-ring-110-ct-tw-24k-yellow-gold-vermeil-sterling-silver/p/V-200775308"

# # Replace these with your actual Oxylabs proxy credentials
# # IG CLIENT PROXY 
# # PROXY_URL="wss://brd-customer-hl_3c9950db-zone-scraping_browser_ig:f0o999rogy8z@brd.superproxy.io:9222"
# # PROXY_SERVER="http://pr.oxylabs.io:7777"
# # PROXY_USERNAME="customer_ig_client_LzD0G"
# # PROXY_PASSWORD="IgClient_7+Data"


# # Office Proxy
# PROXY_SERVER="http://pr.oxylabs.io:7777"
# PROXY_USERNAME="customer-ig_algo_lguO6-cc-US"
# PROXY_PASSWORD="DxzPR7Q4x4vY+"

# proxies = {
#     "http": f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_SERVER}",
#     "https": f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_SERVER}"
# }

# try:
#     response = requests.get(url, proxies=proxies, timeout=30)
#     if response.status_code == 200 and response.content:
#         print(f"✅ Proxy working. Page length: {len(response.content)}.")
#     else:
#         print(f"❌ Proxy error or empty response. Status: {response.status_code}, Content length: {len(response.content)}.")
# except Exception as e:
#     print(f"❌ Proxy request failed: {e}")

