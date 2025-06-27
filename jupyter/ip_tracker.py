import pymssql
from datetime import datetime
import hashlib
import os
from dotenv import load_dotenv
from utils import get_public_ip
# Load environment variables
load_dotenv()
# Database Configuration

DB_CONFIG = {
    "server": os.getenv("DB_SERVER"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}



def generate_unique_id(url):
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    raw = f"{url}-{now}"
    return hashlib.md5(raw.encode()).hexdigest()  # or use UUID if preferred

def insert_scrape_log(scrape_id, url, status='active'):
    ip_address = get_public_ip()
    conn = pymssql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    timestamp = datetime.now()
    cursor.execute("""
        INSERT INTO scraping_logs (id, url, ip_address, request_time, status)
        VALUES (%s, %s, %s, %s, %s)
    """, (scrape_id, url, ip_address, timestamp, status))
    conn.commit()
    conn.close()

def update_scrape_status(scrape_id, status):
    conn = pymssql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("UPDATE scraping_logs SET status = %s WHERE id = %s", (status, scrape_id))
    conn.commit()
    conn.close()
