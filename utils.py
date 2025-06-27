import logging
import os
import requests
import re
from logging_config import logger  # Import logger from logging_config
import socket
# Ensure the log directory exists
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOG_FILE_PATH = os.path.join(LOG_DIR, 'log.txt')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def get_public_ip():
    """Get the local IP address of the machine (e.g., 192.168.x.x)"""
    try:
        # This does not send data; it's just used to determine the local IP used to reach an external address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception as e:
        print(f"Failed to get local IP: {e}")
        return "Unknown IP"
    

def log_event(message):
    """Log events with date, time, and IP address."""
    ip_address = get_public_ip()
    full_message = f"{message} | IP: {ip_address}"
    logger.info(full_message)  # Use logger instead of logging
    print(full_message)

# def sanitize_filename(filename):
#     """Sanitize a filename by replacing invalid characters with an underscore."""
#     return re.sub(r'[<>:"/\\|?*\']', '_', filename)
def sanitize_filename(filename, max_length=255):
    """Sanitize a filename by replacing invalid characters and limiting length."""
    filename = re.sub(r'[<>:"/\\|?*\']', '_', filename)  # Replace invalid chars
    filename = filename.replace("\n", "_").replace("\r", "_").strip()  # Remove newlines & trim
    return filename[:max_length]  # Truncate if too long