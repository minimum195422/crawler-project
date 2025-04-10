"""
Global settings for the e-commerce crawler project.
"""
import os
from pathlib import Path
from typing import Dict, List, Optional, Union
import json

# Project base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Proxy settings
PROXY_ROTATION_INTERVAL = 75  # Rotate proxies every 75 seconds (between 60-90 as required)
PROXY_API_URL = "https://proxyxoay.shop/api/get.php?key={key}&&nhamang=random&&tinhthanh=0"
MAX_TABS_PER_PROXY = 5  # Default number of tabs per proxy

# AWS S3 settings
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-southeast-1')  # Default to Singapore region
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'ecommerce-crawler-data')
S3_UPLOAD_BATCH_SIZE = 50  # Upload to S3 after collecting 50 products

# Crawler settings
HEADLESS = True  # Run browsers in headless mode by default
DEFAULT_WAIT_TIME = 10  # Default wait time for elements in seconds
MAX_RETRIES = 3  # Maximum number of retries for failed requests
RETRY_DELAY = 5  # Delay between retries in seconds
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

# Data storage settings
DATA_DIR = os.path.join(BASE_DIR, "data")
IMAGES_DIR = os.path.join(DATA_DIR, "images")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Ensure directories exist
for directory in [DATA_DIR, IMAGES_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True)

# Load proxy keys from JSON file
def load_proxy_keys() -> Dict[str, List[str]]:
    """Load proxy keys for different platforms from JSON file"""
    proxy_keys_path = os.path.join(BASE_DIR, "config", "proxy_keys.json")
    try:
        if os.path.exists(proxy_keys_path):
            with open(proxy_keys_path, 'r') as f:
                return json.load(f)
        else:
            # Return empty dict if file doesn't exist
            return {"shopee": [], "lazada": [], "tiki": []}
    except Exception as e:
        print(f"Error loading proxy keys: {e}")
        return {"shopee": [], "lazada": [], "tiki": []}

# Function to update proxy keys
def update_proxy_keys(platform: str, keys: List[str]) -> bool:
    """Update proxy keys for a specific platform"""
    proxy_keys_path = os.path.join(BASE_DIR, "config", "proxy_keys.json")
    try:
        # Load existing keys
        proxy_keys = load_proxy_keys()
        # Update keys for the platform
        proxy_keys[platform] = keys
        # Save updated keys
        with open(proxy_keys_path, 'w') as f:
            json.dump(proxy_keys, f, indent=4)
        return True
    except Exception as e:
        print(f"Error updating proxy keys: {e}")
        return False

# Platform specific settings
PLATFORM_SETTINGS = {
    "shopee": {
        "base_url": "https://shopee.vn",
        "categories_endpoint": "/api/v4/pages/get_category_tree",
        "max_tabs_per_proxy": MAX_TABS_PER_PROXY,
    },
    "lazada": {
        "base_url": "https://www.lazada.vn",
        "categories_endpoint": "/api/catalog/category_tree",
        "max_tabs_per_proxy": MAX_TABS_PER_PROXY,
    },
    "tiki": {
        "base_url": "https://tiki.vn",
        "categories_endpoint": "/api/v2/categories",
        "max_tabs_per_proxy": MAX_TABS_PER_PROXY,
    }
}