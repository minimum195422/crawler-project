"""
Base crawler class with common functionality for all platform crawlers.
"""
import os
import time
import random
import logging
import json
import threading
import hashlib
import requests
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any, Tuple
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager

from utils.proxy_manager import ProxyManager
from utils.s3_uploader import S3Uploader
from config.settings import (
    HEADLESS, 
    DEFAULT_WAIT_TIME, 
    MAX_RETRIES, 
    RETRY_DELAY, 
    USER_AGENT,
    DATA_DIR,
    PLATFORM_SETTINGS
)

logger = logging.getLogger(__name__)

class BaseCrawler(ABC):
    """
    Base class for all crawlers with common functionality.
    """
    def __init__(
        self, 
        platform: str,
        headless: bool = HEADLESS,
        wait_time: int = DEFAULT_WAIT_TIME,
        max_tabs: Optional[int] = None
    ):
        """
        Initialize the base crawler.
        
        Args:
            platform: Platform name (shopee, lazada, etc.)
            headless: Whether to run the browser in headless mode
            wait_time: Wait time for elements in seconds
            max_tabs: Maximum number of tabs per proxy, overrides default if provided
        """
        self.platform = platform
        self.headless = headless
        self.wait_time = wait_time
        
        # Get platform specific settings
        self.platform_settings = PLATFORM_SETTINGS.get(platform, {})
        
        # Max tabs per proxy
        if max_tabs is not None:
            self.max_tabs_per_proxy = max_tabs
        else:
            self.max_tabs_per_proxy = self.platform_settings.get('max_tabs_per_proxy', 5)
        
        # Initialize proxy manager
        self.proxy_manager = ProxyManager(platform, self.max_tabs_per_proxy)
        
        # Initialize S3 uploader
        self.s3_uploader = S3Uploader(platform)
        
        # Current driver and proxy for this crawler instance
        self.driver = None
        self.current_proxy = None
        
        # Data collection counters
        self.products_processed = 0
        
        # Platform specific directory
        self.platform_data_dir = os.path.join(DATA_DIR, platform)
        os.makedirs(self.platform_data_dir, exist_ok=True)
    
    def setup_driver(self):
        """Set up the Selenium WebDriver with proxy."""
        # Get a proxy from the manager
        proxy_url = self.proxy_manager.get_proxy()
        if not proxy_url:
            raise RuntimeError("No proxies available")
        
        self.current_proxy = proxy_url
        
        # Set up Chrome options
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        
        # Add proxy settings
        chrome_options.add_argument(f'--proxy-server={proxy_url}')
        
        # Common options
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument(f"--user-agent={USER_AGENT}")
        
        # Setup driver
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, self.wait_time)
            logger.info(f"Driver set up successfully with proxy: {proxy_url}")
            return True
        except Exception as e:
            logger.error(f"Error setting up driver: {e}")
            if self.current_proxy:
                self.proxy_manager.release_proxy(self.current_proxy)
                self.current_proxy = None
            return False
    
    def close_driver(self):
        """Close the WebDriver and release the proxy."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.error(f"Error closing driver: {e}")
            finally:
                self.driver = None
        
        if self.current_proxy:
            self.proxy_manager.release_proxy(self.current_proxy)
            self.current_proxy = None
    
    def random_sleep(self, min_time: float = 1.0, max_time: float = 3.0):
        """Sleep for a random time interval to avoid detection."""
        time.sleep(random.uniform(min_time, max_time))
    
    def safe_find_element(self, by: By, value: str, wait_time: Optional[int] = None) -> Optional[Any]:
        """
        Safely find an element with explicit wait.
        
        Args:
            by: Selenium By locator strategy
            value: Locator value
            wait_time: Custom wait time, uses instance default if None
            
        Returns:
            The element if found, None otherwise
        """
        wait_time = wait_time or self.wait_time
        try:
            element = WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except Exception as e:
            logger.debug(f"Element not found: {by}={value}, Error: {e}")
            return None
    
    def safe_find_elements(self, by: By, value: str, wait_time: Optional[int] = None) -> List[Any]:
        """
        Safely find multiple elements with explicit wait.
        
        Args:
            by: Selenium By locator strategy
            value: Locator value
            wait_time: Custom wait time, uses instance default if None
            
        Returns:
            List of elements if found, empty list otherwise
        """
        wait_time = wait_time or self.wait_time
        try:
            # Wait for at least one element to be present
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((by, value))
            )
            # Then get all elements
            return self.driver.find_elements(by, value)
        except Exception as e:
            logger.debug(f"Elements not found: {by}={value}, Error: {e}")
            return []
    
    def scroll_to_element(self, element):
        """Scroll to an element to ensure it's in view."""
        try:
            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
            self.random_sleep(0.5, 1.0)
        except Exception as e:
            logger.debug(f"Error scrolling to element: {e}")
    
    def handle_popups(self):
        """Handle common popups that might interfere with crawling."""
        # Implementation will vary by platform
        pass
    
    def download_image(self, image_url: str, image_dir: str, filename: Optional[str] = None) -> Optional[str]:
        """
        Download an image and save it locally.
        
        Args:
            image_url: URL of the image
            image_dir: Directory to save the image
            filename: Optional filename, will generate one based on URL if None
            
        Returns:
            Path to saved image if successful, None otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(image_dir, exist_ok=True)
            
            # Generate filename if not provided
            if not filename:
                image_hash = hashlib.md5(image_url.encode()).hexdigest()
                filename = f"{image_hash}.jpg"
            
            image_path = os.path.join(image_dir, filename)
            
            # Check if file already exists
            if os.path.exists(image_path):
                return image_path
            
            # Download image with retries
            for attempt in range(MAX_RETRIES):
                try:
                    response = requests.get(image_url, timeout=10)
                    if response.status_code == 200:
                        with open(image_path, 'wb') as f:
                            f.write(response.content)
                        logger.info(f"Image downloaded successfully: {image_path}")
                        return image_path
                    else:
                        logger.warning(f"Failed to download image, status code: {response.status_code}")
                except Exception as e:
                    logger.warning(f"Attempt {attempt+1} failed: {e}")
                
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
            
            return None
                
        except Exception as e:
            logger.error(f"Error downloading image: {e}")
            return None
    
    def upload_product_to_s3(self, product_data: Dict):
        """
        Process a product and upload to S3.
        
        Args:
            product_data: Product data dictionary
        """
        # Add timestamp and platform info
        product_data['crawl_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        product_data['platform'] = self.platform
        
        # Upload product data
        self.s3_uploader.upload_product(product_data)
        
        # Upload images to S3 if they exist locally
        if 'images_dir' in product_data and 'shop_id' in product_data and 'product_id' in product_data:
            images_dir = product_data['images_dir']
            shop_id = product_data['shop_id']
            product_id = product_data['product_id']
            
            # Upload main images
            if 'main_images' in product_data and isinstance(product_data['main_images'], list):
                for i, img_data in enumerate(product_data['main_images']):
                    if 'local_path' in img_data and os.path.exists(img_data['local_path']):
                        # Upload to S3 and update URL
                        s3_url = self.s3_uploader.upload_image(
                            img_data['local_path'], 
                            shop_id, 
                            product_id
                        )
                        if s3_url:
                            product_data['main_images'][i]['s3_url'] = s3_url
            
            # Upload variation images
            if 'variations' in product_data and isinstance(product_data['variations'], dict):
                for var_name, var_options in product_data['variations'].items():
                    if isinstance(var_options, list):
                        for i, option in enumerate(var_options):
                            if 'image_local_path' in option and os.path.exists(option['image_local_path']):
                                # Upload to S3 and update URL
                                s3_url = self.s3_uploader.upload_image(
                                    option['image_local_path'],
                                    shop_id,
                                    product_id
                                )
                                if s3_url:
                                    product_data['variations'][var_name][i]['image_s3_url'] = s3_url
        
        # Increment counter
        self.products_processed += 1
        
        # Log progress
        if self.products_processed % 10 == 0:
            logger.info(f"Processed {self.products_processed} products so far")
    
    def save_product_locally(self, product_data: Dict, filename: Optional[str] = None) -> str:
        """
        Save product data to local JSON file.
        
        Args:
            product_data: Product data dictionary
            filename: Optional filename, will generate one based on product ID if None
            
        Returns:
            Path to saved JSON file
        """
        try:
            # Generate filename if not provided
            if not filename:
                if 'product_id' in product_data:
                    product_id = product_data['product_id']
                    filename = f"{self.platform}_{product_id}.json"
                else:
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    filename = f"{self.platform}_product_{timestamp}.json"
            
            # Add timestamp if not present
            if 'crawl_time' not in product_data:
                product_data['crawl_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Add platform if not present
            if 'platform' not in product_data:
                product_data['platform'] = self.platform
            
            # Save to file
            json_path = os.path.join(self.platform_data_dir, filename)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(product_data, f, ensure_ascii=False, indent=4)
            
            logger.info(f"Product data saved to {json_path}")
            return json_path
            
        except Exception as e:
            logger.error(f"Error saving product data: {e}")
            # Create a backup with timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            backup_path = os.path.join(self.platform_data_dir, f"backup_{timestamp}.json")
            try:
                with open(backup_path, 'w', encoding='utf-8') as f:
                    json.dump(product_data, f, ensure_ascii=False, indent=4)
                logger.info(f"Backup saved to {backup_path}")
                return backup_path
            except:
                logger.error("Failed to save backup")
                return ""
    
    def navigate_with_retry(self, url: str, max_retries: int = MAX_RETRIES) -> bool:
        """
        Navigate to a URL with retry logic.
        
        Args:
            url: URL to navigate to
            max_retries: Maximum number of retries
            
        Returns:
            True if navigation successful, False otherwise
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Navigating to: {url}")
                self.driver.get(url)
                
                # Wait for page to load
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                
                # Handle popups
                self.handle_popups()
                
                # Random sleep to mimic human behavior
                self.random_sleep(2, 4)
                
                return True
                
            except Exception as e:
                logger.warning(f"Navigation attempt {attempt+1} failed: {e}")
                
                if attempt < max_retries - 1:
                    # Check if driver is still responsive
                    try:
                        current_url = self.driver.current_url
                        logger.info(f"Current URL: {current_url}")
                    except:
                        logger.warning("Driver appears to be unresponsive, restarting")
                        self.close_driver()
                        if not self.setup_driver():
                            return False
                    
                    time.sleep(RETRY_DELAY)
        
        return False
    
    @abstractmethod
    def extract_product_details(self, url: str) -> Optional[Dict]:
        """
        Extract product details from a product page.
        To be implemented by platform-specific crawlers.
        
        Args:
            url: URL of the product page
            
        Returns:
            Dictionary with product details if successful, None otherwise
        """
        pass
    
    @abstractmethod
    def get_product_links(self, category_url: str, max_products: int = 100) -> List[str]:
        """
        Get product links from a category page.
        To be implemented by platform-specific crawlers.
        
        Args:
            category_url: URL of the category page
            max_products: Maximum number of products to get
            
        Returns:
            List of product URLs
        """
        pass
    
    def crawl_product(self, product_url: str) -> Optional[Dict]:
        """
        Crawl a single product page.
        
        Args:
            product_url: URL of the product page
            
        Returns:
            Product data if successful, None otherwise
        """
        # Make sure driver is set up
        if not self.driver and not self.setup_driver():
            logger.error("Failed to set up driver")
            return None
        
        try:
            # Navigate to product page
            if not self.navigate_with_retry(product_url):
                logger.error(f"Failed to navigate to: {product_url}")
                return None
            
            # Extract product details
            product_data = self.extract_product_details(product_url)
            
            if product_data:
                # Save locally
                self.save_product_locally(product_data)
                
                # Upload to S3
                self.upload_product_to_s3(product_data)
                
                return product_data
            else:
                logger.error(f"Failed to extract product details from: {product_url}")
                return None
                
        except Exception as e:
            logger.error(f"Error crawling product: {e}")
            return None
    
    def crawl_category(self, category_url: str, max_products: int = 100) -> List[Dict]:
        """
        Crawl products from a category page.
        
        Args:
            category_url: URL of the category page
            max_products: Maximum number of products to crawl
            
        Returns:
            List of product data dictionaries
        """
        results = []
        
        try:
            # Get product links
            product_links = self.get_product_links(category_url, max_products)
            
            if not product_links:
                logger.warning(f"No product links found in: {category_url}")
                return results
            
            logger.info(f"Found {len(product_links)} product links, crawling up to {max_products}")
            
            # Crawl each product
            for i, link in enumerate(product_links[:max_products]):
                logger.info(f"Crawling product {i+1}/{min(len(product_links), max_products)}: {link}")
                
                product_data = self.crawl_product(link)
                
                if product_data:
                    results.append(product_data)
                
                # Random sleep between products
                self.random_sleep(1, 3)
            
            return results
            
        except Exception as e:
            logger.error(f"Error crawling category: {e}")
            return results
        
    def crawl_multiple_products(self, product_urls: List[str]) -> List[Dict]:
        """
        Crawl multiple product URLs.
        
        Args:
            product_urls: List of product URLs to crawl
            
        Returns:
            List of product data dictionaries
        """
        results = []
        
        for i, url in enumerate(product_urls):
            logger.info(f"Crawling product {i+1}/{len(product_urls)}: {url}")
            
            product_data = self.crawl_product(url)
            
            if product_data:
                results.append(product_data)
            
            # Random sleep between products
            self.random_sleep(1, 3)
        
        return results
    
    def cleanup(self):
        """Clean up resources."""
        # Close the driver and release proxy
        self.close_driver()
        
        # Flush any remaining products to S3
        self.s3_uploader.flush()
        
        logger.info(f"Crawler cleanup complete. Processed {self.products_processed} products.")