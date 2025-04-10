"""
Shopee-specific crawler implementation.
Builds on the provided Shopee crawler code with enhancements for proxy rotation and S3 integration.
"""
import os
import time
import random
import logging
import json
import re
import hashlib
import requests
from typing import Dict, List, Optional, Union, Any, Tuple
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    WebDriverException
)
from PIL import Image
from io import BytesIO
import queue

from crawlers.base_crawler import BaseCrawler
from config.settings import DATA_DIR, MAX_RETRIES

logger = logging.getLogger(__name__)

class ShopeeCrawler(BaseCrawler):
    """
    Shopee-specific crawler implementation.
    """
    def __init__(self, headless=True, wait_time=10, max_tabs=None):
        """
        Initialize the Shopee crawler.
        
        Args:
            headless: Whether to run the browser in headless mode
            wait_time: Wait time for elements in seconds
            max_tabs: Maximum number of tabs per proxy, overrides default if provided
        """
        super().__init__('shopee', headless, wait_time, max_tabs)
        
        # Base URL for Shopee
        self.base_url = "https://shopee.vn"
        
        # Queue for links that need to be retried with a new proxy
        self.retry_links_queue = queue.Queue()
        
        # Flag to identify bot check pages
        self.bot_check_identifiers = [
            "Robot Verification",
            "Xác minh Robot",
            "captcha",
            "CAPTCHA",
            "Are you a robot",
            "Bạn có phải là robot",
            "human verification",
            "xác minh con người"
        ]
    
    def handle_popups(self):
        """Handle Shopee-specific popups."""
        try:
            # Find close buttons for popups
            close_buttons = self.driver.find_elements(By.CSS_SELECTOR, "svg.shopee-svg-icon.icon-close-thin")
            for btn in close_buttons:
                try:
                    btn.click()
                    self.random_sleep(0.5, 1)
                except:
                    pass
            
            # Handle language selection popup if present
            try:
                language_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button.language-selection__list-item")
                for btn in language_buttons:
                    if "Tiếng Việt" in btn.text or "Vietnamese" in btn.text:
                        btn.click()
                        self.random_sleep(0.5, 1)
                        break
            except:
                pass
            
            # Handle location popup
            try:
                location_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button.shopee-button-solid")
                for btn in location_buttons:
                    if "Tiếp tục" in btn.text or "Continue" in btn.text:
                        btn.click()
                        self.random_sleep(0.5, 1)
                        break
            except:
                pass
            
        except Exception as e:
            logger.debug(f"Error handling popups: {e}")
    
    def is_bot_check_page(self) -> bool:
        """
        Check if the current page is a bot verification page.
        
        Returns:
            True if bot check page detected, False otherwise
        """
        try:
            # Get page title and body text
            title = self.driver.title
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            
            # Check if any of the bot check identifiers are in the title or body
            for identifier in self.bot_check_identifiers:
                if identifier in title or identifier in body_text:
                    logger.warning(f"Bot check page detected: {identifier} found in page")
                    return True
            
            # Check for specific CAPTCHA elements
            captcha_elements = self.driver.find_elements(By.CSS_SELECTOR, 
                "iframe[src*='captcha'], .captcha, .g-recaptcha, img[src*='captcha']")
            
            if captcha_elements:
                logger.warning("Bot check page detected: CAPTCHA elements found")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking for bot verification page: {e}")
            return False
    
    def extract_ids_from_url(self, url: str) -> Dict[str, str]:
        """
        Extract shop_id and product_id from Shopee product URL.
        
        Args:
            url: Shopee product URL
            
        Returns:
            Dictionary with shop_id and product_id
        """
        try:
            # Pattern for Shopee URLs: https://shopee.vn/[product-name]-i.[shop_id].[product_id]
            pattern = r"i\.(\d+)\.(\d+)"
            match = re.search(pattern, url)
            
            if match:
                shop_id = match.group(1)
                product_id = match.group(2)
                return {
                    "shop_id": shop_id,
                    "product_id": product_id
                }
            else:
                logger.warning(f"Could not extract IDs from URL: {url}")
                return {"shop_id": "unknown", "product_id": "unknown"}
        except Exception as e:
            logger.error(f"Error extracting IDs from URL: {e}")
            return {"shop_id": "unknown", "product_id": "unknown"}
    
    def navigate_with_retry(self, url: str, max_retries: int = MAX_RETRIES) -> bool:
        """
        Navigate to a URL with retry logic and bot check detection.
        
        Args:
            url: URL to navigate to
            max_retries: Maximum number of retries
            
        Returns:
            True if navigation successful, False otherwise
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Navigating to: {url} (Attempt {attempt+1}/{max_retries})")
                self.driver.get(url)
                
                # Wait for page to load
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                
                # Check if this is a bot verification page
                if self.is_bot_check_page():
                    logger.warning(f"Bot check detected on attempt {attempt+1} for {url}")
                    
                    # If this is the last attempt, add to retry queue and return False
                    if attempt >= max_retries - 1:
                        logger.error(f"Max retries reached for {url}. Adding to retry queue for new proxy.")
                        self.retry_links_queue.put(url)
                        return False
                    
                    # Wait a bit longer before retrying
                    self.random_sleep(5, 10)
                    continue
                
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
                        
                        # Check if we're on a bot check page
                        if self.is_bot_check_page():
                            logger.warning(f"Bot check detected after error on attempt {attempt+1}")
                            
                            # If this is the last attempt, add to retry queue
                            if attempt >= max_retries - 2:  # One less since we'll increment
                                logger.error(f"Max retries nearly reached for {url}. Adding to retry queue for new proxy.")
                                self.retry_links_queue.put(url)
                                return False
                    except:
                        logger.warning("Driver appears to be unresponsive, restarting")
                        self.close_driver()
                        if not self.setup_driver():
                            # Add URL to retry queue since we couldn't restart the driver
                            self.retry_links_queue.put(url)
                            return False
                    
                    time.sleep(self.RETRY_DELAY)
                else:
                    # Max retries reached, add to retry queue for a new proxy
                    logger.error(f"Max retries reached for {url}. Adding to retry queue for new proxy.")
                    self.retry_links_queue.put(url)
        
        return False
    
    def extract_product_details(self, url: str) -> Optional[Dict]:
        """
        Extract product details from a Shopee product page.
        
        Args:
            url: URL of the product page
            
        Returns:
            Dictionary with product details if successful, None otherwise
        """
        try:
            # Extract product and shop IDs from URL
            ids = self.extract_ids_from_url(url)
            
            # Create product-specific directory
            product_data_dir = os.path.join(self.platform_data_dir, f"{ids['shop_id']}.{ids['product_id']}")
            os.makedirs(product_data_dir, exist_ok=True)
            
            # Create images directory
            images_dir = os.path.join(product_data_dir, "images")
            os.makedirs(images_dir, exist_ok=True)
            
            # Wait for the page to load completely
            self.random_sleep(3, 5)
            
            # Scroll down to load more content
            self.driver.execute_script("window.scrollBy(0, 300);")
            self.random_sleep(1, 2)
            
            # Check again if we're on a bot check page after scrolling
            if self.is_bot_check_page():
                logger.error(f"Bot check page detected after scrolling for URL: {url}")
                self.retry_links_queue.put(url)
                return None
            
            # Extract product details
            product_data = {
                "platform": "shopee",
                "url": url,
                "shop_id": ids["shop_id"],
                "product_id": ids["product_id"],
                "data_directory": product_data_dir,
                "images_dir": images_dir,
                "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "product_name": self._get_product_name(),
                "category": self._get_product_category(),
                "rating": self._get_product_rating(),
                "total_rating": self._get_total_rating(),
                "total_sold": self._get_total_sold(),
                "price": self._get_product_price(),
                "variations": self._get_product_variations(images_dir),
                "shop_info": self._get_shop_info(),
                "description": self._get_product_description(),
                "tags": self._get_product_tags()
            }
            
            # Verify we actually got product data by checking if key fields have real values
            # If 3 or more key fields are empty/default, consider this a failed extraction
            empty_fields = 0
            for key_field in ['product_name', 'price', 'description', 'category']:
                if key_field not in product_data or product_data[key_field] in ["N/A", 0, [], {}]:
                    empty_fields += 1
            
            if empty_fields >= 3:
                logger.error(f"Failed to extract key product data from {url} ({empty_fields} empty fields)")
                self.retry_links_queue.put(url)
                return None
            
            return product_data
            
        except Exception as e:
            logger.error(f"Error extracting product details from {url}: {e}")
            self.retry_links_queue.put(url)
            return None
    
    def _get_product_name(self) -> str:
        """Get the product name."""
        try:
            name_element = self.safe_find_element(
                By.CSS_SELECTOR, "h1.vR6K3w, div.WBVL_7 h1"
            )
            return name_element.text if name_element else "N/A"
        except Exception as e:
            logger.debug(f"Error getting product name: {e}")
            return "N/A"
    
    def _get_product_category(self) -> List[str]:
        """Get the product category hierarchy."""
        try:
            category_elements = self.driver.find_elements(
                By.CSS_SELECTOR, "div.ybxj32:nth-child(1) a.EtYbJs, div.ybxj32:nth-child(1) a.R7vGdX, div.idLK2l a.EtYbJs"
            )
            
            categories = []
            for element in category_elements:
                cat_text = element.text.strip()
                if cat_text and cat_text != "Shopee":
                    categories.append(cat_text)
                    
            return categories if categories else ["N/A"]
        except Exception as e:
            logger.debug(f"Error getting product category: {e}")
            return ["N/A"]
    
    def _get_product_rating(self) -> float:
        """Get the product rating score."""
        try:
            rating_element = self.safe_find_element(
                By.CSS_SELECTOR, "div.F9RHbS, div.jMXp4d"
            )
            return float(rating_element.text) if rating_element else 0.0
        except Exception as e:
            logger.debug(f"Error getting product rating: {e}")
            return 0.0
    
    def _get_total_rating(self) -> int:
        """Get the total number of ratings."""
        try:
            total_rating_element = self.safe_find_element(
                By.CSS_SELECTOR, "div.x1i_He, div.e2p50f:nth-child(2) div.F9RHbS"
            )
            
            if not total_rating_element:
                return 0
            
            # Process text, removing "đánh giá" (rating) if present
            rating_text = total_rating_element.text.strip()
            if "đánh giá" in rating_text:
                rating_text = rating_text.replace("đánh giá", "").strip()
                
            # Convert string to number (handle 'k' for thousands)
            if 'k' in rating_text.lower():
                return int(float(rating_text.lower().replace('k', '')) * 1000)
            else:
                return int(rating_text.replace(',', ''))
        except Exception as e:
            logger.debug(f"Error getting total ratings: {e}")
            return 0
    
    def _get_total_sold(self) -> int:
        """Get the total number of items sold."""
        try:
            sold_element = self.safe_find_element(
                By.CSS_SELECTOR, "div.aleSBU span.AcmPRb, div.mnzVGI span"
            )
            
            if not sold_element:
                return 0
            
            # Process text, removing "Sold", "Đã bán" if present
            sold_text = sold_element.text.strip()
            sold_text = re.sub(r'Sold|Đã bán', '', sold_text).strip()
            
            # Convert string to number (handle 'k' for thousands)
            if 'k' in sold_text.lower():
                return int(float(sold_text.lower().replace('k', '')) * 1000)
            else:
                return int(sold_text.replace(',', '').replace('.', ''))
        except Exception as e:
            logger.debug(f"Error getting total sold: {e}")
            return 0
    
    def _get_product_price(self) -> int:
        """Get the current product price."""
        try:
            price_element = self.safe_find_element(
                By.CSS_SELECTOR, "div.IZPeQz, div.jRlVo0 div.IZPeQz"
            )
            
            if not price_element:
                return 0
            
            # Process text, removing currency symbols and separators
            price_text = price_element.text.strip()
            price_text = re.sub(r'[^\d]', '', price_text)
            
            return int(price_text) if price_text else 0
        except Exception as e:
            logger.debug(f"Error getting product price: {e}")
            return 0
    
    def _get_product_variations(self, images_dir: str) -> Dict:
        """
        Get product variations (color, size, etc.).
        
        Args:
            images_dir: Directory to save variation images
        """
        try:
            variations = {}
            
            # Get main product images
            main_image_elements = self.driver.find_elements(
                By.CSS_SELECTOR, "div.UdI7e2 img.uXN1L5"
            )
            
            main_images = []
            for idx, img_elem in enumerate(main_image_elements[:5]):  # Limit to 5 main images
                img_url = img_elem.get_attribute("src")
                if img_url:
                    local_path = self.download_image(
                        img_url, 
                        images_dir, 
                        f"main_{idx+1}_{hashlib.md5(img_url.encode()).hexdigest()[:8]}.jpg"
                    )
                    main_images.append({
                        "original_url": img_url,
                        "local_path": local_path
                    })
            
            # Get variation sections
            variation_sections = self.driver.find_elements(
                By.CSS_SELECTOR, "section.flex.items-center, div.flex.KIoPj6 > div.flex.flex-column > section"
            )
            
            for section in variation_sections:
                try:
                    # Get variation type (e.g., "Color", "Size")
                    title_element = section.find_element(By.CSS_SELECTOR, "h2.Dagtcd")
                    title = title_element.text.strip()
                    
                    # Find variation options
                    option_elements = section.find_elements(By.CSS_SELECTOR, "button.sApkZm")
                    options = []
                    
                    for option in option_elements:
                        option_data = {
                            "name": option.text.strip()
                        }
                        
                        # Check if option has an image (usually for color variations)
                        try:
                            img_element = option.find_element(By.TAG_NAME, "img")
                            img_url = img_element.get_attribute("src")
                            option_data["image_url"] = img_url
                            
                            # Download and save the image
                            if img_url:
                                safe_name = re.sub(r'[^\w\-_]', '_', option_data['name'])
                                local_path = self.download_image(
                                    img_url,
                                    images_dir,
                                    f"{title}_{safe_name}_{hashlib.md5(img_url.encode()).hexdigest()[:8]}.jpg"
                                )
                                option_data["image_local_path"] = local_path
                        except:
                            pass
                        
                        options.append(option_data)
                    
                    if title and options:
                        variations[title] = options
                
                except Exception as e:
                    logger.debug(f"Error processing variation section: {e}")
                    continue
            
            # Add main images to variations
            variations["main_images"] = main_images
            
            return variations
            
        except Exception as e:
            logger.error(f"Error getting product variations: {e}")
            return {}
    
    def _get_shop_info(self) -> Dict:
        """Get information about the shop."""
        try:
            shop_info = {}
            
            # Find shop section
            shop_section = self.safe_find_element(
                By.CSS_SELECTOR, "section.page-product__shop, div#sll2-pdp-product-shop section"
            )
            
            if not shop_section:
                return {"name": "N/A", "url": "N/A"}
            
            # Get shop name
            try:
                shop_name_element = shop_section.find_element(By.CSS_SELECTOR, "div.fV3TIn")
                shop_info["name"] = shop_name_element.text.strip()
            except:
                shop_info["name"] = "N/A"
            
            # Get shop URL
            try:
                shop_url_element = shop_section.find_element(By.CSS_SELECTOR, "a.Z6yFUs, a.btn-light--link")
                shop_info["url"] = shop_url_element.get_attribute("href")
            except:
                shop_info["url"] = "N/A"
            
            # Get shop metrics
            try:
                shop_metrics = shop_section.find_elements(By.CSS_SELECTOR, "div.YnZi6x")
                for metric in shop_metrics:
                    try:
                        label = metric.find_element(By.CSS_SELECTOR, "label.ffHYws").text.strip().lower()
                        value = metric.find_element(By.CSS_SELECTOR, "span.Cs6w3G").text.strip()
                        
                        if "đánh giá" in label or "rating" in label:
                            shop_info["rating_count"] = value
                        elif "tỉ lệ phản hồi" in label or "response rate" in label:
                            shop_info["response_rate"] = value
                        elif "tham gia" in label or "joined" in label:
                            shop_info["joined"] = value
                        elif "sản phẩm" in label or "product" in label:
                            shop_info["product_count"] = value
                        elif "thời gian phản hồi" in label or "response time" in label:
                            shop_info["response_time"] = value
                        elif "người theo dõi" in label or "follower" in label:
                            shop_info["follower_count"] = value
                    except:
                        continue
            except Exception as e:
                logger.debug(f"Error getting shop metrics: {e}")
            
            return shop_info
            
        except Exception as e:
            logger.error(f"Error getting shop info: {e}")
            return {"name": "N/A", "url": "N/A"}
    
    def _get_product_description(self) -> str:
        """Get the product description."""
        try:
            # Scroll to description section
            self.driver.execute_script("window.scrollBy(0, 800);")
            self.random_sleep(1, 2)
            
            # Find description section
            description_section = self.safe_find_element(
                By.CSS_SELECTOR, "div.e8lZp3, div.Gf4Ro0 > div"
            )
            
            if not description_section:
                return "N/A"
            
            # Get all paragraphs in the description
            paragraphs = description_section.find_elements(By.CSS_SELECTOR, "p, span, div")
            description_texts = [p.text.strip() for p in paragraphs if p.text.strip()]
            
            # Join paragraphs with newlines
            return "\n".join(description_texts)
            
        except Exception as e:
            logger.error(f"Error getting product description: {e}")
            return "N/A"
    
    def _get_product_tags(self) -> List[str]:
        """Get product tags (usually found at the end of description)."""
        try:
            # Find description section
            description_section = self.safe_find_element(
                By.CSS_SELECTOR, "div.e8lZp3, div.Gf4Ro0 > div"
            )
            
            if not description_section:
                return []
            
            # Get paragraphs
            paragraphs = description_section.find_elements(By.CSS_SELECTOR, "p, span, div")
            
            # Look for hashtags, starting from the end
            for p in reversed(paragraphs):
                text = p.text.strip()
                if text and '#' in text:
                    # Extract hashtags
                    tags = re.findall(r'#\w+', text)
                    if not tags:
                        # If no hashtags found, try splitting by spaces
                        potential_tags = [word.strip() for word in text.split() if word.strip()]
                        return potential_tags
                    return [tag.replace('#', '') for tag in tags]
            
            # If no hashtags found in paragraphs, search the whole description
            full_text = description_section.text
            tags = re.findall(r'#\w+', full_text)
            
            return [tag.replace('#', '') for tag in tags] if tags else []
            
        except Exception as e:
            logger.error(f"Error getting product tags: {e}")
            return []
    
    def get_product_links(self, category_url: str, max_products: int = 100) -> List[str]:
        """
        Get product links from a Shopee category page.
        
        Args:
            category_url: URL of the category page
            max_products: Maximum number of products to get
            
        Returns:
            List of product URLs
        """
        try:
            product_links = []
            
            # Navigate to the category page
            if not self.navigate_with_retry(category_url):
                logger.error(f"Failed to navigate to category page: {category_url}")
                return product_links
            
            # Get page count for pagination
            self.random_sleep(2, 4)
            
            # Initial scroll to load products
            for _ in range(3):
                self.driver.execute_script("window.scrollBy(0, 800);")
                self.random_sleep(1, 2)
            
            # Extract product links from current page
            self._extract_product_links_from_page(product_links)
            
            # If we need more products, navigate through pagination
            page_num = 2
            while len(product_links) < max_products:
                # Check if there's a next page button
                next_button = self.safe_find_element(
                    By.CSS_SELECTOR, 
                    "button.shopee-icon-button--right",
                    wait_time=3
                )
                
                if not next_button:
                    logger.info("No more pages available")
                    break
                
                # Click next page
                try:
                    next_button.click()
                    self.random_sleep(2, 4)
                    
                    # Scroll to load products
                    for _ in range(3):
                        self.driver.execute_script("window.scrollBy(0, 800);")
                        self.random_sleep(1, 2)
                    
                    # Extract product links from current page
                    self._extract_product_links_from_page(product_links)
                    
                    page_num += 1
                    logger.info(f"Navigated to page {page_num}, collected {len(product_links)} product links so far")
                    
                except Exception as e:
                    logger.error(f"Error navigating to next page: {e}")
                    break
                
                # Exit if we have enough products
                if len(product_links) >= max_products:
                    break
            
            return product_links[:max_products]
            
        except Exception as e:
            logger.error(f"Error getting product links: {e}")
            return []
    
    def _extract_product_links_from_page(self, product_links: List[str]):
        """
        Extract product links from the current page and add to the list.
        
        Args:
            product_links: List to append product links to
        """
        try:
            # Find product elements
            product_elements = self.safe_find_elements(
                By.CSS_SELECTOR, 
                "div.shopee-search-item-result__item a[data-sqe='link']"
            )
            
            # If no product elements found, try alternative selectors
            if not product_elements:
                product_elements = self.safe_find_elements(
                    By.CSS_SELECTOR,
                    "div.JF0prw a[data-sqe='link'], div.shop-search-result-view__item a[href]"
                )
            
            # Extract links
            for element in product_elements:
                link = element.get_attribute("href")
                if link and "i." in link:  # Verify it's a product link
                    product_links.append(link)
            
            logger.info(f"Found {len(product_elements)} product elements on current page")
            
        except Exception as e:
            logger.error(f"Error extracting product links from page: {e}")
    
    def get_recommended_product_links(self, max_products: int = 100) -> List[str]:
        """
        Get product links from Shopee's recommended products section.
        
        Args:
            max_products: Maximum number of products to get
            
        Returns:
            List of product URLs
        """
        try:
            # Navigate to Shopee homepage
            if not self.navigate_with_retry(self.base_url):
                logger.error("Failed to navigate to Shopee homepage")
                return []
            
            # Wait for page to load and scroll down to recommended section
            self.random_sleep(3, 5)
            
            # Scroll down multiple times to load recommended products
            for _ in range(5):
                self.driver.execute_script("window.scrollBy(0, 500);")
                self.random_sleep(1, 2)
            
            # Find recommended product containers
            product_links = []
            product_elements = self.safe_find_elements(
                By.CSS_SELECTOR,
                "div.siT3A0 a[href], div.home-recommend-products__item a[href]"
            )
            
            # Extract links
            for element in product_elements:
                link = element.get_attribute("href")
                if link and "i." in link:  # Verify it's a product link
                    product_links.append(link)
                
                # Break if we have enough products
                if len(product_links) >= max_products:
                    break
            
            logger.info(f"Found {len(product_links)} recommended product links")
            return product_links[:max_products]
            
        except Exception as e:
            logger.error(f"Error getting recommended product links: {e}")
            return []
    
    def crawl_with_retry_queue(self, product_urls: List[str], max_retries: int = 3) -> List[Dict]:
        """
        Crawl products with support for retry queue for bot check pages.
        
        Args:
            product_urls: List of product URLs to crawl
            max_retries: Maximum number of retry cycles for the queue
            
        Returns:
            List of successfully crawled product data
        """
        results = []
        attempted_urls = set()  # Track URLs we've already tried
        retry_count = 0
        
        # Add initial URLs to queue
        for url in product_urls:
            self.retry_links_queue.put(url)
        
        while not self.retry_links_queue.empty() and retry_count < max_retries:
            # Get the current queue size before processing
            queue_size = self.retry_links_queue.qsize()
            logger.info(f"Processing retry queue cycle {retry_count+1}/{max_retries}, {queue_size} URLs in queue")
            
            urls_in_current_cycle = []
            # Get all URLs from the current queue cycle
            for _ in range(queue_size):
                if not self.retry_links_queue.empty():
                    url = self.retry_links_queue.get()
                    urls_in_current_cycle.append(url)
            
            # If we need a new proxy for this cycle, close and reopen the driver
            if retry_count > 0:
                logger.info(f"Cycle {retry_count+1}: Closing driver and getting new proxy")
                self.close_driver()
                success = self.setup_driver()
                if not success:
                    logger.error("Failed to set up new driver with fresh proxy")
                    # Put URLs back in queue and continue to next cycle
                    for url in urls_in_current_cycle:
                        self.retry_links_queue.put(url)
                    retry_count += 1
                    continue
                
                # Wait a bit with the new proxy before starting
                self.random_sleep(3, 5)
            
            # Process all URLs in the current cycle
            for url in urls_in_current_cycle:
                # Skip if we've already attempted this URL too many times
                if url in attempted_urls:
                    logger.warning(f"Skipping previously attempted URL: {url}")
                    continue
                
                logger.info(f"Crawling URL from retry queue (cycle {retry_count+1}): {url}")
                attempted_urls.add(url)
                
                try:
                    # Navigate to product page
                    if self.navigate_with_retry(url):
                        # Extract product details
                        product_data = self.extract_product_details(url)
                        
                        if product_data:
                            # Save locally
                            self.save_product_locally(product_data)
                            
                            # Upload to S3
                            self.upload_product_to_s3(product_data)
                            
                            # Add to results
                            results.append(product_data)
                            logger.info(f"Successfully crawled product: {url}")
                    
                    # Random sleep between products
                    self.random_sleep(2, 4)
                    
                except Exception as e:
                    logger.error(f"Error crawling product {url} from retry queue: {e}")
                    # URL will be added back to retry queue if it was a bot check page
            
            # Move to next retry cycle
            retry_count += 1
        
        # Log stats about uncrawled URLs
        remaining_urls = self.retry_links_queue.qsize()
        if remaining_urls > 0:
            logger.warning(f"Completed all retry cycles. {remaining_urls} URLs still in queue (not crawled)")
        
        return results
    
    def crawl_multiple_products(self, product_urls: List[str]) -> List[Dict]:
        """
        Crawl multiple product URLs with support for bot detection and retries.
        
        Args:
            product_urls: List of product URLs to crawl
            
        Returns:
            List of product data dictionaries
        """
        return self.crawl_with_retry_queue(product_urls)
    
    def crawl_category(self, category_url: str, max_products: int = 100) -> List[Dict]:
        """
        Crawl products from a category page with support for bot detection and retries.
        
        Args:
            category_url: URL of the category page
            max_products: Maximum number of products to crawl
            
        Returns:
            List of product data dictionaries
        """
        try:
            # Get product links
            product_links = self.get_product_links(category_url, max_products)
            
            if not product_links:
                logger.warning(f"No product links found in: {category_url}")
                return []
            
            logger.info(f"Found {len(product_links)} product links, crawling up to {max_products}")
            
            # Crawl products with retry queue
            return self.crawl_with_retry_queue(product_links[:max_products])
            
        except Exception as e:
            logger.error(f"Error crawling category: {e}")
            return []
    
    def cleanup(self):
        """Clean up resources and process any remaining retry links."""
        # Log retry queue stats
        retry_count = self.retry_links_queue.qsize()
        if retry_count > 0:
            logger.warning(f"Cleanup: {retry_count} URLs still in retry queue")
        
        # Close the driver and release proxy
        self.close_driver()
        
        # Flush any remaining products to S3
        self.s3_uploader.flush()
        
        logger.info(f"Crawler cleanup complete. Processed {self.products_processed} products.")