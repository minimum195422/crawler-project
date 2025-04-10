#!/usr/bin/env python3
"""
Script for running the Shopee crawler manually.
Useful for testing and one-off crawl tasks.
"""
import os
import sys
import argparse
import logging
from datetime import datetime

# Add project directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from crawlers.shopee_crawler import ShopeeCrawler
from utils.logger import get_logger

logger = get_logger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run Shopee crawler manually")
    
    # Required arguments
    parser.add_argument(
        "--mode",
        type=str,
        choices=["category", "product", "recommended"],
        default="category",
        help="Crawling mode: category, product, or recommended"
    )
    
    # Optional arguments
    parser.add_argument(
        "--url",
        type=str,
        help="URL to crawl (category or product URL)"
    )
    
    parser.add_argument(
        "--category",
        type=str,
        help="Predefined category name: electronics, fashion, beauty"
    )
    
    parser.add_argument(
        "--max-products",
        type=int,
        default=50,
        help="Maximum number of products to crawl (for category mode)"
    )
    
    parser.add_argument(
        "--headless",
        action="store_true",
        default=True,
        help="Run in headless mode (no browser UI)"
    )
    
    parser.add_argument(
        "--wait-time",
        type=int,
        default=10,
        help="Wait time for page elements in seconds"
    )
    
    parser.add_argument(
        "--max-tabs",
        type=int,
        default=5,
        help="Maximum number of tabs per proxy"
    )
    
    return parser.parse_args()

def get_category_url(category_name):
    """Get URL for predefined category."""
    categories = {
        "electronics": "https://shopee.vn/Thiết-bị-điện-tử-cat.11036132",
        "fashion": "https://shopee.vn/Thời-trang-nam-cat.11035567",
        "beauty": "https://shopee.vn/Sắc-đẹp-cat.11036279",
        "home": "https://shopee.vn/Nhà-cửa-và-đời-sống-cat.11036670",
        "phones": "https://shopee.vn/Điện-thoại-cat.11036030"
    }
    
    return categories.get(category_name.lower())

def main():
    """Main function to run the crawler."""
    args = parse_args()
    
    # Initialize crawler
    crawler = ShopeeCrawler(
        headless=args.headless,
        wait_time=args.wait_time,
        max_tabs=args.max_tabs
    )
    
    try:
        if args.mode == "category":
            # Determine category URL
            category_url = args.url
            if not category_url and args.category:
                category_url = get_category_url(args.category)
            
            if not category_url:
                logger.error("No category URL provided. Use --url or --category")
                return 1
            
            logger.info(f"Crawling category: {category_url}")
            logger.info(f"Max products: {args.max_products}")
            
            # Crawl category
            results = crawler.crawl_category(category_url, max_products=args.max_products)
            logger.info(f"Successfully crawled {len(results)} products")
            
        elif args.mode == "product":
            if not args.url:
                logger.error("No product URL provided. Use --url")
                return 1
            
            logger.info(f"Crawling product: {args.url}")
            
            # Crawl product
            product_data = crawler.crawl_product(args.url)
            if product_data:
                logger.info(f"Successfully crawled product: {product_data.get('product_name', 'N/A')}")
            else:
                logger.error("Failed to crawl product")
                
        elif args.mode == "recommended":
            max_products = args.max_products
            logger.info(f"Crawling recommended products, max: {max_products}")
            
            # Get recommended product links
            product_links = crawler.get_recommended_product_links(max_products=max_products)
            logger.info(f"Found {len(product_links)} recommended product links")
            
            # Crawl products
            results = crawler.crawl_multiple_products(product_links)
            logger.info(f"Successfully crawled {len(results)} recommended products")
            
        return 0
            
    except Exception as e:
        logger.exception(f"Error running crawler: {e}")
        return 1
    
    finally:
        # Cleanup
        crawler.cleanup()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)