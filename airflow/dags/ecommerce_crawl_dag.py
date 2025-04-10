"""
DAG definition for e-commerce crawling tasks.
Schedules crawling jobs for different platforms and categories.
"""
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from typing import Dict, List, Optional

# Add project directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from crawlers.shopee_crawler import ShopeeCrawler
# Import other platform crawlers as they're implemented
# from crawlers.lazada_crawler import LazadaCrawler
# from crawlers.tiki_crawler import TikiCrawler

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

# Create DAG
dag = DAG(
    'ecommerce_crawler',
    default_args=default_args,
    description='Crawl data from e-commerce platforms',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=days_ago(1),
    tags=['ecommerce', 'crawler', 'data'],
    catchup=False,
)

# Get Shopee category URLs from Airflow Variables or use defaults
def get_shopee_categories():
    """Get Shopee category URLs from Airflow Variables or use defaults."""
    try:
        categories = Variable.get("shopee_categories", deserialize_json=True)
    except:
        # Default categories if not set in Airflow Variables
        categories = [
            {
                "name": "electronics",
                "url": "https://shopee.vn/Thiết-bị-điện-tử-cat.11036132"
            },
            {
                "name": "fashion",
                "url": "https://shopee.vn/Thời-trang-nam-cat.11035567"
            },
            {
                "name": "beauty",
                "url": "https://shopee.vn/Sắc-đẹp-cat.11036279"
            }
        ]
    return categories

# Function to crawl Shopee category
def crawl_shopee_category(category_name, category_url, **kwargs):
    """
    Crawl products from a Shopee category.
    
    Args:
        category_name: Name of the category
        category_url: URL of the category
    """
    # Get configuration from Airflow Variables
    max_products = int(Variable.get("shopee_max_products_per_category", default_var=50))
    headless = bool(Variable.get("crawler_headless", default_var=True))
    wait_time = int(Variable.get("crawler_wait_time", default_var=10))
    max_tabs = int(Variable.get("crawler_max_tabs_per_proxy", default_var=5))
    
    print(f"Starting Shopee crawler for category '{category_name}' at {category_url}")
    print(f"Config: max_products={max_products}, headless={headless}, wait_time={wait_time}, max_tabs={max_tabs}")
    
    # Initialize crawler
    crawler = ShopeeCrawler(headless=headless, wait_time=wait_time, max_tabs=max_tabs)
    
    try:
        # Crawl category
        results = crawler.crawl_category(category_url, max_products=max_products)
        print(f"Successfully crawled {len(results)} products from '{category_name}'")
    except Exception as e:
        print(f"Error crawling category '{category_name}': {e}")
        raise
    finally:
        # Cleanup
        crawler.cleanup()

# Create a task for each Shopee category
shopee_categories = get_shopee_categories()
for category in shopee_categories:
    task_id = f"crawl_shopee_{category['name']}"
    
    shopee_task = PythonOperator(
        task_id=task_id,
        python_callable=crawl_shopee_category,
        op_kwargs={
            'category_name': category['name'],
            'category_url': category['url'],
        },
        dag=dag,
    )

# Add task for Shopee recommended products
def crawl_shopee_recommended(**kwargs):
    """Crawl recommended products from Shopee homepage."""
    # Get configuration from Airflow Variables
    max_products = int(Variable.get("shopee_max_recommended_products", default_var=100))
    headless = bool(Variable.get("crawler_headless", default_var=True))
    wait_time = int(Variable.get("crawler_wait_time", default_var=10))
    max_tabs = int(Variable.get("crawler_max_tabs_per_proxy", default_var=5))
    
    print(f"Starting Shopee crawler for recommended products")
    print(f"Config: max_products={max_products}, headless={headless}, wait_time={wait_time}, max_tabs={max_tabs}")
    
    # Initialize crawler
    crawler = ShopeeCrawler(headless=headless, wait_time=wait_time, max_tabs=max_tabs)
    
    try:
        # Get recommended product links
        product_links = crawler.get_recommended_product_links(max_products=max_products)
        print(f"Found {len(product_links)} recommended product links")
        
        # Crawl each product
        results = crawler.crawl_multiple_products(product_links)
        print(f"Successfully crawled {len(results)} recommended products")
    except Exception as e:
        print(f"Error crawling recommended products: {e}")
        raise
    finally:
        # Cleanup
        crawler.cleanup()

shopee_recommended_task = PythonOperator(
    task_id='crawl_shopee_recommended',
    python_callable=crawl_shopee_recommended,
    dag=dag,
)

# Add tasks for other platforms as they're implemented

# Add a final task to check S3 upload status
def check_s3_upload_status(**kwargs):
    """Check the status of S3 uploads and report summary."""
    # This could query S3 to verify uploads and report statistics
    print("Checking S3 upload status...")
    # Implementation would depend on your specific S3 tracking method

s3_check_task = PythonOperator(
    task_id='check_s3_upload_status',
    python_callable=check_s3_upload_status,
    dag=dag,
)

# Set task dependencies - run all crawling tasks in parallel, then check S3
category_tasks = [dag.get_task(f"crawl_shopee_{category['name']}") for category in shopee_categories]
category_tasks.append(shopee_recommended_task)

for task in category_tasks:
    task >> s3_check_task