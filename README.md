# E-Commerce Data Crawler

A modular, scalable data crawler for multiple e-commerce platforms with proxy rotation, AWS S3 integration, and Airflow scheduling.

## Features

- **Modular design**: Easily add new e-commerce platform crawlers
- **Proxy rotation**: Automatic rotation of proxy connections with customizable intervals
- **Multi-tab crawling**: Configurable number of tabs per proxy to maximize throughput
- **AWS S3 integration**: Automatic upload of data to S3 in batches
- **Airflow scheduling**: Flexible task scheduling and monitoring
- **Robust error handling**: Automatic retries and comprehensive logging

## Supported Platforms

- Shopee
- (More platforms can be easily added)

## Project Structure

```
ecommerce-crawler/
├── config/                  # Configuration settings
├── crawlers/                # Platform-specific crawlers
│   ├── base_crawler.py      # Base crawler with common functionality
│   ├── shopee_crawler.py    # Shopee-specific implementation
│   └── ...                  # Other platform crawlers
├── utils/                   # Utility modules
│   ├── proxy_manager.py     # Manages proxy rotation
│   ├── s3_uploader.py       # Handles S3 uploads
│   └── logger.py            # Logging utility
├── airflow/                 # Airflow DAGs and plugins
├── data/                    # Local data storage
├── logs/                    # Application logs
├── tests/                   # Test cases
├── docker-compose.yml       # Docker Compose setup
├── Dockerfile               # Docker image definition
├── requirements.txt         # Python dependencies
├── setup.py                 # Package setup
└── README.md                # Project documentation
```

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- AWS Account with S3 access (for S3 integration)
- Rotating proxy API keys

### Environment Variables

Create a `.env` file in the project root with the following variables:

```
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=ap-southeast-1
S3_BUCKET_NAME=your-bucket-name

# Crawler Configuration
PROXY_ROTATION_INTERVAL=75
MAX_TABS_PER_PROXY=5
```

### Proxy Configuration

Add your proxy rotation keys to `config/proxy_keys.json`:

```json
{
  "shopee": [
    "your_shopee_proxy_key_1",
    "your_shopee_proxy_key_2"
  ],
  "lazada": [
    "your_lazada_proxy_key_1"
  ],
  "tiki": [
    "your_tiki_proxy_key_1"
  ]
}
```

### Starting the Application

1. Build and start the containers:

```bash
docker-compose up -d
```

2. Access the Airflow dashboard at `http://localhost:8080` with credentials:
   - Username: admin
   - Password: admin

3. Configure the Airflow variables for crawler settings through the Airflow UI:
   - `shopee_categories`: JSON list of category names and URLs
   - `shopee_max_products_per_category`: Number of products to crawl per category
   - `crawler_headless`: Whether to run in headless mode (true/false)
   - `crawler_wait_time`: Default wait time for page elements
   - `crawler_max_tabs_per_proxy`: Max tabs per proxy

## Running Crawlers

### Using Airflow

Trigger the DAGs from the Airflow UI or enable them for automatic scheduling.

### Manual Execution

For testing or one-off crawling tasks:

```bash
# Start the crawler service
docker-compose --profile manual up -d crawler

# Access the crawler container
docker-compose exec crawler bash

# Run a crawl task
python -m scripts.run_shopee_crawler --category electronics --max-products 50
```

## Adding a New Platform Crawler

1. Create a new crawler class in `crawlers/` that inherits from `BaseCrawler`
2. Implement the required abstract methods
3. Add platform-specific settings in `config/settings.py`
4. Create a new DAG task in Airflow

Example:

```python
# crawlers/new_platform_crawler.py
from crawlers.base_crawler import BaseCrawler

class NewPlatformCrawler(BaseCrawler):
    def __init__(self, headless=True, wait_time=10, max_tabs=None):
        super().__init__('new_platform', headless, wait_time, max_tabs)
        
    def extract_product_details(self, url):
        # Implement platform-specific extraction
        
    def get_product_links(self, category_url, max_products=100):
        # Implement platform-specific link extraction
```

## Monitoring and Logs

- Airflow logs: Available in the Airflow UI
- Application logs: Located in the `logs/` directory

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit your changes: `git commit -am 'Add new feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a pull request

## License

[MIT License](LICENSE)