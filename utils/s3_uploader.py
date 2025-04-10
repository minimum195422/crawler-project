"""
AWS S3 integration for uploading crawled data.
"""
import os
import json
import logging
import threading
import boto3
from botocore.exceptions import ClientError
from typing import Dict, List, Optional, Union
from datetime import datetime

from config.settings import (
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, 
    AWS_REGION, 
    S3_BUCKET_NAME,
    S3_UPLOAD_BATCH_SIZE
)

logger = logging.getLogger(__name__)

class S3Uploader:
    """
    Handles uploading crawled data to AWS S3.
    Implements batch uploading to minimize API calls.
    """
    def __init__(self, platform: str):
        """
        Initialize the S3 uploader.
        
        Args:
            platform: The platform name (shopee, lazada, etc.)
        """
        self.platform = platform
        self.batch_size = S3_UPLOAD_BATCH_SIZE
        self.product_buffer = []
        self.lock = threading.Lock()
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        # Verify bucket exists and we have access
        self._verify_bucket()
    
    def _verify_bucket(self):
        """Verify that the S3 bucket exists and we have access."""
        try:
            self.s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            logger.info(f"Successfully connected to S3 bucket: {S3_BUCKET_NAME}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket {S3_BUCKET_NAME} does not exist")
            elif error_code == '403':
                logger.error(f"No permission to access bucket {S3_BUCKET_NAME}")
            else:
                logger.error(f"Error accessing bucket {S3_BUCKET_NAME}: {e}")
            raise
    
    def upload_product(self, product_data: Dict):
        """
        Add a product to the buffer and upload if batch size is reached.
        
        Args:
            product_data: The product data to upload
        """
        with self.lock:
            self.product_buffer.append(product_data)
            
            if len(self.product_buffer) >= self.batch_size:
                self._upload_batch()
    
    def _upload_batch(self):
        """Upload the current batch of products to S3."""
        if not self.product_buffer:
            return
        
        try:
            # Create a batch filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            batch_id = f"{self.platform}_batch_{timestamp}.json"
            s3_key = f"{self.platform}/{timestamp[:8]}/{batch_id}"
            
            # Convert data to JSON
            batch_data = json.dumps(self.product_buffer, ensure_ascii=False)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=batch_data,
                ContentType='application/json'
            )
            
            logger.info(f"Successfully uploaded batch of {len(self.product_buffer)} products to S3: {s3_key}")
            
            # Clear the buffer
            self.product_buffer = []
            
        except Exception as e:
            logger.error(f"Error uploading batch to S3: {e}")
            # Keep the buffer for retry later
    
    def upload_image(self, image_path: str, shop_id: str, product_id: str) -> Optional[str]:
        """
        Upload an image to S3.
        
        Args:
            image_path: Path to the local image file
            shop_id: Shop ID for organizing in S3
            product_id: Product ID for organizing in S3
            
        Returns:
            S3 URL of the uploaded image if successful, None otherwise
        """
        try:
            if not os.path.exists(image_path):
                logger.error(f"Image file not found: {image_path}")
                return None
            
            # Extract filename from path
            filename = os.path.basename(image_path)
            
            # Create S3 key with organized structure
            s3_key = f"{self.platform}/images/{shop_id}/{product_id}/{filename}"
            
            # Upload to S3
            with open(image_path, 'rb') as image_file:
                self.s3_client.upload_fileobj(
                    image_file,
                    S3_BUCKET_NAME,
                    s3_key,
                    ExtraArgs={'ContentType': 'image/jpeg'}
                )
            
            # Generate S3 URL
            s3_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
            logger.info(f"Successfully uploaded image to S3: {s3_url}")
            
            return s3_url
            
        except Exception as e:
            logger.error(f"Error uploading image to S3: {e}")
            return None
    
    def flush(self):
        """
        Upload any remaining products in the buffer.
        Should be called when crawling is complete.
        """
        with self.lock:
            if self.product_buffer:
                logger.info(f"Flushing remaining {len(self.product_buffer)} products to S3")
                self._upload_batch()