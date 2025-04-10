"""
Logging utility for the e-commerce crawler project.
Provides consistent logging configuration across all modules.
"""
import os
import logging
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path

from config.settings import LOG_DIR

def setup_logger(name: str, log_level=logging.INFO) -> logging.Logger:
    """
    Set up a logger with consistent formatting and handlers.
    
    Args:
        name: Name of the logger (usually __name__ from the module)
        log_level: Logging level (INFO, DEBUG, etc.)
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Don't add handlers if they already exist
    if logger.hasHandlers():
        return logger
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    
    # Create file handler
    # Use date-based log files
    today = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(LOG_DIR, f"{today}_{name.replace('.', '_')}.log")
    
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def get_logger(name: str = None) -> logging.Logger:
    """
    Get or create a logger with the given name.
    
    Args:
        name: Name of the logger (uses the root logger if None)
        
    Returns:
        Logger instance
    """
    if name is None:
        name = "ecommerce_crawler"
    
    return setup_logger(name)

# Configure the root logger with a NullHandler by default
# This prevents logging messages from being propagated to the root logger
# unless explicitly configured
logging.getLogger().addHandler(logging.NullHandler())