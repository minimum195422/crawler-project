"""
Proxy rotation manager for the e-commerce crawler project.
"""
import time
import threading
import requests
import logging
import random
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from config.settings import PROXY_API_URL, PROXY_ROTATION_INTERVAL, load_proxy_keys

logger = logging.getLogger(__name__)

class ProxyManager:
    """
    Manages a pool of rotating proxies for crawlers.
    Handles proxy rotation at specified intervals and tracks usage.
    """
    def __init__(self, platform: str, max_tabs_per_proxy: int = 5):
        """
        Initialize the proxy manager.
        
        Args:
            platform: The platform name (shopee, lazada, etc.)
            max_tabs_per_proxy: Maximum number of tabs per proxy
        """
        self.platform = platform
        self.max_tabs_per_proxy = max_tabs_per_proxy
        self.proxy_keys = load_proxy_keys().get(platform, [])
        
        if not self.proxy_keys:
            logger.warning(f"No proxy keys found for platform: {platform}")
        
        # Structure: {proxy_url: {'last_rotated': datetime, 'in_use': int, 'active': bool}}
        self.proxies = {}
        
        # Lock for thread-safe operations
        self.lock = threading.Lock()
        
        # Start rotation threads for each key
        self.rotation_threads = []
        for key in self.proxy_keys:
            self._initialize_proxy(key)
            thread = threading.Thread(target=self._rotation_worker, args=(key,), daemon=True)
            thread.start()
            self.rotation_threads.append(thread)
    
    def _initialize_proxy(self, key: str) -> Optional[str]:
        """Initialize a proxy for the given key."""
        try:
            proxy_url = self._fetch_new_proxy(key)
            if proxy_url:
                with self.lock:
                    self.proxies[proxy_url] = {
                        'last_rotated': datetime.now(),
                        'in_use': 0,
                        'active': True,
                        'key': key
                    }
                logger.info(f"Initialized proxy: {proxy_url} for key: {key}")
                return proxy_url
            else:
                logger.error(f"Failed to initialize proxy for key: {key}")
                return None
        except Exception as e:
            logger.error(f"Error initializing proxy for key {key}: {e}")
            return None
    
    def _fetch_new_proxy(self, key: str) -> Optional[str]:
        """Fetch a new proxy from the API."""
        try:
            url = PROXY_API_URL.format(key=key)
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                proxy_data = response.text.strip()
                # API might return different formats, make sure to handle them
                if proxy_data.startswith('http'):
                    return proxy_data
                else:
                    # Format: IP:PORT
                    return f"http://{proxy_data}"
            else:
                logger.error(f"Failed to fetch proxy. Status code: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching proxy: {e}")
            return None
    
    def _rotation_worker(self, key: str):
        """Worker thread that rotates proxies at specified intervals."""
        while True:
            try:
                # Find proxy for this key
                proxy_url = None
                with self.lock:
                    for url, data in self.proxies.items():
                        if data['key'] == key and data['active']:
                            proxy_url = url
                            break
                
                if proxy_url:
                    # Check if it's time to rotate
                    with self.lock:
                        last_rotated = self.proxies[proxy_url]['last_rotated']
                        in_use = self.proxies[proxy_url]['in_use']
                    
                    now = datetime.now()
                    if (now - last_rotated).total_seconds() >= PROXY_ROTATION_INTERVAL:
                        # Time to rotate
                        logger.info(f"Rotating proxy {proxy_url} after {PROXY_ROTATION_INTERVAL} seconds")
                        
                        # Wait until no crawler is using this proxy
                        while True:
                            with self.lock:
                                in_use = self.proxies[proxy_url]['in_use']
                            
                            if in_use == 0:
                                break
                            
                            logger.info(f"Waiting for proxy {proxy_url} to be free. Currently in use: {in_use}")
                            time.sleep(1)
                        
                        # Fetch new proxy
                        new_proxy = self._fetch_new_proxy(key)
                        if new_proxy:
                            with self.lock:
                                # Mark old proxy as inactive
                                self.proxies[proxy_url]['active'] = False
                                
                                # Add new proxy
                                self.proxies[new_proxy] = {
                                    'last_rotated': now,
                                    'in_use': 0,
                                    'active': True,
                                    'key': key
                                }
                            
                            logger.info(f"Rotated proxy from {proxy_url} to {new_proxy}")
                        else:
                            logger.error(f"Failed to rotate proxy for key {key}")
                            # Reset last_rotated to try again later
                            with self.lock:
                                self.proxies[proxy_url]['last_rotated'] = now
                else:
                    # No active proxy for this key, try to initialize one
                    self._initialize_proxy(key)
            
            except Exception as e:
                logger.error(f"Error in proxy rotation worker for key {key}: {e}")
            
            # Sleep for a short period before checking again
            time.sleep(5)
    
    def get_proxy(self) -> Optional[str]:
        """
        Get an available proxy.
        
        Returns:
            A proxy URL if available, None otherwise
        """
        with self.lock:
            # Find an active proxy with available slots
            available_proxies = []
            for url, data in self.proxies.items():
                if data['active'] and data['in_use'] < self.max_tabs_per_proxy:
                    available_proxies.append(url)
            
            if available_proxies:
                # Select a proxy with the fewest tabs in use
                selected_proxy = min(
                    available_proxies, 
                    key=lambda url: self.proxies[url]['in_use']
                )
                
                # Increment in_use count
                self.proxies[selected_proxy]['in_use'] += 1
                
                logger.info(f"Assigned proxy {selected_proxy}, now in use by {self.proxies[selected_proxy]['in_use']} tabs")
                return selected_proxy
            else:
                logger.warning("No available proxies")
                return None
    
    def release_proxy(self, proxy_url: str):
        """
        Release a proxy after use.
        
        Args:
            proxy_url: The proxy URL to release
        """
        with self.lock:
            if proxy_url in self.proxies:
                self.proxies[proxy_url]['in_use'] = max(0, self.proxies[proxy_url]['in_use'] - 1)
                logger.info(f"Released proxy {proxy_url}, now in use by {self.proxies[proxy_url]['in_use']} tabs")
            else:
                logger.warning(f"Attempted to release unknown proxy: {proxy_url}")
    
    def get_status(self) -> Dict:
        """Get the status of all proxies."""
        with self.lock:
            return {url: {
                'in_use': data['in_use'],
                'active': data['active'],
                'last_rotated': data['last_rotated'].isoformat(),
                'key': data['key']
            } for url, data in self.proxies.items()}
    
    def shutdown(self):
        """Shutdown the proxy manager."""
        logger.info("Shutting down proxy manager...")
        # Just let the threads terminate naturally since they're daemon threads