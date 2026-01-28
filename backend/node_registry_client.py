"""
NodeRegistry HTTP API Client

Client for accessing Rust NodeRegistry via HTTP API.
"""

import logging
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class NodeRegistryClient:
    """Client for NodeRegistry HTTP API."""
    
    def __init__(self, base_url: Optional[str] = None):
        """
        Initialize NodeRegistry client.
        
        Args:
            base_url: Base URL for NodeRegistry server (defaults to http://127.0.0.1:8081)
        """
        import os
        self.base_url = (base_url or os.getenv('NODE_REGISTRY_URL', 'http://127.0.0.1:8081')).rstrip('/')
        self.session = requests.Session()
        self.timeout = int(os.getenv('NODE_REGISTRY_TIMEOUT', '5'))
        logger.info(f"NodeRegistry client initialized: {self.base_url}")
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check if NodeRegistry server is healthy.
        
        Returns:
            Health check response dict
        """
        try:
            response = self.session.get(
                f"{self.base_url}/health",
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"NodeRegistry health check failed: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def search(self, query: str) -> Dict[str, Any]:
        """
        Search NodeRegistry for query.
        
        Args:
            query: Search query string
            
        Returns:
            {
                'nodes': [...],
                'knowledge_pages': [...],
                'metadata_pages': [...]
            }
        """
        try:
            response = self.session.get(
                f"{self.base_url}/search",
                params={'q': query},
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"NodeRegistry search failed: {e}")
            return {
                'nodes': [],
                'knowledge_pages': [],
                'metadata_pages': [],
                'error': str(e)
            }
    
    def get_node(self, ref_id: str) -> Optional[Dict[str, Any]]:
        """
        Get node by reference ID.
        
        Args:
            ref_id: Node reference ID
            
        Returns:
            Node dict or None if not found
        """
        try:
            response = self.session.get(
                f"{self.base_url}/node/{ref_id}",
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to get node {ref_id}: {e}")
            return None

