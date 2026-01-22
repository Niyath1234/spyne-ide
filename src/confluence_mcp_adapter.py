"""
Confluence MCP Adapter

Uses MCP (Model Context Protocol) to connect to Confluence instead of direct API calls.
This adapter provides the same interface as ConfluenceIngester but uses MCP tools.
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class ConfluenceMCPAdapter:
    """
    Adapter for Confluence using MCP (Model Context Protocol).
    
    This adapter uses MCP tools to interact with Confluence instead of direct API calls.
    It provides the same interface as ConfluenceIngester for compatibility.
    """
    
    def __init__(
        self,
        space_key: Optional[str] = None,
        raw_dir: str = "data/raw",
        processed_dir: str = "data/processed"
    ):
        """
        Initialize Confluence MCP adapter.
        
        Args:
            space_key: Confluence space key to search (optional, defaults to HOR)
            raw_dir: Directory to save downloaded files
            processed_dir: Directory for processed files
        """
        self.space_key = space_key or os.getenv("CONFLUENCE_SPACE_KEY", "HOR")
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Note: MCP tools are called via the MCP server, not directly in Python
        # This adapter provides a bridge for code that expects ConfluenceIngester interface
        self._mcp_enabled = True
    
    def get_page_by_id(self, page_id: str, convert_to_markdown: bool = False) -> Optional[Dict]:
        """
        Get a Confluence page by ID using MCP.
        
        Args:
            page_id: Confluence page ID
            convert_to_markdown: Whether to convert content to markdown
            
        Returns:
            Page data dictionary or None if not found
            
        Note: This method should be called via MCP tools in the actual implementation.
        For now, it returns a structure that indicates MCP should be used.
        """
        # This is a placeholder - actual implementation would call MCP tools
        # The actual MCP call would be: mcp_mcp-atlassian_confluence_get_page
        return {
            "_mcp_method": "mcp_mcp-atlassian_confluence_get_page",
            "page_id": page_id,
            "convert_to_markdown": convert_to_markdown,
            "include_metadata": True
        }
    
    def get_page_by_title(self, title: str, space_key: Optional[str] = None, convert_to_markdown: bool = False) -> Optional[Dict]:
        """
        Get a Confluence page by title and space using MCP.
        
        Args:
            title: Page title
            space_key: Space key (defaults to instance space_key)
            convert_to_markdown: Whether to convert content to markdown
            
        Returns:
            Page data dictionary or None if not found
        """
        space = space_key or self.space_key
        return {
            "_mcp_method": "mcp_mcp-atlassian_confluence_get_page",
            "title": title,
            "space_key": space,
            "convert_to_markdown": convert_to_markdown,
            "include_metadata": True
        }
    
    def search_pages(self, query: str, limit: int = 50, spaces_filter: Optional[str] = None) -> List[Dict]:
        """
        Search Confluence pages using MCP.
        
        Args:
            query: Search query (can be simple text or CQL)
            limit: Maximum number of results
            spaces_filter: Comma-separated space keys to filter by
            
        Returns:
            List of page dictionaries
        """
        spaces = spaces_filter or self.space_key
        return {
            "_mcp_method": "mcp_mcp-atlassian_confluence_search",
            "query": query,
            "limit": limit,
            "spaces_filter": spaces
        }
    
    def extract_product_from_title(self, title: str) -> Optional[str]:
        """
        Extract product name from Confluence page title.
        
        Args:
            title: Page title (e.g., "ARD: slice mini passbook")
            
        Returns:
            Product name or None
        """
        # Remove document type prefixes
        prefixes = ["ARD:", "PRD:", "TRD:", "ARD -", "PRD -", "TRD -"]
        product = title.strip()
        
        for prefix in prefixes:
            if product.startswith(prefix):
                product = product[len(prefix):].strip()
                break
        
        # Clean up any extra whitespace
        product = ' '.join(product.split())
        
        return product if product else None
    
    def get_page_children(self, parent_id: str, limit: int = 25) -> List[Dict]:
        """
        Get child pages of a Confluence page using MCP.
        
        Args:
            parent_id: Parent page ID
            limit: Maximum number of children to return
            
        Returns:
            List of child page dictionaries
        """
        return {
            "_mcp_method": "mcp_mcp-atlassian_confluence_get_page_children",
            "parent_id": parent_id,
            "limit": limit,
            "include_content": False,
            "convert_to_markdown": True
        }


class ConfluenceMCPClient:
    """
    Client wrapper for making actual MCP calls.
    
    This class provides methods that can be used to call MCP tools.
    In practice, these would be called via the MCP server interface.
    """
    
    @staticmethod
    def get_page(page_id: Optional[str] = None, title: Optional[str] = None, 
                 space_key: Optional[str] = None, convert_to_markdown: bool = True,
                 include_metadata: bool = True) -> Dict:
        """
        Get a Confluence page using MCP.
        
        This method signature matches the MCP tool signature.
        """
        # This would be called via MCP: mcp_mcp-atlassian_confluence_get_page
        # Parameters match the MCP tool signature
        params = {
            "convert_to_markdown": convert_to_markdown,
            "include_metadata": include_metadata
        }
        
        if page_id:
            params["page_id"] = page_id
        elif title and space_key:
            params["title"] = title
            params["space_key"] = space_key
        else:
            raise ValueError("Either page_id or (title and space_key) must be provided")
        
        return params
    
    @staticmethod
    def search(query: str, limit: int = 10, spaces_filter: Optional[str] = None) -> Dict:
        """
        Search Confluence pages using MCP.
        
        This method signature matches the MCP tool signature.
        """
        # This would be called via MCP: mcp_mcp-atlassian_confluence_search
        return {
            "query": query,
            "limit": limit,
            "spaces_filter": spaces_filter
        }
    
    @staticmethod
    def get_page_children(parent_id: str, limit: int = 25, include_content: bool = False,
                         convert_to_markdown: bool = True) -> Dict:
        """
        Get child pages using MCP.
        
        This method signature matches the MCP tool signature.
        """
        # This would be called via MCP: mcp_mcp-atlassian_confluence_get_page_children
        return {
            "parent_id": parent_id,
            "limit": limit,
            "include_content": include_content,
            "convert_to_markdown": convert_to_markdown
        }

