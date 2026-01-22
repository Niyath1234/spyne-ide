"""
Confluence API Integration for Document Ingestion

Fetches ARD/PRD/TRD documents from Confluence and extracts product information
from titles/metadata. Creates product indexes for future table relations.
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import requests
from urllib.parse import quote

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from atlassian import Confluence
except ImportError:
    print("Warning: atlassian-python-api not installed. Install with: pip install atlassian-python-api")
    Confluence = None

from src.document_mapper import DocumentMapper


class ConfluenceIngester:
    """
    Fetches documents from Confluence and extracts product information.
    
    Workflow:
    1. Fetch ARD/PRD/TRD from Confluence via API
    2. Extract product from title/metadata
    3. Download and save to data/raw/
    4. Create product index structure
    """
    
    def __init__(
        self,
        url: Optional[str] = None,
        username: Optional[str] = None,
        api_token: Optional[str] = None,
        space_key: Optional[str] = None,
        raw_dir: str = "data/raw",
        processed_dir: str = "data/processed"
    ):
        """
        Initialize Confluence ingester.
        
        Args:
            url: Confluence URL (e.g., "https://your-domain.atlassian.net")
            username: Confluence username/email
            api_token: Confluence API token (or set CONFLUENCE_API_TOKEN env var)
            space_key: Confluence space key to search (optional)
            raw_dir: Directory to save downloaded files
            processed_dir: Directory for processed files
        """
        # Import Config to ensure .env is loaded
        from src.config import Config
        
        self.url = url or Config.get_confluence_url()
        self.username = username or Config.get_confluence_username()
        self.api_token = api_token or Config.get_confluence_api_token()
        self.space_key = space_key or Config.get_confluence_space_key()
        
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Confluence client
        if Confluence is None:
            raise ImportError(
                "atlassian-python-api is required. Install with: pip install atlassian-python-api"
            )
        
        if not all([self.url, self.username, self.api_token]):
            raise ValueError(
                "Confluence credentials required. Set CONFLUENCE_URL, CONFLUENCE_USERNAME, "
                "and CONFLUENCE_API_TOKEN environment variables."
            )
        
        # Ensure URL is Confluence URL (with /wiki)
        # Base URL should be: https://slicepay.atlassian.net/wiki
        confluence_url = self.url.rstrip('/')
        if not confluence_url.endswith("/wiki"):
            if ".atlassian.net" in confluence_url and "/wiki" not in confluence_url:
                # Convert Jira URL to Confluence URL
                confluence_url = confluence_url.replace(".atlassian.net", ".atlassian.net/wiki")
            else:
                confluence_url = f"{confluence_url}/wiki"
        
        # Use requests directly instead of atlassian library for better control
        self.confluence_url = confluence_url
        self.base_url = confluence_url  # https://slicepay.atlassian.net/wiki
        self.api_base = confluence_url  # API endpoints are relative to this
        
        # Store credentials for direct API calls
        self._auth = (self.username, self.api_token)
        
        # Try to use atlassian library if available, but we'll use direct requests as fallback
        try:
            self.confluence = Confluence(
                url=confluence_url,
                username=self.username,
                password=self.api_token
            )
        except Exception as e:
            print(f"Warning: Could not initialize Confluence library: {e}")
            print("Will use direct API calls instead")
            self.confluence = None
        
        # Initialize document mapper
        self.mapper = DocumentMapper()
        
        # Product index
        self.product_index_file = self.processed_dir / "product_index.json"
        self.product_index = self._load_product_index()
    
    def _load_product_index(self) -> Dict:
        """Load product index from file."""
        if self.product_index_file.exists():
            try:
                return json.loads(self.product_index_file.read_text(encoding='utf-8'))
            except Exception as e:
                print(f"Warning: Could not load product index: {e}")
        return {
            "products": {},
            "last_updated": None,
            "version": "1.0"
        }
    
    def _save_product_index(self):
        """Save product index to file."""
        self.product_index["last_updated"] = datetime.now().isoformat()
        self.product_index_file.write_text(
            json.dumps(self.product_index, indent=2),
            encoding='utf-8'
        )
    
    def extract_product_from_title(self, title: str) -> Optional[str]:
        """
        Extract product name from Confluence page title.
        
        Common patterns:
        - "ARD - Product Name"
        - "PRD: Product Name"
        - "Product Name - TRD"
        - "[PROD-101] Product Name"
        
        Args:
            title: Page title
            
        Returns:
            Product name if found, None otherwise
        """
        title_lower = title.lower()
        
        # Remove document type prefixes
        for prefix in ["ard", "prd", "trd", "architecture", "product", "technical"]:
            if title_lower.startswith(prefix):
                title = title[len(prefix):].strip()
                # Remove separators
                title = title.lstrip(" -:[]")
        
        # Extract from brackets [PROD-101] Product Name
        import re
        bracket_match = re.search(r'\[([^\]]+)\]', title)
        if bracket_match:
            # Could be product ID or name
            content = bracket_match.group(1)
            if not content.startswith("PROD-"):
                return content
            title = title.replace(bracket_match.group(0), "").strip()
        
        # Extract product name (first meaningful words)
        words = title.split()
        if words:
            # Take first 2-3 words as product name
            product = " ".join(words[:3]).strip()
            if len(product) > 3:  # Minimum length
                return product
        
        return None
    
    def extract_product_from_metadata(self, page: Dict) -> Optional[str]:
        """
        Extract product from Confluence page metadata/labels.
        
        Args:
            page: Confluence page object
            
        Returns:
            Product name if found
        """
        # Check labels
        labels = page.get("metadata", {}).get("labels", {}).get("results", [])
        for label in labels:
            label_name = label.get("name", "").lower()
            if "product" in label_name:
                return label_name.replace("product:", "").strip()
        
        # Check space name (sometimes product is in space)
        space = page.get("space", {})
        space_name = space.get("name", "")
        if space_name and space_name not in ["Documentation", "Technical"]:
            return space_name
        
        return None
    
    def search_confluence_pages(
        self,
        document_types: List[str] = ["ARD", "PRD", "TRD"],
        space_key: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Search Confluence for ARD/PRD/TRD pages.
        
        Args:
            document_types: List of document types to search for
            space_key: Space key to search (None = all spaces)
            limit: Maximum number of pages to return
            
        Returns:
            List of page objects
        """
        all_pages = []
        
        # Use direct API calls
        try:
            import requests
            from requests.auth import HTTPBasicAuth
            
            # Get all content and filter
            api_url = f"{self.api_base}/rest/api/content"
            params = {"limit": limit, "expand": "space,metadata.labels"}
            
            if space_key:
                params["spaceKey"] = space_key
            
            response = requests.get(
                api_url,
                auth=HTTPBasicAuth(self.username, self.api_token),
                headers={"Accept": "application/json"},
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                content = response.json()
                all_content = content.get("results", [])
                
                # Filter for ARD/PRD/TRD in title
                for page in all_content:
                    title = page.get("title", "").upper()
                    if any(doc_type in title for doc_type in document_types):
                        all_pages.append(page)
                
                print(f"Found {len(all_pages)} ARD/PRD/TRD pages out of {len(all_content)} total")
            else:
                print(f"Error fetching content: {response.status_code}")
                print(response.text[:200])
        
        except Exception as e:
            print(f"Error searching for documents: {e}")
            import traceback
            traceback.print_exc()
        
        return all_pages
    
    def download_page_as_pdf(self, page_id: str, output_path: Path) -> bool:
        """
        Download Confluence page as PDF.
        
        Args:
            page_id: Confluence page ID
            output_path: Path to save PDF
            
        Returns:
            True if successful
        """
        try:
            # Confluence export API - use correct URL format
            export_url = f"{self.confluence_url}/exportpdf.action"
            
            params = {
                "pageId": page_id
            }
            
            from requests.auth import HTTPBasicAuth
            response = requests.get(
                export_url,
                params=params,
                auth=HTTPBasicAuth(self.username, self.api_token),
                stream=True,
                timeout=30
            )
            
            if response.status_code == 200:
                output_path.write_bytes(response.content)
                return True
            else:
                print(f"Error downloading page {page_id}: {response.status_code}")
                if response.status_code == 404:
                    print(f"  Note: PDF export may not be available. Trying alternative method...")
                    # Try getting page content and converting
                    return self._download_page_content(page_id, output_path)
                return False
        
        except Exception as e:
            print(f"Error downloading page {page_id}: {e}")
            return False
    
    def _download_page_content(self, page_id: str, output_path: Path) -> bool:
        """
        Alternative: Get page content and save as markdown.
        
        Args:
            page_id: Confluence page ID
            output_path: Path to save (will be .md instead of .pdf)
            
        Returns:
            True if successful
        """
        try:
            # Get page content
            page = self.confluence.get_page_by_id(page_id, expand="body.storage")
            
            if page:
                # Save as markdown (change extension)
                md_path = output_path.with_suffix('.md')
                content = page.get("body", {}).get("storage", {}).get("value", "")
                
                # Basic HTML to markdown conversion (simplified)
                import re
                # Remove HTML tags (basic)
                content = re.sub(r'<[^>]+>', '', content)
                
                md_path.write_text(content, encoding='utf-8')
                print(f"  Saved as markdown: {md_path}")
                return True
            
            return False
        
        except Exception as e:
            print(f"  Error getting page content: {e}")
            return False
    
    def process_confluence_page(self, page: Dict) -> Dict:
        """
        Process a single Confluence page.
        
        Args:
            page: Confluence page object
            
        Returns:
            Processing result dictionary
        """
        page_id = page.get("id")
        title = page.get("title", "")
        space = page.get("space", {})
        space_key = space.get("key", "")
        
        # Extract document type
        doc_type = "UNKNOWN"
        title_lower = title.lower()
        if "ard" in title_lower or "architecture" in title_lower:
            doc_type = "ARD"
        elif "prd" in title_lower or "product" in title_lower:
            doc_type = "PRD"
        elif "trd" in title_lower or "technical" in title_lower:
            doc_type = "TRD"
        
        # Extract product
        product = (
            self.extract_product_from_metadata(page) or
            self.extract_product_from_title(title) or
            space.get("name", "Unknown")
        )
        
        # Generate filename
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = safe_title.replace(' ', '_')
        filename = f"{doc_type}_{safe_title}_{page_id}.pdf"
        file_path = self.raw_dir / filename
        
        # Download page as PDF
        print(f"Downloading {title} (Product: {product})...")
        success = self.download_page_as_pdf(page_id, file_path)
        
        if not success:
            return {
                "success": False,
                "page_id": page_id,
                "title": title,
                "error": "Failed to download"
            }
        
        # Update product index
        if product not in self.product_index["products"]:
            self.product_index["products"][product] = {
                "name": product,
                "documents": [],
                "created_at": datetime.now().isoformat(),
                "tables": []  # For future table relations
            }
        
        # Add document to product
        doc_info = {
            "file_name": filename,
            "page_id": page_id,
            "title": title,
            "document_type": doc_type,
            "space_key": space_key,
            "confluence_url": f"{self.confluence_url}{page.get('_links', {}).get('webui', '')}",
            "downloaded_at": datetime.now().isoformat()
        }
        
        self.product_index["products"][product]["documents"].append(doc_info)
        
        # Create reference ID from product
        # Format: PROD-{product_hash} or use product name
        ref_id = f"PROD-{abs(hash(product)) % 10000}"
        
        # Update document mapping
        mapping_doc = {
            "file_name": filename,
            "reference_id": ref_id,
            "document_type": doc_type,
            "project": product,
            "tags": [doc_type.lower(), product.lower().replace(" ", "-")],
            "description": f"{doc_type} for {product}",
            "confluence_page_id": page_id,
            "confluence_title": title
        }
        
        return {
            "success": True,
            "page_id": page_id,
            "title": title,
            "product": product,
            "document_type": doc_type,
            "file_name": filename,
            "file_path": str(file_path),
            "reference_id": ref_id,
            "mapping": mapping_doc
        }
    
    def ingest_from_confluence(
        self,
        document_types: List[str] = ["ARD", "PRD", "TRD"],
        space_key: Optional[str] = None,
        limit: int = 100
    ) -> Dict:
        """
        Main ingestion method: Fetch from Confluence and process.
        
        Args:
            document_types: Document types to fetch
            space_key: Space key to search
            limit: Maximum pages per type
            
        Returns:
            Processing results
        """
        print("="*60)
        print("Confluence Document Ingestion")
        print("="*60)
        print(f"URL: {self.url}")
        print(f"Space: {space_key or 'All spaces'}")
        print(f"Document Types: {document_types}")
        print()
        
        # Search for pages
        pages = self.search_confluence_pages(
            document_types=document_types,
            space_key=space_key,
            limit=limit
        )
        
        if not pages:
            print("No pages found")
            return {"success": False, "error": "No pages found"}
        
        print(f"\nProcessing {len(pages)} pages...\n")
        
        results = {
            "success": True,
            "total_pages": len(pages),
            "processed": 0,
            "failed": 0,
            "products": {},
            "mappings": []
        }
        
        # Process each page
        for page in pages:
            result = self.process_confluence_page(page)
            
            if result.get("success"):
                results["processed"] += 1
                product = result.get("product")
                if product:
                    if product not in results["products"]:
                        results["products"][product] = []
                    results["products"][product].append(result)
                    results["mappings"].append(result.get("mapping"))
            else:
                results["failed"] += 1
        
        # Save product index
        self._save_product_index()
        
        # Update document mapping file
        self._update_document_mapping(results["mappings"])
        
        print("\n" + "="*60)
        print("Ingestion Complete")
        print("="*60)
        print(f"Processed: {results['processed']}/{results['total_pages']}")
        print(f"Products found: {len(results['products'])}")
        for product, docs in results["products"].items():
            print(f"  - {product}: {len(docs)} documents")
        
        return results
    
    def _update_document_mapping(self, mappings: List[Dict]):
        """Update document_mapping.yaml with new mappings."""
        import yaml
        
        mapping_file = Path("config/document_mapping.yaml")
        
        # Load existing mapping
        if mapping_file.exists():
            with open(mapping_file, 'r', encoding='utf-8') as f:
                existing = yaml.safe_load(f) or {}
        else:
            existing = {"documents": [], "patterns": [], "aliases": {}}
        
        # Add new mappings (avoid duplicates)
        existing_files = {doc.get("file_name") for doc in existing.get("documents", [])}
        
        for mapping in mappings:
            if mapping.get("file_name") not in existing_files:
                existing.setdefault("documents", []).append(mapping)
        
        # Save updated mapping
        mapping_file.parent.mkdir(parents=True, exist_ok=True)
        with open(mapping_file, 'w', encoding='utf-8') as f:
            yaml.dump(existing, f, default_flow_style=False, sort_keys=False)
        
        print(f"\n✓ Updated document mapping: {len(mappings)} new documents")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest documents from Confluence")
    parser.add_argument("--url", type=str, help="Confluence URL")
    parser.add_argument("--username", type=str, help="Confluence username")
    parser.add_argument("--api-token", type=str, help="Confluence API token")
    parser.add_argument("--space-key", type=str, help="Confluence space key")
    parser.add_argument("--doc-types", nargs="+", default=["ARD", "PRD", "TRD"],
                       help="Document types to fetch")
    parser.add_argument("--limit", type=int, default=100, help="Max pages per type")
    
    args = parser.parse_args()
    
    ingester = ConfluenceIngester(
        url=args.url,
        username=args.username,
        api_token=args.api_token,
        space_key=args.space_key
    )
    
    results = ingester.ingest_from_confluence(
        document_types=args.doc_types,
        space_key=args.space_key,
        limit=args.limit
    )
    
    if results.get("success"):
        print("\n✓ Documents downloaded to data/raw/")
        print("✓ Product index created/updated")
        print("✓ Document mapping updated")
        print("\nNext steps:")
        print("  1. Review product_index.json")
        print("  2. Run: python src/pipeline.py --step ingest")
        print("  3. Run: python src/pipeline.py --step chunk")
        print("  4. Run: python src/pipeline.py --step index")


if __name__ == "__main__":
    main()

