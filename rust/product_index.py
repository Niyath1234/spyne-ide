"""
Product Index Management

Manages product indexes created from ARD/PRD documents.
Each product can have multiple table relations (for future use).
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime


class ProductIndex:
    """
    Manages product index structure.
    
    Structure:
    {
        "products": {
            "Product Name": {
                "name": "Product Name",
                "documents": [...],
                "tables": [...],  # Future: table relations
                "created_at": "...",
                "updated_at": "..."
            }
        }
    }
    """
    
    def __init__(self, index_file: str = "data/processed/product_index.json"):
        """
        Initialize product index.
        
        Args:
            index_file: Path to product index JSON file
        """
        self.index_file = Path(index_file)
        self.index_file.parent.mkdir(parents=True, exist_ok=True)
        self.index = self._load_index()
    
    def _load_index(self) -> Dict:
        """Load product index from file."""
        if self.index_file.exists():
            try:
                return json.loads(self.index_file.read_text(encoding='utf-8'))
            except Exception as e:
                print(f"Warning: Could not load product index: {e}")
        
        return {
            "products": {},
            "last_updated": None,
            "version": "1.0"
        }
    
    def save(self):
        """Save product index to file."""
        self.index["last_updated"] = datetime.now().isoformat()
        self.index_file.write_text(
            json.dumps(self.index, indent=2),
            encoding='utf-8'
        )
    
    def get_product(self, product_name: str) -> Optional[Dict]:
        """Get product information."""
        return self.index.get("products", {}).get(product_name)
    
    def add_product(self, product_name: str, metadata: Optional[Dict] = None):
        """Add or update a product."""
        if product_name not in self.index["products"]:
            self.index["products"][product_name] = {
                "name": product_name,
                "documents": [],
                "tables": [],  # For future table relations
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
        
        if metadata:
            self.index["products"][product_name].update(metadata)
            self.index["products"][product_name]["updated_at"] = datetime.now().isoformat()
    
    def add_document_to_product(self, product_name: str, document_info: Dict):
        """Add a document to a product."""
        self.add_product(product_name)
        
        documents = self.index["products"][product_name]["documents"]
        
        # Check if document already exists
        file_name = document_info.get("file_name")
        existing = next(
            (doc for doc in documents if doc.get("file_name") == file_name),
            None
        )
        
        if existing:
            existing.update(document_info)
        else:
            documents.append(document_info)
        
        self.index["products"][product_name]["updated_at"] = datetime.now().isoformat()
    
    def add_table_to_product(self, product_name: str, table_info: Dict):
        """
        Add a table relation to a product (for future use).
        
        Args:
            product_name: Name of the product
            table_info: Table information dictionary
                {
                    "table_name": "...",
                    "schema": "...",
                    "description": "...",
                    "related_documents": [...]
                }
        """
        self.add_product(product_name)
        
        tables = self.index["products"][product_name]["tables"]
        
        # Check if table already exists
        table_name = table_info.get("table_name")
        existing = next(
            (tbl for tbl in tables if tbl.get("table_name") == table_name),
            None
        )
        
        if existing:
            existing.update(table_info)
        else:
            tables.append(table_info)
        
        self.index["products"][product_name]["updated_at"] = datetime.now().isoformat()
    
    def get_all_products(self) -> List[str]:
        """Get list of all product names."""
        return list(self.index.get("products", {}).keys())
    
    def get_product_documents(self, product_name: str) -> List[Dict]:
        """Get all documents for a product."""
        product = self.get_product(product_name)
        if product:
            return product.get("documents", [])
        return []
    
    def get_product_tables(self, product_name: str) -> List[Dict]:
        """Get all tables for a product (for future use)."""
        product = self.get_product(product_name)
        if product:
            return product.get("tables", [])
        return []
    
    def search_products(self, query: str) -> List[str]:
        """
        Search products by name.
        
        Args:
            query: Search query
            
        Returns:
            List of matching product names
        """
        query_lower = query.lower()
        matches = []
        
        for product_name in self.get_all_products():
            if query_lower in product_name.lower():
                matches.append(product_name)
        
        return matches
    
    def get_statistics(self) -> Dict:
        """Get statistics about the product index."""
        products = self.index.get("products", {})
        
        total_docs = sum(len(p.get("documents", [])) for p in products.values())
        total_tables = sum(len(p.get("tables", [])) for p in products.values())
        
        return {
            "total_products": len(products),
            "total_documents": total_docs,
            "total_tables": total_tables,
            "products": {
                name: {
                    "documents": len(p.get("documents", [])),
                    "tables": len(p.get("tables", []))
                }
                for name, p in products.items()
            }
        }


def main():
    """Test the product index."""
    index = ProductIndex()
    
    # Add a test product
    index.add_product("Authentication")
    index.add_document_to_product("Authentication", {
        "file_name": "ARD_Authentication_v2.0.pdf",
        "document_type": "ARD",
        "reference_id": "PROD-101"
    })
    
    # Add a table (future use)
    index.add_table_to_product("Authentication", {
        "table_name": "user_authentication",
        "schema": "auth",
        "description": "Stores user authentication data"
    })
    
    index.save()
    
    print("Product Index:")
    print(json.dumps(index.index, indent=2))
    
    print("\nStatistics:")
    stats = index.get_statistics()
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()





