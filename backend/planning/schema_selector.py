"""
Schema Selector

Selects appropriate tables and schema based on user intent.
"""

from typing import Dict, Any, List, Optional
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SchemaSelector:
    """Selects schema (tables) based on intent."""
    
    def __init__(self, metadata_path: Optional[str] = None):
        """
        Initialize schema selector.
        
        Args:
            metadata_path: Path to metadata directory
        """
        self.metadata_path = metadata_path or Path(__file__).parent.parent.parent / 'metadata'
        self.tables = self._load_tables()
        self.semantic_registry = self._load_semantic_registry()
    
    def _load_tables(self) -> List[Dict[str, Any]]:
        """Load table metadata."""
        try:
            # Try to use MetadataProvider first
            from backend.metadata_provider import MetadataProvider
            metadata = MetadataProvider.load()
            return metadata.get('tables', {}).get('tables', [])
        except Exception:
            # Fallback to direct file loading
            try:
                for filename in ['tables.json', 'ad_hoc_tables.json']:
                    tables_file = self.metadata_path / filename
                    if tables_file.exists():
                        with open(tables_file, 'r') as f:
                            data = json.load(f)
                            return data.get('tables', [])
            except Exception as e:
                logger.warning(f"Failed to load tables: {e}")
        return []
    
    def _load_semantic_registry(self) -> Dict[str, Any]:
        """Load semantic registry."""
        try:
            # Try to use MetadataProvider first
            from backend.metadata_provider import MetadataProvider
            metadata = MetadataProvider.load()
            return metadata.get('semantic_registry', {})
        except Exception:
            # Fallback to direct file loading
            try:
                for filename in ['semantic_registry.json', 'ad_hoc_semantic_registry.json']:
                    registry_file = self.metadata_path / filename
                    if registry_file.exists():
                        with open(registry_file, 'r') as f:
                            return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load semantic registry: {e}")
        return {}
    
    def select(self, intent: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Select schema based on intent.
        
        Args:
            intent: Intent dictionary with query information
            context: Context dictionary with additional information
        
        Returns:
            Schema dictionary with selected tables and metadata
        """
        query = intent.get('query', '').lower()
        metric = intent.get('metric')
        dimensions = intent.get('dimensions', [])
        filters = intent.get('filters', [])
        
        selected_tables = []
        required_joins = []
        
        # If metric is specified, find tables from semantic registry
        if metric:
            metric_info = self._find_metric(metric)
            if metric_info:
                # Get base table from metric
                base_table = metric_info.get('base_table')
                if base_table and base_table != 'varies_by_product':
                    # Find table metadata
                    table_meta = self._find_table(base_table)
                    if table_meta:
                        selected_tables.append(table_meta)
                
                # Check for product-specific tables
                product_specific = metric_info.get('product_specific', {})
                if product_specific:
                    # Try to infer product from context or query
                    product = self._infer_product(query, context)
                    if product and product in product_specific:
                        product_table = product_specific[product].get('base_table')
                        if product_table:
                            table_meta = self._find_table(product_table)
                            if table_meta:
                                selected_tables.append(table_meta)
                        
                        # Check for required joins
                        required_joins_list = product_specific[product].get('requires_join', [])
                        for join_table_name in required_joins_list:
                            join_table = self._find_table(join_table_name)
                            if join_table:
                                selected_tables.append(join_table)
        
        # If no tables found, try keyword matching
        if not selected_tables:
            selected_tables = self._select_by_keywords(query, dimensions, filters)
        
        # Remove duplicates
        seen_names = set()
        unique_tables = []
        for table in selected_tables:
            if table['name'] not in seen_names:
                seen_names.add(table['name'])
                unique_tables.append(table)
        
        # Build schema result
        schema = {
            'tables': unique_tables,
            'joins': required_joins,
            'grain': self._determine_grain(unique_tables, intent),
            'time_column': self._find_time_column(unique_tables),
        }
        
        logger.info(f"Selected schema: {[t['name'] for t in unique_tables]}")
        return schema
    
    def _find_metric(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Find metric in semantic registry."""
        metrics = self.semantic_registry.get('metrics', [])
        for metric in metrics:
            if metric.get('name') == metric_name:
                return metric
        return None
    
    def _find_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Find table metadata by name."""
        for table in self.tables:
            if table.get('name') == table_name:
                return table
        return None
    
    def _infer_product(self, query: str, context: Dict[str, Any]) -> Optional[str]:
        """Infer product type from query or context."""
        query_lower = query.lower()
        
        # Check context first
        product = context.get('product') or context.get('product_type')
        if product:
            return product.lower()
        
        # Infer from query keywords
        if any(kw in query_lower for kw in ['bank', 'edl', 'cash credit', 'term loan']):
            return 'bank'
        elif any(kw in query_lower for kw in ['digital', 'credin']):
            return 'digital'
        elif any(kw in query_lower for kw in ['credit card', 'credit_card']):
            return 'credit_card'
        elif any(kw in query_lower for kw in ['khatabook', 'kb']):
            return 'khatabook'
        elif any(kw in query_lower for kw in ['da']):
            return 'da'
        
        return None
    
    def _select_by_keywords(self, query: str, dimensions: List[str], filters: List[Any]) -> List[Dict[str, Any]]:
        """Select tables by keyword matching."""
        selected = []
        query_lower = query.lower()
        
        for table in self.tables:
            table_name = table.get('name', '').lower()
            description = table.get('description', '').lower()
            labels = [l.lower() for l in table.get('labels', [])]
            
            # Check if query mentions table name or description
            if (table_name in query_lower or 
                any(word in query_lower for word in description.split()[:5]) or
                any(label in query_lower for label in labels)):
                selected.append(table)
            
            # Check if dimensions match table columns
            table_columns = [c.get('name', '').lower() for c in table.get('columns', [])]
            if any(dim.lower() in table_columns for dim in dimensions):
                selected.append(table)
        
        return selected
    
    def _determine_grain(self, tables: List[Dict[str, Any]], intent: Dict[str, Any]) -> List[str]:
        """Determine grain from tables and intent."""
        if not tables:
            return []
        
        # Use grain from first table, or primary key
        first_table = tables[0]
        grain = first_table.get('grain') or first_table.get('primary_key', [])
        return grain if isinstance(grain, list) else [grain] if grain else []
    
    def _find_time_column(self, tables: List[Dict[str, Any]]) -> Optional[str]:
        """Find time column from tables."""
        for table in tables:
            time_col = table.get('time_column')
            if time_col:
                return time_col
        return None

