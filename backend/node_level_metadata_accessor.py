#!/usr/bin/env python3
"""
Node-Level Metadata Accessor

Provides isolated, node-level access to metadata, only loading what's needed
for a specific query rather than loading all tables/registers/pages/indexes.
"""

import json
import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict

logger = logging.getLogger(__name__)

# Try to import NodeRegistry client
try:
    from backend.node_registry_client import NodeRegistryClient
    NODE_REGISTRY_AVAILABLE = True
except ImportError:
    NODE_REGISTRY_AVAILABLE = False
    NodeRegistryClient = None
    logger.warning("NodeRegistry client not available")


class NodeLevelMetadataAccessor:
    """Provides isolated node-level metadata access."""
    
    def __init__(
        self,
        metadata_dir: Optional[str] = None,
        use_node_registry: Optional[bool] = None,
    ):
        """
        Initialize node-level metadata accessor.
        
        Args:
            metadata_dir: Directory containing metadata files
            use_node_registry: Whether to use NodeRegistry (defaults to env var)
        """
        if metadata_dir:
            self.metadata_dir = Path(metadata_dir)
        else:
            self.metadata_dir = Path(__file__).parent.parent / "metadata"
        
        # Initialize NodeRegistry client if available
        self.node_registry_client = None
        self.use_node_registry = (
            use_node_registry
            if use_node_registry is not None
            else os.getenv('USE_NODE_REGISTRY', 'false').lower() == 'true'
        ) and NODE_REGISTRY_AVAILABLE
        
        if self.use_node_registry:
            try:
                self.node_registry_client = NodeRegistryClient()
                # Test connection
                health = self.node_registry_client.health_check()
                if health.get('status') != 'ok':
                    logger.warning("NodeRegistry server not healthy, falling back to JSON")
                    self.use_node_registry = False
                else:
                    logger.info("NodeRegistry client initialized successfully")
            except Exception as e:
                logger.warning(f"NodeRegistry not available: {e}, falling back to JSON")
                self.use_node_registry = False
        
        # Cache for loaded metadata (lazy loading)
        self._tables_cache: Dict[str, Dict[str, Any]] = {}
        self._lineage_cache: Optional[Dict[str, Any]] = None
        self._registry_cache: Optional[Dict[str, Any]] = None
        self._rules_cache: Optional[List[Dict[str, Any]]] = None
        self._knowledge_base_cache: Optional[Dict[str, Any]] = None
        
        # Indexes for fast lookup
        self._table_name_index: Dict[str, str] = {}  # table_name -> file_key
        self._column_to_tables_index: Dict[str, List[str]] = defaultdict(list)  # column_name -> [table_names]
        self._term_to_tables_index: Dict[str, List[str]] = defaultdict(list)  # term -> [table_names]
    
    def get_tables_for_query(self, query_text: str, mentioned_tables: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get only tables relevant to the query.
        
        Args:
            query_text: User query text
            mentioned_tables: Optional list of explicitly mentioned table names
        
        Returns:
            Dictionary of table_name -> table_metadata
        """
        # Try NodeRegistry first if available
        if self.use_node_registry and self.node_registry_client:
            try:
                # Search NodeRegistry
                results = self.node_registry_client.search(query_text)
                nodes = results.get('nodes', [])
                metadata_pages = results.get('metadata_pages', [])
                
                # Extract table nodes
                table_nodes = [n for n in nodes if n.get('node_type') == 'table']
                
                if table_nodes:
                    # Convert metadata pages to table format
                    relevant_tables = {}
                    for node in table_nodes:
                        ref_id = node.get('ref_id')
                        table_name = node.get('name')
                        
                        # Find corresponding metadata page
                        metadata_page = next(
                            (p for p in metadata_pages if p.get('node_ref_id') == ref_id),
                            None
                        )
                        
                        if metadata_page:
                            # Convert metadata page to table format
                            table_metadata = self._convert_metadata_page_to_table(metadata_page, node)
                            if table_metadata:
                                relevant_tables[table_name] = table_metadata
                    
                    if relevant_tables:
                        return relevant_tables
            except Exception as e:
                logger.warning(f"NodeRegistry search failed: {e}, falling back to JSON")
        
        # Fallback: Extract table names from query if not provided
        if not mentioned_tables:
            mentioned_tables = self._extract_table_names_from_query(query_text)
        
        # Load only mentioned tables from JSON
        relevant_tables = {}
        for table_name in mentioned_tables:
            table_metadata = self.get_table(table_name)
            if table_metadata:
                relevant_tables[table_name] = table_metadata
        
        return relevant_tables
    
    def _convert_metadata_page_to_table(
        self,
        metadata_page: Dict[str, Any],
        node: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Convert metadata page to table format."""
        try:
            schema = metadata_page.get('schema', {})
            return {
                'name': node.get('name'),
                'ref_id': node.get('ref_id'),
                'node_type': node.get('node_type'),
                'schema': schema,
                'columns': schema.get('columns', []),
                'metadata': node.get('metadata', {}),
            }
        except Exception as e:
            logger.warning(f"Failed to convert metadata page: {e}")
            return None
    
    def get_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific table (isolated access).
        
        Args:
            table_name: Table name
        
        Returns:
            Table metadata dictionary or None
        """
        # Check cache first
        if table_name in self._tables_cache:
            return self._tables_cache[table_name]
        
        # Load tables.json if not loaded
        if not self._tables_cache:
            self._load_tables_index()
        
        # Try to find table
        tables_file = self.metadata_dir / "tables.json"
        if tables_file.exists():
            try:
                with open(tables_file, 'r') as f:
                    tables_data = json.load(f)
                    tables = tables_data.get('tables', [])
                    
                    for table in tables:
                        if table.get('name') == table_name:
                            self._tables_cache[table_name] = table
                            return table
            except Exception as e:
                print(f"Error loading table {table_name}: {e}")
        
        return None
    
    def get_joins_for_tables(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """
        Get only joins relevant to the specified tables.
        
        Args:
            table_names: List of table names
        
        Returns:
            List of join metadata dictionaries
        """
        if not self._lineage_cache:
            self._load_lineage()
        
        relevant_joins = []
        table_set = set(table_names)
        
        # Get edges from lineage
        edges = self._lineage_cache.get('edges', [])
        for edge in edges:
            from_table = edge.get('from', '')
            to_table = edge.get('to', '')
            
            # Include join if either table is in our set
            if from_table in table_set or to_table in table_set:
                relevant_joins.append(edge)
        
        return relevant_joins
    
    def get_metrics_for_query(self, query_text: str, mentioned_metrics: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get only metrics relevant to the query.
        
        Args:
            query_text: User query text
            mentioned_metrics: Optional list of explicitly mentioned metric names
        
        Returns:
            List of metric metadata dictionaries
        """
        if not self._registry_cache:
            self._load_registry()
        
        # Extract metric names from query if not provided
        if not mentioned_metrics:
            mentioned_metrics = self._extract_metric_names_from_query(query_text)
        
        # Get relevant metrics
        all_metrics = self._registry_cache.get('metrics', [])
        relevant_metrics = []
        
        for metric in all_metrics:
            metric_name = metric.get('name', '').lower()
            # Check if metric is mentioned or if its base table is relevant
            if any(m.lower() in metric_name or metric_name in m.lower() for m in mentioned_metrics):
                relevant_metrics.append(metric)
        
        return relevant_metrics
    
    def get_dimensions_for_query(self, query_text: str, mentioned_dimensions: Optional[List[str]] = None, 
                                 relevant_tables: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get only dimensions relevant to the query.
        
        Args:
            query_text: User query text
            mentioned_dimensions: Optional list of explicitly mentioned dimension names
            relevant_tables: Optional list of relevant table names
        
        Returns:
            List of dimension metadata dictionaries
        """
        if not self._registry_cache:
            self._load_registry()
        
        # Extract dimension names from query if not provided
        if not mentioned_dimensions:
            mentioned_dimensions = self._extract_dimension_names_from_query(query_text)
        
        # Get relevant dimensions
        all_dimensions = self._registry_cache.get('dimensions', [])
        relevant_dimensions = []
        relevant_tables_set = set(relevant_tables or [])
        
        for dim in all_dimensions:
            dim_name = dim.get('name', '').lower()
            base_table = dim.get('base_table', '')
            
            # Include if:
            # 1. Dimension name is mentioned, OR
            # 2. Base table is in relevant tables
            if (any(d.lower() in dim_name or dim_name in d.lower() for d in mentioned_dimensions) or
                base_table in relevant_tables_set):
                relevant_dimensions.append(dim)
        
        return relevant_dimensions
    
    def get_rules_for_tables(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """
        Get only rules relevant to the specified tables.
        
        Args:
            table_names: List of table names
        
        Returns:
            List of rule metadata dictionaries
        """
        if not self._rules_cache:
            self._load_rules()
        
        relevant_rules = []
        table_set = set(table_names)
        
        for rule in self._rules_cache:
            # Check if rule mentions any of our tables
            computation = rule.get('computation', {})
            source_table = computation.get('source_table', '')
            
            if source_table in table_set:
                relevant_rules.append(rule)
        
        return relevant_rules
    
    def get_knowledge_base_terms_for_query(self, query_text: str, mentioned_terms: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get only knowledge base terms relevant to the query.
        
        Args:
            query_text: User query text
            mentioned_terms: Optional list of explicitly mentioned term names
        
        Returns:
            Dictionary of term_name -> term_metadata
        """
        if not self._knowledge_base_cache:
            self._load_knowledge_base()
        
        # Extract terms from query if not provided
        if not mentioned_terms:
            mentioned_terms = self._extract_terms_from_query(query_text)
        
        # Get relevant terms
        all_terms = self._knowledge_base_cache.get('terms', {})
        relevant_terms = {}
        
        query_lower = query_text.lower()
        for term_name, term_data in all_terms.items():
            # Check if term is mentioned or if query contains term/aliases
            if (term_name.lower() in query_lower or
                any(alias.lower() in query_lower for alias in term_data.get('aliases', [])) or
                term_name.lower() in mentioned_terms):
                relevant_terms[term_name] = term_data
        
        return relevant_terms
    
    def _extract_table_names_from_query(self, query_text: str) -> List[str]:
        """Extract table names mentioned in query."""
        # This is simplified - in production, use NLP/LLM to extract
        query_lower = query_text.lower()
        mentioned_tables = []
        
        # Check against known table names
        if not self._tables_cache:
            self._load_tables_index()
        
        for table_name in self._tables_cache.keys():
            if table_name.lower() in query_lower:
                mentioned_tables.append(table_name)
        
        return mentioned_tables
    
    def _extract_metric_names_from_query(self, query_text: str) -> List[str]:
        """Extract metric names mentioned in query."""
        query_lower = query_text.lower()
        mentioned_metrics = []
        
        # Common metric keywords
        metric_keywords = ['total', 'sum', 'count', 'average', 'avg', 'revenue', 'sales', 
                          'outstanding', 'position', 'principal', 'interest']
        
        for keyword in metric_keywords:
            if keyword in query_lower:
                mentioned_metrics.append(keyword)
        
        return mentioned_metrics
    
    def _extract_dimension_names_from_query(self, query_text: str) -> List[str]:
        """Extract dimension names mentioned in query."""
        query_lower = query_text.lower()
        mentioned_dimensions = []
        
        # Common dimension keywords
        dimension_keywords = ['by', 'group by', 'order type', 'region', 'product', 'customer', 
                            'date', 'time', 'branch', 'order']
        
        for keyword in dimension_keywords:
            if keyword in query_lower:
                mentioned_dimensions.append(keyword)
        
        return mentioned_dimensions
    
    def _extract_terms_from_query(self, query_text: str) -> List[str]:
        """Extract business terms mentioned in query."""
        query_lower = query_text.lower()
        mentioned_terms = []
        
        # Common business terms
        business_terms = ['khatabook', 'writeoff', 'write off', 'arc', 'originator', 
                         'settled', 'unsettled', 'customer', 'loan', 'order']
        
        for term in business_terms:
            if term in query_lower:
                mentioned_terms.append(term)
        
        return mentioned_terms
    
    def _load_tables_index(self):
        """Load tables index for fast lookup."""
        tables_file = self.metadata_dir / "tables.json"
        if tables_file.exists():
            try:
                with open(tables_file, 'r') as f:
                    tables_data = json.load(f)
                    tables = tables_data.get('tables', [])
                    
                    for table in tables:
                        table_name = table.get('name')
                        if table_name:
                            self._tables_cache[table_name] = table
                            
                            # Build column index
                            for col in table.get('columns', []):
                                col_name = col.get('name', '')
                                if col_name:
                                    self._column_to_tables_index[col_name].append(table_name)
            except Exception as e:
                print(f"Error loading tables index: {e}")
    
    def _load_lineage(self):
        """Load lineage metadata."""
        lineage_file = self.metadata_dir / "lineage.json"
        if lineage_file.exists():
            try:
                with open(lineage_file, 'r') as f:
                    self._lineage_cache = json.load(f)
            except Exception as e:
                print(f"Error loading lineage: {e}")
                self._lineage_cache = {}
        else:
            self._lineage_cache = {}
    
    def _load_registry(self):
        """Load semantic registry."""
        registry_file = self.metadata_dir / "semantic_registry.json"
        if registry_file.exists():
            try:
                with open(registry_file, 'r') as f:
                    self._registry_cache = json.load(f)
            except Exception as e:
                print(f"Error loading registry: {e}")
                self._registry_cache = {}
        else:
            self._registry_cache = {}
    
    def _load_rules(self):
        """Load business rules."""
        rules_file = self.metadata_dir / "rules.json"
        if rules_file.exists():
            try:
                with open(rules_file, 'r') as f:
                    self._rules_cache = json.load(f)
            except Exception as e:
                print(f"Error loading rules: {e}")
                self._rules_cache = []
        else:
            self._rules_cache = []
    
    def _load_knowledge_base(self):
        """Load knowledge base."""
        kb_file = self.metadata_dir / "knowledge_base.json"
        if kb_file.exists():
            try:
                with open(kb_file, 'r') as f:
                    self._knowledge_base_cache = json.load(f)
            except Exception as e:
                print(f"Error loading knowledge base: {e}")
                self._knowledge_base_cache = {}
        else:
            self._knowledge_base_cache = {}
    
    def build_isolated_context(self, query_text: str, mentioned_tables: Optional[List[str]] = None,
                              mentioned_metrics: Optional[List[str]] = None,
                              mentioned_dimensions: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Build isolated context for a query - only what's needed.
        
        Args:
            query_text: User query text
            mentioned_tables: Optional explicitly mentioned tables
            mentioned_metrics: Optional explicitly mentioned metrics
            mentioned_dimensions: Optional explicitly mentioned dimensions
        
        Returns:
            Isolated metadata dictionary with only relevant nodes
        """
        # Get relevant tables
        relevant_tables = self.get_tables_for_query(query_text, mentioned_tables)
        table_names = list(relevant_tables.keys())
        
        # Get relevant joins
        relevant_joins = self.get_joins_for_tables(table_names)
        
        # Get relevant metrics
        relevant_metrics = self.get_metrics_for_query(query_text, mentioned_metrics)
        
        # Get relevant dimensions
        relevant_dimensions = self.get_dimensions_for_query(query_text, mentioned_dimensions, table_names)
        
        # Get relevant rules
        relevant_rules = self.get_rules_for_tables(table_names)
        
        # Get relevant knowledge base terms
        relevant_terms = self.get_knowledge_base_terms_for_query(query_text)
        
        # Build isolated metadata
        isolated_metadata = {
            'tables': {
                'tables': list(relevant_tables.values())
            },
            'lineage': {
                'edges': relevant_joins
            },
            'semantic_registry': {
                'metrics': relevant_metrics,
                'dimensions': relevant_dimensions
            },
            'rules': relevant_rules,
            'knowledge_base': {
                'terms': relevant_terms
            }
        }
        
        return isolated_metadata


# Global instance
_node_accessor: Optional[NodeLevelMetadataAccessor] = None


def get_node_level_accessor(metadata_dir: Optional[str] = None) -> NodeLevelMetadataAccessor:
    """Get or create global node-level metadata accessor."""
    global _node_accessor
    if _node_accessor is None:
        _node_accessor = NodeLevelMetadataAccessor(metadata_dir)
    return _node_accessor

