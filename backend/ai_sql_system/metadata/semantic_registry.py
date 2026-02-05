"""
Semantic Registry - Interface for accessing metadata
Falls back to JSON files if Postgres is not available
"""
from typing import List, Dict, Any, Optional
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)


class SemanticRegistry:
    """Unified interface for metadata access with Postgres fallback to JSON"""
    
    def __init__(self, ingestion: Optional[Any] = None, 
                 vector_store: Optional[Any] = None):
        """
        Initialize semantic registry
        
        Args:
            ingestion: MetadataIngestion instance (optional)
            vector_store: VectorStore instance (optional)
        """
        self.ingestion = ingestion
        self.vector_store = vector_store
        self._use_postgres = False
        self._postgres_available = None  # Lazy check
        
        # Don't initialize Postgres here - check lazily when needed
        # This allows the system to work without Postgres
        # Load JSON metadata immediately as fallback
        self._load_metadata_from_json()
        
        # Load metadata from JSON files as fallback
        self._metadata_cache = None
        if not self._use_postgres:
            self._load_metadata_from_json()
    
    def _load_metadata_from_json(self):
        """Load metadata from JSON files"""
        try:
            metadata_dir = Path(__file__).parent.parent.parent.parent / "metadata"
            
            self._metadata_cache = {
                'tables': [],
                'metrics': []
            }
            
            # Load tables.json
            tables_file = metadata_dir / "tables.json"
            if tables_file.exists():
                with open(tables_file, 'r') as f:
                    tables_data = json.load(f)
                    self._metadata_cache['tables'] = tables_data.get('tables', [])
                    logger.info(f"Loaded {len(self._metadata_cache['tables'])} tables from JSON")
            
            # Load semantic_registry.json
            registry_file = metadata_dir / "semantic_registry.json"
            if registry_file.exists():
                with open(registry_file, 'r') as f:
                    registry_data = json.load(f)
                    self._metadata_cache['metrics'] = registry_data.get('metrics', [])
                    logger.info(f"Loaded {len(self._metadata_cache['metrics'])} metrics from JSON")
            
        except Exception as e:
            logger.error(f"Error loading metadata from JSON: {e}")
            self._metadata_cache = {'tables': [], 'metrics': []}
    
    def _check_postgres(self) -> bool:
        """Check if Postgres is available (lazy check)"""
        if self._postgres_available is not None:
            return self._postgres_available
        
        if self.ingestion is None:
            try:
                from .ingestion import MetadataIngestion
                self.ingestion = MetadataIngestion()
                self._postgres_available = True
                self._use_postgres = True
                logger.info("Postgres available for metadata")
                return True
            except Exception as e:
                logger.warning(f"Postgres not available, using JSON files: {e}")
                self._postgres_available = False
                self._use_postgres = False
                return False
        else:
            # Test connection
            try:
                conn = self.ingestion._get_connection()
                conn.close()
                self._postgres_available = True
                self._use_postgres = True
                return True
            except Exception as e:
                logger.warning(f"Postgres connection failed: {e}")
                self._postgres_available = False
                self._use_postgres = False
                return False
    
    def get_tables(self) -> List[Dict[str, Any]]:
        """Get all tables"""
        if self._check_postgres() and self.ingestion:
            try:
                conn = self.ingestion._get_connection()
                cur = conn.cursor()
                
                try:
                    cur.execute("SELECT table_name, schema, description FROM tables_metadata")
                    results = cur.fetchall()
                    return [
                        {'table_name': r[0], 'schema': r[1], 'description': r[2]}
                        for r in results
                    ]
                finally:
                    cur.close()
                    conn.close()
            except Exception as e:
                logger.warning(f"Postgres query failed, using JSON: {e}")
                self._use_postgres = False
        
        # Fallback to JSON
        if self._metadata_cache:
            return [
                {
                    'table_name': table.get('name', ''),
                    'schema': table.get('name', '').split('.')[0] if '.' in table.get('name', '') else '',
                    'description': f"Table: {table.get('entity', '')}"
                }
                for table in self._metadata_cache.get('tables', [])
            ]
        return []
    
    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get columns for a table"""
        if self._check_postgres() and self.ingestion:
            try:
                conn = self.ingestion._get_connection()
                cur = conn.cursor()
                
                try:
                    cur.execute("""
                        SELECT column_name, datatype, description 
                        FROM columns_metadata 
                        WHERE table_name = %s
                    """, (table_name,))
                    results = cur.fetchall()
                    return [
                        {'column_name': r[0], 'datatype': r[1], 'description': r[2]}
                        for r in results
                    ]
                finally:
                    cur.close()
                    conn.close()
            except Exception as e:
                logger.warning(f"Postgres query failed: {e}")
        
        # Fallback to JSON
        if self._metadata_cache:
            table_short_name = table_name.split('.')[-1] if '.' in table_name else table_name
            # Try multiple matching strategies
            for table in self._metadata_cache.get('tables', []):
                table_full_name = table.get('name', '')
                table_entity = table.get('entity', '')
                # Match by full name, short name, or entity name
                if (table_full_name.endswith(table_short_name) or 
                    table_full_name == table_name or
                    table_entity == table_short_name or
                    table_entity == table_name):
                    columns = table.get('columns', [])
                    logger.info(f"Found {len(columns)} columns for table {table_name} from JSON")
                    return [
                        {
                            'column_name': col.get('name', ''),
                            'datatype': col.get('type', ''),
                            'description': col.get('description', '')
                        }
                        for col in columns
                    ]
        logger.warning(f"No columns found for table {table_name}")
        return []
    
    def get_metrics(self) -> List[Dict[str, Any]]:
        """Get all metrics"""
        if self._check_postgres() and self.ingestion:
            try:
                conn = self.ingestion._get_connection()
                cur = conn.cursor()
                
                try:
                    cur.execute("""
                        SELECT metric_name, sql_formula, base_table, grain, description
                        FROM metrics_registry
                    """)
                    results = cur.fetchall()
                    return [
                        {
                            'metric_name': r[0],
                            'sql_formula': r[1],
                            'base_table': r[2],
                            'grain': r[3],
                            'description': r[4]
                        }
                        for r in results
                    ]
                finally:
                    cur.close()
                    conn.close()
            except Exception as e:
                logger.warning(f"Postgres query failed: {e}")
        
        # Fallback to JSON
        if self._metadata_cache:
            return self._metadata_cache.get('metrics', [])
        return []
    
    def get_metric(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Get specific metric"""
        metrics = self.get_metrics()
        return next((m for m in metrics if m.get('metric_name') == metric_name), None)
    
    def search_semantic(self, query_embedding: List[float], top_k: int = 10) -> List[Dict[str, Any]]:
        """Semantic search across all metadata"""
        if self.vector_store:
            try:
                return self.vector_store.search_similar(query_embedding, top_k=top_k)
            except Exception as e:
                logger.warning(f"Vector search failed: {e}")
        return []
