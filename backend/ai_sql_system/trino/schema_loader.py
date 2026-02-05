"""
Trino Schema Loader - Extract table and column metadata
"""
from typing import List, Dict, Any, Optional
from .client import TrinoClient
import logging

logger = logging.getLogger(__name__)


class SchemaLoader:
    """Loads schema metadata from Trino information_schema"""
    
    def __init__(self, trino_client: TrinoClient):
        """
        Initialize schema loader
        
        Args:
            trino_client: TrinoClient instance
        """
        self.client = trino_client
    
    def get_tables(self, catalog: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all tables from information_schema
        
        Args:
            catalog: Catalog name (uses client default if None)
            schema: Schema name (uses client default if None)
            
        Returns:
            List of table metadata dictionaries
        """
        catalog = catalog or self.client.catalog
        schema = schema or self.client.schema
        
        sql = f"""
        SELECT 
            table_catalog,
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_catalog = '{catalog}'
        AND table_schema = '{schema}'
        ORDER BY table_name
        """
        
        try:
            results = self.client.execute_query(sql)
            logger.info(f"Found {len(results)} tables in {catalog}.{schema}")
            return results
        except Exception as e:
            logger.error(f"Error loading tables: {e}")
            return []
    
    def get_columns(self, table_name: str, catalog: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all columns for a table
        
        Args:
            table_name: Table name
            catalog: Catalog name (uses client default if None)
            schema: Schema name (uses client default if None)
            
        Returns:
            List of column metadata dictionaries
        """
        catalog = catalog or self.client.catalog
        schema = schema or self.client.schema
        
        sql = f"""
        SELECT 
            column_name,
            data_type,
            ordinal_position,
            is_nullable
        FROM information_schema.columns
        WHERE table_catalog = '{catalog}'
        AND table_schema = '{schema}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        try:
            results = self.client.execute_query(sql)
            logger.info(f"Found {len(results)} columns in {table_name}")
            return results
        except Exception as e:
            logger.error(f"Error loading columns for {table_name}: {e}")
            return []
    
    def get_sample_rows(self, table_name: str, limit: int = 5, catalog: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get sample rows from a table
        
        Args:
            table_name: Table name
            limit: Number of sample rows
            catalog: Catalog name (uses client default if None)
            schema: Schema name (uses client default if None)
            
        Returns:
            List of sample row dictionaries
        """
        catalog = catalog or self.client.catalog
        schema = schema or self.client.schema
        
        full_table_name = f"{catalog}.{schema}.{table_name}"
        sql = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        
        try:
            results = self.client.execute_query(sql)
            logger.info(f"Retrieved {len(results)} sample rows from {table_name}")
            return results
        except Exception as e:
            logger.error(f"Error loading sample rows from {table_name}: {e}")
            return []
    
    def get_table_info(self, table_name: str, catalog: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get complete table information including columns and sample rows
        
        Args:
            table_name: Table name
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            Dictionary with table, columns, and sample data
        """
        tables = self.get_tables(catalog, schema)
        table_info = next((t for t in tables if t['table_name'] == table_name), None)
        
        if not table_info:
            return {}
        
        columns = self.get_columns(table_name, catalog, schema)
        samples = self.get_sample_rows(table_name, limit=3, catalog=catalog, schema=schema)
        
        return {
            'table': table_info,
            'columns': columns,
            'sample_rows': samples
        }
