"""
Trino Client - Connection and query execution
"""
import os
from typing import Optional, List, Dict, Any
from trino.dbapi import connect
from trino.exceptions import TrinoQueryError
import logging

logger = logging.getLogger(__name__)


class TrinoClient:
    """Trino database client for connection and query execution"""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 8080,
        user: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ):
        """
        Initialize Trino client
        
        Args:
            host: Trino coordinator host (defaults to TRINO_HOST env var)
            port: Trino coordinator port (defaults to TRINO_PORT env var or 8080)
            user: Trino user (defaults to TRINO_USER env var)
            catalog: Default catalog (defaults to TRINO_CATALOG env var)
            schema: Default schema (defaults to TRINO_SCHEMA env var)
        """
        self.host = host or os.getenv('TRINO_HOST', 'localhost')
        self.port = int(os.getenv('TRINO_PORT', port))
        self.user = user or os.getenv('TRINO_USER', 'admin')
        self.catalog = catalog or os.getenv('TRINO_CATALOG', 'tpch')
        self.schema = schema or os.getenv('TRINO_SCHEMA', 'tiny')
        self._connection = None
    
    def connect(self):
        """Establish connection to Trino"""
        try:
            self._connection = connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema
            )
            logger.info(f"Connected to Trino at {self.host}:{self.port}")
            return self._connection
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {e}")
            raise
    
    def execute_query(self, sql: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results
        
        Args:
            sql: SQL query to execute
            limit: Optional limit to add to query
            
        Returns:
            List of dictionaries representing rows
        """
        if not self._connection:
            self.connect()
        
        cursor = self._connection.cursor()
        
        try:
            if limit:
                sql = f"{sql.rstrip(';')} LIMIT {limit}"
            
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
        except TrinoQueryError as e:
            logger.error(f"Trino query error: {e}")
            raise
        finally:
            cursor.close()
    
    def explain_query(self, sql: str) -> str:
        """
        Get EXPLAIN output for a query
        
        Args:
            sql: SQL query to explain
            
        Returns:
            EXPLAIN output as string
        """
        if not self._connection:
            self.connect()
        
        cursor = self._connection.cursor()
        
        try:
            explain_sql = f"EXPLAIN {sql}"
            cursor.execute(explain_sql)
            result = cursor.fetchall()
            return '\n'.join([row[0] for row in result])
        except TrinoQueryError as e:
            logger.error(f"Trino EXPLAIN error: {e}")
            raise
        finally:
            cursor.close()
    
    def run_limit_query(self, sql: str, limit: int = 1) -> List[Dict[str, Any]]:
        """
        Run query with LIMIT for validation
        
        Args:
            sql: SQL query to validate
            limit: Limit to apply (default 1)
            
        Returns:
            Query results
        """
        return self.execute_query(sql, limit=limit)
    
    def close(self):
        """Close connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
