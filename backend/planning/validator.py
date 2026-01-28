#!/usr/bin/env python3
"""
Query Validator

Validates queries before processing to ensure they meet requirements.
"""

from typing import Dict, Any, Optional, List
import logging
import re

logger = logging.getLogger(__name__)


class QueryValidator:
    """Validates queries before processing."""
    
    def __init__(self, metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize query validator.
        
        Args:
            metadata: Optional metadata for validation
        """
        self.metadata = metadata or {}
    
    def validate(self, query: str, context: Optional[Dict[str, Any]] = None) -> tuple[bool, Optional[str], List[str]]:
        """
        Validate a query.
        
        Args:
            query: Query string to validate
            context: Optional context for validation
            
        Returns:
            Tuple of (is_valid, error_message, warnings)
        """
        warnings = []
        
        # Basic validation
        if not query or not query.strip():
            return False, "Query cannot be empty", warnings
        
        # Check for potentially dangerous patterns
        dangerous_patterns = [
            (r'\bDROP\s+TABLE\b', "DROP TABLE statements are not allowed"),
            (r'\bDELETE\s+FROM\b', "DELETE statements are not allowed"),
            (r'\bTRUNCATE\b', "TRUNCATE statements are not allowed"),
            (r'\bALTER\s+TABLE\b', "ALTER TABLE statements are not allowed"),
        ]
        
        query_upper = query.upper()
        for pattern, message in dangerous_patterns:
            if re.search(pattern, query_upper, re.IGNORECASE):
                return False, message, warnings
        
        # Check query length
        if len(query) > 10000:
            warnings.append("Query is very long, may take time to process")
        
        # Check for SQL injection patterns
        sql_injection_patterns = [
            r';\s*DROP',
            r';\s*DELETE',
            r'UNION\s+SELECT.*--',
            r'OR\s+1\s*=\s*1',
        ]
        
        for pattern in sql_injection_patterns:
            if re.search(pattern, query_upper, re.IGNORECASE):
                warnings.append("Query contains potentially suspicious patterns")
        
        return True, None, warnings


class TableValidator:
    """Validates table names and schemas."""
    
    def __init__(self, metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize table validator.
        
        Args:
            metadata: Metadata containing table information
        """
        self.metadata = metadata or {}
        self._build_table_registry()
    
    def _build_table_registry(self):
        """Build registry of valid table names."""
        self.valid_tables = set()
        tables = self.metadata.get('tables', {}).get('tables', [])
        for table in tables:
            table_name = table.get('name') or table.get('table_name')
            if table_name:
                self.valid_tables.add(table_name.lower())
    
    def validate_table(self, table_name: str) -> tuple[bool, Optional[str]]:
        """
        Validate a table name.
        
        Args:
            table_name: Table name to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not table_name:
            return False, "Table name cannot be empty"
        
        table_name_lower = table_name.lower()
        
        # Check if table exists in metadata
        if self.valid_tables and table_name_lower not in self.valid_tables:
            # Warning, not error - table might exist but not in metadata
            logger.warning(f"Table '{table_name}' not found in metadata")
        
        # Check for valid table name format
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            return False, f"Invalid table name format: {table_name}"
        
        return True, None
    
    def validate_tables(self, table_names: List[str]) -> tuple[bool, Optional[str], List[str]]:
        """
        Validate multiple table names.
        
        Args:
            table_names: List of table names to validate
            
        Returns:
            Tuple of (is_valid, error_message, invalid_tables)
        """
        invalid_tables = []
        
        for table_name in table_names:
            is_valid, error = self.validate_table(table_name)
            if not is_valid:
                invalid_tables.append(table_name)
        
        if invalid_tables:
            return False, f"Invalid tables: {', '.join(invalid_tables)}", invalid_tables
        
        return True, None, []

