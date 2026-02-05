"""
Column Name Fixer - Post-process SQL to use correct column names from metadata
"""
from typing import Dict, Any, List, Optional
import re
import logging
from ..metadata.semantic_registry import SemanticRegistry

logger = logging.getLogger(__name__)


class ColumnFixer:
    """Fixes column names in SQL to match actual metadata"""
    
    def __init__(self, semantic_registry: Optional[SemanticRegistry] = None):
        """
        Initialize column fixer
        
        Args:
            semantic_registry: SemanticRegistry instance
        """
        self.registry = semantic_registry or SemanticRegistry()
        self._column_cache = {}  # Cache table columns
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table"""
        if table_name not in self._column_cache:
            columns = self.registry.get_columns(table_name)
            self._column_cache[table_name] = [col.get('column_name', '') for col in columns]
        return self._column_cache.get(table_name, [])
    
    def _find_matching_column(self, table_name: str, user_term: str) -> Optional[str]:
        """
        Find the actual column name that matches a user term
        
        Args:
            table_name: Table name (can be short like "customer" or full like "tpch.tiny.customer")
            user_term: User-provided term (e.g., "customer_id", "extendedprice")
            
        Returns:
            Actual column name or None
        """
        # Try different table name variations
        table_variations = [table_name]
        table_short = table_name.split('.')[-1] if '.' in table_name else table_name
        table_variations.append(table_short)
        table_variations.append(f"tpch.tiny.{table_short}")
        
        columns = []
        for table_var in table_variations:
            cols = self._get_table_columns(table_var)
            if cols:
                columns = cols
                break
        
        if not columns:
            logger.warning(f"No columns found for table {table_name} (tried: {table_variations})")
            return None
        
        user_term_lower = user_term.lower()
        
        # Direct match
        for col in columns:
            if col.lower() == user_term_lower:
                return col
        
        # Table-specific mappings
        table_lower = table_short.lower()
        if 'customer' in table_lower:
            if user_term_lower in ['customer_id', 'id', 'customer_key']:
                for col in columns:
                    if 'custkey' in col.lower():
                        return col
        elif 'order' in table_lower and 'item' not in table_lower:
            if user_term_lower in ['order_id', 'id', 'order_key']:
                for col in columns:
                    if 'orderkey' in col.lower():
                        return col
            if user_term_lower in ['customer_id', 'customer_key']:
                for col in columns:
                    if 'custkey' in col.lower():
                        return col
        elif 'lineitem' in table_lower or 'order_item' in table_lower:
            if user_term_lower in ['extendedprice', 'extended_price']:
                for col in columns:
                    if 'extendedprice' in col.lower():
                        return col
            if user_term_lower in ['discount']:
                for col in columns:
                    if 'discount' in col.lower():
                        return col
            if user_term_lower in ['order_id', 'order_key']:
                for col in columns:
                    if 'orderkey' in col.lower():
                        return col
        
        # Common mappings
        mappings = {
            'customer_id': ['c_custkey', 'custkey'],
            'customer_key': ['c_custkey', 'custkey'],
            'order_id': ['o_orderkey', 'orderkey'],
            'order_key': ['o_orderkey', 'orderkey'],
            'extendedprice': ['l_extendedprice'],
            'discount': ['l_discount'],
            'quantity': ['l_quantity'],
        }
        
        if user_term_lower in mappings:
            for pattern in mappings[user_term_lower]:
                for col in columns:
                    if pattern.lower() in col.lower():
                        return col
        
        # Fuzzy match - find column containing key parts
        key_parts = [p for p in user_term_lower.split('_') if len(p) > 2]
        for col in columns:
            col_lower = col.lower()
            # Check if key parts match
            matches = sum(1 for part in key_parts if part in col_lower)
            if matches >= len(key_parts) * 0.5:  # At least 50% of parts match
                return col
        
        return None
    
    def fix_column_names(self, sql: str, query_plan: Optional[Dict[str, Any]] = None) -> str:
        """
        Fix column names in SQL to match actual metadata
        
        Args:
            sql: Generated SQL query
            query_plan: Query plan (optional, for context)
            
        Returns:
            Fixed SQL with correct column names
        """
        if not sql:
            return sql
        
        # Extract table names and aliases from SQL
        # Pattern: FROM table_name [alias] or JOIN table_name [alias]
        table_pattern = r'(?:FROM|JOIN)\s+(\w+)(?:\s+(\w+))?'
        table_matches = re.findall(table_pattern, sql, re.IGNORECASE)
        
        # Build table alias map
        table_alias_map = {}
        for match in table_matches:
            table_name = match[0].lower()
            alias = match[1].lower() if match[1] else table_name
            table_alias_map[alias] = table_name
            # Also map table name to itself
            table_alias_map[table_name] = table_name
        
        # Also get base table without alias (handle case where FROM has no alias)
        from_match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        if from_match:
            base_table = from_match.group(1).lower()
            if base_table not in table_alias_map:
                table_alias_map[base_table] = base_table
        
        # Pattern to match column references: alias.column or table.column
        column_pattern = r'(\w+)\.(\w+)'
        
        def replace_column(match):
            table_or_alias = match.group(1).lower()
            column_name = match.group(2)
            
            # Get actual table name
            actual_table = table_alias_map.get(table_or_alias, table_or_alias)
            
            # Find matching column
            fixed_column = self._find_matching_column(actual_table, column_name)
            
            if fixed_column and fixed_column != column_name:
                logger.info(f"Fixed column: {table_or_alias}.{column_name} -> {table_or_alias}.{fixed_column} (table: {actual_table})")
                return f"{match.group(1)}.{fixed_column}"
            elif not fixed_column:
                logger.warning(f"Could not find matching column for {table_or_alias}.{column_name} (table: {actual_table})")
            
            return match.group(0)
        
        # Replace column references
        fixed_sql = re.sub(column_pattern, replace_column, sql)
        
        logger.info(f"Column fixer: Original SQL had {len(re.findall(column_pattern, sql))} column references")
        logger.info(f"Column fixer: Fixed SQL has {len(re.findall(column_pattern, fixed_sql))} column references")
        
        # Also fix unqualified column names in GROUP BY and SELECT
        # This is trickier, so we'll be conservative
        group_by_pattern = r'GROUP\s+BY\s+(\w+\.\w+)'
        group_by_match = re.search(group_by_pattern, fixed_sql, re.IGNORECASE)
        if group_by_match:
            # Already qualified, should be fixed by above
            pass
        
        return fixed_sql
