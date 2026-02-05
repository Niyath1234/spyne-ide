"""
Deterministic SQL Builder - Builds SQL from query plan ensuring join path is followed exactly
"""
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class DeterministicSQLBuilder:
    """Builds SQL deterministically from query plan, ensuring join path is followed"""
    
    def _normalize_join_condition(self, condition: str, table1: str, table2: str, all_tables: Optional[Dict[str, str]] = None) -> str:
        """
        Normalize join condition to use table aliases instead of full table names.
        Trino-compatible: ensures aliases reference tables that are already in scope.
        CRITICAL: Always puts table1 (already in scope) first in the condition.
        
        Args:
            condition: Join condition string (e.g., "orders.o_custkey = customer.c_custkey")
            table1: First table name (already in scope)
            table2: Second table name (being joined)
            all_tables: Optional dict mapping table names to aliases for all tables in query
            
        Returns:
            Normalized condition with aliases, table1 first (e.g., "c.c_custkey = o.o_custkey")
        """
        if not condition:
            return condition
        
        import re
        
        # Create alias mapping - use first letter of table name
        alias1 = table1[0].lower() if table1 else ''
        alias2 = table2[0].lower() if table2 else ''
        
        # Extract short table names (last component after dots)
        short_table1 = table1.split('.')[-1].lower() if table1 else ''
        short_table2 = table2.split('.')[-1].lower() if table2 else ''
        
        # Build mapping of all possible table name variations to aliases
        replacements = {}
        
        # Map table1 (already in scope) to its alias
        if table1:
            replacements[table1.lower()] = alias1
            replacements[table1] = alias1
            if short_table1:
                replacements[short_table1] = alias1
        
        # Map table2 (being joined) to its alias
        if table2:
            replacements[table2.lower()] = alias2
            replacements[table2] = alias2
            if short_table2:
                replacements[short_table2] = alias2
        
        # Also handle fully qualified names if provided
        if all_tables:
            for full_table_name, alias in all_tables.items():
                short_name = full_table_name.split('.')[-1].lower()
                replacements[full_table_name.lower()] = alias
                replacements[full_table_name] = alias
                if short_name:
                    replacements[short_name] = alias
        
        # Replace table names with aliases using word boundaries to avoid partial matches
        normalized = condition
        # Sort by length (longest first) to replace fully qualified names before short names
        for table_name in sorted(replacements.keys(), key=len, reverse=True):
            alias = replacements[table_name]
            # Use word boundary to ensure we match whole table names, not parts of column names
            pattern = re.compile(r'\b' + re.escape(table_name) + r'\.', re.IGNORECASE)
            normalized = pattern.sub(f"{alias}.", normalized)
        
        # CRITICAL FIX: Ensure table1 (already in scope) comes first in the condition
        # Trino requires the already-scoped table reference to be first
        # This prevents "Column 'o.o_custkey' cannot be resolved" errors
        # Trino can't resolve aliases that are being defined in the same JOIN clause
        if '=' in normalized:
            parts = [p.strip() for p in normalized.split('=')]
            if len(parts) == 2:
                left = parts[0]
                right = parts[1]
                
                # Check if left side references table2 (being joined) and right references table1 (already in scope)
                # Pattern: "o.column = c.column" should become "c.column = o.column"
                left_refs_t2 = (left.startswith(f"{alias2}.") or 
                               left.startswith(f"{short_table2}.") or
                               left.startswith(f"{table2.lower()}.") or
                               left.startswith(f"{table2}."))
                right_refs_t1 = (right.startswith(f"{alias1}.") or 
                                 right.startswith(f"{short_table1}.") or
                                 right.startswith(f"{table1.lower()}.") or
                                 right.startswith(f"{table1}."))
                
                # Also check reverse: left might reference t1 and right t2 (already correct)
                left_refs_t1 = (left.startswith(f"{alias1}.") or 
                               left.startswith(f"{short_table1}.") or
                               left.startswith(f"{table1.lower()}.") or
                               left.startswith(f"{table1}."))
                right_refs_t2 = (right.startswith(f"{alias2}.") or 
                                 right.startswith(f"{short_table2}.") or
                                 right.startswith(f"{table2.lower()}.") or
                                 right.startswith(f"{table2}."))
                
                # If left references t2 (being joined) and right references t1 (already in scope), swap
                if left_refs_t2 and right_refs_t1:
                    # Swap: put table1 (already in scope) first
                    normalized = f"{right} = {left}"
                    logger.info(f"Swapped JOIN condition order for Trino: {normalized} (was: {left} = {right})")
                elif not (left_refs_t1 and right_refs_t2):
                    # If neither pattern matches, try to ensure t1 comes first by checking column names
                    # This is a fallback for complex conditions
                    logger.warning(f"Could not determine JOIN condition order for: {normalized}")
        
        return normalized
    
    def build_sql(self, query_plan: Dict[str, Any]) -> str:
        """
        Build SQL deterministically from query plan
        
        Args:
            query_plan: Structured query plan with joins and join_path
            
        Returns:
            SQL query string
        """
        base_table = query_plan.get('base_table', '')
        joins = query_plan.get('joins', [])
        join_path = query_plan.get('join_path', [])
        metric_sql = query_plan.get('metric_sql', '')
        group_by = query_plan.get('group_by', [])
        
        if not base_table:
            logger.warning("No base table in query plan")
            return ""
        
        # Build SELECT clause
        select_parts = []
        
        # Add group by columns
        for gb_col in group_by:
            # Handle table.column format
            if '.' in gb_col:
                # Normalize to use alias if it's a table.column reference
                parts = gb_col.split('.')
                if len(parts) == 2:
                    table_part, col_part = parts
                    # Check if this table is in our join path
                    table_short = table_part.split('.')[-1].lower()
                    # Use alias (first letter) for the table
                    alias = table_short[0] if table_short else ''
                    select_parts.append(f"{alias}.{col_part}")
                else:
                    select_parts.append(gb_col)
            else:
                # Use base table alias
                select_parts.append(f"{base_alias}.{gb_col}")
        
        # Add metric
        if metric_sql:
            select_parts.append(metric_sql)
        
        select_clause = "SELECT\n    " + ",\n    ".join(select_parts)
        
        # Build FROM clause - use fully qualified table name for Trino compatibility
        base_table_qualified = base_table if '.' in base_table else f"tpch.tiny.{base_table}"
        base_alias = base_table[0].lower() if base_table else ''
        from_clause = f"FROM\n    {base_table_qualified} {base_alias}"
        
        # Build JOIN clauses - MUST follow join_path exactly
        join_clauses = []
        
        # Prefer join_path over joins to ensure correct order
        if join_path:
            logger.info(f"Building joins from join_path: {join_path}")
            # Build joins from join_path - this ensures correct order
            current_table = base_table
            # Build mapping of all tables to aliases for proper normalization
            all_table_aliases = {}
            all_table_aliases[base_table] = base_table[0].lower() if base_table else ''
            for _, t2 in join_path:
                all_table_aliases[t2] = t2[0].lower() if t2 else ''
            
            for t1, t2 in join_path:
                # Try to get join condition from joins list
                condition = None
                join_type = 'LEFT'
                
                if joins and isinstance(joins[0], dict):
                    for j in joins:
                        j_from = j.get('from_table', '').lower()
                        j_to = j.get('to_table', '').lower()
                        # Handle both short names and fully qualified names
                        t1_short = t1.split('.')[-1].lower() if '.' in t1 else t1.lower()
                        t2_short = t2.split('.')[-1].lower() if '.' in t2 else t2.lower()
                        if (j_from == t1.lower() or j_from == t1_short) and (j_to == t2.lower() or j_to == t2_short):
                            condition = j.get('condition', '')
                            join_type = j.get('type', 'LEFT')
                            # Normalize condition to use aliases with all table context
                            # CRITICAL: Pass t1 first (already in scope) so normalization can ensure correct order
                            condition = self._normalize_join_condition(condition, t1, t2, all_table_aliases)
                            logger.info(f"Found join condition for {t1} -> {t2}: {condition} (normalized)")
                            break
                
                if not condition:
                    # Fallback: construct from known TPC-H relationships
                    t1_short = t1.split('.')[-1].lower() if '.' in t1 else t1.lower()
                    t2_short = t2.split('.')[-1].lower() if '.' in t2 else t2.lower()
                    alias1 = t1[0].lower() if t1 else ''
                    alias2 = t2[0].lower() if t2 else ''
                    
                    # CRITICAL: For Trino compatibility, put the already-scoped table (t1) first
                    # This ensures Trino can resolve the reference since t1 is already in scope
                    if t1_short == 'customer' and t2_short == 'orders':
                        condition = f"{alias1}.c_custkey = {alias2}.o_custkey"
                    elif t1_short == 'orders' and t2_short == 'lineitem':
                        condition = f"{alias1}.o_orderkey = {alias2}.l_orderkey"
                    elif t1_short == 'customer' and t2_short == 'lineitem':
                        # Indirect join through orders
                        condition = f"{alias1}.c_custkey = {alias2}.l_custkey"  # Fallback
                    else:
                        logger.warning(f"No join condition found for {t1} -> {t2}, using fallback")
                        condition = f"{alias1}.{t1_short}_id = {alias2}.{t1_short}_id"
                
                alias = t2[0].lower() if t2 else ''
                alias1 = t1[0].lower() if t1 else ''
                
                # CRITICAL FIX: Ensure condition has table1 (already in scope) first for Trino
                # Trino cannot resolve aliases that are being defined in the JOIN clause itself
                # Pattern must be: "already_scoped_table.column = new_table.column"
                # This is a safety check even after normalization
                if '=' in condition:
                    parts = [p.strip() for p in condition.split('=')]
                    if len(parts) == 2:
                        left, right = parts
                        t1_short = t1.split('.')[-1].lower() if '.' in t1 else t1.lower()
                        t2_short = t2.split('.')[-1].lower() if '.' in t2 else t2.lower()
                        
                        # Check if left references t2 (being joined) and right references t1 (already in scope)
                        # If so, swap them so table1 comes first
                        left_refs_t2 = (left.startswith(f"{alias}.") or 
                                       left.startswith(f"{t2_short}.") or
                                       left.startswith(f"{t2.lower()}."))
                        right_refs_t1 = (right.startswith(f"{alias1}.") or 
                                         right.startswith(f"{t1_short}.") or
                                         right.startswith(f"{t1.lower()}."))
                        
                        if left_refs_t2 and right_refs_t1:
                            # Swap: put table1 (already in scope) first
                            condition = f"{right} = {left}"
                            logger.info(f"Fixed JOIN condition order for Trino compatibility: {condition}")
                
                # For Trino compatibility, use fully qualified table names in JOIN clauses
                # This ensures proper schema resolution
                t2_qualified = t2 if '.' in t2 else f"tpch.tiny.{t2}"
                join_clauses.append(f"{join_type} JOIN\n    {t2_qualified} {alias} ON {condition}")
                current_table = t2
                logger.info(f"Added join: {join_type} JOIN {t2_qualified} {alias} ON {condition}")
        elif joins and isinstance(joins[0], dict):
            # Use enhanced joins with conditions (fallback if no join_path)
            logger.info(f"Building joins from joins list: {joins}")
            for join in joins:
                to_table = join.get('to_table', '')
                from_table = join.get('from_table', '')
                join_type = join.get('type', 'LEFT')
                condition = join.get('condition', '')
                
                if to_table and condition:
                    # Normalize condition to use aliases
                    condition = self._normalize_join_condition(condition, from_table, to_table)
                    alias = to_table[0].lower() if to_table else ''
                    # Use fully qualified table name for Trino compatibility
                    to_table_qualified = to_table if '.' in to_table else f"tpch.tiny.{to_table}"
                    join_clauses.append(f"{join_type} JOIN\n    {to_table_qualified} {alias} ON {condition}")
        elif join_path:
            # Build joins from join_path
            # Get join conditions from join graph
            current_table = base_table
            for t1, t2 in join_path:
                # Try to get join condition from query plan
                condition = None
                if joins and isinstance(joins[0], dict):
                    for j in joins:
                        if j.get('from_table') == t1 and j.get('to_table') == t2:
                            condition = j.get('condition', '')
                            break
                
                if not condition:
                    # Fallback: construct from table names (this shouldn't happen if join graph is loaded)
                    logger.warning(f"No join condition found for {t1} -> {t2}, using fallback")
                    condition = f"{t1[0]}.{t1}_id = {t2[0]}.{t1}_id"
                
                alias = t2[0] if t2 else ''
                join_clauses.append(f"LEFT JOIN\n    {t2} {alias} ON {condition}")
                current_table = t2
        
        # Build GROUP BY clause
        group_by_clause = ""
        if group_by:
            gb_cols = []
            for gb_col in group_by:
                if '.' in gb_col:
                    # Normalize to use alias
                    parts = gb_col.split('.')
                    if len(parts) == 2:
                        table_part, col_part = parts
                        table_short = table_part.split('.')[-1].lower()
                        alias = table_short[0] if table_short else ''
                        gb_cols.append(f"{alias}.{col_part}")
                    else:
                        gb_cols.append(gb_col)
                else:
                    gb_cols.append(f"{base_alias}.{gb_col}")
            group_by_clause = f"GROUP BY\n    {', '.join(gb_cols)}"
        
        # Combine all clauses
        sql_parts = [select_clause, from_clause]
        sql_parts.extend(join_clauses)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        
        sql = "\n".join(sql_parts)
        
        logger.info(f"Built deterministic SQL with {len(join_clauses)} joins")
        return sql
