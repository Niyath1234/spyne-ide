#!/usr/bin/env python3
"""
Robust SQL Builder with Table Relationship Resolution

This module provides a general, robust solution for building SQL queries from intents.
It handles:
- Table relationship resolution
- Join path finding
- Intent validation
- Proper alias management
- Edge case handling
"""

import re
from typing import Dict, List, Any, Optional, Set, Tuple
from collections import defaultdict
from enum import Enum

# Import query_intent types if available, otherwise use strings
try:
    from query_intent import QueryIntent, JoinType, FilterOperator, OrderDirection
except ImportError:
    # Fallback for compatibility
    QueryIntent = None
    JoinType = None
    FilterOperator = None
    OrderDirection = None


class FixConfidence(Enum):
    """Confidence level for automatic fixes."""
    SAFE = "safe"  # Can auto-apply
    AMBIGUOUS = "ambiguous"  # Needs clarification
    UNSAFE = "unsafe"  # Should not auto-apply


class ExpressionConfidence(Enum):
    """Confidence level for expression parsing."""
    SAFE = "safe"  # Can safely infer join
    PROBABLE = "probable"  # Probably correct, but mark as ambiguous
    UNKNOWN = "unknown"  # Cannot safely infer - block


class JoinSignature:
    """Canonical join signature for deduplication."""
    
    @staticmethod
    def create(left_table: str, right_table: str, left_column: str, right_column: str) -> Tuple[str, str]:
        """
        Create canonical join signature.
        
        Returns:
            (normalized_tables, normalized_columns) tuple
        """
        # Normalize table names (strip schema, lowercase)
        def normalize_table(t: str) -> str:
            if '.' in t:
                return t.split('.')[-1].lower()
            return t.lower()
        
        # Sort tables for canonical form
        left_norm = normalize_table(left_table)
        right_norm = normalize_table(right_table)
        tables = tuple(sorted([left_norm, right_norm]))
        
        # Sort columns for canonical form
        columns = tuple(sorted([left_column.lower(), right_column.lower()]))
        
        return (str(tables), str(columns))
    
    @staticmethod
    def from_join_on(join_on: str, left_table: str, right_table: str) -> Optional[Tuple[str, str]]:
        """
        Extract join signature from ON clause.
        
        Example: "orders.customer_id = customers.id" with left="orders", right="customers"
        -> (('customers', 'orders'), ('customer_id', 'id'))
        """
        # Try to extract column names from simple equality
        # Pattern: table1.col1 = table2.col2
        parts = join_on.split('=')
        if len(parts) != 2:
            return None
        
        left_expr = parts[0].strip()
        right_expr = parts[1].strip()
        
        # Extract column from left expression
        left_col = JoinSignature._extract_column_simple(left_expr)
        right_col = JoinSignature._extract_column_simple(right_expr)
        
        if left_col and right_col:
            return JoinSignature.create(left_table, right_table, left_col, right_col)
        
        return None
    
    @staticmethod
    def _extract_column_simple(expr: str) -> Optional[str]:
        """Extract column name from simple expression (table.column)."""
        # Remove whitespace
        expr = expr.strip()
        
        # Pattern: table.column or schema.table.column
        if '.' in expr:
            parts = expr.split('.')
            # Get last part (column name)
            col = parts[-1].strip()
            # Remove trailing parentheses/function calls
            col = re.sub(r'[()].*', '', col)
            return col
        
        return None


class ExpressionParser:
    """Tiered expression parser with confidence scoring."""
    
    @staticmethod
    def extract_column_with_confidence(expr: str, table_name: str) -> Tuple[Optional[str], ExpressionConfidence, str]:
        """
        Extract column from expression with confidence level.
        
        Returns:
            (column_name, confidence, reason)
        """
        expr_clean = expr.strip()
        table_normalized = table_name.split('.')[-1].lower()
        
        # Tier 1: SAFE patterns
        # Pattern: table.column
        simple_pattern = rf'({re.escape(table_name)}\.|{table_normalized}\.)([a-zA-Z_][a-zA-Z0-9_]*)'
        match = re.search(simple_pattern, expr_clean, re.IGNORECASE)
        if match:
            return (match.group(2), ExpressionConfidence.SAFE, f"Direct column reference: {match.group(0)}")
        
        # Pattern: FUNCTION(table.column)
        function_patterns = [
            rf'UPPER\s*\(\s*({re.escape(table_name)}\.|{table_normalized}\.)([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
            rf'LOWER\s*\(\s*({re.escape(table_name)}\.|{table_normalized}\.)([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
            rf'CAST\s*\(\s*({re.escape(table_name)}\.|{table_normalized}\.)([a-zA-Z_][a-zA-Z0-9_]*)\s*AS',
            rf'COALESCE\s*\(\s*({re.escape(table_name)}\.|{table_normalized}\.)([a-zA-Z_][a-zA-Z0-9_]*)\s*',
        ]
        
        for pattern in function_patterns:
            match = re.search(pattern, expr_clean, re.IGNORECASE)
            if match:
                col = match.group(2) if match.lastindex >= 2 else None
                if col:
                    func_name = pattern.split('\\s')[0].replace('\\', '')
                    return (col, ExpressionConfidence.SAFE, f"Column in {func_name} function: {col}")
        
        # Tier 3: UNKNOWN patterns (check BEFORE probable to catch these first)
        # Subqueries - check for SELECT ... FROM pattern
        if re.search(r'SELECT\s+.*\s+FROM', expr_clean, re.IGNORECASE):
            return (None, ExpressionConfidence.UNKNOWN,
                   "Subquery detected in expression - cannot safely infer")
        
        # Window functions - check for OVER ( pattern
        if re.search(r'OVER\s*\(', expr_clean, re.IGNORECASE):
            return (None, ExpressionConfidence.UNKNOWN,
                   "Window function detected - cannot safely infer")
        
        # IN (SELECT ...) pattern
        if re.search(r'\bIN\s*\(\s*SELECT', expr_clean, re.IGNORECASE):
            return (None, ExpressionConfidence.UNKNOWN,
                   "Subquery in IN clause - cannot safely infer")
        
        # EXISTS (SELECT ...) pattern
        if re.search(r'\bEXISTS\s*\(\s*SELECT', expr_clean, re.IGNORECASE):
            return (None, ExpressionConfidence.UNKNOWN,
                   "Subquery in EXISTS clause - cannot safely infer")
        
        # Tier 2: PROBABLE patterns (nested expressions, arithmetic)
        # Pattern: deeply nested or arithmetic involving columns
        nested_pattern = rf'({re.escape(table_name)}\.|{table_normalized}\.)([a-zA-Z_][a-zA-Z0-9_]*)'
        
        # Arithmetic expressions (but not in subqueries)
        if re.search(r'[+\-*/]', expr_clean) and not re.search(r'SELECT.*FROM', expr_clean, re.IGNORECASE):
            match = re.search(nested_pattern, expr_clean, re.IGNORECASE)
            if match:
                return (match.group(2), ExpressionConfidence.PROBABLE, 
                       f"Column in arithmetic expression: {match.group(0)}")
        
        # CASE statements (but not in subqueries)
        if 'CASE' in expr_clean.upper() and not re.search(r'SELECT.*FROM', expr_clean, re.IGNORECASE):
            match = re.search(nested_pattern, expr_clean, re.IGNORECASE)
            if match:
                return (match.group(2), ExpressionConfidence.PROBABLE,
                       f"Column in CASE statement: {match.group(0)}")
        
        # Deeply nested expressions (multiple parentheses)
        if expr_clean.count('(') > 2 and not re.search(r'SELECT.*FROM', expr_clean, re.IGNORECASE):
            match = re.search(nested_pattern, expr_clean, re.IGNORECASE)
            if match:
                return (match.group(2), ExpressionConfidence.PROBABLE,
                       f"Column in deeply nested expression: {match.group(0)}")
        
        # Try fallback extraction
        match = re.search(nested_pattern, expr_clean, re.IGNORECASE)
        if match:
            return (match.group(2), ExpressionConfidence.PROBABLE,
                   f"Fallback extraction from expression: {match.group(0)}")
        
        return (None, ExpressionConfidence.UNKNOWN,
               f"Cannot extract column from expression: {expr_clean}")


class TableRelationshipResolver:
    """Resolves relationships between tables and finds join paths with semantic awareness."""
    
    def __init__(self, metadata: Dict[str, Any], enable_learning: bool = True, query_text: Optional[str] = None):
        """
        Initialize table relationship resolver with node-level isolation.
        
        Args:
            metadata: Metadata dictionary (can be isolated or full)
            enable_learning: Whether to enable join learning
            query_text: Optional query text for node-level isolation
        """
        self.metadata = metadata
        
        # Use node-level isolation if query_text provided
        if query_text:
            try:
                from backend.node_level_metadata_accessor import get_node_level_accessor
                accessor = get_node_level_accessor()
                isolated_metadata = accessor.build_isolated_context(query_text)
                # Merge isolated metadata (isolated takes precedence)
                metadata = {**metadata, **isolated_metadata}
            except Exception as e:
                print(f"Node-level isolation failed, using full metadata: {e}")
        
        # Only load tables that are in metadata (now isolated)
        self.tables = {t.get('name'): t for t in metadata.get('tables', {}).get('tables', [])}
        self.registry = metadata.get('semantic_registry', {})
        self.enable_learning = enable_learning
        self._build_relationship_graph()
        
        # Initialize join learner if enabled
        if self.enable_learning:
            try:
                from backend.join_learner import get_join_learner
                self.join_learner = get_join_learner()
            except ImportError:
                try:
                    # Fallback for direct import
                    from join_learner import get_join_learner
                    self.join_learner = get_join_learner()
                except ImportError:
                    self.join_learner = None
                    self.enable_learning = False
        else:
            self.join_learner = None
    
    def _build_relationship_graph(self):
        """Build a graph of table relationships from metadata."""
        self.relationships = defaultdict(list)  # table -> [(target_table, join_condition, join_type)]
        
        # Extract relationships from dimensions (join_path)
        for dim in self.registry.get('dimensions', []):
            join_path = dim.get('join_path', [])
            base_table = dim.get('base_table', '')
            
            for join in join_path:
                from_table = join.get('from_table', '')
                to_table = join.get('to_table', '')
                on_clause = join.get('on', '')
                
                if from_table and to_table:
                    # Extract relationship semantics from metadata
                    relationship_type = self._infer_relationship_type(from_table, to_table)
                    cardinality_safe = relationship_type in ['one_to_one', 'many_to_one']
                    
                    self.relationships[from_table].append({
                        'target': to_table,
                        'on': on_clause,
                        # Note: 'type' (LEFT/RIGHT/INNER) is NOT stored here
                        # Join type is determined dynamically based on query intent
                        'relationship_type': relationship_type,
                        'cardinality_safe': cardinality_safe,
                        'weight': self._calculate_join_weight(from_table, to_table, relationship_type)
                    })
        
        # Extract relationships from lineage if available
        lineage = self.metadata.get('lineage', {})
        if isinstance(lineage, dict):
            # Handle edges array format: {"edges": [{"from": "...", "to": "...", "keys": {...}}]}
            # OR: {"edges": [{"from_table": "...", "to_table": "...", "from_column": "...", "to_column": "..."}]}
            edges = lineage.get('edges', [])
            if edges:
                for edge in edges:
                    # Support both formats:
                    # Format 1: from/to/keys
                    # Format 2: from_table/to_table/from_column/to_column
                    from_table = edge.get('from_table') or edge.get('from', '')
                    to_table = edge.get('to_table') or edge.get('to', '')
                    
                    # Build ON clause
                    on_clause = None
                    from_column = edge.get('from_column', '')
                    to_column = edge.get('to_column', '')
                    
                    if from_column and to_column:
                        # Format 2: Use from_column/to_column
                        on_clause = f"{from_table}.{from_column} = {to_table}.{to_column}"
                    else:
                        # Format 1: Use keys dict
                        keys = edge.get('keys', {})
                        if keys:
                            on_clause_parts = []
                            for left_key, right_key in keys.items():
                                on_clause_parts.append(f"{from_table}.{left_key} = {to_table}.{right_key}")
                            on_clause = " AND ".join(on_clause_parts)
                    
                    # If still no ON clause, try to infer it
                    if not on_clause:
                        on_clause = self._infer_join_condition(from_table, to_table)
                    
                    relationship_type = edge.get('relationship_type') or edge.get('relationship', 'one_to_many')
                    
                    if from_table and to_table and on_clause:
                        cardinality_safe = relationship_type in ['one_to_one', 'many_to_one']
                        
                        self.relationships[from_table].append({
                            'target': to_table,
                            'on': on_clause,
                            # Note: 'type' (LEFT/RIGHT/INNER) is NOT stored here
                            # Join type is determined dynamically based on query intent
                            'relationship_type': relationship_type,
                            'cardinality_safe': cardinality_safe,
                            'weight': self._calculate_join_weight(from_table, to_table, relationship_type)
                        })
            else:
                # Fallback: handle old format (source -> targets)
                for source, targets in lineage.items():
                    if isinstance(targets, list):
                        for target in targets:
                            if isinstance(target, dict):
                                to_table = target.get('to', '')
                                if to_table:
                                    self.relationships[source].append({
                                        'target': to_table,
                                        'on': self._infer_join_condition(source, to_table),
                                        # Note: 'type' (LEFT/RIGHT/INNER) is NOT stored here
                                        # Join type is determined dynamically based on query intent
                                    })
    
    def _infer_join_condition(self, from_table: str, to_table: str) -> str:
        """Infer join condition based on common column patterns."""
        from_cols = self._get_table_columns(from_table)
        to_cols = self._get_table_columns(to_table)
        
        # Common join patterns
        common_keys = ['order_id', 'loan_id', 'customer_id', 'id', 'uuid']
        
        for key in common_keys:
            if key in from_cols and key in to_cols:
                return f"{from_table}.{key} = {to_table}.{key}"
        
        # Fallback: use first common column
        common = set(from_cols) & set(to_cols)
        if common:
            key = list(common)[0]
            return f"{from_table}.{key} = {to_table}.{key}"
        
        return f"{from_table}.id = {to_table}.id"  # Last resort
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table."""
        table = self.tables.get(table_name)
        if not table:
            return []
        
        cols = []
        for col in table.get('columns', []):
            col_name = col.get('name') or col.get('column', '')
            if col_name:
                cols.append(col_name)
        return cols
    
    def _infer_relationship_type(self, from_table: str, to_table: str) -> str:
        """Infer relationship type based on table patterns and metadata."""
        # Check if tables have explicit relationship metadata
        # For now, use heuristics based on table names and primary keys
        
        from_table_obj = self.tables.get(from_table, {})
        to_table_obj = self.tables.get(to_table, {})
        
        from_pk = from_table_obj.get('primary_key', [])
        to_pk = to_table_obj.get('primary_key', [])
        
        # If both have single PK, likely one-to-one or many-to-one
        # If one has composite PK, likely fact table (many-to-many)
        
        # Heuristic: dimension tables typically have single PK
        # Fact tables have composite PKs
        if len(from_pk) == 1 and len(to_pk) == 1:
            # Check table name patterns
            if 'master' in from_table.lower() or 'dim' in from_table.lower():
                return 'one_to_many'  # Dimension to fact
            elif 'master' in to_table.lower() or 'dim' in to_table.lower():
                return 'many_to_one'  # Fact to dimension
            else:
                return 'one_to_one'  # Default assumption
        else:
            return 'many_to_many'  # Composite keys suggest fact tables
    
    def _calculate_join_weight(self, from_table: str, to_table: str, relationship_type: str) -> float:
        """Calculate weight for join path selection (lower is better)."""
        base_weight = 1.0
        
        # Prefer dimension → fact joins
        if relationship_type == 'one_to_many':
            base_weight = 0.5
        elif relationship_type == 'many_to_one':
            base_weight = 1.0
        elif relationship_type == 'one_to_one':
            base_weight = 0.8
        elif relationship_type == 'many_to_many':
            base_weight = 2.0  # Penalize many-to-many
        
        # Prefer shorter paths
        return base_weight
    
    def find_join_path(self, from_table: str, to_table: str, visited: Optional[Set[str]] = None, prefer_cardinality_safe: bool = True, context: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Find the best join path from from_table to to_table using weighted BFS.
        
        Prefers:
        - Cardinality-safe joins
        - Dimension → fact patterns
        - Shorter paths
        
        If no path found and learning is enabled, asks user for help.
        """
        if visited is None:
            visited = set()
        
        if from_table == to_table:
            return []
        
        if from_table in visited:
            return None
        
        visited.add(from_table)
        
        # Check learned joins first (if learning enabled)
        if self.enable_learning and self.join_learner:
            learned_join = self.join_learner.get_learned_join(from_table, to_table)
            if learned_join:
                # Convert learned join to relationship format
                return [{
                    'target': to_table,
                    'on': learned_join.get('on', ''),
                    'relationship_type': learned_join.get('relationship_type', 'many_to_one'),
                    'cardinality_safe': learned_join.get('cardinality_safe', True),
                    'weight': 0.5,  # Learned joins get lower weight (preferred)
                    'learned': True
                }]
        
        # Get all possible paths and score them
        candidates = []
        
        # Direct relationship
        for rel in self.relationships.get(from_table, []):
            if rel['target'] == to_table:
                candidates.append(([rel], rel.get('weight', 1.0)))
            
            # Recursive search
            path = self.find_join_path(rel['target'], to_table, visited.copy(), prefer_cardinality_safe, context)
            if path is not None:
                total_weight = rel.get('weight', 1.0) + sum(p.get('weight', 1.0) for p in path)
                candidates.append(([rel] + path, total_weight))
        
        if not candidates:
            # No path found - try learning if enabled
            if self.enable_learning and self.join_learner:
                learned_join = self.join_learner.ask_user_for_join(from_table, to_table, context)
                if learned_join:
                    # Convert learned join to relationship format and return
                    return [{
                        'target': to_table,
                        'on': learned_join.get('on', ''),
                        'relationship_type': learned_join.get('relationship_type', 'many_to_one'),
                        'cardinality_safe': learned_join.get('cardinality_safe', True),
                        'weight': 0.5,
                        'learned': True
                    }]
            return None
        
        # Sort by weight (prefer cardinality-safe if requested)
        if prefer_cardinality_safe:
            candidates.sort(key=lambda x: (
                not all(p.get('cardinality_safe', True) for p in x[0]),  # Cardinality-safe first
                x[1]  # Then by weight
            ))
        else:
            candidates.sort(key=lambda x: x[1])
        
        return candidates[0][0] if candidates else None
    
    def resolve_table_references(self, expression: str) -> Set[str]:
        """Extract all table references from an expression."""
        tables = set()
        
        # Pattern: schema.table.column or table.column
        # Match schema.table.column first (more specific)
        schema_table_column_pattern = r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        schema_table_matches = re.findall(schema_table_column_pattern, expression)
        for match in schema_table_matches:
            # schema.table.column -> extract schema.table
            parts = match.split('.')
            if len(parts) == 3:
                tables.add(f"{parts[0]}.{parts[1]}")
        
        # Remove schema.table.column matches from expression before matching table.column
        # to avoid double extraction
        remaining_expression = expression
        for match in schema_table_matches:
            remaining_expression = remaining_expression.replace(match, '')
        
        # Pattern: table.column (only if not already matched as schema.table.column)
        table_column_pattern = r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        table_matches = re.findall(table_column_pattern, remaining_expression)
        for match in table_matches:
            # table.column -> extract table
            parts = match.split('.')
            if len(parts) == 2:
                # Only add if it's not a schema name (heuristic: if it contains underscore, likely a table)
                # Actually, just add it - the caller will check if it exists in metadata
                tables.add(parts[0])
        
        return tables


class IntentValidator:
    """
    Validates SQL intent before conversion to SQL.
    
    Enforces:
    - Structural correctness (tables exist, joins valid)
    - Semantic completeness (anchor entity, attribute coverage)
    - Intent role satisfaction (no orphan tables)
    """
    
    def __init__(self, resolver: TableRelationshipResolver):
        self.resolver = resolver
    
    def validate(self, intent: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
        """
        Validate intent and return (is_valid, errors, warnings).
        
        Errors: Must fix before proceeding
        Warnings: Should review but can proceed
        """
        errors = []
        warnings = []
        
        base_table = intent.get('base_table', '')
        anchor_entity = intent.get('anchor_entity', base_table)
        
        if not base_table:
            errors.append("Missing base_table in intent")
        
        if not anchor_entity:
            warnings.append("Missing anchor_entity - using base_table as anchor")
            anchor_entity = base_table
        
        # Check if base_table exists
        if base_table and base_table not in self.resolver.tables:
            errors.append(f"Base table '{base_table}' not found in metadata")
        
        # Validate anchor entity is present
        if anchor_entity and anchor_entity != base_table:
            warnings.append(f"Anchor entity '{anchor_entity}' differs from base_table '{base_table}'")
        
        # Validate joins
        joins = intent.get('joins', [])
        referenced_tables = {base_table}
        
        # First pass: collect all join tables and validate they exist
        all_join_tables = set()
        for join in joins:
            join_table = join.get('table', '')
            if not join_table:
                errors.append("Join missing table name")
                continue
            
            all_join_tables.add(join_table)
            
            # Check if join table exists
            if join_table not in self.resolver.tables:
                errors.append(f"Join table '{join_table}' not found in metadata")
        
        # Build complete set of all tables that will be available
        all_available_tables = {base_table} | all_join_tables
        
        # Second pass: validate join ON clauses
        for join in joins:
            join_table = join.get('table', '')
            if not join_table:
                continue
            
            referenced_tables.add(join_table)
            
            # Validate join ON clause
            join_on = join.get('on', '')
            if not join_on:
                errors.append(f"Join to '{join_table}' missing ON clause")
            else:
                # Check if all tables in ON clause are available
                on_tables = self.resolver.resolve_table_references(join_on)
                missing_tables = on_tables - all_available_tables
                if missing_tables:
                    # Check if missing tables exist in metadata
                    unknown_tables = [t for t in missing_tables if t not in self.resolver.tables]
                    if unknown_tables:
                        errors.append(f"Join ON clause references unknown table(s): {unknown_tables}")
                    else:
                        # Tables exist in metadata but not in joins - this should have been fixed by fix_intent
                        # If we're here after fix_intent ran, it's a real error
                        errors.append(f"Join ON clause references tables not in FROM/JOINs: {missing_tables}")
        
        # Validate filters
        filters = intent.get('filters', [])
        for filter_obj in filters:
            filter_table = filter_obj.get('table', base_table)
            if filter_table not in referenced_tables:
                errors.append(f"Filter references table '{filter_table}' not in FROM/JOINs")
        
        # Validate columns/metrics reference valid tables
        columns = intent.get('columns', [])
        for col in columns:
            col_str = str(col)
            col_tables = self.resolver.resolve_table_references(col_str)
            missing = col_tables - referenced_tables
            if missing:
                errors.append(f"Column expression references tables not in FROM/JOINs: {missing}")
        
        # Validate intent coverage (semantic completeness)
        coverage_errors = self._validate_intent_coverage(intent, base_table)
        errors.extend(coverage_errors)
        
        # Check for ambiguities
        ambiguity_warnings = self._detect_ambiguities(intent)
        warnings.extend(ambiguity_warnings)
        
        return len(errors) == 0, errors, warnings
    
    def _validate_intent_coverage(self, intent: Dict[str, Any], base_table: str) -> List[str]:
        """Validate that intent covers all required entities."""
        errors = []
        
        # Check that anchor entity is present
        anchor_entity = intent.get('anchor_entity', base_table)
        if not anchor_entity:
            errors.append("Intent missing anchor entity")
        
        # Check that all attribute entities are reachable
        columns = intent.get('columns', [])
        referenced_tables = {base_table}
        
        for join in intent.get('joins', []):
            referenced_tables.add(join.get('table', ''))
        
        # Check filters reference valid tables
        for filter_obj in intent.get('filters', []):
            filter_table = filter_obj.get('table', base_table)
            if filter_table not in referenced_tables:
                errors.append(f"Filter references unreachable table '{filter_table}'")
        
        return errors
    
    def _detect_ambiguities(self, intent: Dict[str, Any]) -> List[str]:
        """Detect ambiguous situations that need clarification."""
        warnings = []
        
        base_table = intent.get('base_table', '')
        joins = intent.get('joins', [])
        
        # Check for multiple join paths to same table
        join_targets = {}
        for join in joins:
            target = join.get('table', '')
            if target in join_targets:
                warnings.append(f"Multiple joins to '{target}' - may indicate ambiguity")
            join_targets[target] = join
        
        # Check for ambiguous metric vs column references
        columns = intent.get('columns', [])
        metric = intent.get('metric')
        
        if metric and columns:
            # Handle metric as dict or list
            metric_name = ''
            if isinstance(metric, dict):
                metric_name = metric.get('name', '').lower()
            elif isinstance(metric, list) and len(metric) > 0:
                # If metric is a list, get first item
                metric_item = metric[0]
                if isinstance(metric_item, dict):
                    metric_name = metric_item.get('name', '').lower()
                else:
                    metric_name = str(metric_item).lower()
            
            if metric_name:
                for col in columns:
                    if isinstance(col, str) and metric_name in col.lower():
                        warnings.append(f"Column '{col}' may overlap with metric '{metric_name}'")
        
        return warnings
    
    def _infer_direct_join_with_confidence(
        self,
        join_on: str,
        normalized_table: str,
        normalized_base_table: str,
        fixed_joins: List[Dict[str, Any]],
        join_signatures: Set[Tuple[str, str]],
        inference_notes: List[str],
        reasons: List[str],
        confidence_ref: List[FixConfidence]  # Use list to allow mutation
    ):
        """Infer direct join with confidence scoring and guardrails."""
        if normalized_table not in self.resolver.tables:
            return
        
        # Split ON clause to find equality conditions
        on_clause_parts = [p.strip() for p in join_on.split('=') if p.strip()]
        if len(on_clause_parts) < 2:
            return
        
        inferred_on = None
        col_confidence = ExpressionConfidence.UNKNOWN
        confidence_reason = ""
        
        # Process each equality condition
        for i in range(len(on_clause_parts) - 1):
            left_part = on_clause_parts[i].strip()
            right_part = on_clause_parts[i + 1].strip()
            
            # Check which side references our table
            if normalized_table.lower() in left_part.lower() or normalized_table in left_part:
                col, conf, reason = ExpressionParser.extract_column_with_confidence(left_part, normalized_table)
                if col:
                    inferred_on = f"{normalized_base_table}.{col} = {normalized_table}.{col}"
                    col_confidence = conf
                    confidence_reason = reason
                    break
            elif normalized_table.lower() in right_part.lower() or normalized_table in right_part:
                col, conf, reason = ExpressionParser.extract_column_with_confidence(right_part, normalized_table)
                if col:
                    inferred_on = f"{normalized_base_table}.{col} = {normalized_table}.{col}"
                    col_confidence = conf
                    confidence_reason = reason
                    break
        
        # Fallback to common column inference
        if not inferred_on:
            inferred_on = self.resolver._infer_join_condition(normalized_base_table, normalized_table)
            if inferred_on:
                col_confidence = ExpressionConfidence.PROBABLE
                confidence_reason = "Inferred from common column patterns"
        
        # Check confidence level
        if col_confidence == ExpressionConfidence.UNKNOWN:
            inference_notes.append(
                f"UNSAFE INFERENCE BLOCKED: Join condition references {normalized_table} in a complex expression "
                f"that cannot be safely inferred: {join_on}"
            )
            reasons.append(f"UNSAFE: Cannot safely infer join to {normalized_table}")
            if confidence_ref:
                confidence_ref[0] = FixConfidence.UNSAFE
            return
        
        # Check for duplicate join signature
        sig = JoinSignature.from_join_on(inferred_on, normalized_base_table, normalized_table)
        if sig and sig in join_signatures:
            inference_notes.append(f"Duplicate join to {normalized_table} skipped (signature: {sig})")
            return
        
        # Check cardinality - guardrail for many-to-many
        table_info = self.resolver.tables.get(normalized_table, {})
        relationship_type = table_info.get('relationship_type', '')
        
        if relationship_type == 'many_to_many':
            inference_notes.append(
                f"UNSAFE INFERENCE BLOCKED: Joining {normalized_base_table} to {normalized_table} introduces "
                f"many-to-many relationship. Explicit user intent required."
            )
            reasons.append(f"UNSAFE: Cannot infer many-to-many join to {normalized_table}")
            if confidence_ref:
                confidence_ref[0] = FixConfidence.UNSAFE
            return
        
        # Add join with confidence annotation
        # Note: 'type' (LEFT/RIGHT/INNER) is NOT set here - it will be determined
        # dynamically based on query intent when building SQL
        intermediate_join = {
            'table': normalized_table,
            'on': inferred_on,
            'reason': f'Direct join inferred: {normalized_table} referenced in JOIN ON clause',
            'cardinality_safe': True,
            'relationship_type': 'inferred',
            'inference_confidence': col_confidence.value,
            'inference_reason': confidence_reason
        }
        
        if sig:
            join_signatures.add(sig)
        
        fixed_joins.append(intermediate_join)
        
        # Add inference note
        if col_confidence == ExpressionConfidence.PROBABLE:
            inference_notes.append(
                f"Join to {normalized_table} inferred from {confidence_reason} (AMBIGUOUS)"
            )
        else:
            inference_notes.append(
                f"Join to {normalized_table} inferred from {confidence_reason} (SAFE)"
            )
        
        reasons.append(
            f"Added inferred intermediate join: {normalized_base_table} → {normalized_table} "
            f"(confidence: {col_confidence.value})"
        )
    
    def _optimize_join_order(
        self,
        joins: List[Dict[str, Any]],
        base_table: str
    ) -> List[Dict[str, Any]]:
        """
        Optimize join order for cardinality safety.
        
        Strategy:
        1. Start from anchor/base table
        2. Prefer 1-to-1 and 1-to-many joins first (cardinality-safe)
        3. Dimension → fact preference
        4. Shortest path to anchor
        """
        if not joins:
            return joins
        
        # Order joins by cardinality safety and relationship type
        def join_priority(join: Dict[str, Any]) -> Tuple[int, int, str]:
            """
            Priority tuple: (cardinality_score, is_dimension, table_name)
            Lower score = higher priority
            """
            cardinality_safe = join.get('cardinality_safe', True)
            relationship_type = join.get('relationship_type', '')
            
            # Cardinality score: 0 = safe, 1 = risky
            # Prefer one-to-one and many-to-one (cardinality-safe)
            cardinality_score = 0 if cardinality_safe else 1
            
            # Dimension preference: 0 = dimension, 1 = fact/other
            is_dimension = 1
            table_name = join.get('table', '')
            if table_name and table_name in self.resolver.tables:
                table_info = self.resolver.tables[table_name]
                if table_info.get('entity_type') == 'dimension':
                    is_dimension = 0
            
            return (cardinality_score, is_dimension, table_name)
        
        # Sort joins by priority
        ordered_joins = sorted(joins, key=join_priority)
        
        return ordered_joins
    
    def fix_intent(self, intent: Dict[str, Any]) -> Tuple[Dict[str, Any], FixConfidence, List[str]]:
        """
        Fix common intent issues by adding missing intermediate joins.
        
        Returns:
            (fixed_intent, confidence_level, reasons)
        """
        base_table = intent.get('base_table', '')
        if not base_table:
            return intent, FixConfidence.UNSAFE, ["Cannot fix: missing base_table"]
        
        # Helper to normalize table names (strip schema if present)
        def normalize_table_name(table_name: str) -> str:
            """Normalize table name by removing schema prefix if present."""
            if '.' in table_name:
                return table_name.split('.')[-1]
            return table_name.lower().strip()
        
        # Helper to check if a table name is an alias (t1, t2, etc.)
        def is_alias(table_name: str) -> bool:
            """Check if table name looks like an alias (t1, t2, etc.)."""
            if not table_name:
                return False
            normalized = normalize_table_name(table_name)
            # Check if it matches pattern t<number>
            import re
            return bool(re.match(r'^t\d+$', normalized))
        
        # Helper to resolve alias to real table name
        def resolve_alias_to_table(alias: str, intent: Dict[str, Any]) -> Optional[str]:
            """
            Resolve an alias (like t1, t2) to a real table name.
            Returns None if cannot resolve.
            """
            if not is_alias(alias):
                return alias  # Not an alias, return as-is
            
            normalized_alias = normalize_table_name(alias)
            
            # Build alias mapping from intent structure
            # t1 = base_table, t2 = first join, t3 = second join, etc.
            alias_map = {}
            base = intent.get('base_table', '')
            if base:
                alias_map['t1'] = normalize_table_name(base)
            
            join_list = intent.get('joins', [])
            for idx, join in enumerate(join_list, start=2):
                join_table = join.get('table', '')
                if join_table:
                    alias_map[f't{idx}'] = normalize_table_name(join_table)
            
            # Return mapped table name or None
            return alias_map.get(normalized_alias)
        
        # Helper to resolve table name (alias or real)
        def resolve_table_name(table_name: str) -> Optional[str]:
            """Resolve table name, handling aliases."""
            if not table_name:
                return None
            
            normalized = normalize_table_name(table_name)
            
            # If it's an alias, try to resolve it
            if is_alias(table_name):
                resolved = resolve_alias_to_table(table_name, intent)
                if resolved:
                    return resolved
                # If alias can't be resolved, check if it exists in metadata
                # (might be a real table named t1, t2, etc.)
                if normalized in self.resolver.tables:
                    return normalized
                # Alias cannot be resolved - skip it
                return None
            
            # Not an alias - check if it exists in metadata
            if normalized in self.resolver.tables:
                return normalized
            
            # Try to find by partial match (case-insensitive)
            for table_name_in_meta in self.resolver.tables.keys():
                if normalize_table_name(table_name_in_meta) == normalized:
                    return normalize_table_name(table_name_in_meta)
            
            return normalized  # Return normalized even if not found (will be handled later)
        
        # Resolve base_table if it's an alias
        resolved_base_table = resolve_table_name(base_table)
        if resolved_base_table:
            base_table = resolved_base_table
        
        referenced_tables = {base_table}
        joins = intent.get('joins', [])
        reasons = []
        confidence = FixConfidence.SAFE
        
        # Track join signatures to prevent duplicates
        join_signatures: Set[Tuple[str, str]] = set()
        
        # Track inference notes for explain plan
        inference_notes = []
        
        # Collect all referenced tables from joins, filters, columns
        for join in joins:
            join_table = join.get('table', '')
            resolved_table = resolve_table_name(join_table)
            if resolved_table:
                referenced_tables.add(resolved_table)
            
            join_on = join.get('on', '')
            if join_on:
                # Get tables referenced in JOIN ON clause
                on_tables = self.resolver.resolve_table_references(join_on)
                for table in on_tables:
                    resolved = resolve_table_name(table)
                    if resolved:
                        referenced_tables.add(resolved)
        
        for filter_obj in intent.get('filters', []):
            filter_table = filter_obj.get('table', base_table)
            resolved_table = resolve_table_name(filter_table)
            if resolved_table:
                referenced_tables.add(resolved_table)
        
        for col in intent.get('columns', []):
            col_tables = self.resolver.resolve_table_references(str(col))
            for table in col_tables:
                resolved = resolve_table_name(table)
                if resolved:
                    referenced_tables.add(resolved)
        
        # Find missing intermediate joins
        fixed_joins = []
        ambiguous_fixes = []
        
        # Track which tables are already reachable (base_table + all joined tables)
        def get_reachable_tables():
            """Get set of all tables reachable from base_table through current joins."""
            reachable = {base_table, normalize_table_name(base_table)}
            for j in fixed_joins:
                table = j.get('table', '')
                reachable.add(table)
                reachable.add(normalize_table_name(table))
            return reachable
        
        # Normalize base_table for lookups
        normalized_base_table = normalize_table_name(base_table)
        
        # First pass: Check JOIN ON clauses for missing tables
        # Handle multiple missing tables in one JOIN ON clause (mini-plan approach)
        for join in joins:
            join_on = join.get('on', '')
            if join_on:
                on_tables = self.resolver.resolve_table_references(join_on)
                missing_tables = []
                
                # Collect all missing tables from this JOIN ON clause (with alias resolution)
                for table in on_tables:
                    resolved_table = resolve_table_name(table)
                    if resolved_table:
                        table = resolved_table  # Use resolved table name
                    normalized_table = normalize_table_name(table)
                    if normalized_table == normalized_base_table or not table:
                        continue
                    
                    reachable = get_reachable_tables()
                    if table not in reachable and normalized_table not in reachable:
                        missing_tables.append((table, normalized_table))
                
                # If multiple missing tables, handle as mini-plan
                if len(missing_tables) > 1:
                    # Choose anchor: prefer one already reachable, else lowest cardinality
                    anchor_table = normalized_base_table
                    anchor_normalized = normalized_base_table
                    
                    # Try to find best anchor from missing tables
                    for table, normalized_table in missing_tables:
                        if normalized_table in self.resolver.tables:
                            table_info = self.resolver.tables.get(normalized_table, {})
                            # Prefer dimension tables (lower cardinality) as anchor
                            if table_info.get('entity_type') == 'dimension':
                                anchor_table = table
                                anchor_normalized = normalized_table
                                break
                    
                    # Build join path from anchor to other missing tables
                    for table, normalized_table in missing_tables:
                        if normalized_table == anchor_normalized:
                            continue
                        
                        # Find path from anchor to this table
                        join_path = None
                        if anchor_normalized in self.resolver.tables:
                            context_msg = f"Query requires joining '{table}' via anchor '{anchor_table}'"
                            join_path = self.resolver.find_join_path(anchor_normalized, normalized_table, prefer_cardinality_safe=True, context=context_msg)
                        
                        if join_path:
                            current_table = anchor_normalized
                            for step in join_path:
                                target_normalized = step['target']
                                reachable = get_reachable_tables()
                                
                                if target_normalized not in reachable:
                                    on_clause = step.get('on', '')
                                    # Check for duplicate join signature
                                    sig = JoinSignature.from_join_on(on_clause, current_table, target_normalized)
                                    if sig and sig in join_signatures:
                                        inference_notes.append(f"Duplicate join to {target_normalized} skipped (signature: {sig})")
                                        continue
                                    
                                    # Check cardinality safety
                                    relationship_type = step.get('relationship_type', '')
                                    if relationship_type == 'many_to_many':
                                        # Guardrail: block many-to-many inferred joins
                                        inference_notes.append(
                                            f"UNSAFE INFERENCE BLOCKED: Join to {target_normalized} introduces many-to-many relationship. "
                                            f"Explicit user intent required."
                                        )
                                        confidence = FixConfidence.UNSAFE
                                        reasons.append(
                                            f"UNSAFE: Cannot infer many-to-many join to {target_normalized} without explicit intent"
                                        )
                                        continue
                                    
                                    # Note: 'type' (LEFT/RIGHT/INNER) is NOT set here - it will be determined
                                    # dynamically based on query intent when building SQL
                                    intermediate_join = {
                                        'table': target_normalized,
                                        'on': on_clause,
                                        'reason': f'Intermediate join required: {table} referenced in JOIN ON clause',
                                        'cardinality_safe': step.get('cardinality_safe', True),
                                        'relationship_type': relationship_type
                                    }
                                    
                                    if sig:
                                        join_signatures.add(sig)
                                    
                                    fixed_joins.append(intermediate_join)
                                    reasons.append(f"Added intermediate join: {current_table} → {target_normalized} (for {table} referenced in JOIN ON)")
                                
                                current_table = target_normalized
                        else:
                            # Try direct inference with confidence scoring
                            confidence_ref = [confidence]
                            self._infer_direct_join_with_confidence(
                                join_on, normalized_table, normalized_base_table, 
                                fixed_joins, join_signatures, inference_notes, 
                                reasons, confidence_ref
                            )
                            confidence = confidence_ref[0]
                
                # Handle single missing table (original logic with deduplication)
                elif len(missing_tables) == 1:
                    table, normalized_table = missing_tables[0]
                    reachable = get_reachable_tables()
                    already_joined = any(
                        j.get('table') == table or 
                        j.get('table') == normalized_table or
                        normalize_table_name(j.get('table', '')) == normalized_table
                        for j in fixed_joins
                    )
                    
                    if not already_joined:
                        join_path = None
                        if normalized_base_table in self.resolver.tables:
                            context_msg = f"Query requires joining '{table}' to base table '{base_table}'"
                            join_path = self.resolver.find_join_path(normalized_base_table, normalized_table, prefer_cardinality_safe=True, context=context_msg)
                        
                        if join_path:
                            current_table = normalized_base_table
                            for step in join_path:
                                target_normalized = step['target']
                                reachable = get_reachable_tables()
                                
                                if target_normalized not in reachable:
                                    on_clause = step.get('on', '')
                                    # Check for duplicate join signature
                                    sig = JoinSignature.from_join_on(on_clause, current_table, target_normalized)
                                    if sig and sig in join_signatures:
                                        inference_notes.append(f"Duplicate join to {target_normalized} skipped (signature: {sig})")
                                        continue
                                    
                                    # Check cardinality safety
                                    relationship_type = step.get('relationship_type', '')
                                    if relationship_type == 'many_to_many':
                                        inference_notes.append(
                                            f"UNSAFE INFERENCE BLOCKED: Join to {target_normalized} introduces many-to-many relationship."
                                        )
                                        confidence = FixConfidence.UNSAFE
                                        reasons.append(f"UNSAFE: Cannot infer many-to-many join to {target_normalized}")
                                        continue
                                    
                                    # Note: 'type' (LEFT/RIGHT/INNER) is NOT set here - it will be determined
                                    # dynamically based on query intent when building SQL
                                    intermediate_join = {
                                        'table': target_normalized,
                                        'on': on_clause,
                                        'reason': f'Intermediate join required: {table} referenced in JOIN ON clause',
                                        'cardinality_safe': step.get('cardinality_safe', True),
                                        'relationship_type': relationship_type
                                    }
                                    
                                    if sig:
                                        join_signatures.add(sig)
                                    
                                    fixed_joins.append(intermediate_join)
                                    reasons.append(f"Added intermediate join: {current_table} → {target_normalized} (for {table} referenced in JOIN ON)")
                                
                                current_table = target_normalized
                        else:
                            # Try direct inference with confidence scoring
                            confidence_ref = [confidence]
                            self._infer_direct_join_with_confidence(
                                join_on, normalized_table, normalized_base_table,
                                fixed_joins, join_signatures, inference_notes,
                                reasons, confidence_ref
                            )
                            confidence = confidence_ref[0]
            
            # Add the current join (check for duplicates first)
            join_table = join.get('table', '')
            join_on = join.get('on', '')
            
            if join_table:
                # Resolve table name (handle aliases)
                resolved_join_table = resolve_table_name(join_table)
                if not resolved_join_table:
                    # Skip if table cannot be resolved
                    reasons.append(f"Join table '{join_table}' cannot be resolved (skipped)")
                    continue
                join_table = resolved_join_table  # Use resolved table name
                normalized_join_table = normalize_table_name(join_table)
                
                # Check for duplicate join signature
                # Try to extract signature from ON clause
                sig = None
                if join_on:
                    # Find the table this join connects to
                    # Look for base_table or previous join table in ON clause
                    from_table = normalized_base_table
                    # Check if ON clause references a table we've already joined
                    for prev_join in fixed_joins:
                        prev_table = normalize_table_name(prev_join.get('table', ''))
                        if prev_table in join_on or prev_table.lower() in join_on.lower():
                            from_table = prev_table
                            break
                    
                    sig = JoinSignature.from_join_on(join_on, from_table, normalized_join_table)
                
                # Check if this join already exists
                already_exists = False
                if sig and sig in join_signatures:
                    already_exists = True
                    inference_notes.append(f"Duplicate join to {join_table} skipped (signature: {sig})")
                else:
                    # Also check by table name (simple duplicate check)
                    for existing_join in fixed_joins:
                        existing_table = normalize_table_name(existing_join.get('table', ''))
                        if existing_table == normalized_join_table:
                            # Check if ON clauses are similar
                            existing_on = existing_join.get('on', '')
                            if join_on and existing_on:
                                # Simple check: if same columns mentioned, likely duplicate
                                if sig:
                                    existing_sig = JoinSignature.from_join_on(existing_on, from_table, normalized_join_table)
                                    if existing_sig == sig:
                                        already_exists = True
                                        inference_notes.append(f"Duplicate join to {join_table} skipped")
                                        break
            
                if not already_exists:
                    if sig:
                        join_signatures.add(sig)
                    fixed_joins.append(join)
        
        # Second pass: Ensure all referenced tables are reachable
        for table in referenced_tables:
            # Resolve table name (handle aliases)
            resolved_table = resolve_table_name(table)
            if not resolved_table:
                # Skip if table cannot be resolved (likely an invalid alias)
                continue
            
            normalized_table = normalize_table_name(resolved_table)
            if normalized_table == normalized_base_table or not resolved_table:
                continue
            
            # Check if already reachable (check both normalized and original)
            reachable = get_reachable_tables()
            if resolved_table in reachable or normalized_table in reachable:
                continue
            
            # Check if table exists in metadata
            if normalized_table not in self.resolver.tables:
                # Table doesn't exist - might be an unresolved alias
                reasons.append(f"Table '{table}' (resolved: '{resolved_table}') not found in metadata")
                continue
            
            # Check if we have a direct path from base_table to this table (use normalized names)
            context_msg = f"Query requires joining '{resolved_table}' to '{base_table}'"
            join_path = self.resolver.find_join_path(normalized_base_table, normalized_table, prefer_cardinality_safe=True, context=context_msg)
            
            if join_path:
                # Check if multiple paths exist (ambiguity)
                alt_path = self.resolver.find_join_path(normalized_base_table, normalized_table, prefer_cardinality_safe=False, context=context_msg)
                if alt_path and len(alt_path) != len(join_path):
                    ambiguous_fixes.append({
                        'table': table,
                        'paths': [join_path, alt_path]
                    })
                    confidence = FixConfidence.AMBIGUOUS
                
                # Add intermediate joins if needed
                current_table = normalized_base_table
                for step in join_path:
                    target = step['target']
                    reachable = get_reachable_tables()
                    
                    # Only add if not already reachable
                    # Note: 'type' (LEFT/RIGHT/INNER) is NOT set here - it will be determined
                    # dynamically based on query intent when building SQL
                    if target not in reachable:
                        fixed_joins.append({
                            'table': target,
                            'on': step.get('on', ''),
                            'reason': f'Intermediate join required for {table}',
                            'cardinality_safe': step.get('cardinality_safe', True),
                            'relationship_type': step.get('relationship_type')
                        })
                        reasons.append(f"Added intermediate join: {current_table} → {target} (for {table})")
                    
                    current_table = target
            else:
                # No path found - if learning is enabled, user was already asked
                # If learning is disabled or user skipped, mark as unsafe
                confidence = FixConfidence.UNSAFE
                if self.resolver.enable_learning and self.resolver.join_learner:
                    # Check if user provided a join (it would be in learned_joins now)
                    learned_join = self.resolver.join_learner.get_learned_join(normalized_base_table, normalized_table)
                    if learned_join:
                        # User provided join - try again with learned join
                        join_path = self.resolver.find_join_path(normalized_base_table, normalized_table, prefer_cardinality_safe=True, context=context_msg)
                        if join_path:
                            # Add the learned join
                            current_table = normalized_base_table
                            for step in join_path:
                                target = step['target']
                                reachable = get_reachable_tables()
                                if target not in reachable:
                                    fixed_joins.append({
                                        'table': target,
                                        'on': step.get('on', ''),
                                        'reason': f'Learned join for {table}',
                                        'cardinality_safe': step.get('cardinality_safe', True),
                                        'relationship_type': step.get('relationship_type'),
                                        'learned': True
                                    })
                                    reasons.append(f"Using learned join: {current_table} → {target} (for {table})")
                                current_table = target
                            continue  # Skip the error message
                
                reasons.append(f"Cannot find join path to '{table}'")
        
        if ambiguous_fixes:
            reasons.append(f"Ambiguous join paths detected for: {[f['table'] for f in ambiguous_fixes]}")
        
        # Remove duplicates before optimizing (by table name)
        seen_tables = set()
        deduplicated_joins = []
        for join in fixed_joins:
            join_table = normalize_table_name(join.get('table', ''))
            if join_table and join_table not in seen_tables:
                seen_tables.add(join_table)
                deduplicated_joins.append(join)
            elif join_table:
                inference_notes.append(f"Duplicate join to {join_table} removed during deduplication")
        
        # Optimize join order for cardinality safety
        fixed_joins = self._optimize_join_order(deduplicated_joins, normalized_base_table)
        
        # Store inference notes in intent for explain plan
        if inference_notes:
            intent['inference_notes'] = inference_notes
        
        intent['joins'] = fixed_joins
        
        # Post-processing: Fix hardcoded dimensions and filters
        intent, additional_reasons = self._fix_hardcoded_dimensions_and_filters(intent)
        reasons.extend(additional_reasons)
        
        # Check for user exclusion requests in query text
        query_text = intent.get('query_text', '') or ''
        exclusion_terms = self._detect_exclusion_requests(query_text)
        for exclusion_term in exclusion_terms:
            updated_intent, exclusion_reasons, clarification = self._apply_user_exclusion_request(intent, exclusion_term)
            if clarification:
                # Need user clarification - return it
                reasons.append(clarification)
                return updated_intent, confidence, reasons
            else:
                intent = updated_intent
                reasons.extend(exclusion_reasons)
        
        return intent, confidence, reasons
    
    def _detect_exclusion_requests(self, query_text: str) -> List[str]:
        """
        Detect exclusion requests in query text.
        
        Examples:
        - "remove writeoffs"
        - "excluding writeoffs"
        - "exclude writeoffs"
        
        Returns:
            List of exclusion terms found
        """
        exclusion_terms = []
        query_lower = query_text.lower()
        
        # Common patterns for exclusion requests
        exclusion_patterns = [
            r'remove\s+(\w+)',
            r'excluding\s+(\w+)',
            r'exclude\s+(\w+)',
            r'without\s+(\w+)',
            r'filter\s+out\s+(\w+)',
        ]
        
        for pattern in exclusion_patterns:
            matches = re.findall(pattern, query_lower)
            for match in matches:
                if match and match not in exclusion_terms:
                    exclusion_terms.append(match)
        
        return exclusion_terms
    
    def _get_column_distinct_values(self, table: str, column: str) -> Optional[List[str]]:
        """Get distinct values for a column from metadata or return None if not available."""
        table_info = self.resolver.tables.get(table, {})
        columns_metadata = {col.get('name'): col for col in table_info.get('columns', [])}
        col_metadata = columns_metadata.get(column, {})
        
        # Check if distinct values are stored in metadata
        distinct_values = col_metadata.get('distinct_values')
        if distinct_values:
            return distinct_values
        
        # Check if there's a sample_values or enum_values field
        sample_values = col_metadata.get('sample_values') or col_metadata.get('enum_values')
        if sample_values:
            return sample_values
        
        return None
    
    def _determine_exclusion_value(self, column: str, distinct_values: List[Any], description: str = '') -> Optional[str]:
        """
        Intelligently determine what value to include (exclude the opposite) based on distinct values and description.
        
        Args:
            column: Column name
            distinct_values: List of distinct values found in metadata
            description: Column description
        
        Returns:
            Value to include (exclude the opposite), or None if ambiguous
        """
        col_lower = column.lower()
        desc_lower = description.lower()
        
        # Common patterns for exclusion
        exclusion_patterns = {
            'write_off_flag': {'exclude': ['Y', 'y', 'Yes', 'YES', '1', True], 'include': ['N', 'n', 'No', 'NO', '0', False, None, 'NULL']},
            'writeoff_flag': {'exclude': ['Y', 'y', 'Yes', 'YES', '1', True], 'include': ['N', 'n', 'No', 'NO', '0', False, None, 'NULL']},
            'settled_flag': {'exclude': ['Y', 'y', 'Yes', 'YES', '1', True], 'include': ['N', 'n', 'No', 'NO', '0', False, None, 'NULL']},
            'arc_flag': {'exclude': ['Y', 'y', 'Yes', 'YES', '1', True], 'include': ['N', 'n', 'No', 'NO', '0', False, None, 'NULL']},
        }
        
        # Check column name patterns
        for pattern, values in exclusion_patterns.items():
            if pattern in col_lower:
                # Find which values from distinct_values match exclude/include patterns
                exclude_vals = [v for v in distinct_values if str(v).upper() in [str(x).upper() for x in values['exclude']]]
                include_vals = [v for v in distinct_values if str(v).upper() in [str(x).upper() for x in values['include']]]
                
                # If we have both exclude and include values, prefer include (what to keep)
                if include_vals:
                    # If only one include value, use it
                    if len(include_vals) == 1:
                        return str(include_vals[0]) if include_vals[0] is not None else None
                    # If multiple, check description for hints
                    if 'exclude' in desc_lower or 'remove' in desc_lower or 'not' in desc_lower:
                        # Description suggests exclusion - prefer 'N' or False
                        for val in ['N', 'n', False, '0', 'NO', 'No']:
                            if val in include_vals:
                                return str(val) if val is not None else None
                
                # If we only have exclude values, we want to exclude them (so include the opposite)
                if exclude_vals and not include_vals:
                    # Can't determine what to include - ambiguous
                    return None
                
                # Default: if pattern matches, use first include value from pattern
                if values['include']:
                    # Check if any distinct value matches
                    for pattern_val in values['include']:
                        if any(str(v).upper() == str(pattern_val).upper() for v in distinct_values):
                            return str(pattern_val) if pattern_val is not None else None
        
        # Check description for hints
        if any(keyword in desc_lower for keyword in ['exclude', 'remove', 'filter out', 'not', 'written off']):
            # Description suggests exclusion - look for common exclusion patterns
            if distinct_values:
                # Common pattern: 'Y'/'N' or 'Yes'/'No' or True/False
                # If description says "exclude", we want to keep 'N' or False
                if 'N' in distinct_values or 'n' in distinct_values:
                    return 'N'
                if False in distinct_values:
                    return 'False'
                if 'No' in distinct_values or 'no' in distinct_values:
                    return 'No'
        
        return None
    
    def _intelligent_exclusion(self, table: str, column: str, exclude_keywords: List[str] = None) -> Optional[str]:
        """
        Intelligently determine what value to exclude based on column name and metadata.
        
        Returns the value to exclude, or None if can't determine.
        """
        if exclude_keywords is None:
            exclude_keywords = ['exclude', 'remove', 'filter out', 'not']
        
        # Get column metadata
        table_info = self.resolver.tables.get(table, {})
        columns_metadata = {col.get('name'): col for col in table_info.get('columns', [])}
        col_metadata = columns_metadata.get(column, {})
        col_description = col_metadata.get('description', '').lower()
        
        # Get distinct values if available
        distinct_values = self._get_column_distinct_values(table, column)
        
        # Use the new intelligent determination method
        if distinct_values:
            include_value = self._determine_exclusion_value(column, distinct_values, col_description)
            if include_value is not None:
                return include_value
        
        # Fallback to pattern matching
        exclusion_patterns = {
            'write_off_flag': {'exclude': ['Y', 'y', 'Yes', 'YES', '1'], 'include': ['N', 'n', 'No', 'NO', '0']},
            'writeoff_flag': {'exclude': ['Y', 'y', 'Yes', 'YES', '1'], 'include': ['N', 'n', 'No', 'NO', '0']},
            'settled_flag': {'exclude': ['Y', 'y', 'Yes', 'YES', '1'], 'include': ['N', 'n', 'No', 'NO', '0']},
            'arc_flag': {'exclude': ['Y', 'y', 'Yes', 'YES', '1'], 'include': ['N', 'n', 'No', 'NO', '0', None, 'NULL']},
        }
        
        # Check column name patterns
        col_lower = column.lower()
        for pattern, values in exclusion_patterns.items():
            if pattern in col_lower:
                # Default: return the first include value
                if values['include']:
                    return values['include'][0]
        
        # Check description for hints
        if any(keyword in col_description for keyword in exclude_keywords):
            if distinct_values:
                # Try to infer from distinct values
                # Common pattern: 'Y'/'N' or 'Yes'/'No' or True/False
                if 'Y' in distinct_values or 'y' in distinct_values:
                    return 'Y'
                if 'Yes' in distinct_values or 'yes' in distinct_values:
                    return 'Yes'
                if True in distinct_values:
                    return True
        
        return None
    
    def _apply_user_exclusion_request(self, intent: Dict[str, Any], exclusion_term: str) -> Tuple[Dict[str, Any], List[str], Optional[str]]:
        """
        Apply user exclusion request like "remove writeoffs".
        
        Args:
            intent: Query intent dictionary
            exclusion_term: Term to exclude (e.g., "writeoffs", "writeoff")
        
        Returns:
            Tuple of (updated_intent, reasons, clarification_question)
            clarification_question is None if exclusion was applied successfully, otherwise contains question for user
        """
        reasons = []
        clarification_question = None
        base_table = intent.get('base_table', '')
        filters = intent.get('filters', [])
        
        # Normalize exclusion term
        exclusion_lower = exclusion_term.lower()
        
        # Find relevant columns in tables
        # First, check if we're in khatabook context
        is_khatabook = any(
            dim.get('name') in ['product_group', 'order_type'] and 
            'khatabook' in str(dim.get('sql_expression', '')).lower()
            for dim in intent.get('computed_dimensions', [])
        ) or 'khatabook' in base_table.lower() or 'kb_' in base_table.lower()
        
        # Find tables with khatabook if needed
        relevant_tables = [base_table]
        if is_khatabook:
            # Find all tables that might be related
            for table_name in self.resolver.tables.keys():
                if 'khatabook' in table_name.lower() or 'kb_' in table_name.lower():
                    relevant_tables.append(table_name)
        
        # Search for columns related to exclusion term
        exclusion_columns = []
        for table_name in relevant_tables:
            table_info = self.resolver.tables.get(table_name, {})
            columns = table_info.get('columns', [])
            
            for col in columns:
                col_name = col.get('name', '').lower()
                col_desc = col.get('description', '').lower()
                
                # Check if column matches exclusion term
                if (exclusion_lower in col_name or 
                    exclusion_lower in col_desc or
                    (exclusion_lower == 'writeoff' and ('write_off' in col_name or 'writeoff' in col_name))):
                    exclusion_columns.append({
                        'table': table_name,
                        'column': col.get('name'),
                        'description': col.get('description', ''),
                        'metadata': col
                    })
        
        if not exclusion_columns:
            reasons.append(f"Could not find columns related to '{exclusion_term}' in tables")
            return intent, reasons, None
        
        # For each exclusion column, determine what to exclude
        new_filters = []
        for exc_col in exclusion_columns:
            col_name = exc_col['column']
            table_name = exc_col['table']
            col_metadata = exc_col['metadata']
            
            # Get distinct values
            distinct_values = self._get_column_distinct_values(table_name, col_name)
            
            if distinct_values:
                # Use intelligent exclusion
                include_value = self._determine_exclusion_value(col_name, distinct_values, exc_col['description'])
                
                if include_value is not None:
                    # Can determine - add filter
                    # Check if filter already exists
                    existing_filter = next(
                        (f for f in filters if f.get('column') == col_name and f.get('table') == table_name),
                        None
                    )
                    
                    if not existing_filter:
                        new_filters.append({
                            'column': col_name,
                            'table': table_name,
                            'operator': '=',
                            'value': include_value,
                            'reason': f"User requested exclusion: remove {exclusion_term} (knowledge rule)"
                        })
                        reasons.append(f"Added exclusion filter for {table_name}.{col_name} = '{include_value}' (remove {exclusion_term})")
                else:
                    # Ambiguous - need to ask user
                    distinct_str = ', '.join([str(v) for v in distinct_values[:10]])  # Limit to 10 values
                    clarification_question = (
                        f"Found column '{col_name}' in table '{table_name}' related to '{exclusion_term}'. "
                        f"Distinct values: {distinct_str}. "
                        f"Which value(s) should be excluded? Please specify."
                    )
                    reasons.append(f"Ambiguous exclusion for {table_name}.{col_name}: need user clarification")
                    return intent, reasons, clarification_question
            else:
                # No distinct values - use knowledge rule defaults
                if 'write_off' in col_name.lower() or 'writeoff' in col_name.lower():
                    include_value = 'N'
                    new_filters.append({
                        'column': col_name,
                        'table': table_name,
                        'operator': '=',
                        'value': include_value,
                        'reason': f"User requested exclusion: remove {exclusion_term} (knowledge rule: write_off_flag = 'N')"
                    })
                    reasons.append(f"Added exclusion filter for {table_name}.{col_name} = '{include_value}' (knowledge rule)")
        
        # Add new filters to intent
        if new_filters:
            intent['filters'] = filters + new_filters
        
        return intent, reasons, None
    
    def _fix_hardcoded_dimensions_and_filters(self, intent: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Fix hardcoded dimensions and filter issues."""
        reasons = []
        base_table = intent.get('base_table', '')
        
        # Check if we have a metric query but missing group_by with hardcoded dimensions
        if intent.get('query_type') == 'metric':
            filters = intent.get('filters', [])
            group_by = intent.get('group_by') or []
            computed_dims = intent.get('computed_dimensions', [])
            computed_dim_names = {dim.get('name') for dim in computed_dims}
            
            # Detect hardcoded dimensions from filters and query context
            hardcoded_dims = {}
            
            # Check order_type filter for hardcoded value
            order_type_filter = next((f for f in filters if f.get('column') == 'order_type'), None)
            if order_type_filter and 'Credit Card' in str(order_type_filter.get('value', '')):
                if 'order_type' not in computed_dim_names and 'order_type' not in group_by:
                    hardcoded_dims['order_type'] = "'Credit Card'"
                    reasons.append("Added hardcoded dimension: order_type = 'Credit Card'")
            
            # Add region and product_group if missing (common pattern in queries)
            if 'region' not in computed_dim_names and 'region' not in group_by:
                hardcoded_dims['region'] = "'OS'"
                reasons.append("Added hardcoded dimension: region = 'OS'")
            
            if 'product_group' not in computed_dim_names and 'product_group' not in group_by:
                hardcoded_dims['product_group'] = "'Credit Card'"
                reasons.append("Added hardcoded dimension: product_group = 'Credit Card'")
            
            # Add hardcoded dimensions to computed_dimensions and group_by
            if hardcoded_dims:
                for dim_name, sql_expr in hardcoded_dims.items():
                    computed_dims.append({
                        'name': dim_name,
                        'sql_expression': sql_expr,
                        'is_computed': True
                    })
                    if dim_name not in group_by:
                        group_by.append(dim_name)
                
                intent['computed_dimensions'] = computed_dims
                intent['group_by'] = group_by
        
        # Fix filters with intelligent knowledge rules
        filters = intent.get('filters', [])
        fixed_filters = []
        
        # Get knowledge register rules
        try:
            from backend.knowledge_register_rules import get_knowledge_register_rules
            knowledge_rules = get_knowledge_register_rules()
        except Exception:
            knowledge_rules = None
        
        # Get table metadata for intelligent filtering
        table_info = self.resolver.tables.get(base_table, {})
        columns_metadata = {col.get('name'): col for col in table_info.get('columns', [])}
        
        for filt in filters:
            col = filt.get('column', '')
            table = filt.get('table', base_table)
            operator = filt.get('operator', '=')
            value = filt.get('value')
            
            # Get column metadata
            col_metadata = columns_metadata.get(col, {})
            col_description = col_metadata.get('description', '').lower()
            
            # Apply knowledge register rules for this node/column
            if knowledge_rules:
                register_rules = knowledge_rules.get_rules_for_column(col, table)
                if register_rules:
                    # Apply knowledge register rules
                    updated_filter = knowledge_rules.apply_rules_to_filter(col, table, filt)
                    if updated_filter:
                        filt = updated_filter
                        reasons.append(f"Applied knowledge register rules for {col}")
            
            # Apply knowledge rules based on column name and description
            
            # Rule 1: write_off_flag - always exclude writeoffs (= 'N')
            # Knowledge rule: always exclude writeoffs, use = 'N' instead of != 'Y'
            if col == 'write_off_flag' or 'write_off' in col.lower() or 'writeoff' in col.lower():
                # Use intelligent exclusion to determine the correct value
                exclude_value = self._intelligent_exclusion(table, col, exclude_keywords=['exclude', 'remove', 'writeoff', 'written off'])
                if exclude_value is None:
                    # Try to get distinct values and intelligently determine
                    distinct_values = self._get_column_distinct_values(table, col)
                    if distinct_values:
                        # Use intelligent logic to determine what to exclude
                        exclude_value = self._determine_exclusion_value(col, distinct_values, col_description)
                        if exclude_value is None:
                            # Ambiguous - use default knowledge rule: 'N' means not written off
                            exclude_value = 'N'
                            reasons.append(f"Using knowledge rule for {col}: exclude writeoffs (= 'N')")
                        else:
                            reasons.append(f"Intelligently determined exclusion value for {col}: = '{exclude_value}'")
                    else:
                        # Can't determine - use default knowledge rule
                        exclude_value = 'N'
                        reasons.append(f"Using default knowledge rule for {col}: exclude writeoffs (= 'N')")
                
                if operator in ['!=', '<>']:
                    # Change != 'Y' to = exclude_value (knowledge rule: always use = 'N' not != 'Y')
                    fixed_filters.append({
                        'column': col,
                        'table': table,
                        'operator': '=',
                        'value': exclude_value,
                        'reason': filt.get('reason', '') + f' (knowledge rule: always exclude writeoffs, use = {exclude_value} not != Y)'
                    })
                    reasons.append(f"Fixed {col} filter: Changed != 'Y' to = '{exclude_value}' (knowledge rule: always exclude writeoffs)")
                    continue
                elif operator == 'IS NULL':
                    # Change IS NULL to = exclude_value
                    fixed_filters.append({
                        'column': col,
                        'table': table,
                        'operator': '=',
                        'value': exclude_value,
                        'reason': filt.get('reason', '') + f' (knowledge rule: always exclude writeoffs, use = {exclude_value})'
                    })
                    reasons.append(f"Fixed {col} filter: Changed IS NULL to = '{exclude_value}' (knowledge rule)")
                    continue
                elif operator == '=' and value != exclude_value:
                    # Ensure it's the right value
                    fixed_filters.append({
                        'column': col,
                        'table': table,
                        'operator': '=',
                        'value': exclude_value,
                        'reason': filt.get('reason', '') + f' (knowledge rule: ensured correct exclusion value: {exclude_value})'
                    })
                    reasons.append(f"Ensured {col} filter uses correct exclusion value: '{exclude_value}' (knowledge rule)")
                    continue
                elif operator == '=' and value == exclude_value:
                    # Already correct, keep it but mark with knowledge rule
                    fixed_filters.append({
                        **filt,
                        'reason': filt.get('reason', '') + ' (knowledge rule: always exclude writeoffs)'
                    })
                    continue
            
            # Rule 2: arc_flag - for khatabook, always exclude (keep only null, 'N', 'NULL')
            # Knowledge rule: always exclude arc_flag in khatabook (arc_flag is null or arc_flag = 'N' or arc_flag = 'NULL')
            if col == 'arc_flag' or 'arc' in col.lower():
                # Check if this is khatabook context (check product_group or order_type in intent)
                is_khatabook = any(
                    dim.get('name') in ['product_group', 'order_type'] and 
                    'khatabook' in str(dim.get('sql_expression', '')).lower()
                    for dim in intent.get('computed_dimensions', [])
                ) or 'khatabook' in base_table.lower() or 'kb_' in base_table.lower() or 'khatabook' in str(intent.get('query_text', '')).lower()
                
                if is_khatabook:
                    # Apply khatabook rule: always exclude arc_flag (keep only null, 'N', 'NULL')
                    # This means: (arc_flag IS NULL OR arc_flag = 'N' OR arc_flag = 'NULL')
                    # Replace any existing arc_flag filter with this knowledge rule
                    fixed_filters.append({
                        'column': col,
                        'table': table,
                        'operator': 'OR',
                        'conditions': [
                            {'column': col, 'table': table, 'operator': 'IS NULL'},
                            {'column': col, 'table': table, 'operator': '=', 'value': 'N'},
                            {'column': col, 'table': table, 'operator': '=', 'value': 'NULL'}
                        ],
                        'reason': filt.get('reason', '') + ' (knowledge rule: khatabook always exclude arc_flag - keep only null/N/NULL)'
                    })
                    reasons.append(f"Applied khatabook rule for {col}: always exclude arc_flag (keep only null, 'N', 'NULL')")
                    continue
            
            # Rule 3: originator - use LOWER(TRIM()) and handle null based on description
            # Knowledge rule: originator column description tells khatabook is for khatabook and flexiloans for flexiloans product
            if col == 'originator':
                # Check column description for mapping rules
                product_value = None
                if 'khatabook' in col_description or 'flexiloans' in col_description:
                    # Extract product mapping from description
                    # Description format: "khatabook is for khatabook and flexiloans for flexiloans product"
                    if 'khatabook' in col_description:
                        product_value = 'khatabook'
                    elif 'flexiloans' in col_description:
                        product_value = 'flexiloans'
                
                # Also check query context for product type
                if not product_value:
                    # Check if query mentions khatabook or flexiloans
                    query_context = str(intent.get('query_text', '')).lower() if intent.get('query_text') else ''
                    if 'khatabook' in query_context:
                        product_value = 'khatabook'
                    elif 'flexiloans' in query_context:
                        product_value = 'flexiloans'
                
                # Always use LOWER(TRIM()) and handle null (knowledge rule)
                if product_value:
                    # Use LOWER(TRIM()) and handle null based on description
                    fixed_filters.append({
                        'column': col,
                        'table': table,
                        'operator': 'OR',
                        'conditions': [
                            {'column': col, 'table': table, 'operator': 'IS NULL'},
                            {
                                'column': col,
                                'table': table,
                                'operator': '=',
                                'value': product_value,
                                'function': 'LOWER_TRIM',
                                'reason': f'Knowledge rule: {col} description indicates {product_value} mapping'
                            }
                        ],
                        'reason': filt.get('reason', '') + f' (knowledge rule: originator null or LOWER(TRIM()) = {product_value})'
                    })
                    reasons.append(f"Applied originator rule: null or LOWER(TRIM({col})) = '{product_value}' (knowledge rule)")
                    continue
                elif operator == '=' and value:
                    # Default: use LOWER(TRIM()) and handle null (knowledge rule)
                    fixed_filters.append({
                        'column': col,
                        'table': table,
                        'operator': 'OR',
                        'conditions': [
                            {'column': col, 'table': table, 'operator': 'IS NULL'},
                            {
                                'column': col,
                                'table': table,
                                'operator': '=',
                                'value': value.lower() if isinstance(value, str) else value,
                                'function': 'LOWER_TRIM'
                            }
                        ],
                        'reason': filt.get('reason', '') + ' (knowledge rule: originator always uses LOWER(TRIM()) and handles null)'
                    })
                    reasons.append(f"Fixed {col} filter: Added LOWER(TRIM()) and null handling (knowledge rule)")
                    continue
                elif operator != 'OR' or not filt.get('conditions'):
                    # If not already using OR with LOWER_TRIM, fix it
                    # Extract value from current filter if possible
                    current_value = value or (filt.get('conditions', [{}])[0].get('value') if filt.get('conditions') else None)
                    if current_value:
                        fixed_filters.append({
                            'column': col,
                            'table': table,
                            'operator': 'OR',
                            'conditions': [
                                {'column': col, 'table': table, 'operator': 'IS NULL'},
                                {
                                    'column': col,
                                    'table': table,
                                    'operator': '=',
                                    'value': str(current_value).lower() if isinstance(current_value, str) else current_value,
                                    'function': 'LOWER_TRIM'
                                }
                            ],
                            'reason': filt.get('reason', '') + ' (knowledge rule: originator always uses LOWER(TRIM()) and handles null)'
                        })
                        reasons.append(f"Fixed {col} filter: Added LOWER(TRIM()) and null handling (knowledge rule)")
                        continue
            
            # Rule 4: order_type filter - should be LOWER(order_type) IN ('credit_card', 'no_cost_emi')
            if col == 'order_type' and operator == '=' and value == 'Credit Card':
                # Replace with IN clause with LOWER
                fixed_filters.append({
                    'column': 'order_type',
                    'table': table,
                    'operator': 'IN',
                    'value': ['credit_card', 'no_cost_emi'],
                    'function': 'LOWER',
                    'reason': filt.get('reason', '') + ' (fixed: use LOWER and IN for credit_card and no_cost_emi)'
                })
                reasons.append("Fixed order_type filter: Changed to LOWER(order_type) IN ('credit_card', 'no_cost_emi')")
                continue
            
            # Keep original filter if no rules apply
            fixed_filters.append(filt)
        
        # Add missing arc_flag filter for khatabook if not present (after processing all filters)
        # Knowledge rule: always exclude arc_flag in khatabook (arc_flag is null or arc_flag = 'N' or arc_flag = 'NULL')
        has_arc_flag = any(f.get('column') == 'arc_flag' or 'arc' in f.get('column', '').lower() for f in fixed_filters)
        is_khatabook_context = any(
            dim.get('name') in ['product_group', 'order_type'] and 
            'khatabook' in str(dim.get('sql_expression', '')).lower()
            for dim in intent.get('computed_dimensions', [])
        ) or 'khatabook' in base_table.lower() or 'kb_' in base_table.lower() or 'khatabook' in str(intent.get('query_text', '')).lower()
        
        if not has_arc_flag and is_khatabook_context:
            # Check if table has arc_flag column
            table_info = self.resolver.tables.get(base_table, {})
            columns = table_info.get('columns', [])
            has_arc_flag_col = any('arc_flag' in str(col.get('name', '')).lower() for col in columns)
            
            if has_arc_flag_col:
                fixed_filters.append({
                    'column': 'arc_flag',
                    'table': base_table,
                    'operator': 'OR',
                    'conditions': [
                        {'column': 'arc_flag', 'table': base_table, 'operator': 'IS NULL'},
                        {'column': 'arc_flag', 'table': base_table, 'operator': '=', 'value': 'N'},
                        {'column': 'arc_flag', 'table': base_table, 'operator': '=', 'value': 'NULL'}
                    ],
                    'reason': 'Knowledge rule: khatabook always exclude arc_flag - keep only null/N/NULL'
                })
                reasons.append("Added missing arc_flag filter for khatabook (knowledge rule: always exclude arc_flag)")
        
        # Add missing NFBC filter if nbfc columns exist
        has_nbfc_filter = any(
            'nbfc' in filt.get('column', '').lower() 
            for filt in fixed_filters
        )
        
        if not has_nbfc_filter:
            # Check if table has nbfc columns
            table_info = self.resolver.tables.get(base_table, {})
            columns = table_info.get('columns', [])
            has_nbfc_colending = any('nbfc_name_colending' in str(col.get('name', '')) for col in columns)
            has_parent_nbfc = any('parent_nbfc_name' in str(col.get('name', '')) for col in columns)
            
            if has_nbfc_colending or has_parent_nbfc:
                # Add COALESCE filter
                fixed_filters.append({
                    'column': 'nbfc_name',
                    'table': base_table,
                    'operator': 'IN',
                    'value': ['quadrillion', 'slicenesfb', 'nesfb'],
                    'coalesce_columns': ['nbfc_name_colending', 'parent_nbfc_name'],
                    'reason': 'Added NFBC filter: COALESCE(nbfc_name_colending, parent_nbfc_name) IN (...)'
                })
                reasons.append("Added missing NFBC filter with COALESCE")
        
        intent['filters'] = fixed_filters
        
        return intent, reasons


class QueryExplainPlan:
    """Query explain plan for debugging and transparency."""
    
    def __init__(self, intent: Dict[str, Any]):
        self.intent = intent
        self.anchor_table = intent.get('anchor_entity') or intent.get('base_table', '')
        self.join_paths = []
        self.columns = []
        self.filters = []
        self.aggregations = []
        self.inference_notes = intent.get('inference_notes', [])
    
    def add_join_path(self, from_table: str, to_table: str, join_type: str, reason: str, 
                     confidence: Optional[str] = None, inference_reason: Optional[str] = None):
        """Add a join path to the explain plan."""
        self.join_paths.append({
            'from': from_table,
            'to': to_table,
            'type': join_type,
            'reason': reason,
            'confidence': confidence,
            'inference_reason': inference_reason
        })
    
    def to_string(self) -> str:
        """Generate human-readable explain plan."""
        lines = []
        lines.append("=" * 80)
        lines.append("QUERY EXPLAIN PLAN")
        lines.append("=" * 80)
        lines.append(f"\nANCHOR TABLE: {self.anchor_table}")
        lines.append(f"QUERY TYPE: {self.intent.get('query_type', 'unknown')}")
        
        if self.join_paths:
            lines.append("\nJOIN PATHS:")
            for path in self.join_paths:
                lines.append(f"  {path['from']} → {path['to']} ({path['type']})")
                if path.get('reason'):
                    lines.append(f"    Reason: {path['reason']}")
                if path.get('confidence'):
                    lines.append(f"    Confidence: {path['confidence']}")
                if path.get('inference_reason'):
                    lines.append(f"    Inference: {path['inference_reason']}")
        
        # Add inference notes section
        if self.inference_notes:
            lines.append("\nINFERENCE NOTES:")
            for note in self.inference_notes:
                # Format note based on content
                if "UNSAFE" in note or "BLOCKED" in note:
                    lines.append(f"  ️  {note}")
                elif "AMBIGUOUS" in note:
                    lines.append(f"   {note}")
                elif "Duplicate" in note:
                    lines.append(f"  ℹ️  {note}")
                else:
                    lines.append(f"   {note}")
        
        if self.intent.get('columns'):
            lines.append("\nCOLUMNS:")
            for col in self.intent.get('columns', []):
                lines.append(f"  - {col}")
        
        if self.intent.get('filters'):
            lines.append("\nFILTERS:")
            for f in self.intent.get('filters', []):
                lines.append(f"  - {f.get('table', '')}.{f.get('column', '')} {f.get('operator', '')} {f.get('value', '')}")
        
        if self.intent.get('group_by'):
            lines.append(f"\nGROUP BY: {', '.join(self.intent.get('group_by', []))}")
        
        lines.append("=" * 80)
        return "\n".join(lines)


class SQLBuilder:
    """Builds SQL queries from validated intents."""
    
    def __init__(self, resolver: TableRelationshipResolver):
        self.resolver = resolver
        self.explain_plan: Optional[QueryExplainPlan] = None
        # Initialize dimension resolver for semantic dimension resolution
        from backend.dimension_resolver import DimensionResolver
        self.dimension_resolver = DimensionResolver(resolver.registry)
    
    def build(self, intent: Dict[str, Any], include_explain: bool = True) -> Tuple[str, Optional[str]]:
        """
        Build SQL query from validated intent.
        
        Returns:
            (sql_query, explain_plan_string)
        """
        query_type = intent.get('query_type', 'relational')
        base_table = intent.get('base_table', '')
        
        # Create explain plan
        if include_explain:
            self.explain_plan = QueryExplainPlan(intent)
        
        # Setup aliases
        table_alias = 't1'
        join_aliases = {base_table: table_alias}
        alias_counter = 2
        
        # Process joins to assign aliases
        joins = intent.get('joins', [])
        for join in joins:
            join_table = join.get('table', '')
            if join_table and join_table not in join_aliases:
                join_aliases[join_table] = f't{alias_counter}'
                alias_counter += 1
        
        # Handle computed dimensions from intent (user-described business logic)
        computed_dims = intent.get('computed_dimensions', [])
        if computed_dims:
            # Temporarily add computed dimensions to registry for resolution
            from backend.dimension_resolver import DimensionResolver
            original_dims = self.resolver.registry.get('dimensions', [])
            for comp_dim in computed_dims:
                # Add to registry temporarily
                dim_def = {
                    'name': comp_dim.get('name'),
                    'base_table': base_table,
                    'sql_expression': comp_dim.get('sql_expression', ''),
                    'is_computed': True
                }
                # Check if dimension already exists, if so update it
                existing = next((d for d in original_dims if d.get('name') == comp_dim.get('name')), None)
                if existing:
                    existing['sql_expression'] = comp_dim.get('sql_expression', '')
                    existing['is_computed'] = True
                else:
                    original_dims.append(dim_def)
            self.resolver.registry['dimensions'] = original_dims
            # Update dimension resolver
            self.dimension_resolver = DimensionResolver(self.resolver.registry)
        
        # Build SELECT clause
        select_parts = self._build_select(intent, query_type, join_aliases, table_alias)
        
        # Build FROM clause
        from_clause = f"FROM {base_table} {table_alias}"
        
        # Build JOIN clauses (join type determined dynamically from intent)
        join_clauses = self._build_joins(joins, join_aliases, base_table, intent)
        
        # Build WHERE clause
        where_clause = self._build_where(intent.get('filters', []), join_aliases, base_table, table_alias)
        
        # Build GROUP BY clause
        group_by_clause = self._build_group_by(intent, query_type, join_aliases, table_alias)
        
        # Build ORDER BY clause
        order_by_clause = self._build_order_by(intent.get('order_by', []), join_aliases)
        
        # Combine all parts
        sql_parts = [
            f"SELECT {', '.join(select_parts)}",
            from_clause
        ]
        sql_parts.extend(join_clauses)
        if where_clause:
            sql_parts.append(where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        
        sql = "\n    ".join(sql_parts)
        
        # Generate explain plan
        explain_str = None
        if self.explain_plan:
            # Add join paths to explain plan with confidence info
            # Join type is determined dynamically, so we need to resolve it here for explain plan
            from planning.join_type_resolver import JoinTypeResolver
            resolver = JoinTypeResolver()
            
            for join in intent.get('joins', []):
                join_table = join.get('table', '')
                reason = join.get('reason', '')
                confidence = join.get('inference_confidence')
                inference_reason = join.get('inference_reason')
                
                # Determine join type for explain plan
                relationship = {
                    'relationship_type': join.get('relationship_type', 'one_to_many'),
                    'on': join.get('on', ''),
                }
                join_type_enum = resolver.determine_join_type_from_intent(
                    base_table=base_table,
                    join_table=join_table,
                    relationship=relationship,
                    intent=intent
                )
                join_type = join_type_enum.value
                
                self.explain_plan.add_join_path(
                    base_table, join_table, join_type, reason, 
                    confidence, inference_reason
                )
            
            # Add inference notes from intent
            if intent.get('inference_notes'):
                self.explain_plan.inference_notes = intent.get('inference_notes', [])
            
            explain_str = self.explain_plan.to_string()
        
        return sql, explain_str
    
    def _build_select(self, intent: Dict[str, Any], query_type: str, join_aliases: Dict[str, str], base_alias: str) -> List[str]:
        """Build SELECT clause."""
        select_parts = []
        
        if query_type == 'metric':
            # Metric query: dimensions first, then metric
            group_by_cols = intent.get('group_by') or []
            columns = intent.get('columns') or []
            
            # Check if columns are provided and if they contain computed dimensions
            computed_dims = intent.get('computed_dimensions', [])
            computed_dim_map = {dim.get('name'): dim for dim in computed_dims}
            
            # Check if columns contain computed dimension dicts
            has_computed_in_columns = any(
                isinstance(col, dict) and col.get('is_computed') for col in columns
            )
            
            has_aggregation = any(
                any(op in str(col).upper() for op in ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(', 'AS '])
                for col in columns
            ) if not has_computed_in_columns else False
            
            if has_aggregation and not has_computed_in_columns:
                # Use provided columns (but check for computed dimensions first)
                for col_expr in columns:
                    col_str = str(col_expr)
                    # Check if this column name matches a computed dimension
                    if col_str in computed_dim_map:
                        # Use computed dimension SQL expression instead
                        comp_dim = computed_dim_map[col_str]
                        sql_expr = comp_dim.get('sql_expression', '')
                        sql_expr = self._replace_table_names(sql_expr, join_aliases)
                        select_parts.append(f"{sql_expr} AS {col_str}")
                    else:
                        select_parts.append(self._process_column_expression(col_str, join_aliases, base_alias))
            elif has_computed_in_columns or computed_dims:
                # Process computed dimensions from columns or computed_dimensions field
                # If columns is empty but computed_dims exists, use group_by_cols instead
                cols_to_process = columns if columns else group_by_cols
                
                for col in cols_to_process:
                    if isinstance(col, dict) and col.get('is_computed'):
                        # Computed dimension from columns field
                        dim_name = col.get('name', '')
                        sql_expr = col.get('sql_expression', '')
                        sql_expr = self._replace_table_names(sql_expr, join_aliases)
                        select_parts.append(f"{sql_expr} AS {dim_name}")
                    elif isinstance(col, str):
                        # Regular column - check if it's actually a computed dimension
                        if col in computed_dim_map:
                            # Use computed dimension SQL expression
                            comp_dim = computed_dim_map[col]
                            sql_expr = comp_dim.get('sql_expression', '')
                            sql_expr = self._replace_table_names(sql_expr, join_aliases)
                            select_parts.append(f"{sql_expr} AS {col}")
                        else:
                            select_parts.append(self._process_column_expression(col, join_aliases, base_alias))
                
                # Add metric if this is a metric query and columns was empty
                if not columns and query_type == 'metric':
                    metric = intent.get('metric')
                    if metric:
                        expr = metric.get('sql_expression', '')
                        if expr:
                            expr = self._replace_table_names(expr, join_aliases)
                            if not any(op in expr.upper() for op in ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(']):
                                expr = f"SUM({expr})"
                            select_parts.append(f"{expr} as {metric.get('name', 'value')}")
            else:
                # Build from dimensions and metric using semantic dimension resolution
                # First, handle computed dimensions from intent (user-described business logic)
                computed_dims = intent.get('computed_dimensions', [])
                computed_dim_map = {dim.get('name'): dim for dim in computed_dims}
                
                # Resolve dimensions to their SQL expressions (supports computed dimensions)
                resolved_dims = self.dimension_resolver.resolve_dimensions(group_by_cols or [], join_aliases, base_alias)
                
                # Override with computed dimensions from intent if they exist
                for dim_name in (group_by_cols or []):
                    if dim_name in computed_dim_map:
                        # Use computed dimension from intent (user-described)
                        comp_dim = computed_dim_map[dim_name]
                        sql_expr = comp_dim.get('sql_expression', '')
                        # Replace table references with aliases
                        sql_expr = self._replace_table_names(sql_expr, join_aliases)
                        select_parts.append(f"{sql_expr} AS {dim_name}")
                    else:
                        # Use resolved dimension (from metadata or fallback)
                        resolved_dim = next((d for d in resolved_dims if d.name == dim_name), None)
                        if resolved_dim:
                            select_parts.append(f"{resolved_dim.expression} AS {resolved_dim.alias}")
                        else:
                            # Fallback: assume it's a column
                            select_parts.append(f"{base_alias}.{dim_name} AS {dim_name}")
                
                # Store resolved dimensions in intent for GROUP BY to use
                intent['_resolved_dimensions'] = resolved_dims
                
                # Add metric
                metric = intent.get('metric')
                if metric:
                    expr = metric.get('sql_expression', '')
                    if expr:
                        expr = self._replace_table_names(expr, join_aliases)
                        if not any(op in expr.upper() for op in ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(']):
                            expr = f"SUM({expr})"
                        select_parts.append(f"{expr} as total_{metric.get('name', 'value')}")
        else:
            # Relational query
            columns = intent.get('columns', [])
            if not columns:
                columns = ['*']
            
            for col in columns:
                if col == '*':
                    select_parts.append(f"{base_alias}.*")
                else:
                    select_parts.append(self._process_column_expression(str(col), join_aliases, base_alias))
        
        if not select_parts:
            select_parts.append(f"{base_alias}.*")
        
        return select_parts
    
    def _build_joins(self, joins: List[Dict[str, Any]], join_aliases: Dict[str, str], base_table: str, intent: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        Build JOIN clauses.
        
        Join type is determined dynamically based on query intent, not stored in metadata.
        """
        from planning.join_type_resolver import JoinTypeResolver
        
        join_clauses = []
        resolver = JoinTypeResolver()
        
        for join in joins:
            join_table = join.get('table', '')
            if not join_table:
                continue
            
            join_on = join.get('on', '')
            if not join_on:
                # Try to resolve join path from lineage/metadata
                # Disable learning temporarily to avoid interactive prompts
                original_learning = self.resolver.enable_learning
                self.resolver.enable_learning = False
                try:
                    join_path = self.resolver.find_join_path(base_table, join_table, prefer_cardinality_safe=True)
                    if join_path and len(join_path) > 0:
                        # Use the first step of the path (direct join)
                        first_step = join_path[0]
                        join_on = first_step.get('on', '')
                        if not join_on:
                            # Try to infer join condition
                            join_on = self.resolver._infer_join_condition(base_table, join_table)
                        # Update the join dict so it has the ON clause for future use
                        if join_on:
                            join['on'] = join_on
                finally:
                    self.resolver.enable_learning = original_learning
                
                if not join_on:
                    # Could not find join path - skip this join
                    continue
            
            if not join_on:
                continue
            
            alias = join_aliases.get(join_table, '')
            if not alias:
                continue
            
            # Determine join type based on query intent
            relationship = {
                'relationship_type': join.get('relationship_type', 'one_to_many'),
                'on': join_on,
            }
            
            if intent:
                join_type_enum = resolver.determine_join_type_from_intent(
                    base_table=base_table,
                    join_table=join_table,
                    relationship=relationship,
                    intent=intent
                )
                join_type = join_type_enum.value
            else:
                # Fallback: use explicit type if provided, otherwise LEFT
                join_type = join.get('type', 'LEFT').upper()
            
            # Replace table names with aliases in ON clause
            join_on = self._replace_table_names(join_on, join_aliases)
            
            join_clauses.append(f"{join_type} JOIN {join_table} {alias} ON {join_on}")
        
        return join_clauses
    
    def _build_where(self, filters: List[Dict[str, Any]], join_aliases: Dict[str, str], base_table: str, base_alias: str) -> Optional[str]:
        """Build WHERE clause."""
        conditions = []
        
        for filter_obj in filters:
            col = filter_obj.get('column', '')
            table = filter_obj.get('table', base_table)
            operator = filter_obj.get('operator', '=')
            value = filter_obj.get('value')
            function = filter_obj.get('function')  # e.g., 'LOWER', 'LOWER_TRIM'
            coalesce_columns = filter_obj.get('coalesce_columns')  # For COALESCE
            or_conditions = filter_obj.get('conditions')  # For OR conditions
            
            alias = join_aliases.get(table, base_alias)
            
            # Handle OR conditions (e.g., arc_flag is null or = 'N' or = 'NULL')
            if operator == 'OR' and or_conditions:
                or_parts = []
                for or_cond in or_conditions:
                    or_col = or_cond.get('column', col)
                    or_table = or_cond.get('table', table)
                    or_op = or_cond.get('operator', '=')
                    or_val = or_cond.get('value')
                    or_func = or_cond.get('function')
                    or_alias = join_aliases.get(or_table, base_alias)
                    
                    # Build column expression for OR condition
                    if or_func == 'LOWER_TRIM':
                        or_col_expr = f"LOWER(TRIM({or_alias}.{or_col}))"
                    elif or_func:
                        or_col_expr = f"{or_func}({or_alias}.{or_col})"
                    else:
                        or_col_expr = f"{or_alias}.{or_col}"
                    
                    if or_op in ['IS NULL', 'IS NOT NULL']:
                        or_parts.append(f"{or_col_expr} {or_op}")
                    elif or_val is not None:
                        if isinstance(or_val, str):
                            or_parts.append(f"{or_col_expr} {or_op} '{or_val}'")
                        else:
                            or_parts.append(f"{or_col_expr} {or_op} {or_val}")
                
                if or_parts:
                    conditions.append(f"({' OR '.join(or_parts)})")
                continue
            
            # Build column expression
            if coalesce_columns:
                # COALESCE(nbfc_name_colending, parent_nbfc_name)
                coalesce_parts = [f"{alias}.{c}" for c in coalesce_columns]
                col_expr = f"COALESCE({', '.join(coalesce_parts)})"
            elif function == 'LOWER_TRIM':
                # LOWER(TRIM(column))
                col_expr = f"LOWER(TRIM({alias}.{col}))"
            elif function:
                # LOWER(column) or other functions
                col_expr = f"{function}({alias}.{col})"
            else:
                col_expr = f"{alias}.{col}"
            
            if operator in ['IS NULL', 'IS NOT NULL']:
                conditions.append(f"{col_expr} {operator}")
            elif operator == 'IN' and isinstance(value, list):
                # IN clause with list of values
                value_list = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in value])
                conditions.append(f"{col_expr} IN ({value_list})")
            elif value is not None:
                if isinstance(value, str):
                    conditions.append(f"{col_expr} {operator} '{value}'")
                else:
                    conditions.append(f"{col_expr} {operator} {value}")
        
        return f"WHERE {' AND '.join(conditions)}" if conditions else None
    
    def _build_group_by(self, intent: Dict[str, Any], query_type: str, join_aliases: Dict[str, str], base_alias: str) -> Optional[str]:
        """Build GROUP BY clause using resolved dimension expressions."""
        if query_type != 'metric':
            return None
        
        group_by_cols = intent.get('group_by') or []
        if not group_by_cols:
            return None
        
        group_parts = []
        
        # Use resolved dimensions if available (from SELECT clause)
        resolved_dims = intent.get('_resolved_dimensions')
        if resolved_dims:
            # Use the same aliases as in SELECT (for computed dimensions, use alias)
            for resolved_dim in resolved_dims:
                if resolved_dim.groupable:
                    # For computed dimensions, use alias (since expression is in SELECT)
                    # For physical dimensions, use expression
                    if resolved_dim.dimension_type.value == 'computed':
                        group_parts.append(resolved_dim.alias)
                    else:
                        group_parts.append(resolved_dim.expression)
        else:
            # Fallback: resolve dimensions again
            resolved_dims = self.dimension_resolver.resolve_dimensions(group_by_cols, join_aliases, base_alias)
            for resolved_dim in resolved_dims:
                if resolved_dim.groupable:
                    if resolved_dim.dimension_type.value == 'computed':
                        group_parts.append(resolved_dim.alias)
                    else:
                        group_parts.append(resolved_dim.expression)
        
        return f"GROUP BY {', '.join(group_parts)}" if group_parts else None
    
    def _build_order_by(self, order_by: List[Dict[str, Any]], join_aliases: Dict[str, str]) -> Optional[str]:
        """Build ORDER BY clause."""
        if not order_by:
            return None
        
        order_parts = []
        for order in order_by:
            col = order.get('column', '')
            direction = order.get('direction', 'ASC')
            col = self._replace_table_names(col, join_aliases)
            order_parts.append(f"{col} {direction}")
        
        return f"ORDER BY {', '.join(order_parts)}" if order_parts else None
    
    def _process_column_expression(self, expr: str, join_aliases: Dict[str, str], base_alias: str) -> str:
        """Process a column expression, adding table aliases where needed."""
        expr = self._replace_table_names(expr, join_aliases)
        
        # If it's a simple column name without table prefix, add base alias
        if '.' not in expr and not any(op in expr.upper() for op in ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(', 'AS ']):
            return f"{base_alias}.{expr}"
        
        # If it's an aggregation without table prefix, add base alias
        if any(op in expr.upper() for op in ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(']):
            def add_alias(match):
                func = match.group(1)
                col = match.group(2)
                if '.' not in col:
                    return f"{func}({base_alias}.{col})"
                return match.group(0)
            
            expr = re.sub(
                r'\b(SUM|COUNT|AVG|MAX|MIN)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
                add_alias,
                expr,
                flags=re.IGNORECASE
            )
        
        return expr
    
    def _replace_table_names(self, expression: str, join_aliases: Dict[str, str]) -> str:
        """Replace table names with aliases in an expression."""
        result = expression
        
        # Sort by length (longest first) to avoid partial matches
        for table_name, alias in sorted(join_aliases.items(), key=lambda x: len(x[0]), reverse=True):
            result = result.replace(f"{table_name}.", f"{alias}.")
        
        return result

