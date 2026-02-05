#!/usr/bin/env python3
"""
Knowledge Register Rules Lookup

Fetches and applies rules from the knowledge register for specific nodes/columns.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict


class KnowledgeRegisterRules:
    """Manages knowledge register rules for nodes/columns."""
    
    def __init__(self, metadata_dir: Optional[str] = None):
        """
        Initialize knowledge register rules.
        
        Args:
            metadata_dir: Directory containing metadata files
        """
        if metadata_dir:
            self.metadata_dir = Path(metadata_dir)
        else:
            self.metadata_dir = Path(__file__).parent.parent / "metadata"
        
        self.rules_cache: Dict[str, List[Dict[str, Any]]] = {}
        self.node_rules_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._load_rules()
    
    def _load_rules(self):
        """Load rules from metadata files."""
        # Load rules.json
        rules_file = self.metadata_dir / "rules.json"
        if rules_file.exists():
            try:
                with open(rules_file, 'r') as f:
                    self.rules_cache['general'] = json.load(f)
            except Exception as e:
                print(f"Error loading rules.json: {e}")
                self.rules_cache['general'] = []
        
        # Load knowledge_base.json for node-specific rules
        kb_file = self.metadata_dir / "knowledge_base.json"
        if kb_file.exists():
            try:
                with open(kb_file, 'r') as f:
                    kb_data = json.load(f)
                    # Extract rules from knowledge base
                    self._extract_node_rules_from_kb(kb_data)
            except Exception as e:
                print(f"Error loading knowledge_base.json: {e}")
    
    def _extract_node_rules_from_kb(self, kb_data: Dict[str, Any]):
        """Extract node-specific rules from knowledge base."""
        terms = kb_data.get('terms', {})
        
        for term_name, term_data in terms.items():
            # Extract rules for this term/node
            rules = []
            
            # Check for filter rules
            if 'filter_rules' in term_data:
                rules.extend(term_data['filter_rules'])
            
            # Check for exclusion rules
            if 'exclusion_rules' in term_data:
                rules.extend(term_data['exclusion_rules'])
            
            # Check for default filters
            if 'default_filters' in term_data:
                rules.extend(term_data['default_filters'])
            
            # Check related columns for rules
            related_columns = term_data.get('related_columns', [])
            for col in related_columns:
                if col not in self.node_rules_cache:
                    self.node_rules_cache[col] = []
                self.node_rules_cache[col].extend(rules)
            
            # Also index by term name
            if term_name not in self.node_rules_cache:
                self.node_rules_cache[term_name] = []
            self.node_rules_cache[term_name].extend(rules)
    
    def get_rules_for_node(self, node_name: str, node_type: str = 'column') -> List[Dict[str, Any]]:
        """
        Get rules for a specific node (column, table, etc.).
        
        Args:
            node_name: Name of the node
            node_type: Type of node ('column', 'table', 'term')
        
        Returns:
            List of rules applicable to this node
        """
        rules = []
        
        # Check node-specific rules cache
        node_name_lower = node_name.lower()
        
        # Direct match
        if node_name_lower in self.node_rules_cache:
            rules.extend(self.node_rules_cache[node_name_lower])
        
        # Partial match (for column names like 'write_off_flag' matching 'writeoff')
        for cached_node, cached_rules in self.node_rules_cache.items():
            if (node_name_lower in cached_node or cached_node in node_name_lower):
                rules.extend(cached_rules)
        
        # Check general rules for node references
        for rule in self.rules_cache.get('general', []):
            # Handle both dict and string formats
            if isinstance(rule, str):
                continue  # Skip string rules
            if not isinstance(rule, dict):
                continue
            # Check if rule applies to this node
            computation = rule.get('computation', {})
            if not isinstance(computation, dict):
                continue
            filter_conditions = computation.get('filter_conditions', {})
            
            # Check if node is mentioned in filter conditions
            for filter_key in filter_conditions.keys():
                if node_name_lower in filter_key.lower():
                    rules.append({
                        'type': 'filter_condition',
                        'node': node_name,
                        'condition': filter_key,
                        'value': filter_conditions[filter_key],
                        'rule_id': rule.get('id'),
                        'source': 'rules.json'
                    })
        
        return rules
    
    def get_rules_for_column(self, column_name: str, table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get rules for a specific column.
        
        Args:
            column_name: Name of the column
            table_name: Optional table name
        
        Returns:
            List of rules applicable to this column
        """
        rules = []
        
        # Get node rules
        rules.extend(self.get_rules_for_node(column_name, 'column'))
        
        # If table name provided, check table.column rules
        if table_name:
            full_name = f"{table_name}.{column_name}"
            rules.extend(self.get_rules_for_node(full_name, 'column'))
        
        return rules
    
    def apply_rules_to_filter(self, column_name: str, table_name: Optional[str] = None, 
                             existing_filter: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Apply knowledge register rules to a filter.
        
        Args:
            column_name: Column name
            table_name: Optional table name
            existing_filter: Existing filter dictionary (if any)
        
        Returns:
            Updated filter dictionary with rules applied
        """
        rules = self.get_rules_for_column(column_name, table_name)
        
        if not rules and not existing_filter:
            return None
        
        # Start with existing filter or create new one
        filter_dict = existing_filter.copy() if existing_filter else {
            'column': column_name,
            'table': table_name or '',
            'operator': '=',
            'value': None
        }
        
        # Apply rules
        for rule in rules:
            rule_type = rule.get('type')
            
            if rule_type == 'filter_condition':
                # Apply filter condition from rule
                condition_value = rule.get('value')
                if condition_value:
                    filter_dict['value'] = condition_value
                    filter_dict['reason'] = filter_dict.get('reason', '') + f" (knowledge register rule: {rule.get('rule_id')})"
            
            elif rule_type == 'exclusion_rule':
                # Apply exclusion rule
                exclude_values = rule.get('exclude_values', [])
                if exclude_values:
                    # Convert to appropriate filter format
                    if len(exclude_values) == 1:
                        filter_dict['operator'] = '='
                        filter_dict['value'] = exclude_values[0]
                    else:
                        # Multiple values - use OR condition
                        filter_dict['operator'] = 'OR'
                        filter_dict['conditions'] = [
                            {'column': column_name, 'table': table_name or '', 'operator': '=', 'value': val}
                            for val in exclude_values
                        ]
                    filter_dict['reason'] = filter_dict.get('reason', '') + f" (knowledge register rule: {rule.get('rule_id')})"
        
        return filter_dict
    
    def get_default_filters_for_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get default filters for a table based on knowledge register rules.
        
        Args:
            table_name: Table name
        
        Returns:
            List of default filters
        """
        filters = []
        
        # Check rules for table-specific filters
        for rule in self.rules_cache.get('general', []):
            # Handle both dict and string formats
            if isinstance(rule, str):
                continue  # Skip string rules
            if not isinstance(rule, dict):
                continue
            computation = rule.get('computation', {})
            if not isinstance(computation, dict):
                continue
            source_table = computation.get('source_table', '')
            
            if source_table.lower() == table_name.lower():
                filter_conditions = computation.get('filter_conditions', {})
                for filter_key, filter_value in filter_conditions.items():
                    # Parse filter_key (e.g., "table.column" or "column")
                    if '.' in filter_key:
                        table, column = filter_key.split('.', 1)
                    else:
                        table = source_table
                        column = filter_key
                    
                    filters.append({
                        'column': column,
                        'table': table,
                        'operator': '=',
                        'value': filter_value,
                        'reason': f"Knowledge register default filter (rule: {rule.get('id')})"
                    })
        
        return filters


# Global instance
_knowledge_register_rules: Optional[KnowledgeRegisterRules] = None


def get_knowledge_register_rules(metadata_dir: Optional[str] = None) -> KnowledgeRegisterRules:
    """Get or create global knowledge register rules instance."""
    global _knowledge_register_rules
    if _knowledge_register_rules is None:
        _knowledge_register_rules = KnowledgeRegisterRules(metadata_dir)
    return _knowledge_register_rules

