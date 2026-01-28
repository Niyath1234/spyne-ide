#!/usr/bin/env python3
"""
SQL Code Completion Provider

Provides intelligent SQL autocomplete suggestions based on metadata.
"""

from typing import List, Dict, Any, Optional, Tuple
import logging
import re

logger = logging.getLogger(__name__)


class SQLCompletionProvider:
    """Provides SQL code completion suggestions."""
    
    def __init__(self, metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize SQL completion provider.
        
        Args:
            metadata: Metadata containing tables, columns, metrics, dimensions
        """
        self.metadata = metadata or {}
        self._build_completion_index()
    
    def _build_completion_index(self):
        """Build index of completable items."""
        self.tables = []
        self.columns = {}
        self.metrics = []
        self.dimensions = []
        self.keywords = [
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING',
            'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN',
            'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DISTINCT', 'LIMIT'
        ]
        
        # Extract tables
        tables_data = self.metadata.get('tables', {}).get('tables', [])
        for table in tables_data:
            table_name = table.get('name') or table.get('table_name')
            if table_name:
                self.tables.append(table_name)
                
                # Extract columns
                columns = table.get('columns', [])
                if isinstance(columns, list):
                    self.columns[table_name] = [
                        col.get('name') or col.get('column_name') or str(col)
                        for col in columns
                    ]
        
        # Extract metrics and dimensions
        semantic_registry = self.metadata.get('semantic_registry', {})
        metrics = semantic_registry.get('metrics', [])
        for metric in metrics:
            metric_name = metric.get('name') or metric.get('metric_name')
            if metric_name:
                self.metrics.append(metric_name)
        
        dimensions = semantic_registry.get('dimensions', [])
        for dimension in dimensions:
            dim_name = dimension.get('name') or dimension.get('dimension_name')
            if dim_name:
                self.dimensions.append(dim_name)
    
    def get_completions(self, text: str, cursor_position: int) -> List[Dict[str, Any]]:
        """
        Get completion suggestions for current position.
        
        Args:
            text: SQL text
            cursor_position: Current cursor position
            
        Returns:
            List of completion suggestions with label, detail, and insertText
        """
        # Get current word/context
        line_start = text.rfind('\n', 0, cursor_position) + 1
        line_text = text[line_start:cursor_position]
        
        # Extract current word
        match = re.search(r'(\w+)$', line_text)
        current_word = match.group(1) if match else ""
        
        # Determine context
        context = self._analyze_context(text[:cursor_position])
        
        completions = []
        
        # Suggest based on context
        if context['in_select']:
            # Suggest columns, metrics, dimensions
            completions.extend(self._suggest_columns(context.get('current_table')))
            completions.extend(self._suggest_metrics())
            completions.extend(self._suggest_dimensions())
        
        if context['after_from']:
            # Suggest tables
            completions.extend(self._suggest_tables(current_word))
        
        if context['after_join']:
            # Suggest tables
            completions.extend(self._suggest_tables(current_word))
        
        if context['after_on']:
            # Suggest columns from joined tables
            completions.extend(self._suggest_columns(context.get('current_table')))
        
        # Always suggest keywords
        completions.extend(self._suggest_keywords(current_word))
        
        # Filter and sort by relevance
        filtered = [c for c in completions if current_word.lower() in c['label'].lower()]
        filtered.sort(key=lambda x: (
            not x['label'].lower().startswith(current_word.lower()),
            len(x['label'])
        ))
        
        return filtered[:50]  # Limit to 50 suggestions
    
    def _analyze_context(self, text: str) -> Dict[str, Any]:
        """Analyze SQL context to determine what to suggest."""
        text_upper = text.upper()
        
        context = {
            'in_select': 'SELECT' in text_upper,
            'after_from': bool(re.search(r'\bFROM\s+(\w+)?$', text_upper)),
            'after_join': bool(re.search(r'\bJOIN\s+(\w+)?$', text_upper)),
            'after_on': bool(re.search(r'\bON\s+(\w+)?$', text_upper)),
            'current_table': None
        }
        
        # Try to extract current table
        from_match = re.search(r'\bFROM\s+(\w+)', text_upper)
        if from_match:
            context['current_table'] = from_match.group(1).lower()
        
        return context
    
    def _suggest_tables(self, prefix: str = "") -> List[Dict[str, Any]]:
        """Suggest table names."""
        suggestions = []
        for table in self.tables:
            if not prefix or table.lower().startswith(prefix.lower()):
                suggestions.append({
                    'label': table,
                    'kind': 'table',
                    'detail': f'Table: {table}',
                    'insertText': table
                })
        return suggestions
    
    def _suggest_columns(self, table_name: Optional[str] = None, prefix: str = "") -> List[Dict[str, Any]]:
        """Suggest column names."""
        suggestions = []
        
        if table_name and table_name in self.columns:
            # Suggest columns from specific table
            for col in self.columns[table_name]:
                if not prefix or col.lower().startswith(prefix.lower()):
                    suggestions.append({
                        'label': col,
                        'kind': 'field',
                        'detail': f'Column: {table_name}.{col}',
                        'insertText': col
                    })
        else:
            # Suggest columns from all tables
            for table, cols in self.columns.items():
                for col in cols:
                    if not prefix or col.lower().startswith(prefix.lower()):
                        suggestions.append({
                            'label': f'{table}.{col}',
                            'kind': 'field',
                            'detail': f'Column: {table}.{col}',
                            'insertText': f'{table}.{col}'
                        })
        
        return suggestions
    
    def _suggest_metrics(self, prefix: str = "") -> List[Dict[str, Any]]:
        """Suggest metric names."""
        suggestions = []
        for metric in self.metrics:
            if not prefix or metric.lower().startswith(prefix.lower()):
                suggestions.append({
                    'label': metric,
                    'kind': 'function',
                    'detail': f'Metric: {metric}',
                    'insertText': metric
                })
        return suggestions
    
    def _suggest_dimensions(self, prefix: str = "") -> List[Dict[str, Any]]:
        """Suggest dimension names."""
        suggestions = []
        for dimension in self.dimensions:
            if not prefix or dimension.lower().startswith(prefix.lower()):
                suggestions.append({
                    'label': dimension,
                    'kind': 'variable',
                    'detail': f'Dimension: {dimension}',
                    'insertText': dimension
                })
        return suggestions
    
    def _suggest_keywords(self, prefix: str = "") -> List[Dict[str, Any]]:
        """Suggest SQL keywords."""
        suggestions = []
        for keyword in self.keywords:
            if not prefix or keyword.lower().startswith(prefix.lower()):
                suggestions.append({
                    'label': keyword,
                    'kind': 'keyword',
                    'detail': f'SQL keyword: {keyword}',
                    'insertText': keyword
                })
        return suggestions

