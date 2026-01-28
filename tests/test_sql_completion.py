#!/usr/bin/env python3
"""
Tests for SQL Completion Provider
"""

import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.sql_completion import SQLCompletionProvider


class TestSQLCompletionProvider(unittest.TestCase):
    """Test cases for SQL Completion Provider."""
    
    def setUp(self):
        """Set up test fixtures."""
        metadata = {
            'tables': {
                'tables': [
                    {
                        'name': 'users',
                        'columns': [
                            {'name': 'id'},
                            {'name': 'name'},
                            {'name': 'email'}
                        ]
                    },
                    {
                        'name': 'orders',
                        'columns': [
                            {'name': 'id'},
                            {'name': 'user_id'},
                            {'name': 'amount'}
                        ]
                    }
                ]
            },
            'semantic_registry': {
                'metrics': [
                    {'name': 'total_revenue'},
                    {'name': 'user_count'}
                ],
                'dimensions': [
                    {'name': 'date'},
                    {'name': 'region'}
                ]
            }
        }
        self.provider = SQLCompletionProvider(metadata)
    
    def test_suggest_tables(self):
        """Test table suggestions."""
        completions = self.provider._suggest_tables()
        self.assertGreater(len(completions), 0)
        self.assertIn('users', [c['label'] for c in completions])
    
    def test_suggest_columns(self):
        """Test column suggestions."""
        completions = self.provider._suggest_columns('users')
        self.assertGreater(len(completions), 0)
        self.assertIn('id', [c['label'] for c in completions])
    
    def test_suggest_metrics(self):
        """Test metric suggestions."""
        completions = self.provider._suggest_metrics()
        self.assertGreater(len(completions), 0)
        self.assertIn('total_revenue', [c['label'] for c in completions])
    
    def test_get_completions_after_from(self):
        """Test completions after FROM keyword."""
        text = "SELECT * FROM "
        completions = self.provider.get_completions(text, len(text))
        
        # Should suggest tables
        table_labels = [c['label'] for c in completions if c['kind'] == 'table']
        self.assertGreater(len(table_labels), 0)
    
    def test_get_completions_in_select(self):
        """Test completions in SELECT clause."""
        text = "SELECT "
        completions = self.provider.get_completions(text, len(text))
        
        # Should suggest columns, metrics, dimensions
        self.assertGreater(len(completions), 0)


if __name__ == '__main__':
    unittest.main()

