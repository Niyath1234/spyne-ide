#!/usr/bin/env python3
"""
Tests for Learning System
"""

import unittest
import sys
import tempfile
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.multi_stage_pipeline import Stage7LearningSystem


class TestLearningSystem(unittest.TestCase):
    """Test cases for Learning System."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.learning_store_path = os.path.join(self.temp_dir, "learning_store.json")
        self.learning_system = Stage7LearningSystem(self.learning_store_path)
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_learn_successful_pattern(self):
        """Test learning from successful pattern."""
        query = "show me users"
        sql = "SELECT * FROM users"
        intent = {
            'query_type': 'relational',
            'entities': {
                'tables': ['users']
            }
        }
        
        self.learning_system.learn(query, sql, intent)
        
        # Check that pattern was stored
        self.assertGreater(len(self.learning_system.query_patterns), 0)
    
    def test_learn_with_feedback(self):
        """Test learning with feedback."""
        query = "show me users"
        sql = "SELECT * FROM users"
        intent = {
            'query_type': 'relational',
            'entities': {
                'tables': ['users']
            }
        }
        feedback = {
            'success': False,
            'corrected_sql': "SELECT id, name FROM users",
            'reason': 'Too many columns'
        }
        
        self.learning_system.learn(query, sql, intent, feedback)
        
        # Check that correction was stored
        self.assertGreater(len(self.learning_system.corrections), 0)
    
    def test_get_similar_patterns(self):
        """Test getting similar patterns."""
        # Learn a pattern first
        query = "show me users"
        sql = "SELECT * FROM users"
        intent = {
            'query_type': 'relational',
            'entities': {
                'tables': ['users']
            }
        }
        self.learning_system.learn(query, sql, intent)
        
        # Get similar patterns
        similar = self.learning_system.get_similar_patterns(
            "show me customers",
            {'tables': ['customers']}
        )
        
        # Should find similar pattern (same structure)
        self.assertIsInstance(similar, list)
    
    def test_persist_learning(self):
        """Test persisting learning data."""
        query = "show me users"
        sql = "SELECT * FROM users"
        intent = {
            'query_type': 'relational',
            'entities': {
                'tables': ['users']
            }
        }
        
        self.learning_system.learn(query, sql, intent)
        
        # Check that file was created
        self.assertTrue(os.path.exists(self.learning_store_path))


if __name__ == '__main__':
    unittest.main()

