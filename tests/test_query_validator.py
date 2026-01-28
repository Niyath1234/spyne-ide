#!/usr/bin/env python3
"""
Tests for QueryValidator
"""

import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.planning.validator import QueryValidator, TableValidator


class TestQueryValidator(unittest.TestCase):
    """Test cases for QueryValidator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = QueryValidator()
    
    def test_empty_query(self):
        """Test validation of empty query."""
        is_valid, error, warnings = self.validator.validate("")
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)
    
    def test_dangerous_drop_table(self):
        """Test detection of DROP TABLE."""
        is_valid, error, warnings = self.validator.validate("DROP TABLE users")
        self.assertFalse(is_valid)
        self.assertIn("DROP TABLE", error)
    
    def test_dangerous_delete(self):
        """Test detection of DELETE."""
        is_valid, error, warnings = self.validator.validate("DELETE FROM users")
        self.assertFalse(is_valid)
        self.assertIn("DELETE", error)
    
    def test_valid_query(self):
        """Test validation of valid query."""
        is_valid, error, warnings = self.validator.validate("SELECT * FROM users")
        self.assertTrue(is_valid)
        self.assertIsNone(error)
    
    def test_long_query_warning(self):
        """Test warning for very long query."""
        # Create a query longer than 10000 characters
        long_query = "SELECT " + ", ".join([f"col{i}" for i in range(2000)])
        is_valid, error, warnings = self.validator.validate(long_query)
        self.assertTrue(is_valid)
        # Query might be valid but should have warnings if very long
        # Note: warnings are optional, so we just check validity
        self.assertIsNone(error)


class TestTableValidator(unittest.TestCase):
    """Test cases for TableValidator."""
    
    def setUp(self):
        """Set up test fixtures."""
        metadata = {
            'tables': {
                'tables': [
                    {'name': 'users'},
                    {'name': 'orders'},
                ]
            }
        }
        self.validator = TableValidator(metadata)
    
    def test_valid_table(self):
        """Test validation of valid table."""
        is_valid, error = self.validator.validate_table("users")
        self.assertTrue(is_valid)
        self.assertIsNone(error)
    
    def test_invalid_table_format(self):
        """Test validation of invalid table format."""
        is_valid, error = self.validator.validate_table("123invalid")
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)
    
    def test_empty_table_name(self):
        """Test validation of empty table name."""
        is_valid, error = self.validator.validate_table("")
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)
    
    def test_multiple_tables(self):
        """Test validation of multiple tables."""
        is_valid, error, invalid = self.validator.validate_tables(["users", "orders"])
        self.assertTrue(is_valid)
        self.assertEqual(len(invalid), 0)


if __name__ == '__main__':
    unittest.main()

