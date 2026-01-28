#!/usr/bin/env python3
"""
Tests for MetadataProvider
"""

import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.metadata_provider import MetadataProvider


class TestMetadataProvider(unittest.TestCase):
    """Test cases for MetadataProvider."""
    
    def test_load_metadata(self):
        """Test loading metadata."""
        metadata = MetadataProvider.load()
        self.assertIsInstance(metadata, dict)
        self.assertIn('tables', metadata)
        self.assertIn('semantic_registry', metadata)
    
    def test_metadata_caching(self):
        """Test that metadata is cached."""
        metadata1 = MetadataProvider.load()
        metadata2 = MetadataProvider.load()
        
        # Should be the same object (cached)
        self.assertIs(metadata1, metadata2)
    
    def test_metadata_structure(self):
        """Test metadata structure."""
        metadata = MetadataProvider.load()
        
        # Check tables structure
        tables = metadata.get('tables', {})
        self.assertIsInstance(tables, dict)
        
        # Check semantic registry structure
        semantic_registry = metadata.get('semantic_registry', {})
        self.assertIsInstance(semantic_registry, dict)
        self.assertIn('metrics', semantic_registry)
        self.assertIn('dimensions', semantic_registry)


if __name__ == '__main__':
    unittest.main()

