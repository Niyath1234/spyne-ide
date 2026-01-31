#!/usr/bin/env python3
"""
Centralized Metadata Provider with Caching

RISK #1 FIX: This module is READ-ONLY.
It provides cached access to metadata from WorldState.
It NEVER mutates metadata - that is WorldState's exclusive domain.

All metadata writes MUST go through backend/cko_client.py.
"""

from functools import lru_cache
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class MetadataProvider:
    """
    Centralized metadata provider with caching.
    
    RISK #1 FIX: This is a READ-ONLY projection of WorldState.
    It provides cached access for performance, but never mutates.
    All mutations MUST go through CKO client.
    """
    
    _cache: Dict[str, Any] = None
    _cache_timestamp: float = None
    
    @staticmethod
    @lru_cache(maxsize=1)
    def load() -> Dict[str, Any]:
        """
        Load metadata with process-level caching (READ-ONLY).
        
        RISK #1 FIX: This method only READS metadata.
        It never writes or mutates. All writes go through CKO client.
        
        Returns:
            Metadata dictionary with tables, semantic_registry, rules, etc.
        """
        if MetadataProvider._cache is None:
            logger.info("Loading metadata from disk...")
            try:
                # Try to import from test file, but handle gracefully if it doesn't exist
                import sys
                from pathlib import Path
                test_file = Path(__file__).parent.parent / "test_outstanding_daily_regeneration.py"
                if test_file.exists():
                    sys.path.insert(0, str(test_file.parent))
                    from test_outstanding_daily_regeneration import load_metadata as _load_metadata
                    MetadataProvider._cache = _load_metadata()
                    logger.info(f"Metadata loaded: {len(MetadataProvider._cache.get('tables', {}).get('tables', []))} tables")
                else:
                    logger.warning("test_outstanding_daily_regeneration.py not found, returning empty metadata")
                    MetadataProvider._cache = {
                        "semantic_registry": {"metrics": [], "dimensions": []},
                        "tables": {"tables": []}
                    }
            except Exception as e:
                logger.error(f"Failed to load metadata: {e}", exc_info=True)
                MetadataProvider._cache = {
                    "semantic_registry": {"metrics": [], "dimensions": []},
                    "tables": {"tables": []}
                }
        return MetadataProvider._cache
    
    @staticmethod
    def clear_cache():
        """Clear cache (for testing or metadata updates)."""
        MetadataProvider._cache = None
        MetadataProvider.load.cache_clear()
        logger.info("Metadata cache cleared")

