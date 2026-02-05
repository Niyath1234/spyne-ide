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
                from pathlib import Path
                import json
                
                metadata_dir = Path(__file__).parent.parent / "metadata"
                
                # Load metadata files from metadata directory
                metadata = {}
                
                # Load tables.json
                tables_file = metadata_dir / "tables.json"
                if tables_file.exists():
                    with open(tables_file, 'r') as f:
                        metadata['tables'] = json.load(f)
                    logger.info(f"Loaded tables from {tables_file}")
                
                # Load semantic_registry.json
                registry_file = metadata_dir / "semantic_registry.json"
                if registry_file.exists():
                    with open(registry_file, 'r') as f:
                        metadata['semantic_registry'] = json.load(f)
                    logger.info(f"Loaded semantic registry from {registry_file}")
                
                # Load lineage.json
                lineage_file = metadata_dir / "lineage.json"
                if lineage_file.exists():
                    with open(lineage_file, 'r') as f:
                        metadata['lineage'] = json.load(f)
                    logger.info(f"Loaded lineage from {lineage_file}")
                
                # Load knowledge_base.json
                kb_file = metadata_dir / "knowledge_base.json"
                if kb_file.exists():
                    with open(kb_file, 'r') as f:
                        metadata['knowledge_base'] = json.load(f)
                    logger.info(f"Loaded knowledge base from {kb_file}")
                
                # Load rules.json
                rules_file = metadata_dir / "rules.json"
                if rules_file.exists():
                    try:
                        with open(rules_file, 'r') as f:
                            rules_data = json.load(f)
                            # Handle both list and dict formats
                            if isinstance(rules_data, list):
                                metadata['rules'] = rules_data
                            elif isinstance(rules_data, dict) and 'rules' in rules_data:
                                metadata['rules'] = rules_data['rules']
                            else:
                                metadata['rules'] = []
                        logger.info(f"Loaded rules from {rules_file}")
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"Failed to parse rules.json: {e}, using empty list")
                        metadata['rules'] = []
                else:
                    metadata['rules'] = []
                
                # Ensure required keys exist
                if 'tables' not in metadata:
                    metadata['tables'] = {"tables": []}
                if 'semantic_registry' not in metadata:
                    metadata['semantic_registry'] = {"metrics": [], "dimensions": []}
                if 'lineage' not in metadata:
                    metadata['lineage'] = {"edges": []}
                if 'knowledge_base' not in metadata:
                    metadata['knowledge_base'] = {}
                if 'rules' not in metadata:
                    metadata['rules'] = []
                
                MetadataProvider._cache = metadata
                table_count = len(metadata.get('tables', {}).get('tables', []))
                edge_count = len(metadata.get('lineage', {}).get('edges', []))
                logger.info(f"Metadata loaded: {table_count} tables, {edge_count} relationships")
                
            except Exception as e:
                logger.error(f"Failed to load metadata: {e}", exc_info=True)
                MetadataProvider._cache = {
                    "semantic_registry": {"metrics": [], "dimensions": []},
                    "tables": {"tables": []},
                    "lineage": {"edges": []},
                    "knowledge_base": {},
                    "rules": []
                }
        return MetadataProvider._cache
    
    @staticmethod
    def clear_cache():
        """Clear cache (for testing or metadata updates)."""
        MetadataProvider._cache = None
        MetadataProvider.load.cache_clear()
        logger.info("Metadata cache cleared")

