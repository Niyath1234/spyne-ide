"""
Knowledge Register Synchronization

Syncs extracted knowledge from Confluence documents to the Knowledge Register
format used by the Rust-based system for vector search.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime


class KnowledgeRegisterSyncer:
    """
    Syncs knowledge to Knowledge Register format.
    
    Knowledge Register Structure (Rust):
    {
        "pages": {
            "ref_id": {
                "page_id": "ref_id",
                "node_ref_id": "ref_id",
                "title": "...",
                "full_text": "...",
                "keywords": ["..."],
                "segments": {
                    "entities": [...],
                    "events": [...],
                    "tables": [...]
                }
            }
        },
        "search_index": {
            "keyword": ["ref_id1", "ref_id2"]
        }
    }
    """
    
    def __init__(self, knowledge_register_path: str = "metadata/knowledge_register.json"):
        """
        Initialize syncer.
        
        Args:
            knowledge_register_path: Path to knowledge register JSON
        """
        self.knowledge_register_path = Path(knowledge_register_path)
        self.knowledge_register_path.parent.mkdir(parents=True, exist_ok=True)
        self.knowledge_register = self._load_knowledge_register()
    
    def _load_knowledge_register(self) -> Dict:
        """Load knowledge register."""
        if self.knowledge_register_path.exists():
            try:
                with open(self.knowledge_register_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load knowledge register: {e}")
        
        return {
            "pages": {},
            "search_index": {}
        }
    
    def add_knowledge_page(
        self,
        ref_id: str,
        title: str,
        full_text: str,
        keywords: List[str],
        segments: Optional[Dict] = None,
        metadata: Optional[Dict] = None
    ):
        """
        Add a knowledge page to the register.
        
        Args:
            ref_id: Reference ID (page_id)
            title: Page title
            full_text: Full text content
            keywords: List of keywords
            segments: Segments dictionary (entities, events, tables, etc.)
            metadata: Additional metadata
        """
        knowledge_page = {
            "page_id": ref_id,
            "node_ref_id": ref_id,
            "title": title,
            "full_text": full_text,
            "keywords": keywords,
            "segments": segments or {},
            "metadata": metadata or {}
        }
        
        # Add to pages
        self.knowledge_register.setdefault("pages", {})[ref_id] = knowledge_page
        
        # Update search index
        self._update_search_index(ref_id, keywords, full_text)
    
    def _update_search_index(self, ref_id: str, keywords: List[str], full_text: str):
        """Update search index."""
        import re
        
        search_index = self.knowledge_register.setdefault("search_index", {})
        
        # Index keywords
        for keyword in keywords:
            keyword_lower = keyword.lower()
            if keyword_lower not in search_index:
                search_index[keyword_lower] = []
            if ref_id not in search_index[keyword_lower]:
                search_index[keyword_lower].append(ref_id)
        
        # Index words from full text
        words = re.findall(r'\b\w+\b', full_text.lower())
        for word in words:
            if len(word) > 3:  # Only index words longer than 3 characters
                if word not in search_index:
                    search_index[word] = []
                if ref_id not in search_index[word]:
                    search_index[word].append(ref_id)
    
    def save(self):
        """Save knowledge register to file."""
        with open(self.knowledge_register_path, 'w', encoding='utf-8') as f:
            json.dump(self.knowledge_register, f, indent=2, ensure_ascii=False)
    
    def get_page(self, ref_id: str) -> Optional[Dict]:
        """Get a knowledge page by ref_id."""
        return self.knowledge_register.get("pages", {}).get(ref_id)
    
    def search(self, query: str) -> List[str]:
        """
        Search for pages by query.
        
        Args:
            query: Search query
            
        Returns:
            List of ref_ids matching the query
        """
        query_lower = query.lower()
        search_index = self.knowledge_register.get("search_index", {})
        
        matching_ref_ids = set()
        
        # Direct keyword match
        if query_lower in search_index:
            matching_ref_ids.update(search_index[query_lower])
        
        # Partial match
        for keyword, ref_ids in search_index.items():
            if query_lower in keyword or keyword in query_lower:
                matching_ref_ids.update(ref_ids)
        
        return list(matching_ref_ids)
    
    def get_statistics(self) -> Dict:
        """Get statistics about the knowledge register."""
        pages = self.knowledge_register.get("pages", {})
        search_index = self.knowledge_register.get("search_index", {})
        
        total_entities = 0
        total_events = 0
        total_tables = 0
        
        for page in pages.values():
            segments = page.get("segments", {})
            total_entities += len(segments.get("entities", []))
            total_events += len(segments.get("events", []))
            total_tables += len(segments.get("tables", []))
        
        return {
            "total_pages": len(pages),
            "total_keywords": len(search_index),
            "total_entities": total_entities,
            "total_events": total_events,
            "total_tables": total_tables
        }





