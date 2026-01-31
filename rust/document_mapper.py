"""
Document Reference ID Mapper

Maps documents to reference IDs and provides search capabilities
by reference ID, project, tags, etc.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime
import json
import re


class DocumentMapper:
    """
    Maps documents to reference IDs and provides search capabilities.
    
    Allows you to:
    - Map documents to your existing reference IDs (e.g., PROJ-101)
    - Search by reference ID
    - Find all documents related to a reference ID
    - Filter by project, tags, document type
    """
    
    def __init__(self, mapping_file: str = "config/document_mapping.yaml"):
        """
        Initialize the document mapper.
        
        Args:
            mapping_file: Path to YAML mapping file
        """
        self.mapping_file = Path(mapping_file)
        self.mapping = self._load_mapping()
        self._build_indexes()
    
    def _load_mapping(self) -> Dict:
        """Load mapping from YAML file."""
        if not self.mapping_file.exists():
            # Create default mapping file
            self._create_default_mapping()
        
        try:
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Warning: Could not load mapping file: {e}")
            return {"documents": [], "patterns": [], "aliases": {}}
    
    def _create_default_mapping(self):
        """Create default mapping file if it doesn't exist."""
        self.mapping_file.parent.mkdir(parents=True, exist_ok=True)
        default_content = """# Document to Reference ID Mapping
documents: []
patterns: []
aliases: {}
"""
        self.mapping_file.write_text(default_content, encoding='utf-8')
        print(f"Created default mapping file at {self.mapping_file}")
    
    def _build_indexes(self):
        """Build search indexes for fast lookup."""
        self.file_to_ref = {}  # file_name -> reference_id
        self.ref_to_files = {}  # reference_id -> [file_names]
        self.ref_to_docs = {}   # reference_id -> [doc_metadata]
        self.alias_to_ref = {}  # alias -> reference_id
        self.tag_index = {}     # tag -> [reference_ids]
        self.project_index = {} # project -> [reference_ids]
        
        # Build indexes from mapping
        documents = self.mapping.get("documents", [])
        for doc in documents:
            file_name = doc.get("file_name")
            ref_id = doc.get("reference_id")
            
            if not file_name or not ref_id:
                continue
            
            # File to reference mapping
            self.file_to_ref[file_name] = ref_id
            
            # Reference to files mapping
            if ref_id not in self.ref_to_files:
                self.ref_to_files[ref_id] = []
            self.ref_to_files[ref_id].append(file_name)
            
            # Reference to document metadata
            if ref_id not in self.ref_to_docs:
                self.ref_to_docs[ref_id] = []
            self.ref_to_docs[ref_id].append(doc)
            
            # Tag index
            tags = doc.get("tags", [])
            for tag in tags:
                if tag not in self.tag_index:
                    self.tag_index[tag] = set()
                self.tag_index[tag].add(ref_id)
            
            # Project index
            project = doc.get("project")
            if project:
                if project not in self.project_index:
                    self.project_index[project] = set()
                self.project_index[project].add(ref_id)
        
        # Build alias index
        aliases = self.mapping.get("aliases", {})
        for alias, ref_id in aliases.items():
            self.alias_to_ref[alias.lower()] = ref_id
    
    def get_reference_id(self, file_name: str) -> Optional[str]:
        """
        Get reference ID for a file.
        
        Args:
            file_name: Name of the file
            
        Returns:
            Reference ID if found, None otherwise
        """
        return self.file_to_ref.get(file_name)
    
    def get_documents_by_ref_id(self, reference_id: str) -> List[Dict]:
        """
        Get all documents for a reference ID.
        
        Args:
            reference_id: Reference ID (e.g., "PROJ-101")
            
        Returns:
            List of document metadata
        """
        return self.ref_to_docs.get(reference_id, [])
    
    def get_files_by_ref_id(self, reference_id: str) -> List[str]:
        """
        Get all file names for a reference ID.
        
        Args:
            reference_id: Reference ID
            
        Returns:
            List of file names
        """
        return self.ref_to_files.get(reference_id, [])
    
    def resolve_alias(self, alias: str) -> Optional[str]:
        """
        Resolve an alias to a reference ID.
        
        Args:
            alias: Alias name (e.g., "auth")
            
        Returns:
            Reference ID if found
        """
        return self.alias_to_ref.get(alias.lower())
    
    def search_by_tag(self, tag: str) -> List[str]:
        """
        Find reference IDs by tag.
        
        Args:
            tag: Tag name
            
        Returns:
            List of reference IDs
        """
        return list(self.tag_index.get(tag, set()))
    
    def search_by_project(self, project: str) -> List[str]:
        """
        Find reference IDs by project name.
        
        Args:
            project: Project name
            
        Returns:
            List of reference IDs
        """
        return list(self.project_index.get(project, set()))
    
    def get_related_documents(self, file_name: str) -> List[str]:
        """
        Get related document file names.
        
        Args:
            file_name: Name of the file
            
        Returns:
            List of related file names
        """
        documents = self.mapping.get("documents", [])
        for doc in documents:
            if doc.get("file_name") == file_name:
                return doc.get("related_docs", [])
        return []
    
    def enhance_metadata(self, file_name: str, base_metadata: Dict) -> Dict:
        """
        Enhance document metadata with mapping information.
        
        Args:
            file_name: Name of the file
            base_metadata: Base metadata from ingestion
            
        Returns:
            Enhanced metadata with reference ID and mapping info
        """
        enhanced = base_metadata.copy()
        
        # Find document in mapping
        documents = self.mapping.get("documents", [])
        doc_info = None
        for doc in documents:
            if doc.get("file_name") == file_name:
                doc_info = doc
                break
        
        if doc_info:
            enhanced["reference_id"] = doc_info.get("reference_id")
            enhanced["project"] = doc_info.get("project")
            enhanced["tags"] = doc_info.get("tags", [])
            enhanced["description"] = doc_info.get("description")
            enhanced["related_docs"] = doc_info.get("related_docs", [])
            enhanced["version"] = doc_info.get("version")
        else:
            # Try to infer reference ID from filename patterns
            ref_id = self._infer_reference_id(file_name)
            if ref_id:
                enhanced["reference_id"] = ref_id
                enhanced["inferred"] = True
        
        return enhanced
    
    def _infer_reference_id(self, file_name: str) -> Optional[str]:
        """
        Try to infer reference ID from filename using patterns.
        
        Args:
            file_name: Name of the file
            
        Returns:
            Inferred reference ID or None
        """
        # Check if filename contains a reference ID pattern
        patterns = self.mapping.get("patterns", [])
        for pattern_info in patterns:
            pattern = pattern_info.get("pattern", "")
            matches = re.findall(pattern, file_name)
            if matches:
                return matches[0]  # Return first match
        
        return None
    
    def get_all_reference_ids(self) -> List[str]:
        """Get all reference IDs in the system."""
        return list(self.ref_to_files.keys())
    
    def search(self, query: str) -> Dict:
        """
        Search for documents by query.
        Supports reference ID, alias, tag, project name.
        
        Args:
            query: Search query
            
        Returns:
            Dictionary with search results
        """
        results = {
            "query": query,
            "reference_ids": [],
            "files": [],
            "documents": []
        }
        
        query_lower = query.lower()
        
        # Check if it's a reference ID
        if query in self.ref_to_files:
            results["reference_ids"].append(query)
            results["files"].extend(self.ref_to_files[query])
            results["documents"].extend(self.ref_to_docs[query])
        
        # Check if it's an alias
        elif query_lower in self.alias_to_ref:
            ref_id = self.alias_to_ref[query_lower]
            results["reference_ids"].append(ref_id)
            results["files"].extend(self.ref_to_files.get(ref_id, []))
            results["documents"].extend(self.ref_to_docs.get(ref_id, []))
        
        # Check if it's a tag
        elif query_lower in self.tag_index:
            ref_ids = list(self.tag_index[query_lower])
            results["reference_ids"].extend(ref_ids)
            for ref_id in ref_ids:
                results["files"].extend(self.ref_to_files.get(ref_id, []))
                results["documents"].extend(self.ref_to_docs.get(ref_id, []))
        
        # Check if it's a project name
        elif query in self.project_index:
            ref_ids = list(self.project_index[query])
            results["reference_ids"].extend(ref_ids)
            for ref_id in ref_ids:
                results["files"].extend(self.ref_to_files.get(ref_id, []))
                results["documents"].extend(self.ref_to_docs.get(ref_id, []))
        
        # Remove duplicates
        results["reference_ids"] = list(set(results["reference_ids"]))
        results["files"] = list(set(results["files"]))
        
        return results


def main():
    """Test the document mapper."""
    mapper = DocumentMapper()
    
    print("="*60)
    print("Document Mapper Test")
    print("="*60)
    
    # Get all reference IDs
    print(f"\nAll Reference IDs: {mapper.get_all_reference_ids()}")
    
    # Test search
    test_queries = ["PROJ-101", "auth", "authentication", "payment"]
    
    for query in test_queries:
        print(f"\nSearch: '{query}'")
        results = mapper.search(query)
        print(f"  Reference IDs: {results['reference_ids']}")
        print(f"  Files: {results['files']}")
    
    # Test getting documents by ref ID
    ref_id = "PROJ-101"
    print(f"\nDocuments for {ref_id}:")
    docs = mapper.get_documents_by_ref_id(ref_id)
    for doc in docs:
        print(f"  - {doc.get('file_name')} ({doc.get('document_type')})")


if __name__ == "__main__":
    main()





