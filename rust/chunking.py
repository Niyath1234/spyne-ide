"""
Hierarchical Chunking for Technical Documents

Uses Markdown-aware splitting to preserve parent-child relationships
between requirements, features, and sub-requirements.
"""

import os
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime
import json

try:
    from llama_index.core.node_parser import MarkdownElementNodeParser
    from llama_index.core.schema import Document, NodeWithScore, BaseNode
    from llama_index.core import SimpleDirectoryReader
except ImportError:
    print("Warning: llama-index not installed. Install with: pip install llama-index")
    MarkdownElementNodeParser = None


class HierarchicalChunker:
    """
    Chunks markdown documents while preserving hierarchical structure.
    
    Each chunk maintains a reference to its parent header, ensuring
    that sub-requirements are never separated from their parent features.
    """
    
    def __init__(
        self,
        processed_dir: str = "data/processed",
        chunk_size: int = 1024,
        chunk_overlap: int = 200
    ):
        """
        Initialize the hierarchical chunker.
        
        Args:
            processed_dir: Directory containing processed markdown files
            chunk_size: Target chunk size in characters
            chunk_overlap: Overlap between chunks
        """
        if MarkdownElementNodeParser is None:
            raise ImportError(
                "llama-index is required. Install with: pip install llama-index"
            )
        
        self.processed_dir = Path(processed_dir)
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        
        # Initialize markdown parser
        self.parser = MarkdownElementNodeParser(
            llm=None,  # We don't need LLM for parsing
            num_workers=4
        )
    
    def load_metadata(self, file_path: Path) -> Dict:
        """Load metadata JSON file."""
        metadata_path = file_path.parent / f"{file_path.stem}_metadata.json"
        if metadata_path.exists():
            return json.loads(metadata_path.read_text(encoding='utf-8'))
        return {}
    
    def extract_hierarchy(self, node: BaseNode) -> Dict[str, Any]:
        """
        Extract hierarchical information from a node.
        
        Args:
            node: The node to extract hierarchy from
            
        Returns:
            Dictionary with hierarchy information
        """
        hierarchy = {
            "parent_headers": [],
            "current_header": None,
            "level": 0
        }
        
        # Extract metadata from node
        if hasattr(node, "metadata"):
            metadata = node.metadata
            
            # Get parent headers from metadata
            if "parent_header" in metadata:
                hierarchy["parent_headers"] = metadata.get("parent_header", [])
            
            if "header" in metadata:
                hierarchy["current_header"] = metadata.get("header")
            
            if "level" in metadata:
                hierarchy["level"] = metadata.get("level", 0)
        
        return hierarchy
    
    def chunk_file(self, file_path: Path) -> List[BaseNode]:
        """
        Chunk a single markdown file while preserving hierarchy.
        
        Args:
            file_path: Path to the markdown file
            
        Returns:
            List of chunked nodes with hierarchy metadata
        """
        print(f"Chunking {file_path.name}...")
        
        # Load metadata
        metadata = self.load_metadata(file_path)
        
        # Read markdown file
        reader = SimpleDirectoryReader(
            input_files=[str(file_path)],
            file_metadata=lambda x: metadata
        )
        documents = reader.load_data()
        
        # Parse into nodes
        nodes = self.parser.get_nodes_from_documents(documents)
        
        # Enhance nodes with hierarchy and metadata
        enhanced_nodes = []
        parent_stack = []  # Stack to track parent headers
        
        for node in nodes:
            # Extract current header level
            node_metadata = node.metadata if hasattr(node, "metadata") else {}
            header = node_metadata.get("header", "")
            level = node_metadata.get("level", 0)
            
            # Update parent stack based on header level
            while parent_stack and parent_stack[-1]["level"] >= level:
                parent_stack.pop()
            
            # Add current header to stack if it exists
            if header:
                parent_stack.append({"header": header, "level": level})
            
            # Build parent headers list
            parent_headers = [p["header"] for p in parent_stack[:-1]]  # Exclude self
            
            # Enhance node metadata
            enhanced_metadata = {
                **node_metadata,
                **metadata,  # Add file metadata
                "parent_headers": parent_headers,
                "current_header": header,
                "header_level": level,
                "chunk_id": f"{file_path.stem}_{len(enhanced_nodes)}",
                "file_name": file_path.name,
                "chunked_at": datetime.now().isoformat()
            }
            
            # Update node metadata
            if hasattr(node, "metadata"):
                node.metadata = enhanced_metadata
            else:
                node.metadata = enhanced_metadata
            
            enhanced_nodes.append(node)
        
        print(f"✓ Created {len(enhanced_nodes)} chunks from {file_path.name}")
        return enhanced_nodes
    
    def chunk_all(self) -> List[BaseNode]:
        """
        Chunk all markdown files in the processed directory.
        
        Returns:
            List of all chunked nodes
        """
        markdown_files = list(self.processed_dir.glob("*.md"))
        
        if not markdown_files:
            print(f"No markdown files found in {self.processed_dir}")
            return []
        
        all_nodes = []
        for file_path in markdown_files:
            try:
                nodes = self.chunk_file(file_path)
                all_nodes.extend(nodes)
            except Exception as e:
                print(f"✗ Error chunking {file_path.name}: {e}")
        
        print(f"\n✓ Total chunks created: {len(all_nodes)}")
        return all_nodes
    
    def save_chunks(self, nodes: List[BaseNode], output_file: str = "data/processed/chunks.json"):
        """
        Save chunks to JSON file for inspection.
        
        Args:
            nodes: List of chunked nodes
            output_file: Path to save chunks
        """
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        chunks_data = []
        for node in nodes:
            chunk_data = {
                "id": node.node_id if hasattr(node, "node_id") else None,
                "text": node.text if hasattr(node, "text") else str(node),
                "metadata": node.metadata if hasattr(node, "metadata") else {},
                "parent_headers": node.metadata.get("parent_headers", []) if hasattr(node, "metadata") else [],
                "current_header": node.metadata.get("current_header") if hasattr(node, "metadata") else None
            }
            chunks_data.append(chunk_data)
        
        output_path.write_text(json.dumps(chunks_data, indent=2), encoding='utf-8')
        print(f"✓ Saved {len(chunks_data)} chunks to {output_path}")
    
    def get_chunk_summary(self, nodes: List[BaseNode]) -> Dict:
        """
        Get summary statistics about chunks.
        
        Args:
            nodes: List of chunked nodes
            
        Returns:
            Dictionary with summary statistics
        """
        doc_types = {}
        total_chars = 0
        
        for node in nodes:
            metadata = node.metadata if hasattr(node, "metadata") else {}
            doc_type = metadata.get("document_type", "UNKNOWN")
            doc_types[doc_type] = doc_types.get(doc_type, 0) + 1
            
            if hasattr(node, "text"):
                total_chars += len(node.text)
        
        return {
            "total_chunks": len(nodes),
            "chunks_by_type": doc_types,
            "total_characters": total_chars,
            "avg_chunk_size": total_chars / len(nodes) if nodes else 0
        }


def main():
    """Main entry point for chunking."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Chunk technical documents hierarchically")
    parser.add_argument("--file", type=str, help="Chunk a specific markdown file")
    parser.add_argument("--processed-dir", type=str, default="data/processed", help="Processed documents directory")
    parser.add_argument("--save", action="store_true", help="Save chunks to JSON file")
    parser.add_argument("--output", type=str, default="data/processed/chunks.json", help="Output file for chunks")
    
    args = parser.parse_args()
    
    chunker = HierarchicalChunker(processed_dir=args.processed_dir)
    
    if args.file:
        # Chunk single file
        file_path = Path(args.file)
        nodes = chunker.chunk_file(file_path)
        print(f"\n✓ Created {len(nodes)} chunks")
        
        if args.save:
            chunker.save_chunks(nodes, args.output)
    else:
        # Chunk all files
        nodes = chunker.chunk_all()
        
        # Print summary
        summary = chunker.get_chunk_summary(nodes)
        print("\n" + "="*50)
        print("Chunking Summary:")
        print("="*50)
        for key, value in summary.items():
            print(f"{key}: {value}")
        
        if args.save:
            chunker.save_chunks(nodes, args.output)


if __name__ == "__main__":
    main()





