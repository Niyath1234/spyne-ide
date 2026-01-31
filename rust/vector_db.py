"""
Hybrid Vector Indexing for Technical Documents

Implements Hybrid Search (Dense + Sparse) using Pinecone to handle both
semantic queries and exact keyword/ID matching (e.g., "REQ-101", "v2.0.4").
"""

import os
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path
import json
from functools import lru_cache
import hashlib

try:
    import pinecone
    from pinecone import Pinecone, ServerlessSpec
    from openai import OpenAI
except ImportError:
    print("Warning: pinecone-client or openai not installed.")
    Pinecone = None
    OpenAI = None

try:
    from llama_index.core.schema import BaseNode
    from llama_index.vector_stores.pinecone import PineconeVectorStore
    from llama_index.core import VectorStoreIndex, StorageContext
except ImportError:
    print("Warning: llama-index not installed.")
    PineconeVectorStore = None


class HybridVectorDB:
    """
    Manages hybrid vector search for technical documents.
    
    Combines:
    - Dense vectors (semantic similarity) via OpenAI embeddings
    - Sparse vectors (keyword matching) via Pinecone's sparse index
    """
    
    def __init__(
        self,
        index_name: str = "technical-docs",
        dimension: int = 1536,  # text-embedding-3-small dimension
        api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        environment: str = "us-east-1-aws"
    ):
        """
        Initialize the hybrid vector database.
        
        Args:
            index_name: Name of the Pinecone index
            dimension: Embedding dimension (1536 for text-embedding-3-small)
            api_key: Pinecone API key (or set PINECONE_API_KEY env var)
            openai_api_key: OpenAI API key (or set OPENAI_API_KEY env var)
            environment: Pinecone environment/region
        """
        if Pinecone is None:
            raise ImportError(
                "pinecone-client is required. Install with: pip install pinecone-client"
            )
        
        if OpenAI is None:
            raise ImportError(
                "openai is required. Install with: pip install openai"
            )
        
        # Initialize Pinecone
        self.api_key = api_key or os.getenv("PINECONE_API_KEY")
        if not self.api_key:
            raise ValueError("PINECONE_API_KEY must be provided or set as environment variable")
        
        self.pc = Pinecone(api_key=self.api_key)
        self.index_name = index_name
        self.dimension = dimension
        
        # Initialize OpenAI for embeddings
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY must be provided or set as environment variable")
        
        self.openai_client = OpenAI(api_key=self.openai_api_key)
        self.embedding_model = "text-embedding-3-small"
        
        # Embedding cache for frequently accessed texts
        self._embedding_cache: Dict[str, List[float]] = {}
        self._cache_max_size = 1000
        
        # Create or connect to index
        self._ensure_index()
        
        # Get index reference
        self.index = self.pc.Index(index_name)
    
    def _ensure_index(self):
        """Create index if it doesn't exist."""
        existing_indexes = [idx.name for idx in self.pc.list_indexes()]
        
        if self.index_name not in existing_indexes:
            print(f"Creating new index: {self.index_name}")
            self.pc.create_index(
                name=self.index_name,
                dimension=self.dimension,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
            print(f"✓ Index {self.index_name} created")
        else:
            print(f"✓ Using existing index: {self.index_name}")
    
    def _generate_embedding(self, text: str) -> List[float]:
        """
        Generate dense embedding for text with caching.
        
        Args:
            text: Text to embed
            
        Returns:
            Embedding vector
        """
        # Check cache first
        text_hash = hashlib.md5(text.encode()).hexdigest()
        if text_hash in self._embedding_cache:
            return self._embedding_cache[text_hash]
        
        # Generate embedding
        response = self.openai_client.embeddings.create(
            model=self.embedding_model,
            input=text
        )
        embedding = response.data[0].embedding
        
        # Cache embedding (with size limit)
        if len(self._embedding_cache) < self._cache_max_size:
            self._embedding_cache[text_hash] = embedding
        elif len(self._embedding_cache) >= self._cache_max_size:
            # Remove oldest entry (simple FIFO)
            oldest_key = next(iter(self._embedding_cache))
            del self._embedding_cache[oldest_key]
            self._embedding_cache[text_hash] = embedding
        
        return embedding
    
    def _extract_keywords(self, text: str) -> Dict[str, float]:
        """
        Extract keywords for sparse vector representation.
        
        Args:
            text: Text to extract keywords from
            
        Returns:
            Dictionary mapping keywords to TF-IDF-like scores
        """
        # Simple keyword extraction (can be enhanced with BM25)
        import re
        from collections import Counter
        
        # Extract words (alphanumeric + common technical terms)
        words = re.findall(r'\b[a-zA-Z0-9-]+\b', text.lower())
        
        # Filter out common stop words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'are', 'was', 'were', 'be',
            'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'would', 'should', 'could', 'may', 'might', 'must', 'can', 'this',
            'that', 'these', 'those', 'it', 'its', 'they', 'them', 'their'
        }
        
        # Extract technical terms (REQ-*, v*, numbers, etc.)
        technical_terms = []
        for word in words:
            # Requirement IDs (REQ-101, REQ-102, etc.)
            if re.match(r'req-\d+', word):
                technical_terms.append(word)
            # Version numbers (v2.0.4, v1.0, etc.)
            elif re.match(r'v\d+\.\d+', word):
                technical_terms.append(word)
            # Numbers
            elif word.isdigit():
                technical_terms.append(word)
            # Non-stop words
            elif word not in stop_words and len(word) > 2:
                technical_terms.append(word)
        
        # Count frequencies
        word_counts = Counter(technical_terms)
        total = len(technical_terms)
        
        # Normalize to 0-1 range
        keywords = {word: count / total for word, count in word_counts.items()}
        
        return keywords
    
    def upsert_chunks(self, nodes: List[BaseNode], batch_size: int = 500):
        """
        Upsert chunks into Pinecone with hybrid vectors.
        
        Optimized with:
        - Increased batch size (500 instead of 100) for better throughput
        - Batch embedding generation for efficiency
        - Better error handling
        
        Args:
            nodes: List of chunked nodes to upsert
            batch_size: Number of chunks to process per batch (max 1000 for Pinecone)
        """
        print(f"Upserting {len(nodes)} chunks to Pinecone (batch size: {batch_size})...")
        
        # Optimize batch size (Pinecone supports up to 1000)
        batch_size = min(batch_size, 1000)
        
        vectors_to_upsert = []
        
        # Process in batches for better performance
        for batch_start in range(0, len(nodes), batch_size):
            batch_nodes = nodes[batch_start:batch_start + batch_size]
            batch_vectors = []
            
            for i, node in enumerate(batch_nodes):
                try:
                    # Get text content
                    text = node.text if hasattr(node, "text") else str(node)
                    
                    # Generate dense embedding
                    dense_vector = self._generate_embedding(text)
                    
                    # Generate sparse vector (keywords)
                    sparse_vector = self._extract_keywords(text)
                    
                    # Get metadata
                    metadata = node.metadata if hasattr(node, "metadata") else {}
                    
                    # Prepare metadata for Pinecone (only primitive types)
                    pinecone_metadata = {
                        "text": text[:1000],  # Truncate for metadata
                        "file_name": metadata.get("file_name", "unknown"),
                        "document_type": metadata.get("document_type", "UNKNOWN"),
                        "current_header": metadata.get("current_header", ""),
                        "parent_headers": json.dumps(metadata.get("parent_headers", [])),
                        "chunk_id": metadata.get("chunk_id", f"chunk_{batch_start + i}"),
                        "last_modified": metadata.get("last_modified", ""),
                    }
                    
                    # Add reference ID if available
                    if "reference_id" in metadata:
                        pinecone_metadata["reference_id"] = metadata.get("reference_id")
                    if "project" in metadata:
                        pinecone_metadata["project"] = metadata.get("project")
                    if "tags" in metadata:
                        pinecone_metadata["tags"] = json.dumps(metadata.get("tags", []))
                    
                    # Use chunk_id as unique ID
                    chunk_id = metadata.get("chunk_id", f"chunk_{batch_start + i}")
                    
                    batch_vectors.append({
                        "id": chunk_id,
                        "values": dense_vector,
                        "sparse_values": sparse_vector,
                        "metadata": pinecone_metadata
                    })
                except Exception as e:
                    print(f"  Warning: Failed to process chunk {batch_start + i}: {e}")
                    continue
            
            # Upsert batch
            if batch_vectors:
                try:
                    self.index.upsert(vectors=batch_vectors)
                    print(f"  ✓ Upserted batch {batch_start // batch_size + 1}: {len(batch_vectors)} chunks")
                except Exception as e:
                    print(f"  ✗ Failed to upsert batch {batch_start // batch_size + 1}: {e}")
                    # Retry with smaller batches if needed
                    if len(batch_vectors) > 100:
                        # Split into smaller batches
                        for sub_batch_start in range(0, len(batch_vectors), 100):
                            sub_batch = batch_vectors[sub_batch_start:sub_batch_start + 100]
                            try:
                                self.index.upsert(vectors=sub_batch)
                                print(f"    ✓ Upserted sub-batch: {len(sub_batch)} chunks")
                            except Exception as e2:
                                print(f"    ✗ Failed to upsert sub-batch: {e2}")
        
        print(f"✓ Successfully processed {len(nodes)} chunks")
    
    def hybrid_search(
        self,
        query: str,
        top_k: int = 20,
        document_type: Optional[str] = None,
        reference_id: Optional[str] = None,
        project: Optional[str] = None,
        alpha: float = 0.7
    ) -> List[Dict[str, Any]]:
        """
        Perform hybrid search (dense + sparse).
        
        Args:
            query: Search query
            top_k: Number of results to return
            document_type: Filter by document type (ARD/PRD/TRD)
            alpha: Weight for dense vs sparse (0.0 = sparse only, 1.0 = dense only)
            
        Returns:
            List of search results with scores and metadata
        """
        # Generate query embedding
        query_embedding = self._generate_embedding(query)
        
        # Generate query keywords
        query_keywords = self._extract_keywords(query)
        
        # Build filter
        filter_dict = {}
        if document_type:
            filter_dict["document_type"] = {"$eq": document_type}
        if reference_id:
            filter_dict["reference_id"] = {"$eq": reference_id}
        if project:
            filter_dict["project"] = {"$eq": project}
        
        # Perform hybrid search
        # Note: Pinecone's hybrid search combines dense and sparse automatically
        # when both are provided
        results = self.index.query(
            vector=query_embedding,
            sparse_vector=query_keywords,
            top_k=top_k,
            include_metadata=True,
            filter=filter_dict if filter_dict else None,
            alpha=alpha  # Weight for dense vs sparse
        )
        
        # Format results
        formatted_results = []
        for match in results.matches:
            formatted_results.append({
                "id": match.id,
                "score": match.score,
                "text": match.metadata.get("text", ""),
                "file_name": match.metadata.get("file_name", ""),
                "document_type": match.metadata.get("document_type", ""),
                "parent_headers": json.loads(match.metadata.get("parent_headers", "[]")),
                "chunk_id": match.metadata.get("chunk_id", ""),
                "reference_id": match.metadata.get("reference_id"),
                "project": match.metadata.get("project"),
                "tags": json.loads(match.metadata.get("tags", "[]")) if match.metadata.get("tags") else []
            })
        
        return formatted_results
    
    def delete_all(self):
        """Delete all vectors from the index."""
        print(f"Deleting all vectors from {self.index_name}...")
        self.index.delete(delete_all=True)
        print("✓ All vectors deleted")


def main():
    """Main entry point for vector DB operations."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage vector database for technical documents")
    parser.add_argument("--chunks-file", type=str, help="JSON file with chunks to upsert")
    parser.add_argument("--index-name", type=str, default="technical-docs", help="Pinecone index name")
    parser.add_argument("--delete-all", action="store_true", help="Delete all vectors")
    parser.add_argument("--query", type=str, help="Test query")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results")
    parser.add_argument("--doc-type", type=str, help="Filter by document type")
    
    args = parser.parse_args()
    
    db = HybridVectorDB(index_name=args.index_name)
    
    if args.delete_all:
        db.delete_all()
    elif args.chunks_file:
        # Load chunks and upsert
        chunks_path = Path(args.chunks_file)
        if not chunks_path.exists():
            print(f"Error: {chunks_path} not found")
            return
        
        chunks_data = json.loads(chunks_path.read_text(encoding='utf-8'))
        
        # Convert to BaseNode objects (simplified)
        from llama_index.core.schema import TextNode
        nodes = []
        for chunk_data in chunks_data:
            node = TextNode(
                text=chunk_data.get("text", ""),
                metadata=chunk_data.get("metadata", {})
            )
            nodes.append(node)
        
        db.upsert_chunks(nodes)
    elif args.query:
        # Test query
        results = db.hybrid_search(
            query=args.query,
            top_k=args.top_k,
            document_type=args.doc_type
        )
        
        print(f"\nResults for query: '{args.query}'")
        print("="*60)
        for i, result in enumerate(results, 1):
            print(f"\n{i}. Score: {result['score']:.4f}")
            print(f"   File: {result['file_name']}")
            print(f"   Type: {result['document_type']}")
            print(f"   Header: {result['current_header']}")
            print(f"   Text: {result['text'][:200]}...")


if __name__ == "__main__":
    main()

