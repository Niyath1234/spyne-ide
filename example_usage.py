"""
Example Usage of Document Retrieval System

Demonstrates how to use the system programmatically.
"""

import os
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.ingest import DocumentIngester
from src.chunking import HierarchicalChunker
from src.vector_db import HybridVectorDB
from src.engine import DocumentRetrievalEngine


def example_full_pipeline():
    """Example: Complete pipeline from ingestion to querying."""
    
    print("="*60)
    print("Example: Full Pipeline")
    print("="*60)
    
    # Step 1: Ingest documents
    print("\n1. Ingesting documents...")
    ingester = DocumentIngester(
        raw_dir="data/raw",
        processed_dir="data/processed"
    )
    
    # Process all PDF files
    results = ingester.process_directory("*.pdf")
    print(f"   Processed {len(results)} files")
    
    # Step 2: Chunk documents
    print("\n2. Chunking documents...")
    chunker = HierarchicalChunker(processed_dir="data/processed")
    nodes = chunker.chunk_all()
    print(f"   Created {len(nodes)} chunks")
    
    # Save chunks for inspection
    chunker.save_chunks(nodes, "data/processed/chunks.json")
    
    # Step 3: Index documents
    print("\n3. Indexing documents...")
    vector_db = HybridVectorDB(index_name="technical-docs")
    vector_db.upsert_chunks(nodes)
    print("   Indexing complete")
    
    # Step 4: Query
    print("\n4. Querying documents...")
    engine = DocumentRetrievalEngine(vector_db=vector_db)
    
    questions = [
        "What are the authentication requirements?",
        "What does the TRD say about API versioning?",
        "Find requirement REQ-101"
    ]
    
    for question in questions:
        print(f"\n   Q: {question}")
        result = engine.query(question=question)
        print(f"   A: {result['answer'][:200]}...")
        print(f"   Citations: {len(result['citations'])}")


def example_single_query():
    """Example: Query existing indexed documents."""
    
    print("="*60)
    print("Example: Single Query")
    print("="*60)
    
    # Initialize engine (assumes documents are already indexed)
    engine = DocumentRetrievalEngine(index_name="technical-docs")
    
    # Query
    question = "What are the security requirements for user authentication?"
    result = engine.query(
        question=question,
        document_type="ARD"  # Filter by document type
    )
    
    print(f"\nQuestion: {question}")
    print(f"\nAnswer:\n{result['answer']}")
    
    print("\nCitations:")
    for citation in result['citations']:
        print(f"  {citation['ref']} {citation['source']}")


def example_hybrid_search_only():
    """Example: Use hybrid search without reranking/LLM."""
    
    print("="*60)
    print("Example: Hybrid Search Only")
    print("="*60)
    
    vector_db = HybridVectorDB(index_name="technical-docs")
    
    query = "API versioning requirements"
    results = vector_db.hybrid_search(
        query=query,
        top_k=10,
        document_type="TRD"  # Optional filter
    )
    
    print(f"\nQuery: {query}")
    print(f"\nFound {len(results)} results:\n")
    
    for i, result in enumerate(results, 1):
        print(f"{i}. Score: {result['score']:.4f}")
        print(f"   File: {result['file_name']}")
        print(f"   Header: {result['current_header']}")
        print(f"   Text: {result['text'][:150]}...")
        print()


def example_batch_processing():
    """Example: Process multiple documents in batch."""
    
    print("="*60)
    print("Example: Batch Processing")
    print("="*60)
    
    # Ingest multiple files
    ingester = DocumentIngester()
    
    # Process all PDF and Docx files
    pdf_results = ingester.process_directory("*.pdf")
    docx_results = ingester.process_directory("*.docx")
    
    print(f"\nProcessed {len(pdf_results)} PDF files")
    print(f"Processed {len(docx_results)} Docx files")
    
    # Chunk all
    chunker = HierarchicalChunker()
    all_nodes = chunker.chunk_all()
    
    # Get summary
    summary = chunker.get_chunk_summary(all_nodes)
    print("\nChunking Summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    # Index all chunks
    vector_db = HybridVectorDB()
    vector_db.upsert_chunks(all_nodes)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Example usage of document retrieval system")
    parser.add_argument(
        "--example",
        choices=["full", "query", "search", "batch"],
        default="query",
        help="Which example to run"
    )
    
    args = parser.parse_args()
    
    if args.example == "full":
        example_full_pipeline()
    elif args.example == "query":
        example_single_query()
    elif args.example == "search":
        example_hybrid_search_only()
    elif args.example == "batch":
        example_batch_processing()

