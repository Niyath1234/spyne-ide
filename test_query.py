"""
Interactive Query Test Script

Allows testing the document retrieval system with questions
and displays retrieved chunks, rerank scores, and final answers.
"""

import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Import modules
try:
    from engine import DocumentRetrievalEngine
    from vector_db import HybridVectorDB
except ImportError:
    # Fallback: try absolute import
    sys.path.insert(0, str(Path(__file__).parent))
    from src.engine import DocumentRetrievalEngine
    from src.vector_db import HybridVectorDB


def print_separator():
    """Print a visual separator."""
    print("\n" + "="*80 + "\n")


def print_chunk_details(chunk: dict, index: int):
    """Print detailed information about a chunk."""
    print(f"\nChunk {index}:")
    print(f"  Score: {chunk.get('score', 0):.4f}")
    if 'rerank_score' in chunk:
        print(f"  Rerank Score: {chunk.get('rerank_score', 0):.4f}")
    print(f"  File: {chunk.get('file_name', 'Unknown')}")
    print(f"  Document Type: {chunk.get('document_type', 'Unknown')}")
    
    parent_headers = chunk.get('parent_headers', [])
    if parent_headers:
        print(f"  Parent Headers: {' > '.join(parent_headers)}")
    
    current_header = chunk.get('current_header', '')
    if current_header:
        print(f"  Current Header: {current_header}")
    
    text = chunk.get('text', '')
    print(f"  Text Preview: {text[:200]}...")


def interactive_query():
    """Interactive query interface."""
    print("="*80)
    print("Technical Document Retrieval System")
    print("="*80)
    print("\nThis system retrieves answers from ARD/PRD/TRD documents.")
    print("Type 'quit' or 'exit' to stop.\n")
    
    # Initialize engine
    try:
        print("Initializing retrieval engine...")
        engine = DocumentRetrievalEngine()
        print("✓ Engine initialized\n")
    except Exception as e:
        print(f"✗ Error initializing engine: {e}")
        print("\nMake sure you have:")
        print("  - Set PINECONE_API_KEY environment variable")
        print("  - Set OPENAI_API_KEY environment variable")
        print("  - Set COHERE_API_KEY environment variable")
        print("  - Created and populated the Pinecone index")
        return
    
    while True:
        print_separator()
        
        # Get question
        question = input("Enter your question: ").strip()
        
        if question.lower() in ['quit', 'exit', 'q']:
            print("\nGoodbye!")
            break
        
        if not question:
            print("Please enter a question.")
            continue
        
        # Optional filters
        doc_type = input("Filter by document type (ARD/PRD/TRD, or press Enter for all): ").strip().upper()
        doc_type = doc_type if doc_type in ['ARD', 'PRD', 'TRD'] else None
        
        ref_id = input("Filter by reference ID (e.g., PROJ-101, or press Enter for all): ").strip()
        ref_id = ref_id if ref_id else None
        
        project = input("Filter by project name (or press Enter for all): ").strip()
        project = project if project else None
        
        try:
            # Query
            result = engine.query(
                question=question,
                document_type=doc_type,
                reference_id=ref_id,
                project=project
            )
            
            # Display results
            print_separator()
            print("ANSWER:")
            print_separator()
            print(result['answer'])
            
            # Display citations
            if result.get('citations'):
                print_separator()
                print("CITATIONS:")
                print_separator()
                for citation in result['citations']:
                    print(f"{citation['ref']} {citation['source']}")
            
            # Display statistics
            print_separator()
            print("STATISTICS:")
            print_separator()
            print(f"Retrieved chunks: {result.get('retrieved_chunks', 0)}")
            print(f"Reranked chunks: {result.get('reranked_chunks', 0)}")
            
            if result.get('rerank_scores'):
                print("\nRerank Scores:")
                for i, score in enumerate(result['rerank_scores'], 1):
                    print(f"  {i}. {score:.4f}")
        
        except Exception as e:
            print(f"\n✗ Error processing query: {e}")
            import traceback
            traceback.print_exc()


def test_single_query(question: str, doc_type: str = None, ref_id: str = None, project: str = None):
    """Test a single query (non-interactive)."""
    print(f"Question: {question}")
    if doc_type:
        print(f"Document Type Filter: {doc_type}")
    if ref_id:
        print(f"Reference ID Filter: {ref_id}")
    if project:
        print(f"Project Filter: {project}")
    print_separator()
    
    try:
        engine = DocumentRetrievalEngine()
        result = engine.query(
            question=question,
            document_type=doc_type,
            reference_id=ref_id,
            project=project
        )
        
        print("ANSWER:")
        print_separator()
        print(result['answer'])
        
        if result.get('citations'):
            print("\nCITATIONS:")
            print_separator()
            for citation in result['citations']:
                print(f"{citation['ref']} {citation['source']}")
        
        print("\nSTATISTICS:")
        print(f"Retrieved: {result.get('retrieved_chunks', 0)} chunks")
        print(f"Reranked: {result.get('reranked_chunks', 0)} chunks")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test document retrieval system")
    parser.add_argument("--question", type=str, help="Single question to test (non-interactive)")
    parser.add_argument("--doc-type", type=str, choices=['ARD', 'PRD', 'TRD'], help="Filter by document type")
    parser.add_argument("--ref-id", type=str, help="Filter by reference ID (e.g., PROJ-101)")
    parser.add_argument("--project", type=str, help="Filter by project name")
    
    args = parser.parse_args()
    
    if args.question:
        test_single_query(args.question, args.doc_type, args.ref_id, args.project)
    else:
        interactive_query()


if __name__ == "__main__":
    main()

