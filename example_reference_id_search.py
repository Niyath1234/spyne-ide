"""
Example: Searching by Reference ID

Demonstrates how to search documents using reference IDs
like PROJ-101, FEAT-202, etc.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.engine import DocumentRetrievalEngine
from src.document_mapper import DocumentMapper


def example_search_by_ref_id():
    """Example: Search by reference ID."""
    print("="*60)
    print("Example: Search by Reference ID")
    print("="*60)
    
    engine = DocumentRetrievalEngine()
    
    # Search only documents with reference ID PROJ-101
    result = engine.query(
        question="What are the authentication requirements?",
        reference_id="PROJ-101"
    )
    
    print("\nAnswer:")
    print(result['answer'])
    print(f"\nFound {result['retrieved_chunks']} chunks for PROJ-101")


def example_search_by_project():
    """Example: Search by project name."""
    print("\n" + "="*60)
    print("Example: Search by Project")
    print("="*60)
    
    engine = DocumentRetrievalEngine()
    
    # Search only "Authentication" project documents
    result = engine.query(
        question="What are the requirements?",
        project="Authentication"
    )
    
    print("\nAnswer:")
    print(result['answer'])


def example_get_all_docs_for_ref_id():
    """Example: Get all documents for a reference ID."""
    print("\n" + "="*60)
    print("Example: Get All Documents for Reference ID")
    print("="*60)
    
    mapper = DocumentMapper()
    
    # Get all documents for PROJ-101
    docs = mapper.get_documents_by_ref_id("PROJ-101")
    
    print(f"\nDocuments for PROJ-101:")
    for doc in docs:
        print(f"  - {doc['file_name']} ({doc['document_type']})")
        print(f"    Project: {doc.get('project', 'N/A')}")
        print(f"    Tags: {doc.get('tags', [])}")


def example_find_related_docs():
    """Example: Find related documents."""
    print("\n" + "="*60)
    print("Example: Find Related Documents")
    print("="*60)
    
    mapper = DocumentMapper()
    
    # Get related documents for a file
    related = mapper.get_related_documents("ARD_Authentication_v2.0.pdf")
    
    print(f"\nRelated documents:")
    for doc_name in related:
        ref_id = mapper.get_reference_id(doc_name)
        print(f"  - {doc_name} (Ref: {ref_id})")


def example_search_by_tag():
    """Example: Search by tag."""
    print("\n" + "="*60)
    print("Example: Search by Tag")
    print("="*60)
    
    mapper = DocumentMapper()
    engine = DocumentRetrievalEngine()
    
    # Find all reference IDs with tag "authentication"
    ref_ids = mapper.search_by_tag("authentication")
    
    print(f"\nReference IDs with tag 'authentication': {ref_ids}")
    
    # Query each one
    for ref_id in ref_ids:
        print(f"\n--- Querying {ref_id} ---")
        result = engine.query(
            question="What are the requirements?",
            reference_id=ref_id
        )
        print(f"Found {result['retrieved_chunks']} chunks")


def example_compare_ard_vs_prd():
    """Example: Compare ARD vs PRD for same reference ID."""
    print("\n" + "="*60)
    print("Example: Compare ARD vs PRD")
    print("="*60)
    
    engine = DocumentRetrievalEngine()
    ref_id = "PROJ-101"
    
    # Get ARD perspective
    print("\n--- ARD Perspective ---")
    ard_result = engine.query(
        question="What are the architecture requirements?",
        reference_id=ref_id,
        document_type="ARD"
    )
    print(ard_result['answer'][:300] + "...")
    
    # Get PRD perspective
    print("\n--- PRD Perspective ---")
    prd_result = engine.query(
        question="What are the product requirements?",
        reference_id=ref_id,
        document_type="PRD"
    )
    print(prd_result['answer'][:300] + "...")


def example_use_alias():
    """Example: Use aliases for easier searching."""
    print("\n" + "="*60)
    print("Example: Use Aliases")
    print("="*60)
    
    mapper = DocumentMapper()
    engine = DocumentRetrievalEngine()
    
    # Resolve alias to reference ID
    alias = "auth"
    ref_id = mapper.resolve_alias(alias)
    
    if ref_id:
        print(f"Alias '{alias}' resolves to reference ID: {ref_id}")
        
        result = engine.query(
            question="What are the requirements?",
            reference_id=ref_id
        )
        print(f"\nFound {result['retrieved_chunks']} chunks")
    else:
        print(f"Alias '{alias}' not found")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Reference ID search examples")
    parser.add_argument(
        "--example",
        choices=["ref-id", "project", "all-docs", "related", "tag", "compare", "alias", "all"],
        default="all",
        help="Which example to run"
    )
    
    args = parser.parse_args()
    
    if args.example == "ref-id" or args.example == "all":
        example_search_by_ref_id()
    
    if args.example == "project" or args.example == "all":
        example_search_by_project()
    
    if args.example == "all-docs" or args.example == "all":
        example_get_all_docs_for_ref_id()
    
    if args.example == "related" or args.example == "all":
        example_find_related_docs()
    
    if args.example == "tag" or args.example == "all":
        example_search_by_tag()
    
    if args.example == "compare" or args.example == "all":
        example_compare_ard_vs_prd()
    
    if args.example == "alias" or args.example == "all":
        example_use_alias()

