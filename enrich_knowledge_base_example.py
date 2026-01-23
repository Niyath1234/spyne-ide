#!/usr/bin/env python3
"""
Example script demonstrating knowledge base enrichment
"""

from knowledge_base_enricher import KnowledgeBaseEnricher
from pathlib import Path
import json

def main():
    print("=" * 60)
    print("Knowledge Base Enrichment Example")
    print("=" * 60)
    
    # Initialize enricher
    enricher = KnowledgeBaseEnricher()
    
    # Example document paths (using examples directory)
    examples_dir = Path("examples")
    
    prd_path = examples_dir / "prd_example.json"
    ard_path = examples_dir / "ard_example.json"
    trd_path = examples_dir / "trd_example.json"
    er_path = examples_dir / "er_diagram_example.json"
    
    print("\n📚 Enriching knowledge base from documents...")
    print(f"  - PRD: {prd_path}")
    print(f"  - ARD: {ard_path}")
    print(f"  - TRD: {trd_path}")
    print(f"  - ER Diagram: {er_path}")
    
    # Enrich knowledge base
    results = enricher.enrich_from_documents(
        prd_path=prd_path if prd_path.exists() else None,
        ard_path=ard_path if ard_path.exists() else None,
        trd_path=trd_path if trd_path.exists() else None,
        er_diagram_path=er_path if er_path.exists() else None
    )
    
    print("\n✅ Enrichment Results:")
    print(f"  - Processed documents: {len(results['processed'])}")
    print(f"  - Added terms: {results['added_terms']}")
    print(f"  - Added tables: {results['added_tables']}")
    print(f"  - Added relationships: {results['added_relationships']}")
    print(f"  - Added joins: {results['added_joins']}")
    
    if results['errors']:
        print(f"\n⚠️  Errors: {len(results['errors'])}")
        for error_type, error_msg in results['errors']:
            print(f"  - {error_type}: {error_msg}")
    
    # Display join information
    print("\n🔗 Join Information:")
    joins = enricher.list_all_joins()
    for join in joins[:5]:  # Show first 5 joins
        print(f"  - {join['from_table']} → {join['to_table']}")
        print(f"    Type: {join.get('join_type', 'unknown')}")
        print(f"    Keys: {join.get('keys', {})}")
        print(f"    Source: {join.get('source', 'unknown')}")
        print()
    
    if len(joins) > 5:
        print(f"  ... and {len(joins) - 5} more joins")
    
    # Test join lookup
    print("\n🔍 Testing Join Lookup:")
    test_joins = [
        ("customer_accounts", "loans"),
        ("loans", "loan_transactions")
    ]
    
    for table1, table2 in test_joins:
        join_info = enricher.get_join_information(table1, table2)
        if join_info:
            print(f"  ✅ Found join: {table1} → {table2}")
            print(f"     Keys: {join_info.get('keys', {})}")
        else:
            print(f"  ❌ No join found: {table1} → {table2}")
    
    print(f"\n💾 Knowledge base saved to: {enricher.kb_path}")
    print("=" * 60)


if __name__ == '__main__':
    main()

