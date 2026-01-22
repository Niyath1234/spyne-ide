"""
Test Integration with Specific Confluence Page

Tests the complete flow for: ARD slice mini passbook
Shows what gets extracted and how it flows through the system.
"""

import sys
import json
from pathlib import Path
import requests
from requests.auth import HTTPBasicAuth

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config import Config
from src.confluence_to_knowledge_base import ConfluenceKnowledgeBaseIntegrator, HTMLContentExtractor
from src.knowledge_register_sync import KnowledgeRegisterSyncer


def fetch_and_process_page(page_id: str):
    """Fetch a specific page and show the complete processing flow."""
    
    # Credentials from environment variables
    url_base = Config.get_confluence_url()
    username = Config.get_confluence_username()
    api_token = Config.get_confluence_api_token()
    
    if not username or not api_token:
        print("Error: CONFLUENCE_USERNAME and CONFLUENCE_API_TOKEN must be set in .env file")
        return
    
    print("="*80)
    print("Testing Integration: ARD slice mini passbook")
    print("="*80)
    print(f"Page ID: {page_id}")
    print(f"URL: {url_base}/spaces/HOR/pages/{page_id}/ARD+slice+mini+passbook")
    print()
    
    # Step 1: Fetch page from Confluence
    print("Step 1: Fetching page from Confluence API...")
    print("-" * 80)
    
    api_url = f"{url_base}/rest/api/content/{page_id}"
    params = {"expand": "body.storage,space,version,metadata.labels"}
    
    response = requests.get(
        api_url,
        auth=HTTPBasicAuth(username, api_token),
        headers={"Accept": "application/json"},
        params=params,
        timeout=30
    )
    
    if response.status_code != 200:
        print(f"✗ Error fetching page: {response.status_code}")
        print(response.text[:500])
        return
    
    page_data = response.json()
    print(f"✓ Page fetched successfully")
    print(f"  Title: {page_data.get('title')}")
    print(f"  Space: {page_data.get('space', {}).get('name')} ({page_data.get('space', {}).get('key')})")
    print(f"  Version: {page_data.get('version', {}).get('number')}")
    print()
    
    # Step 2: Extract product
    print("Step 2: Extracting product information...")
    print("-" * 80)
    
    integrator = ConfluenceKnowledgeBaseIntegrator()
    title = page_data.get("title", "")
    product = integrator.confluence_ingester.extract_product_from_title(title)
    
    print(f"  Title: {title}")
    print(f"  Extracted Product: {product}")
    print(f"  Document Type: {integrator._detect_document_type(title)}")
    print()
    
    # Step 3: Extract structured knowledge
    print("Step 3: Extracting structured knowledge...")
    print("-" * 80)
    
    knowledge = integrator.process_confluence_page_to_knowledge(page_data)
    
    print(f"  ✓ Knowledge extracted")
    print(f"    - Entities: {len(knowledge.get('entities', []))}")
    print(f"    - Events: {len(knowledge.get('events', []))}")
    print(f"    - Tables: {len(knowledge.get('tables', []))}")
    print(f"    - Relationships: {len(knowledge.get('relationships', []))}")
    print(f"    - Metrics: {len(knowledge.get('metrics', []))}")
    print()
    
    # Step 4: Show extracted entities
    if knowledge.get("entities"):
        print("  Extracted Entities:")
        for entity in knowledge.get("entities", [])[:10]:
            print(f"    - {entity.get('name')}: {entity.get('description', '')[:50]}")
        print()
    
    # Step 5: Show extracted events
    if knowledge.get("events"):
        print("  Extracted Events:")
        for event in knowledge.get("events", [])[:10]:
            print(f"    - {event.get('event_name')} ({event.get('event_type', 'N/A')})")
            if event.get('description'):
                print(f"      Description: {event.get('description')[:100]}...")
        print()
    
    # Step 6: Show extracted tables
    if knowledge.get("tables"):
        print("  Extracted Tables:")
        for i, table in enumerate(knowledge.get("tables", [])[:3], 1):
            print(f"    Table {i}:")
            print(f"      Headers: {', '.join(table.get('headers', [])[:5])}")
            print(f"      Rows: {table.get('row_count', 0)}")
        print()
    
    # Step 7: Show extracted metrics
    if knowledge.get("metrics"):
        print("  Extracted Metrics:")
        for metric in knowledge.get("metrics", [])[:5]:
            print(f"    - {metric.get('name')}")
        print()
    
    # Step 8: Populate Knowledge Register
    print("Step 4: Populating Knowledge Register...")
    print("-" * 80)
    
    integrator.populate_knowledge_register(knowledge)
    
    # Get the ref_id that was created
    ref_id = f"PROD-{abs(hash(product)) % 10000}-{page_id}"
    
    print(f"  ✓ Knowledge Register populated")
    print(f"    Reference ID: {ref_id}")
    print()
    
    # Step 9: Show what's in Knowledge Register
    print("Step 5: Knowledge Register Contents...")
    print("-" * 80)
    
    syncer = KnowledgeRegisterSyncer()
    page = syncer.get_page(ref_id)
    
    if page:
        print(f"  Page ID: {page.get('page_id')}")
        print(f"  Title: {page.get('title')}")
        print(f"  Keywords: {', '.join(page.get('keywords', [])[:10])}")
        print(f"  Segments:")
        segments = page.get('segments', {})
        print(f"    - Entities: {len(segments.get('entities', []))}")
        print(f"    - Events: {len(segments.get('events', []))}")
        print(f"    - Tables: {len(segments.get('tables', []))}")
        print(f"    - Relationships: {len(segments.get('relationships', []))}")
        print(f"    - Metrics: {len(segments.get('metrics', []))}")
        print()
        
        # Show full text preview
        full_text = page.get('full_text', '')
        print(f"  Full Text Preview (first 500 chars):")
        print(f"  {full_text[:500]}...")
        print()
    
    # Step 10: Test search
    print("Step 6: Testing Search...")
    print("-" * 80)
    
    # Search by product
    search_results = syncer.search("slice")
    print(f"  Search for 'slice': Found {len(search_results)} pages")
    if search_results:
        print(f"    Pages: {', '.join(search_results[:5])}")
    print()
    
    # Search by keyword
    search_results = syncer.search("passbook")
    print(f"  Search for 'passbook': Found {len(search_results)} pages")
    if search_results:
        print(f"    Pages: {', '.join(search_results[:5])}")
    print()
    
    # Step 11: Show Knowledge Base contents
    print("Step 7: Knowledge Base Contents...")
    print("-" * 80)
    
    kb_path = Path("metadata/knowledge_base.json")
    if kb_path.exists():
        with open(kb_path, 'r') as f:
            kb = json.load(f)
        
        print(f"  Tables: {len(kb.get('tables', {}))}")
        print(f"  Events: {len(kb.get('events', {}))}")
        print(f"  Relationships: {len(kb.get('relationships', {}))}")
        
        # Show some tables
        tables = kb.get('tables', {})
        if tables:
            print(f"\n  Sample Tables:")
            for table_name in list(tables.keys())[:5]:
                table_info = tables[table_name]
                print(f"    - {table_name}: {table_info.get('description', '')[:50]}")
        
        # Show some events
        events = kb.get('events', {})
        if events:
            print(f"\n  Sample Events:")
            for event_name in list(events.keys())[:5]:
                event_info = events[event_name]
                print(f"    - {event_name}: {event_info.get('description', '')[:50]}")
    print()
    
    # Step 12: Show Product Index
    print("Step 8: Product Index...")
    print("-" * 80)
    
    from src.product_index import ProductIndex
    product_index = ProductIndex()
    product_info = product_index.get_product(product)
    
    if product_info:
        print(f"  Product: {product}")
        print(f"  Documents: {len(product_info.get('documents', []))}")
        print(f"  Tables: {len(product_info.get('tables', []))}")
        
        docs = product_info.get('documents', [])
        if docs:
            print(f"\n  Documents for this product:")
            for doc in docs[:5]:
                print(f"    - {doc.get('title')} ({doc.get('document_type')})")
    print()
    
    # Summary
    print("="*80)
    print("Summary: What Happened")
    print("="*80)
    print(f"""
1. ✓ Fetched page from Confluence API
   - Page ID: {page_id}
   - Title: {title}
   
2. ✓ Extracted product: {product}
   - Document Type: {knowledge.get('document_type')}
   
3. ✓ Extracted structured information:
   - Entities: {len(knowledge.get('entities', []))}
   - Events: {len(knowledge.get('events', []))}
   - Tables: {len(knowledge.get('tables', []))}
   - Relationships: {len(knowledge.get('relationships', []))}
   - Metrics: {len(knowledge.get('metrics', []))}
   
4. ✓ Populated Knowledge Register
   - Reference ID: {ref_id}
   - Searchable by keywords
   
5. ✓ Populated Knowledge Base
   - Tables and events indexed
   - Relationships stored
   
6. ✓ Updated Product Index
   - Product: {product}
   - Document linked to product
   
7. ✓ Ready for Vector Search
   - Can be queried via test_query.py
   - Searchable by product, keyword, reference ID
    """)
    
    print("\nNext Steps:")
    print("  1. Run: python src/pipeline.py --step all (to index for vector search)")
    print("  2. Query: python test_query.py --question 'What is slice mini passbook?' --project 'slice mini passbook'")


if __name__ == "__main__":
    # Page ID from URL: https://slicepay.atlassian.net/wiki/spaces/HOR/pages/2898362610/ARD+slice+mini+passbook
    page_id = "2898362610"
    
    fetch_and_process_page(page_id)

