"""
Test fetching and processing the ARD slice mini passbook page
URL: https://slicepay.atlassian.net/wiki/spaces/HOR/pages/2898362610/ARD+slice+mini+passbook
"""

import sys
import json
import os
from pathlib import Path
import requests
from requests.auth import HTTPBasicAuth

# Set environment variables
# Set environment variables (use actual values from environment or config)
os.environ["CONFLUENCE_URL"] = os.getenv("CONFLUENCE_URL", "https://slicepay.atlassian.net/wiki")
os.environ["CONFLUENCE_USERNAME"] = os.getenv("CONFLUENCE_USERNAME", "")
os.environ["CONFLUENCE_API_TOKEN"] = os.getenv("CONFLUENCE_API_TOKEN", "")

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.confluence_to_knowledge_base import ConfluenceKnowledgeBaseIntegrator, HTMLContentExtractor
from src.knowledge_register_sync import KnowledgeRegisterSyncer


def fetch_page_details(page_id: str):
    """Fetch detailed information about the page."""
    
    url_base = os.getenv("CONFLUENCE_URL")
    username = os.getenv("CONFLUENCE_USERNAME")
    api_token = os.getenv("CONFLUENCE_API_TOKEN")
    
    print("="*80)
    print("FETCHING PAGE: ARD slice mini passbook")
    print("="*80)
    print(f"Page ID: {page_id}")
    print(f"URL: https://slicepay.atlassian.net/wiki/spaces/HOR/pages/{page_id}/ARD+slice+mini+passbook")
    print()
    
    # Fetch page
    api_url = f"{url_base}/rest/api/content/{page_id}"
    params = {"expand": "body.storage,space,version,metadata.labels,ancestors"}
    
    response = requests.get(
        api_url,
        auth=HTTPBasicAuth(username, api_token),
        headers={"Accept": "application/json"},
        params=params,
        timeout=30
    )
    
    if response.status_code != 200:
        print(f"✗ Error: {response.status_code}")
        print(response.text[:500])
        return None
    
    page_data = response.json()
    
    # Display basic info
    print("PAGE INFORMATION:")
    print("-" * 80)
    print(f"Title: {page_data.get('title')}")
    print(f"Page ID: {page_data.get('id')}")
    print(f"Space: {page_data.get('space', {}).get('name')} ({page_data.get('space', {}).get('key')})")
    print(f"Version: {page_data.get('version', {}).get('number')}")
    print(f"Last Modified: {page_data.get('version', {}).get('when', 'Unknown')}")
    
    # Labels
    metadata = page_data.get('metadata', {})
    labels = metadata.get('labels', {}).get('results', [])
    if labels:
        print(f"Labels: {', '.join([l.get('name') for l in labels])}")
    
    # Ancestors
    ancestors = page_data.get('ancestors', [])
    if ancestors:
        print(f"\nParent Pages:")
        for ancestor in ancestors:
            print(f"  - {ancestor.get('title')} (ID: {ancestor.get('id')})")
    
    # Extract text content
    print("\n" + "="*80)
    print("PAGE CONTENT (First 2000 characters):")
    print("-" * 80)
    
    body = page_data.get('body', {})
    storage = body.get('storage', {})
    html_content = storage.get('value', '')
    
    # Extract text from HTML
    extractor = HTMLContentExtractor()
    extractor.feed(html_content)
    text_content = extractor.get_text()
    
    print(text_content[:2000])
    if len(text_content) > 2000:
        print(f"\n... (content truncated, total length: {len(text_content)} characters)")
    
    # Save raw HTML
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    html_file = output_dir / f"confluence_page_{page_id}.html"
    html_file.write_text(html_content, encoding='utf-8')
    print(f"\n✓ Raw HTML saved to: {html_file}")
    
    # Save metadata
    json_file = output_dir / f"confluence_page_{page_id}_metadata.json"
    json_file.write_text(
        json.dumps(page_data, indent=2, ensure_ascii=False),
        encoding='utf-8'
    )
    print(f"✓ Metadata saved to: {json_file}")
    
    return page_data, text_content


def process_page(page_data: dict):
    """Process the page through the knowledge extraction pipeline."""
    
    print("\n" + "="*80)
    print("PROCESSING THROUGH KNOWLEDGE EXTRACTION PIPELINE")
    print("="*80)
    
    integrator = ConfluenceKnowledgeBaseIntegrator()
    
    # Extract product
    title = page_data.get("title", "")
    product = integrator.confluence_ingester.extract_product_from_title(title)
    doc_type = integrator._detect_document_type(title)
    
    print(f"\n1. PRODUCT EXTRACTION:")
    print(f"   Title: {title}")
    print(f"   Extracted Product: {product}")
    print(f"   Document Type: {doc_type}")
    
    # Extract knowledge
    print(f"\n2. KNOWLEDGE EXTRACTION:")
    knowledge = integrator.process_confluence_page_to_knowledge(page_data)
    
    print(f"   ✓ Knowledge extracted:")
    print(f"     - Entities: {len(knowledge.get('entities', []))}")
    print(f"     - Events: {len(knowledge.get('events', []))}")
    print(f"     - Tables: {len(knowledge.get('tables', []))}")
    print(f"     - Relationships: {len(knowledge.get('relationships', []))}")
    print(f"     - Metrics: {len(knowledge.get('metrics', []))}")
    
    # Show entities
    if knowledge.get("entities"):
        print(f"\n   Entities found:")
        for entity in knowledge.get("entities", []):
            print(f"     - {entity.get('name')}: {entity.get('description', '')[:100]}")
    
    # Show events
    if knowledge.get("events"):
        print(f"\n   Events found:")
        for event in knowledge.get("events", []):
            print(f"     - {event.get('event_name')} ({event.get('event_type', 'N/A')})")
    
    # Show tables
    if knowledge.get("tables"):
        print(f"\n   Tables found:")
        for i, table in enumerate(knowledge.get("tables", []), 1):
            print(f"     Table {i}:")
            print(f"       Headers: {', '.join(table.get('headers', [])[:5])}")
            print(f"       Rows: {table.get('row_count', 0)}")
    
    # Populate Knowledge Register
    print(f"\n3. POPULATING KNOWLEDGE REGISTER:")
    integrator.populate_knowledge_register(knowledge)
    
    ref_id = f"PROD-{abs(hash(product)) % 10000}-{page_data.get('id')}"
    print(f"   ✓ Knowledge Register populated")
    print(f"     Reference ID: {ref_id}")
    
    # Check Knowledge Register
    syncer = KnowledgeRegisterSyncer()
    page = syncer.get_page(ref_id)
    
    if page:
        print(f"\n4. KNOWLEDGE REGISTER CONTENTS:")
        print(f"   Page ID: {page.get('page_id')}")
        print(f"   Title: {page.get('title')}")
        print(f"   Keywords: {', '.join(page.get('keywords', [])[:10])}")
        segments = page.get('segments', {})
        print(f"   Segments:")
        print(f"     - Entities: {len(segments.get('entities', []))}")
        print(f"     - Events: {len(segments.get('events', []))}")
        print(f"     - Tables: {len(segments.get('tables', []))}")
        print(f"     - Relationships: {len(segments.get('relationships', []))}")
        print(f"     - Metrics: {len(segments.get('metrics', []))}")
    
    return knowledge, ref_id


def show_summary(page_data: dict, knowledge: dict, ref_id: str):
    """Show final summary."""
    
    print("\n" + "="*80)
    print("SUMMARY: WHAT HAPPENED")
    print("="*80)
    
    title = page_data.get('title', '')
    product = knowledge.get('product', 'Unknown')
    
    print(f"""
1. ✓ Fetched page from Confluence API
   - Page ID: {page_data.get('id')}
   - Title: {title}
   - Space: {page_data.get('space', {}).get('name')} ({page_data.get('space', {}).get('key')})
   - Version: {page_data.get('version', {}).get('number')}

2. ✓ Extracted product information
   - Product: {product}
   - Document Type: {knowledge.get('document_type')}

3. ✓ Extracted structured knowledge
   - Entities: {len(knowledge.get('entities', []))}
   - Events: {len(knowledge.get('events', []))}
   - Tables: {len(knowledge.get('tables', []))}
   - Relationships: {len(knowledge.get('relationships', []))}
   - Metrics: {len(knowledge.get('metrics', []))}

4. ✓ Populated Knowledge Register
   - Reference ID: {ref_id}
   - Searchable by keywords and product name

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


if __name__ == "__main__":
    page_id = "2898362610"
    
    # Fetch page details
    result = fetch_page_details(page_id)
    if not result:
        sys.exit(1)
    
    page_data, text_content = result
    
    # Process through pipeline
    knowledge, ref_id = process_page(page_data)
    
    # Show summary
    show_summary(page_data, knowledge, ref_id)
    
    print("\n" + "="*80)
    print("FILES CREATED:")
    print("="*80)
    print(f"  - data/raw/confluence_page_{page_id}.html")
    print(f"  - data/raw/confluence_page_{page_id}_metadata.json")
    print(f"  - metadata/knowledge_register.json (updated)")
    print(f"  - metadata/knowledge_base.json (updated)")
    print(f"  - metadata/product_index.json (updated)")

