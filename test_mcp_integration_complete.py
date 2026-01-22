"""
Complete MCP Integration Example

This script shows how to use MCP tools to fetch Confluence pages and process them
through the knowledge extraction pipeline.
"""

import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.confluence_mcp_integrator import ConfluenceMCPKnowledgeBaseIntegrator


def process_mcp_page_response(mcp_response: dict):
    """
    Process a page fetched via MCP and extract knowledge.
    
    Args:
        mcp_response: Response from mcp_mcp-atlassian_confluence_get_page
    """
    
    print("="*80)
    print("PROCESSING MCP CONFLUENCE PAGE RESPONSE")
    print("="*80)
    
    # Extract page data from MCP response
    metadata = mcp_response.get("metadata", {})
    content = mcp_response.get("content", {})
    
    page_id = metadata.get("id")
    title = metadata.get("title")
    space_key = metadata.get("space", {}).get("key")
    
    print(f"\nPage Information:")
    print(f"  ID: {page_id}")
    print(f"  Title: {title}")
    print(f"  Space: {space_key}")
    print(f"  Version: {metadata.get('version')}")
    
    # Convert MCP response to format expected by integrator
    page_data = {
        "id": page_id,
        "page_id": page_id,
        "title": title,
        "space": {
            "key": space_key,
            "name": metadata.get("space", {}).get("name")
        },
        "version": {
            "number": metadata.get("version")
        },
        "body": {
            "storage": {
                "value": content.get("value", "")
            }
        }
    }
    
    # Initialize integrator
    integrator = ConfluenceMCPKnowledgeBaseIntegrator(space_key=space_key)
    
    print(f"\n{'='*80}")
    print("EXTRACTING KNOWLEDGE")
    print("="*80)
    
    # Process page
    knowledge = integrator.process_confluence_page_to_knowledge(page_data)
    
    print(f"\n✓ Knowledge extracted:")
    print(f"  - Product: {knowledge.get('product')}")
    print(f"  - Document Type: {knowledge.get('document_type')}")
    print(f"  - Entities: {len(knowledge.get('entities', []))}")
    print(f"  - Events: {len(knowledge.get('events', []))}")
    print(f"  - Tables: {len(knowledge.get('tables', []))}")
    print(f"  - Relationships: {len(knowledge.get('relationships', []))}")
    print(f"  - Metrics: {len(knowledge.get('metrics', []))}")
    
    # Show extracted events
    if knowledge.get("events"):
        print(f"\n  Extracted Events (first 5):")
        for event in knowledge.get("events", [])[:5]:
            print(f"    - {event.get('event_name')} ({event.get('event_type')})")
    
    print(f"\n{'='*80}")
    print("POPULATING KNOWLEDGE REGISTER")
    print("="*80)
    
    # Populate knowledge register
    integrator.populate_knowledge_register(knowledge)
    
    ref_id = f"PROD-{abs(hash(knowledge.get('product', ''))) % 10000}-{page_id}"
    print(f"\n✓ Knowledge Register populated")
    print(f"  Reference ID: {ref_id}")
    
    print(f"\n{'='*80}")
    print("SUMMARY")
    print("="*80)
    print(f"""
✓ Page fetched via MCP
✓ Knowledge extracted:
  - Product: {knowledge.get('product')}
  - Events: {len(knowledge.get('events', []))}
  - Tables: {len(knowledge.get('tables', []))}
  - Entities: {len(knowledge.get('entities', []))}
✓ Knowledge Register updated
✓ Knowledge Base updated
✓ Product Index updated

The page is now searchable and integrated into the knowledge system!
    """)
    
    return knowledge, ref_id


# Example MCP response (from actual MCP call)
example_mcp_response = {
    "metadata": {
        "id": "2898362610",
        "title": "ARD: slice mini passbook",
        "type": "page",
        "space": {
            "key": "HOR",
            "name": "Horizontal-Analytics"
        },
        "version": 20
    },
    "content": {
        "value": "<p><strong>Context:</strong></p><p>The document covers all the events...</p>",
        "format": "storage"
    }
}


if __name__ == "__main__":
    print("""
This script demonstrates how to process a Confluence page fetched via MCP.

To use this in practice:

1. Fetch page using MCP tool:
   response = mcp_mcp-atlassian_confluence_get_page(page_id='2898362610')

2. Process the response:
   knowledge, ref_id = process_mcp_page_response(response)

The page will be automatically integrated into:
  - Knowledge Register (searchable)
  - Knowledge Base (structured data)
  - Product Index (product mapping)
    """)
    
    # Uncomment to test with example response:
    # process_mcp_page_response(example_mcp_response)

