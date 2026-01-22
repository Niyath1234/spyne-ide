"""
Test Confluence integration using MCP

This script demonstrates how to use MCP tools to fetch and process Confluence pages.
"""

import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.confluence_mcp_integrator import ConfluenceMCPKnowledgeBaseIntegrator


def test_mcp_page_processing():
    """
    Test processing a Confluence page using MCP.
    
    Note: This function should be called from a context where MCP tools are available
    (e.g., via Cursor's MCP integration).
    """
    
    print("="*80)
    print("TESTING CONFLUENCE INTEGRATION VIA MCP")
    print("="*80)
    print()
    print("This test demonstrates how to use MCP tools for Confluence integration.")
    print("The actual MCP calls should be made via Cursor's MCP interface.")
    print()
    
    # Page to test
    page_id = "2898362610"
    page_title = "ARD: slice mini passbook"
    space_key = "HOR"
    
    print(f"Page ID: {page_id}")
    print(f"Title: {page_title}")
    print(f"Space: {space_key}")
    print()
    
    # Initialize integrator
    integrator = ConfluenceMCPKnowledgeBaseIntegrator(space_key=space_key)
    
    print("Step 1: Fetch page using MCP")
    print("-" * 80)
    print("To fetch the page, use MCP tool:")
    print(f"  mcp_mcp-atlassian_confluence_get_page(page_id='{page_id}')")
    print()
    print("Or by title and space:")
    print(f"  mcp_mcp-atlassian_confluence_get_page(title='{page_title}', space_key='{space_key}')")
    print()
    
    # Example of what the MCP response would look like
    print("Step 2: Process page data")
    print("-" * 80)
    print("The MCP response will be processed by the integrator.")
    print("Example MCP call structure:")
    print(json.dumps({
        "method": "mcp_mcp-atlassian_confluence_get_page",
        "params": {
            "page_id": page_id,
            "convert_to_markdown": False,  # Keep HTML for table extraction
            "include_metadata": True
        }
    }, indent=2))
    print()
    
    print("Step 3: Knowledge extraction")
    print("-" * 80)
    print("Once page data is retrieved via MCP, it will be processed to extract:")
    print("  - Product name")
    print("  - Document type (ARD/PRD/TRD)")
    print("  - Events (from ARD tables)")
    print("  - Tables")
    print("  - Entities")
    print("  - Relationships")
    print()
    
    print("Step 4: Integration")
    print("-" * 80)
    print("Extracted knowledge will be added to:")
    print("  - Knowledge Register (for search)")
    print("  - Knowledge Base (structured data)")
    print("  - Product Index (product mapping)")
    print()
    
    print("="*80)
    print("USAGE INSTRUCTIONS")
    print("="*80)
    print("""
To use MCP for Confluence integration:

1. Ensure MCP Confluence server is configured in Cursor
2. Use MCP tools to fetch pages:
   - mcp_mcp-atlassian_confluence_get_page(page_id='...')
   - mcp_mcp-atlassian_confluence_search(query='...')
   - mcp_mcp-atlassian_confluence_get_page_children(parent_id='...')

3. Process the MCP response with ConfluenceMCPKnowledgeBaseIntegrator:
   
   integrator = ConfluenceMCPKnowledgeBaseIntegrator(space_key='HOR')
   page_data = <MCP response>
   knowledge = integrator.process_confluence_page_to_knowledge(page_data)
   integrator.populate_knowledge_register(knowledge)

4. The knowledge will be automatically integrated into:
   - Knowledge Register (searchable)
   - Knowledge Base (structured)
   - Product Index (mapped)
    """)


if __name__ == "__main__":
    test_mcp_page_processing()

