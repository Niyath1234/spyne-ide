# MCP Confluence Integration Guide

This guide explains how to use MCP (Model Context Protocol) to connect to Confluence instead of using direct API calls.

## Overview

The RCA Engine now supports MCP-based Confluence integration, which allows you to:
- Fetch Confluence pages via MCP tools (no need for API tokens in code)
- Process pages through the knowledge extraction pipeline
- Integrate with Knowledge Register, Knowledge Base, and Product Index

## MCP Tools Available

The following MCP Confluence tools are available via Cursor:

1. **`mcp_mcp-atlassian_confluence_get_page`** - Get a page by ID or title
2. **`mcp_mcp-atlassian_confluence_search`** - Search for pages
3. **`mcp_mcp-atlassian_confluence_get_page_children`** - Get child pages
4. **`mcp_mcp-atlassian_confluence_create_page`** - Create a new page
5. **`mcp_mcp-atlassian_confluence_update_page`** - Update an existing page
6. **`mcp_mcp-atlassian_confluence_add_comment`** - Add a comment to a page

## Architecture

### Components

1. **`ConfluenceMCPAdapter`** (`src/confluence_mcp_adapter.py`)
   - Provides interface compatible with `ConfluenceIngester`
   - Wraps MCP tool calls
   - Handles product extraction and page metadata

2. **`ConfluenceMCPKnowledgeBaseIntegrator`** (`src/confluence_mcp_integrator.py`)
   - Main integration class for processing MCP-fetched pages
   - Extracts knowledge (events, tables, entities, relationships)
   - Populates Knowledge Register, Knowledge Base, and Product Index
   - Same interface as `ConfluenceKnowledgeBaseIntegrator`

## Usage Examples

### Example 1: Fetch and Process a Page by ID

```python
from src.confluence_mcp_integrator import ConfluenceMCPKnowledgeBaseIntegrator

# Step 1: Fetch page via MCP (in Cursor, use MCP tool)
# mcp_mcp-atlassian_confluence_get_page(page_id='2898362610', convert_to_markdown=False)

# Step 2: Process the MCP response
integrator = ConfluenceMCPKnowledgeBaseIntegrator(space_key='HOR')

# Convert MCP response to page_data format
page_data = {
    "id": mcp_response["metadata"]["id"],
    "title": mcp_response["metadata"]["title"],
    "body": {
        "storage": {
            "value": mcp_response["content"]["value"]
        }
    },
    "space": mcp_response["metadata"]["space"]
}

# Extract knowledge
knowledge = integrator.process_confluence_page_to_knowledge(page_data)

# Populate knowledge register
integrator.populate_knowledge_register(knowledge)
```

### Example 2: Search and Process Multiple Pages

```python
# Step 1: Search via MCP
# mcp_mcp-atlassian_confluence_search(query='type=page AND space=HOR AND title~"ARD"', limit=50)

# Step 2: Process each page
integrator = ConfluenceMCPKnowledgeBaseIntegrator(space_key='HOR')

for page_result in search_results:
    # Fetch full page
    # mcp_response = mcp_mcp-atlassian_confluence_get_page(page_id=page_result['id'])
    
    # Process page
    page_data = convert_mcp_response_to_page_data(mcp_response)
    knowledge = integrator.process_confluence_page_to_knowledge(page_data)
    integrator.populate_knowledge_register(knowledge)
```

### Example 3: Using in Cursor with MCP

In Cursor, you can directly use MCP tools:

```python
# Fetch page
page_response = mcp_mcp-atlassian_confluence_get_page(
    page_id='2898362610',
    convert_to_markdown=False,  # Keep HTML for table extraction
    include_metadata=True
)

# Process with integrator
from src.confluence_mcp_integrator import ConfluenceMCPKnowledgeBaseIntegrator

integrator = ConfluenceMCPKnowledgeBaseIntegrator(space_key='HOR')

# Convert response format
page_data = {
    "id": page_response["metadata"]["id"],
    "page_id": page_response["metadata"]["id"],
    "title": page_response["metadata"]["title"],
    "body": {
        "storage": {
            "value": page_response["content"]["value"]
        }
    },
    "space": page_response["metadata"]["space"],
    "version": {"number": page_response["metadata"]["version"]}
}

# Extract and integrate
knowledge = integrator.process_confluence_page_to_knowledge(page_data)
integrator.populate_knowledge_register(knowledge)
```

## MCP Response Format

MCP Confluence tools return data in this format:

```json
{
  "metadata": {
    "id": "2898362610",
    "title": "ARD: slice mini passbook",
    "type": "page",
    "space": {
      "key": "HOR",
      "name": "Horizontal-Analytics"
    },
    "version": 20,
    "attachments": [...]
  },
  "content": {
    "value": "<p>HTML content...</p>",
    "format": "storage"
  }
}
```

## Benefits of MCP Integration

1. **No API Credentials in Code**: MCP handles authentication
2. **Unified Interface**: Same interface as direct API calls
3. **Better Error Handling**: MCP provides consistent error responses
4. **Cursor Integration**: Works seamlessly with Cursor's MCP support
5. **Future-Proof**: Easy to switch between MCP and direct API

## Migration from Direct API

To migrate from direct API calls to MCP:

1. Replace `ConfluenceIngester` with `ConfluenceMCPAdapter`
2. Replace `ConfluenceKnowledgeBaseIntegrator` with `ConfluenceMCPKnowledgeBaseIntegrator`
3. Fetch pages using MCP tools instead of direct API calls
4. Convert MCP responses to the expected format (see examples above)

## Testing

Run the test script to see MCP integration in action:

```bash
python test_mcp_confluence.py
python test_mcp_integration_complete.py
```

## Files

- `src/confluence_mcp_adapter.py` - MCP adapter for Confluence
- `src/confluence_mcp_integrator.py` - Knowledge extraction using MCP
- `test_mcp_confluence.py` - Basic MCP integration test
- `test_mcp_integration_complete.py` - Complete integration example

## Next Steps

1. Configure MCP Confluence server in Cursor settings
2. Test MCP connection with a sample page
3. Integrate MCP calls into your workflow
4. Process pages through knowledge extraction pipeline

