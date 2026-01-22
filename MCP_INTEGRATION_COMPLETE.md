# Complete MCP Integration Guide - Confluence + Slack

This guide covers the complete MCP integration for both Confluence (knowledge extraction) and Slack (notifications) in the RCA Engine.

## Overview

The RCA Engine now supports MCP (Model Context Protocol) for both:
- **Confluence**: Fetch and process ARD/PRD/TRD documents
- **Slack**: Send notifications and share knowledge

Both integrations work seamlessly together to provide an end-to-end workflow.

## Architecture

```
┌─────────────────┐
│  Confluence MCP │──┐
│   (Fetch Pages)  │  │
└─────────────────┘  │
                     ├──► Knowledge Extraction ──► Knowledge Register
┌─────────────────┐  │                              Knowledge Base
│   Slack MCP     │──┘                              Product Index
│ (Notifications) │
└─────────────────┘
```

## Quick Start

### 1. Configure MCP Servers

Edit `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "mcp-atlassian": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-atlassian"],
      "env": {
        "ATLASSIAN_URL": "https://slicepay.atlassian.net",
        "ATLASSIAN_USERNAME": "your-email@example.com",
        "ATLASSIAN_API_TOKEN": "your-api-token"
      }
    },
    "mcp-slack": {
      "command": "/opt/homebrew/bin/uvx",
      "args": ["mcp-slack"],
      "env": {
        "SLACK_BOT_TOKEN": "xoxb-your-bot-token"
      }
    }
  }
}
```

### 2. Use Combined Integrator

```python
from src.confluence_slack_mcp_integrator import ConfluenceSlackMCPIntegrator

# Initialize
integrator = ConfluenceSlackMCPIntegrator(
    confluence_space_key='HOR',
    slack_channel='#rca-engine'
)

# Fetch page via MCP (in Cursor)
# mcp_response = mcp_mcp-atlassian_confluence_get_page(page_id='2898362610')

# Convert MCP response
page_data = {
    "id": mcp_response["metadata"]["id"],
    "page_id": mcp_response["metadata"]["id"],
    "title": mcp_response["metadata"]["title"],
    "body": {
        "storage": {
            "value": mcp_response["content"]["value"]
        }
    },
    "space": mcp_response["metadata"]["space"],
    "version": {"number": mcp_response["metadata"]["version"]}
}

# Process with Slack notifications
result = integrator.process_page_with_notifications(
    page_data=page_data,
    notify_slack=True
)
```

## Complete Workflow Example

### Step 1: Fetch Confluence Page

```python
# In Cursor, use MCP tool:
mcp_response = mcp_mcp-atlassian_confluence_get_page(
    page_id='2898362610',
    convert_to_markdown=False,  # Keep HTML for table extraction
    include_metadata=True
)
```

### Step 2: Process and Notify

```python
from src.confluence_slack_mcp_integrator import ConfluenceSlackMCPIntegrator

integrator = ConfluenceSlackMCPIntegrator(
    confluence_space_key='HOR',
    slack_channel='#rca-engine'
)

# Convert MCP response
page_data = convert_mcp_response(mcp_response)

# Process
result = integrator.process_page_with_notifications(
    page_data=page_data,
    notify_slack=True
)

# Result contains:
# - success: bool
# - knowledge: extracted knowledge
# - ref_id: reference ID
# - slack_notifications: notification structures
```

### Step 3: Send Slack Notifications

The notifications are automatically formatted and ready to send:

```python
# Send main notification
mcp_mcp-slack_send_message(**result["slack_notifications"][0])

# Send events if many were extracted
if len(result["knowledge"]["events"]) > 5:
    mcp_mcp-slack_send_message(**result["slack_notifications"][1])
```

## Available MCP Tools

### Confluence MCP Tools

- `mcp_mcp-atlassian_confluence_get_page` - Get page by ID or title
- `mcp_mcp-atlassian_confluence_search` - Search for pages
- `mcp_mcp-atlassian_confluence_get_page_children` - Get child pages
- `mcp_mcp-atlassian_confluence_create_page` - Create new page
- `mcp_mcp-atlassian_confluence_update_page` - Update page
- `mcp_mcp-atlassian_confluence_add_comment` - Add comment

### Slack MCP Tools

- `mcp_mcp-slack_send_message` - Send message
- `mcp_mcp-slack_list_channels` - List channels
- `mcp_mcp-slack_get_channel_history` - Get channel history
- `mcp_mcp-slack_list_users` - List users
- `mcp_mcp-slack_upload_file` - Upload file
- `mcp_mcp-slack_create_channel` - Create channel
- `mcp_mcp-slack_add_reaction` - Add reaction

## Integration Components

### 1. ConfluenceMCPKnowledgeBaseIntegrator

- Fetches pages via MCP
- Extracts knowledge (events, tables, entities)
- Populates Knowledge Register, Knowledge Base, Product Index

### 2. SlackMCPIntegrator

- Sends knowledge extraction notifications
- Shares extracted events and tables
- Uploads reports and files
- Error and success notifications

### 3. ConfluenceSlackMCPIntegrator

- Combined integrator
- End-to-end workflow
- Automatic Slack notifications

## Notification Examples

### Knowledge Extraction Notification

```
📚 Knowledge Extracted

Page: ARD: slice mini passbook
Product: slice mini passbook
Document Type: ARD

Extracted:
• Events: 15
• Tables: 1
• Entities: 1

Reference ID: PROD-3649-2898362610
```

### Events Share

```
Events for slice mini passbook

1. mini_passbook_opened (PAGE OPEN)
2. mini_passbook_search_clicked (CTA)
3. mini_passbook_filter_clicked (CTA)
...
```

## Files Structure

```
src/
├── confluence_mcp_adapter.py          # Confluence MCP adapter
├── confluence_mcp_integrator.py      # Confluence knowledge extraction
├── slack_mcp_adapter.py               # Slack MCP adapter
├── slack_mcp_integrator.py            # Slack notifications
└── confluence_slack_mcp_integrator.py  # Combined integrator

test/
├── test_mcp_confluence.py             # Confluence MCP tests
├── test_slack_mcp_integration.py      # Slack MCP tests
└── test_mcp_integration_complete.py    # Complete integration tests

docs/
├── MCP_INTEGRATION_GUIDE.md           # Confluence MCP guide
├── SLACK_MCP_INTEGRATION_GUIDE.md     # Slack MCP guide
└── MCP_INTEGRATION_COMPLETE.md        # This file
```

## Benefits

1. **No API Credentials in Code**: MCP handles all authentication
2. **Unified Interface**: Same interface for both Confluence and Slack
3. **Automatic Notifications**: Knowledge extraction automatically notifies Slack
4. **Cursor Integration**: Works seamlessly with Cursor's MCP support
5. **Future-Proof**: Easy to extend with more MCP integrations

## Testing

```bash
# Test Confluence integration
python test_mcp_confluence.py

# Test Slack integration
python test_slack_mcp_integration.py

# Test complete integration
python test_mcp_integration_complete.py
```

## Next Steps

1. ✅ Configure MCP servers in Cursor
2. ✅ Test Confluence page fetching
3. ✅ Test Slack notifications
4. ✅ Integrate into knowledge extraction pipeline
5. ✅ Set up automated workflows

## Summary

The RCA Engine now has complete MCP integration for both Confluence and Slack:

- **Confluence MCP**: Fetch and process documents
- **Slack MCP**: Send notifications and share knowledge
- **Combined**: End-to-end workflow with automatic notifications

All integrations use MCP, so no API credentials are needed in code, and everything works seamlessly with Cursor's MCP support.

