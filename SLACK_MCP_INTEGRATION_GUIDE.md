# Slack MCP Integration Guide

This guide explains how to use MCP (Model Context Protocol) to connect to Slack for notifications and integration with the RCA Engine.

## Overview

The RCA Engine now supports MCP-based Slack integration, which allows you to:
- Send notifications about knowledge extraction
- Share extracted knowledge (events, tables, entities) to Slack
- Upload reports and files
- Integrate Slack workflows with the knowledge extraction pipeline

## MCP Tools Available

The following MCP Slack tools are available via Cursor:

1. **`mcp_mcp-slack_send_message`** - Send a message to a channel or thread
2. **`mcp_mcp-slack_list_channels`** - List channels in the workspace
3. **`mcp_mcp-slack_get_channel_history`** - Get message history for a channel
4. **`mcp_mcp-slack_list_users`** - List users in the workspace
5. **`mcp_mcp-slack_upload_file`** - Upload a file to a channel
6. **`mcp_mcp-slack_create_channel`** - Create a new channel
7. **`mcp_mcp-slack_add_reaction`** - Add a reaction to a message

## Architecture

### Components

1. **`SlackMCPAdapter`** (`src/slack_mcp_adapter.py`)
   - Provides interface for Slack operations via MCP
   - Wraps MCP tool calls
   - Handles channel, user, and message operations

2. **`SlackMCPIntegrator`** (`src/slack_mcp_integrator.py`)
   - Main integration class for Slack notifications
   - Formats knowledge extraction results for Slack
   - Sends notifications, shares events, uploads files
   - Provides error and success notification methods

3. **`ConfluenceSlackMCPIntegrator`** (`src/confluence_slack_mcp_integrator.py`)
   - Combined integrator for Confluence + Slack
   - Processes Confluence pages and sends Slack notifications
   - End-to-end workflow integration

## Usage Examples

### Example 1: Send Knowledge Extraction Notification

```python
from src.slack_mcp_integrator import SlackMCPIntegrator

# Initialize integrator
integrator = SlackMCPIntegrator(default_channel='#rca-engine')

# After knowledge extraction
knowledge = {
    "page_id": "2898362610",
    "title": "ARD: slice mini passbook",
    "product": "slice mini passbook",
    "document_type": "ARD",
    "events": [...],
    "tables": [...],
    "entities": [...]
}

# Send notification (returns MCP call structure)
notification = integrator.notify_knowledge_extracted(
    page_title=knowledge["title"],
    product=knowledge["product"],
    knowledge=knowledge,
    channel="#rca-engine"
)

# In Cursor, use MCP tool:
# mcp_mcp-slack_send_message(**notification)
```

### Example 2: Share Extracted Events

```python
from src.slack_mcp_integrator import SlackMCPIntegrator

integrator = SlackMCPIntegrator(default_channel='#rca-engine')

events = [
    {"event_name": "mini_passbook_opened", "event_type": "PAGE OPEN"},
    {"event_name": "mini_passbook_search_clicked", "event_type": "CTA"}
]

# Share events (returns MCP call structure)
share = integrator.share_events(
    product="slice mini passbook",
    events=events,
    channel="#rca-engine",
    limit=10
)

# In Cursor, use MCP tool:
# mcp_mcp-slack_send_message(**share)
```

### Example 3: Combined Confluence + Slack Integration

```python
from src.confluence_slack_mcp_integrator import ConfluenceSlackMCPIntegrator

# Initialize combined integrator
integrator = ConfluenceSlackMCPIntegrator(
    confluence_space_key='HOR',
    slack_channel='#rca-engine'
)

# Fetch page via MCP (in Cursor)
# mcp_response = mcp_mcp-atlassian_confluence_get_page(page_id='2898362610')

# Convert MCP response to page_data format
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
    notify_slack=True,
    slack_channel="#rca-engine"
)

# Result contains:
# - success: bool
# - knowledge: extracted knowledge dict
# - ref_id: reference ID
# - slack_notifications: list of notification structures
```

### Example 4: Using in Cursor with MCP

In Cursor, you can directly use MCP tools:

```python
# Send a message
mcp_mcp-slack_send_message(
    channel='#rca-engine',
    text='Knowledge extraction completed!'
)

# List channels
channels = mcp_mcp-slack_list_channels(
    channel_types='public_channel,private_channel',
    limit=100
)

# Upload a file
mcp_mcp-slack_upload_file(
    channel='#rca-engine',
    file_path='data/reports/knowledge_report.json',
    initial_comment='Knowledge extraction report'
)
```

## Notification Formats

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
• Relationships: 0
• Metrics: 0

Reference ID: PROD-3649-2898362610

Knowledge has been integrated into the Knowledge Register and Knowledge Base.
```

### Events Share Format

```
Events for slice mini passbook

1. mini_passbook_opened (PAGE OPEN)
   Captures the opening of passbook page

2. mini_passbook_search_clicked (CTA)

3. mini_passbook_filter_clicked (CTA)
   ...
```

### Error Notification Format

```
⚠️ Error in RCA Engine

Error: Failed to extract knowledge
Context: Processing page: ARD: slice mini passbook

Time: 2024-01-01T12:00:00
```

## Configuration

### Environment Variables

```bash
# Default Slack channel for notifications
export SLACK_DEFAULT_CHANNEL="#rca-engine"

# Slack workspace name (optional)
export SLACK_WORKSPACE_NAME="Your Workspace"
```

### MCP Configuration

The Slack MCP server should be configured in `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
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

## Integration with Knowledge Extraction Pipeline

### Complete Workflow

```python
from src.confluence_slack_mcp_integrator import ConfluenceSlackMCPIntegrator

# Initialize
integrator = ConfluenceSlackMCPIntegrator(
    confluence_space_key='HOR',
    slack_channel='#rca-engine'
)

# 1. Fetch page via MCP
# mcp_response = mcp_mcp-atlassian_confluence_get_page(page_id='2898362610')

# 2. Convert to page_data format
page_data = convert_mcp_response(mcp_response)

# 3. Process with notifications
result = integrator.process_page_with_notifications(
    page_data=page_data,
    notify_slack=True
)

# 4. Knowledge is automatically:
#    - Extracted and structured
#    - Added to Knowledge Register
#    - Added to Knowledge Base
#    - Indexed in Product Index
#    - Notified to Slack
```

## Benefits of MCP Integration

1. **No API Credentials in Code**: MCP handles authentication
2. **Unified Interface**: Same interface as direct API calls
3. **Better Error Handling**: MCP provides consistent error responses
4. **Cursor Integration**: Works seamlessly with Cursor's MCP support
5. **Automatic Notifications**: Knowledge extraction automatically notifies Slack

## Files

- `src/slack_mcp_adapter.py` - MCP adapter for Slack
- `src/slack_mcp_integrator.py` - Slack notification integration
- `src/confluence_slack_mcp_integrator.py` - Combined Confluence + Slack integration
- `test_slack_mcp_integration.py` - Basic Slack MCP integration test

## Testing

Run the test script to see Slack integration in action:

```bash
python test_slack_mcp_integration.py
```

## Next Steps

1. Configure MCP Slack server in Cursor settings
2. Set up default Slack channel for notifications
3. Integrate Slack notifications into knowledge extraction workflow
4. Test notifications with sample knowledge extractions

## Security Notes

- Never commit Slack tokens to version control
- Use environment variables or secrets manager
- Bot tokens are recommended over user tokens
- Limit token scopes to minimum required permissions

