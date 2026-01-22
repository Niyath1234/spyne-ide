"""
Test Slack MCP Integration

This script demonstrates how to use MCP tools to interact with Slack
and integrate Slack notifications with the knowledge extraction pipeline.
"""

import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.slack_mcp_integrator import SlackMCPIntegrator


def test_slack_integration():
    """
    Test Slack integration via MCP.
    
    Note: This function should be called from a context where MCP tools are available
    (e.g., via Cursor's MCP integration).
    """
    
    print("="*80)
    print("TESTING SLACK INTEGRATION VIA MCP")
    print("="*80)
    print()
    print("This test demonstrates how to use MCP tools for Slack integration.")
    print("The actual MCP calls should be made via Cursor's MCP interface.")
    print()
    
    # Initialize integrator
    integrator = SlackMCPIntegrator(default_channel="#general")
    
    print("Step 1: List channels using MCP")
    print("-" * 80)
    print("To list channels, use MCP tool:")
    print("  mcp_mcp-slack_list_channels(channel_types='public_channel,private_channel')")
    print()
    
    print("Step 2: Send notification")
    print("-" * 80)
    print("To send a message, use MCP tool:")
    print("  mcp_mcp-slack_send_message(channel='#general', text='Hello from RCA Engine!')")
    print()
    
    # Example knowledge data
    example_knowledge = {
        "page_id": "2898362610",
        "title": "ARD: slice mini passbook",
        "product": "slice mini passbook",
        "document_type": "ARD",
        "events": [
            {"event_name": "mini_passbook_opened", "event_type": "PAGE OPEN"},
            {"event_name": "mini_passbook_search_clicked", "event_type": "CTA"}
        ],
        "tables": [{"headers": ["EVENT NAME", "EVENT TYPE"], "row_count": 15}],
        "entities": [{"name": "passbook"}],
        "extracted_at": "2024-01-01T12:00:00"
    }
    
    print("Step 3: Notify about knowledge extraction")
    print("-" * 80)
    notification = integrator.notify_knowledge_extracted(
        page_title=example_knowledge["title"],
        product=example_knowledge["product"],
        knowledge=example_knowledge,
        channel="#general"
    )
    print("Notification structure:")
    print(json.dumps(notification, indent=2))
    print()
    
    print("Step 4: Share events")
    print("-" * 80)
    events_share = integrator.share_events(
        product=example_knowledge["product"],
        events=example_knowledge["events"],
        channel="#general"
    )
    print("Events share structure:")
    print(json.dumps(events_share, indent=2))
    print()
    
    print("="*80)
    print("USAGE INSTRUCTIONS")
    print("="*80)
    print("""
To use MCP for Slack integration:

1. Ensure MCP Slack server is configured in Cursor
2. Use MCP tools to interact with Slack:
   - mcp_mcp-slack_send_message(channel='#channel', text='message')
   - mcp_mcp-slack_list_channels()
   - mcp_mcp-slack_get_channel_history(channel='#channel')
   - mcp_mcp-slack_upload_file(channel='#channel', file_path='path/to/file')

3. Integrate with knowledge extraction:
   
   integrator = SlackMCPIntegrator(default_channel='#rca-engine')
   
   # After knowledge extraction
   integrator.notify_knowledge_extracted(
       page_title=knowledge['title'],
       product=knowledge['product'],
       knowledge=knowledge
   )
   
   # Share events
   integrator.share_events(
       product=knowledge['product'],
       events=knowledge['events']
   )

4. The notifications will be sent to Slack automatically
    """)


if __name__ == "__main__":
    test_slack_integration()

