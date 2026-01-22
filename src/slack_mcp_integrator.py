"""
Slack MCP Integration

Integrates Slack with the RCA Engine using MCP (Model Context Protocol).
Allows sending notifications, sharing knowledge, and integrating Slack workflows.
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.slack_mcp_adapter import SlackMCPAdapter, SlackMCPClient


class SlackMCPIntegrator:
    """
    Integrates Slack with RCA Engine using MCP.
    
    Provides functionality to:
    - Send notifications about knowledge extraction
    - Share extracted knowledge (events, tables, entities)
    - Post updates to Slack channels
    - Integrate with Slack workflows
    """
    
    def __init__(
        self,
        default_channel: Optional[str] = None,
        workspace_name: Optional[str] = None
    ):
        """
        Initialize Slack MCP integrator.
        
        Args:
            default_channel: Default Slack channel for notifications
            workspace_name: Slack workspace name
        """
        self.mcp_adapter = SlackMCPAdapter(workspace_name=workspace_name)
        self.default_channel = default_channel or os.getenv("SLACK_DEFAULT_CHANNEL")
    
    def notify_knowledge_extracted(
        self,
        page_title: str,
        product: str,
        knowledge: Dict,
        channel: Optional[str] = None
    ) -> Dict:
        """
        Send a notification when knowledge is extracted from a Confluence page.
        
        Args:
            page_title: Title of the processed page
            product: Extracted product name
            knowledge: Extracted knowledge dictionary
            channel: Slack channel (defaults to default_channel)
            
        Returns:
            Notification result
        """
        channel = channel or self.default_channel
        if not channel:
            return {"ok": False, "error": "No channel specified"}
        
        # Format message
        events_count = len(knowledge.get("events", []))
        tables_count = len(knowledge.get("tables", []))
        entities_count = len(knowledge.get("entities", []))
        
        message = f"""📚 *Knowledge Extracted*

*Page:* {page_title}
*Product:* {product}
*Document Type:* {knowledge.get('document_type', 'Unknown')}

*Extracted:*
• Events: {events_count}
• Tables: {tables_count}
• Entities: {entities_count}
• Relationships: {len(knowledge.get('relationships', []))}
• Metrics: {len(knowledge.get('metrics', []))}

*Reference ID:* `PROD-{abs(hash(product)) % 10000}-{knowledge.get('page_id', '')}`

Knowledge has been integrated into the Knowledge Register and Knowledge Base.
"""
        
        return {
            "_mcp_method": "mcp_mcp-slack_send_message",
            "channel": channel,
            "text": message
        }
    
    def share_events(
        self,
        product: str,
        events: List[Dict],
        channel: Optional[str] = None,
        limit: int = 10
    ) -> Dict:
        """
        Share extracted events to Slack.
        
        Args:
            product: Product name
            events: List of event dictionaries
            channel: Slack channel
            limit: Maximum number of events to share
            
        Returns:
            Share result
        """
        channel = channel or self.default_channel
        if not channel:
            return {"ok": False, "error": "No channel specified"}
        
        events_to_share = events[:limit]
        
        message = f"*Events for {product}*\n\n"
        for i, event in enumerate(events_to_share, 1):
            event_name = event.get("event_name", "Unknown")
            event_type = event.get("event_type", "N/A")
            description = event.get("description", "")[:100]
            
            message += f"{i}. *{event_name}* ({event_type})\n"
            if description:
                message += f"   _{description}..._\n"
            message += "\n"
        
        if len(events) > limit:
            message += f"\n_... and {len(events) - limit} more events_"
        
        return {
            "_mcp_method": "mcp_mcp-slack_send_message",
            "channel": channel,
            "text": message
        }
    
    def share_knowledge_summary(
        self,
        knowledge: Dict,
        channel: Optional[str] = None
    ) -> Dict:
        """
        Share a summary of extracted knowledge to Slack.
        
        Args:
            knowledge: Knowledge dictionary
            channel: Slack channel
            
        Returns:
            Share result
        """
        channel = channel or self.default_channel
        if not channel:
            return {"ok": False, "error": "No channel specified"}
        
        title = knowledge.get("title", "Unknown")
        product = knowledge.get("product", "Unknown")
        
        message = f"""📊 *Knowledge Summary*

*{title}*

*Product:* {product}
*Type:* {knowledge.get('document_type', 'Unknown')}

*Extracted Knowledge:*
"""
        
        # Events
        events = knowledge.get("events", [])
        if events:
            message += f"\n*Events ({len(events)}):*\n"
            for event in events[:5]:
                message += f"• {event.get('event_name')} ({event.get('event_type')})\n"
            if len(events) > 5:
                message += f"• ... and {len(events) - 5} more\n"
        
        # Tables
        tables = knowledge.get("tables", [])
        if tables:
            message += f"\n*Tables ({len(tables)}):*\n"
            for table in tables[:3]:
                headers = table.get("headers", [])
                row_count = table.get("row_count", 0)
                message += f"• {len(headers)} columns, {row_count} rows\n"
        
        # Entities
        entities = knowledge.get("entities", [])
        if entities:
            message += f"\n*Entities ({len(entities)}):*\n"
            for entity in entities[:5]:
                message += f"• {entity.get('name')}\n"
        
        message += f"\n_Extracted at: {knowledge.get('extracted_at', 'Unknown')}_"
        
        return {
            "_mcp_method": "mcp_mcp-slack_send_message",
            "channel": channel,
            "text": message
        }
    
    def upload_knowledge_report(
        self,
        knowledge: Dict,
        file_path: str,
        channel: Optional[str] = None
    ) -> Dict:
        """
        Upload a knowledge report file to Slack.
        
        Args:
            knowledge: Knowledge dictionary
            file_path: Path to the report file
            channel: Slack channel
            
        Returns:
            Upload result
        """
        channel = channel or self.default_channel
        if not channel:
            return {"ok": False, "error": "No channel specified"}
        
        title = knowledge.get("title", "Unknown")
        comment = f"Knowledge extraction report for: {title}"
        
        return {
            "_mcp_method": "mcp_mcp-slack_upload_file",
            "channel": channel,
            "file_path": file_path,
            "initial_comment": comment
        }
    
    def send_error_notification(
        self,
        error_message: str,
        context: Optional[str] = None,
        channel: Optional[str] = None
    ) -> Dict:
        """
        Send an error notification to Slack.
        
        Args:
            error_message: Error message
            context: Additional context
            channel: Slack channel
            
        Returns:
            Notification result
        """
        channel = channel or self.default_channel
        if not channel:
            return {"ok": False, "error": "No channel specified"}
        
        message = f"⚠️ *Error in RCA Engine*\n\n*Error:* {error_message}\n"
        if context:
            message += f"*Context:* {context}\n"
        message += f"\n_Time: {datetime.now().isoformat()}_"
        
        return {
            "_mcp_method": "mcp_mcp-slack_send_message",
            "channel": channel,
            "text": message
        }
    
    def send_success_notification(
        self,
        message: str,
        details: Optional[str] = None,
        channel: Optional[str] = None
    ) -> Dict:
        """
        Send a success notification to Slack.
        
        Args:
            message: Success message
            details: Additional details
            channel: Slack channel
            
        Returns:
            Notification result
        """
        channel = channel or self.default_channel
        if not channel:
            return {"ok": False, "error": "No channel specified"}
        
        slack_message = f"✅ *Success*\n\n{message}\n"
        if details:
            slack_message += f"\n*Details:* {details}\n"
        
        return {
            "_mcp_method": "mcp_mcp-slack_send_message",
            "channel": channel,
            "text": slack_message
        }

