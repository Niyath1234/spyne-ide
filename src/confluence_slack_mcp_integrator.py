"""
Combined Confluence + Slack MCP Integration

Integrates both Confluence (for knowledge extraction) and Slack (for notifications)
using MCP (Model Context Protocol).
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.confluence_mcp_integrator import ConfluenceMCPKnowledgeBaseIntegrator
from src.slack_mcp_integrator import SlackMCPIntegrator


class ConfluenceSlackMCPIntegrator:
    """
    Combined integrator for Confluence knowledge extraction and Slack notifications.
    
    Workflow:
    1. Fetch page from Confluence via MCP
    2. Extract knowledge (events, tables, entities, relationships)
    3. Populate Knowledge Register, Knowledge Base, Product Index
    4. Send notifications to Slack via MCP
    """
    
    def __init__(
        self,
        confluence_space_key: Optional[str] = None,
        slack_channel: Optional[str] = None,
        knowledge_base_path: str = "metadata/knowledge_base.json",
        knowledge_register_path: str = "metadata/knowledge_register.json"
    ):
        """
        Initialize combined integrator.
        
        Args:
            confluence_space_key: Confluence space key (defaults to HOR)
            slack_channel: Default Slack channel for notifications
            knowledge_base_path: Path to knowledge base JSON
            knowledge_register_path: Path to knowledge register JSON
        """
        # Initialize Confluence integrator
        self.confluence_integrator = ConfluenceMCPKnowledgeBaseIntegrator(
            space_key=confluence_space_key,
            knowledge_base_path=knowledge_base_path,
            knowledge_register_path=knowledge_register_path
        )
        
        # Initialize Slack integrator
        self.slack_integrator = SlackMCPIntegrator(default_channel=slack_channel)
    
    def process_page_with_notifications(
        self,
        page_data: Dict,
        notify_slack: bool = True,
        slack_channel: Optional[str] = None
    ) -> Dict:
        """
        Process a Confluence page and send Slack notifications.
        
        Args:
            page_data: Confluence page data (from MCP)
            notify_slack: Whether to send Slack notifications
            slack_channel: Slack channel (defaults to default_channel)
            
        Returns:
            Processing result dictionary
        """
        result = {
            "success": False,
            "knowledge": None,
            "ref_id": None,
            "slack_notifications": []
        }
        
        try:
            # Extract knowledge
            knowledge = self.confluence_integrator.process_confluence_page_to_knowledge(page_data)
            
            # Populate knowledge register
            self.confluence_integrator.populate_knowledge_register(knowledge)
            
            # Get reference ID
            page_id = knowledge.get("page_id")
            product = knowledge.get("product", "")
            ref_id = f"PROD-{abs(hash(product)) % 10000}-{page_id}"
            
            result["success"] = True
            result["knowledge"] = knowledge
            result["ref_id"] = ref_id
            
            # Send Slack notifications
            if notify_slack:
                notifications = self._send_slack_notifications(
                    knowledge=knowledge,
                    ref_id=ref_id,
                    channel=slack_channel
                )
                result["slack_notifications"] = notifications
            
        except Exception as e:
            result["error"] = str(e)
            if notify_slack:
                # Send error notification
                error_notification = self.slack_integrator.send_error_notification(
                    error_message=str(e),
                    context=f"Processing page: {page_data.get('title', 'Unknown')}",
                    channel=slack_channel
                )
                result["slack_notifications"].append(error_notification)
        
        return result
    
    def _send_slack_notifications(
        self,
        knowledge: Dict,
        ref_id: str,
        channel: Optional[str] = None
    ) -> List[Dict]:
        """
        Send Slack notifications about extracted knowledge.
        
        Args:
            knowledge: Extracted knowledge dictionary
            ref_id: Reference ID
            channel: Slack channel
            
        Returns:
            List of notification dictionaries
        """
        notifications = []
        
        # Main notification
        notification = self.slack_integrator.notify_knowledge_extracted(
            page_title=knowledge.get("title", "Unknown"),
            product=knowledge.get("product", "Unknown"),
            knowledge=knowledge,
            channel=channel
        )
        notifications.append(notification)
        
        # Share events if there are many
        events = knowledge.get("events", [])
        if len(events) > 5:
            events_notification = self.slack_integrator.share_events(
                product=knowledge.get("product", "Unknown"),
                events=events,
                channel=channel,
                limit=10
            )
            notifications.append(events_notification)
        
        return notifications
    
    def process_page_by_id(
        self,
        page_id: str,
        notify_slack: bool = True,
        slack_channel: Optional[str] = None
    ) -> Dict:
        """
        Process a Confluence page by ID with Slack notifications.
        
        This method expects the page to be fetched via MCP first.
        
        Args:
            page_id: Confluence page ID
            notify_slack: Whether to send Slack notifications
            slack_channel: Slack channel
            
        Returns:
            Processing result dictionary
            
        Note: The actual page fetching should be done via MCP:
        mcp_mcp-atlassian_confluence_get_page(page_id=page_id)
        """
        return {
            "_instructions": f"""
            To process this page:
            
            1. Fetch page via MCP:
               page_data = mcp_mcp-atlassian_confluence_get_page(page_id='{page_id}')
            
            2. Convert MCP response to page_data format:
               page_data = {{
                   "id": mcp_response["metadata"]["id"],
                   "page_id": mcp_response["metadata"]["id"],
                   "title": mcp_response["metadata"]["title"],
                   "body": {{
                       "storage": {{
                           "value": mcp_response["content"]["value"]
                       }}
                   }},
                   "space": mcp_response["metadata"]["space"],
                   "version": {{"number": mcp_response["metadata"]["version"]}}
               }}
            
            3. Process with notifications:
               result = integrator.process_page_with_notifications(
                   page_data=page_data,
                   notify_slack={notify_slack},
                   slack_channel={slack_channel or "None"}
               )
            """
        }
    
    def search_and_process_pages(
        self,
        query: str,
        limit: int = 10,
        notify_slack: bool = True,
        slack_channel: Optional[str] = None
    ) -> Dict:
        """
        Search for Confluence pages and process them with Slack notifications.
        
        Args:
            query: Search query (CQL or simple text)
            limit: Maximum number of pages to process
            notify_slack: Whether to send Slack notifications
            slack_channel: Slack channel
            
        Returns:
            Processing results dictionary
            
        Note: The actual search should be done via MCP:
        mcp_mcp-atlassian_confluence_search(query=query, limit=limit)
        """
        return {
            "_instructions": f"""
            To search and process pages:
            
            1. Search via MCP:
               search_results = mcp_mcp-atlassian_confluence_search(
                   query='{query}',
                   limit={limit}
               )
            
            2. For each page in search_results:
               - Fetch full page: mcp_mcp-atlassian_confluence_get_page(page_id=page_id)
               - Convert to page_data format
               - Process: integrator.process_page_with_notifications(page_data)
            
            3. Results will be processed and Slack notifications sent automatically
            """
        }

