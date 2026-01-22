"""
Confluence to Knowledge Base Integration using MCP

This module integrates Confluence documents with Knowledge Base using MCP (Model Context Protocol)
instead of direct API calls. It provides the same interface as ConfluenceKnowledgeBaseIntegrator
but uses MCP tools for Confluence access.
"""

import os
import sys
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from html.parser import HTMLParser
from html import unescape

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.confluence_mcp_adapter import ConfluenceMCPAdapter, ConfluenceMCPClient
from src.product_index import ProductIndex
from src.knowledge_register_sync import KnowledgeRegisterSyncer
from knowledge_base_enricher import KnowledgeBaseEnricher
from src.confluence_to_knowledge_base import HTMLContentExtractor


class ConfluenceMCPKnowledgeBaseIntegrator:
    """
    Integrates Confluence documents with Knowledge Base using MCP.
    
    This class provides the same interface as ConfluenceKnowledgeBaseIntegrator
    but uses MCP tools instead of direct API calls.
    
    Note: This class is designed to work with MCP tools that are called
    via the MCP server interface. The actual MCP calls should be made
    by the calling code (e.g., via Cursor's MCP integration).
    """
    
    def __init__(
        self,
        space_key: Optional[str] = None,
        knowledge_base_path: str = "metadata/knowledge_base.json",
        knowledge_register_path: str = "metadata/knowledge_register.json"
    ):
        """
        Initialize the MCP-based integrator.
        
        Args:
            space_key: Confluence space key (defaults to HOR)
            knowledge_base_path: Path to knowledge base JSON
            knowledge_register_path: Path to knowledge register JSON
        """
        # Initialize MCP adapter
        self.mcp_adapter = ConfluenceMCPAdapter(space_key=space_key)
        
        # Initialize Knowledge Base Enricher
        self.kb_enricher = KnowledgeBaseEnricher(knowledge_base_path=knowledge_base_path)
        
        # Knowledge Register syncer
        self.knowledge_register_syncer = KnowledgeRegisterSyncer(
            knowledge_register_path=knowledge_register_path
        )
        
        # Product index
        self.product_index = ProductIndex()
    
    def _detect_document_type(self, title: str) -> str:
        """Detect document type from title."""
        title_upper = title.upper()
        if title_upper.startswith("ARD"):
            return "ARD"
        elif title_upper.startswith("PRD"):
            return "PRD"
        elif title_upper.startswith("TRD"):
            return "TRD"
        return "UNKNOWN"
    
    def extract_tables_from_html(self, html_content: str) -> List[Dict]:
        """
        Extract table structures from HTML content.
        (Same implementation as ConfluenceKnowledgeBaseIntegrator)
        """
        tables = []
        
        # Find all table tags
        table_pattern = r'<table[^>]*>(.*?)</table>'
        table_matches = re.finditer(table_pattern, html_content, re.DOTALL | re.IGNORECASE)
        
        for table_match in table_matches:
            table_html = table_match.group(1)
            
            # Extract rows first
            row_pattern = r'<tr[^>]*>(.*?)</tr>'
            row_matches = list(re.finditer(row_pattern, table_html, re.DOTALL | re.IGNORECASE))
            
            if not row_matches:
                continue
            
            # Try to extract headers from <th> tags first
            header_pattern = r'<th[^>]*>(.*?)</th>'
            header_matches = list(re.finditer(header_pattern, table_html, re.DOTALL | re.IGNORECASE))
            headers = []
            
            if header_matches:
                # Headers are in <th> tags
                for header_match in header_matches:
                    header_text = self._clean_html(header_match.group(1))
                    if header_text:
                        headers.append(header_text)
            else:
                # Headers might be in first row as <td> with styling
                first_row_html = row_matches[0].group(1)
                cell_pattern = r'<t[dh][^>]*>(.*?)</t[dh]>'
                first_row_cells = []
                for cell_match in re.finditer(cell_pattern, first_row_html, re.DOTALL | re.IGNORECASE):
                    cell_html = cell_match.group(1)
                    cell_text = self._clean_html(cell_html)
                    first_row_cells.append(cell_text)
                
                # Check if first row looks like headers
                is_header_row = (
                    any(keyword in ' '.join(first_row_cells).upper() 
                        for keyword in ['EVENT', 'TYPE', 'NAME', 'DESCRIPTION', 'PROPERTY', 'HEADER', 'COLUMN']) or
                    '<strong>' in first_row_html.lower()
                )
                
                if is_header_row:
                    headers = first_row_cells
                    row_matches = row_matches[1:]
            
            # Extract data rows
            rows = []
            for row_match in row_matches:
                row_html = row_match.group(1)
                cell_pattern = r'<t[dh][^>]*>(.*?)</t[dh]>'
                cells = []
                for cell_match in re.finditer(cell_pattern, row_html, re.DOTALL | re.IGNORECASE):
                    cell_text = self._clean_html(cell_match.group(1))
                    cells.append(cell_text)
                
                if cells and any(cell.strip() for cell in cells):
                    rows.append(cells)
            
            if headers and rows:
                tables.append({
                    "headers": headers,
                    "rows": rows,
                    "row_count": len(rows)
                })
        
        return tables
    
    def _clean_html(self, html: str) -> str:
        """Clean HTML and extract text."""
        text = re.sub(r'<[^>]+>', '', html)
        text = unescape(text)
        text = ' '.join(text.split())
        return text.strip()
    
    def extract_events_from_ard(self, html_content: str) -> List[Dict]:
        """Extract ARD Events from HTML content."""
        events = []
        tables = self.extract_tables_from_html(html_content)
        
        for table in tables:
            headers = [h.lower() for h in table["headers"]]
            
            if "event name" in str(headers).lower() or "event type" in str(headers).lower():
                event_name_idx = None
                event_type_idx = None
                event_description_idx = None
                event_properties_idx = None
                
                for i, header in enumerate(headers):
                    if "event name" in header:
                        event_name_idx = i
                    elif "event type" in header:
                        event_type_idx = i
                    elif "event description" in header or "description" in header:
                        event_description_idx = i
                    elif "event property" in header or "property" in header:
                        event_properties_idx = i
                
                for row in table["rows"]:
                    if event_name_idx is not None and event_name_idx < len(row):
                        event_name = row[event_name_idx].strip()
                        
                        if event_name and event_name.lower() not in ["event name", "event_name"]:
                            event = {
                                "event_name": event_name,
                                "event_type": row[event_type_idx].strip() if event_type_idx and event_type_idx < len(row) else "",
                                "description": row[event_description_idx].strip() if event_description_idx and event_description_idx < len(row) else "",
                                "properties": row[event_properties_idx].strip() if event_properties_idx and event_properties_idx < len(row) else "",
                                "source": "ARD"
                            }
                            events.append(event)
        
        return events
    
    def extract_entities_from_content(self, html_content: str, doc_type: str) -> List[Dict]:
        """Extract entities from document content."""
        entities = []
        parser = HTMLContentExtractor()
        parser.feed(html_content)
        text_content = parser.get_text()
        
        table_patterns = [
            r'table[:\s]+([a-z_][a-z0-9_]*)',
            r'from\s+([a-z_][a-z0-9_]*)',
            r'join\s+([a-z_][a-z0-9_]*)',
            r'entity[:\s]+([a-z_][a-z0-9_]*)',
        ]
        
        found_tables = set()
        for pattern in table_patterns:
            matches = re.finditer(pattern, text_content, re.IGNORECASE)
            for match in matches:
                table_name = match.group(1).lower()
                if len(table_name) > 2:
                    found_tables.add(table_name)
        
        for table_name in found_tables:
            entities.append({
                "name": table_name,
                "type": "table",
                "source": doc_type,
                "description": f"Table mentioned in {doc_type} document"
            })
        
        return entities
    
    def process_confluence_page_to_knowledge(self, page_data: Dict) -> Dict:
        """
        Process a Confluence page and extract knowledge.
        
        Args:
            page_data: Confluence page data dictionary (from MCP)
            
        Returns:
            Extracted knowledge dictionary
        """
        # Handle MCP response format
        if isinstance(page_data, str):
            page_data = json.loads(page_data)
        
        # Extract page ID and title
        page_id = page_data.get("id") or page_data.get("page_id")
        title = page_data.get("title", "")
        doc_type = self._detect_document_type(title)
        
        # Get content (MCP may return markdown or HTML)
        content = page_data.get("content", "") or page_data.get("body", {}).get("storage", {}).get("value", "")
        html_content = content
        
        # If content is markdown, we might need to convert it back to HTML for table extraction
        # For now, assume MCP returns HTML in storage format
        
        # Extract product
        product = self.mcp_adapter.extract_product_from_title(title) or "Unknown"
        
        # Extract structured information
        knowledge = {
            "page_id": page_id,
            "title": title,
            "document_type": doc_type,
            "product": product,
            "extracted_at": datetime.now().isoformat(),
            "entities": [],
            "events": [],
            "tables": [],
            "relationships": [],
            "metrics": [],
            "business_rules": []
        }
        
        # Extract tables
        tables = self.extract_tables_from_html(html_content)
        knowledge["tables"] = tables
        
        # Extract ARD Events if ARD document
        if doc_type == "ARD":
            events = self.extract_events_from_ard(html_content)
            knowledge["events"] = events
        
        # Extract entities
        entities = self.extract_entities_from_content(html_content, doc_type)
        knowledge["entities"] = entities
        
        return knowledge
    
    def populate_knowledge_register(self, knowledge: Dict):
        """Populate Knowledge Register with extracted knowledge."""
        # Same implementation as ConfluenceKnowledgeBaseIntegrator
        page_id = knowledge.get("page_id")
        title = knowledge.get("title", "")
        product = knowledge.get("product", "")
        
        if not page_id:
            return
        
        # Create reference ID
        ref_id = f"PROD-{abs(hash(product)) % 10000}-{page_id}"
        
        # Extract text content
        full_text = title
        if knowledge.get("entities"):
            full_text += " " + " ".join([e.get("name", "") for e in knowledge["entities"]])
        if knowledge.get("events"):
            full_text += " " + " ".join([e.get("event_name", "") for e in knowledge["events"]])
        
        # Keywords
        keywords = [product] + [e.get("name", "") for e in knowledge.get("entities", [])]
        keywords = [k.lower() for k in keywords if k]
        
        # Create page entry
        page_entry = {
            "page_id": str(page_id),
            "ref_id": ref_id,
            "title": title,
            "product": product,
            "document_type": knowledge.get("document_type"),
            "keywords": keywords,
            "full_text": full_text,
            "segments": {
                "entities": knowledge.get("entities", []),
                "events": knowledge.get("events", []),
                "tables": knowledge.get("tables", []),
                "relationships": knowledge.get("relationships", []),
                "metrics": knowledge.get("metrics", [])
            },
            "extracted_at": knowledge.get("extracted_at")
        }
        
        # Add to knowledge register
        self.knowledge_register_syncer.add_page(page_entry)
        
        # Update knowledge base
        self.kb_enricher.enrich_from_knowledge(knowledge)
        
        # Update product index
        self.product_index.add_document(
            product=product,
            document={
                "page_id": page_id,
                "title": title,
                "document_type": knowledge.get("document_type"),
                "ref_id": ref_id
            }
        )

