"""
Confluence to Knowledge Base Integration

Fetches ARD/PRD/TRD from Confluence, extracts structured information,
and populates the Knowledge Register for vector search.
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

from src.confluence_ingest import ConfluenceIngester
from src.product_index import ProductIndex
from src.knowledge_register_sync import KnowledgeRegisterSyncer
from knowledge_base_enricher import KnowledgeBaseEnricher


class HTMLContentExtractor(HTMLParser):
    """Extract text content from HTML."""
    
    def __init__(self):
        super().__init__()
        self.text = []
        self.current_tag = None
    
    def handle_starttag(self, tag, attrs):
        self.current_tag = tag
        if tag in ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'td', 'th']:
            self.text.append('\n')
    
    def handle_endtag(self, tag):
        if tag in ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'tr']:
            self.text.append('\n')
    
    def handle_data(self, data):
        if self.current_tag not in ['script', 'style']:
            self.text.append(data)
    
    def get_text(self) -> str:
        return ''.join(self.text).strip()


class ConfluenceKnowledgeBaseIntegrator:
    """
    Integrates Confluence documents with Knowledge Base.
    
    Workflow:
    1. Fetch ARD/PRD/TRD from Confluence
    2. Extract structured information (tables, events, entities, relationships)
    3. Populate Knowledge Register
    4. Enable vector search
    """
    
    def __init__(
        self,
        confluence_url: Optional[str] = None,
        confluence_username: Optional[str] = None,
        confluence_api_token: Optional[str] = None,
        knowledge_base_path: str = "metadata/knowledge_base.json",
        knowledge_register_path: str = "metadata/knowledge_register.json"
    ):
        """
        Initialize the integrator.
        
        Args:
            confluence_url: Confluence URL
            confluence_username: Confluence username
            confluence_api_token: Confluence API token
            knowledge_base_path: Path to knowledge base JSON
            knowledge_register_path: Path to knowledge register JSON
        """
        # Initialize Confluence ingester
        self.confluence_ingester = ConfluenceIngester(
            url=confluence_url or os.getenv("CONFLUENCE_URL"),
            username=confluence_username or os.getenv("CONFLUENCE_USERNAME"),
            api_token=confluence_api_token or os.getenv("CONFLUENCE_API_TOKEN")
        )
        
        # Initialize Knowledge Base Enricher
        self.kb_enricher = KnowledgeBaseEnricher(knowledge_base_path=knowledge_base_path)
        
        # Knowledge Register syncer
        self.knowledge_register_syncer = KnowledgeRegisterSyncer(
            knowledge_register_path=knowledge_register_path
        )
        
        # Product index
        self.product_index = ProductIndex()
    
    def extract_tables_from_html(self, html_content: str) -> List[Dict]:
        """
        Extract table structures from HTML content.
        
        Args:
            html_content: HTML content from Confluence
            
        Returns:
            List of table dictionaries with headers and rows
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
                # Headers might be in first row as <td> with styling (common in Confluence)
                # Check first row for header-like content (contains <strong> or has highlight styling)
                first_row_html = row_matches[0].group(1)
                cell_pattern = r'<t[dh][^>]*>(.*?)</t[dh]>'
                first_row_cells = []
                for cell_match in re.finditer(cell_pattern, first_row_html, re.DOTALL | re.IGNORECASE):
                    cell_html = cell_match.group(1)
                    cell_text = self._clean_html(cell_html)
                    first_row_cells.append(cell_text)
                
                # Check if first row looks like headers (contains "EVENT", "TYPE", etc. or has <strong>)
                is_header_row = (
                    any(keyword in ' '.join(first_row_cells).upper() 
                        for keyword in ['EVENT', 'TYPE', 'NAME', 'DESCRIPTION', 'PROPERTY', 'HEADER', 'COLUMN']) or
                    '<strong>' in first_row_html.lower()
                )
                
                if is_header_row:
                    headers = first_row_cells
                    # Skip first row when extracting data rows
                    row_matches = row_matches[1:]
            
            # Extract data rows
            rows = []
            for row_match in row_matches:
                row_html = row_match.group(1)
                
                # Extract cells
                cell_pattern = r'<t[dh][^>]*>(.*?)</t[dh]>'
                cells = []
                for cell_match in re.finditer(cell_pattern, row_html, re.DOTALL | re.IGNORECASE):
                    cell_text = self._clean_html(cell_match.group(1))
                    cells.append(cell_text)
                
                # Only add non-empty rows
                if cells and any(cell.strip() for cell in cells):
                    rows.append(cells)
            
            # Only add table if we have headers and at least one data row
            if headers and rows:
                tables.append({
                    "headers": headers,
                    "rows": rows,
                    "row_count": len(rows)
                })
        
        return tables
    
    def _clean_html(self, html: str) -> str:
        """Clean HTML and extract text."""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', html)
        # Decode HTML entities
        text = unescape(text)
        # Clean whitespace
        text = ' '.join(text.split())
        return text.strip()
    
    def extract_events_from_ard(self, html_content: str) -> List[Dict]:
        """
        Extract ARD Events from HTML content.
        
        Looks for tables with event tracking information.
        
        Args:
            html_content: HTML content from ARD
            
        Returns:
            List of event dictionaries
        """
        events = []
        tables = self.extract_tables_from_html(html_content)
        
        for table in tables:
            headers = [h.lower() for h in table["headers"]]
            
            # Check if this is an ARD Events table
            if "event name" in str(headers).lower() or "event type" in str(headers).lower():
                event_name_idx = None
                event_type_idx = None
                event_description_idx = None
                event_properties_idx = None
                
                # Find column indices
                for i, header in enumerate(headers):
                    if "event name" in header:
                        event_name_idx = i
                    elif "event type" in header:
                        event_type_idx = i
                    elif "event description" in header or "description" in header:
                        event_description_idx = i
                    elif "event property" in header or "property" in header:
                        event_properties_idx = i
                
                # Extract events from rows
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
        """
        Extract entities (tables, domains, concepts) from document content.
        
        Args:
            html_content: HTML content
            doc_type: Document type (ARD/PRD/TRD)
            
        Returns:
            List of entity dictionaries
        """
        entities = []
        
        # Extract text content
        parser = HTMLContentExtractor()
        parser.feed(html_content)
        text_content = parser.get_text()
        
        # Look for table mentions (common patterns)
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
                if len(table_name) > 2:  # Filter out very short matches
                    found_tables.add(table_name)
        
        # Create entities from found tables
        for table_name in found_tables:
            entities.append({
                "name": table_name,
                "type": "table",
                "source": doc_type,
                "description": f"Table mentioned in {doc_type} document"
            })
        
        return entities
    
    def extract_relationships_from_content(self, html_content: str) -> List[Dict]:
        """
        Extract relationships between entities.
        
        Args:
            html_content: HTML content
            
        Returns:
            List of relationship dictionaries
        """
        relationships = []
        
        # Extract text content
        parser = HTMLContentExtractor()
        parser.feed(html_content)
        text_content = parser.get_text()
        
        # Look for join/relationship patterns
        join_patterns = [
            r'join\s+([a-z_][a-z0-9_]*)\s+.*?\s+([a-z_][a-z0-9_]*)',
            r'([a-z_][a-z0-9_]*)\s+relates?\s+to\s+([a-z_][a-z0-9_]*)',
            r'([a-z_][a-z0-9_]*)\s+->\s+([a-z_][a-z0-9_]*)',
        ]
        
        for pattern in join_patterns:
            matches = re.finditer(pattern, text_content, re.IGNORECASE)
            for match in matches:
                relationships.append({
                    "from_entity": match.group(1).lower(),
                    "to_entity": match.group(2).lower(),
                    "type": "relationship",
                    "source": "document_analysis"
                })
        
        return relationships
    
    def process_confluence_page_to_knowledge(self, page_data: Dict) -> Dict:
        """
        Process a Confluence page and extract knowledge.
        
        Args:
            page_data: Confluence page data dictionary
            
        Returns:
            Extracted knowledge dictionary
        """
        page_id = page_data.get("id")
        title = page_data.get("title", "")
        doc_type = self._detect_document_type(title)
        
        # Get HTML content
        body = page_data.get("body", {})
        storage = body.get("storage", {})
        html_content = storage.get("value", "")
        
        # Extract product
        product = self.confluence_ingester.extract_product_from_title(title) or "Unknown"
        
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
        
        # Extract relationships
        relationships = self.extract_relationships_from_content(html_content)
        knowledge["relationships"] = relationships
        
        # Extract metrics (look for "metrics" section)
        metrics = self._extract_metrics(html_content)
        knowledge["metrics"] = metrics
        
        return knowledge
    
    def _detect_document_type(self, title: str) -> str:
        """Detect document type from title."""
        title_lower = title.lower()
        if "ard" in title_lower or "architecture" in title_lower:
            return "ARD"
        elif "prd" in title_lower or "product" in title_lower:
            return "PRD"
        elif "trd" in title_lower or "technical" in title_lower:
            return "TRD"
        return "UNKNOWN"
    
    def _extract_metrics(self, html_content: str) -> List[Dict]:
        """Extract metrics from content."""
        metrics = []
        
        parser = HTMLContentExtractor()
        parser.feed(html_content)
        text_content = parser.get_text()
        
        # Look for metrics section
        metrics_pattern = r'metrics?[:\s]+(.*?)(?=\n\n|\n[A-Z]|$)'
        matches = re.finditer(metrics_pattern, text_content, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            metrics_text = match.group(1)
            # Extract individual metrics (numbered lists, bullet points)
            metric_items = re.findall(r'[•\-\d+\.]\s*(.+?)(?=\n|$)', metrics_text)
            for item in metric_items:
                metrics.append({
                    "name": item.strip(),
                    "description": item.strip(),
                    "source": "document"
                })
        
        return metrics
    
    def populate_knowledge_register(self, knowledge_data: Dict):
        """
        Populate Knowledge Register with extracted knowledge.
        
        Args:
            knowledge_data: Extracted knowledge dictionary
        """
        page_id = knowledge_data.get("page_id")
        product = knowledge_data.get("product", "Unknown")
        
        # Create ref_id
        ref_id = f"PROD-{abs(hash(product)) % 10000}-{page_id}"
        
        # Create segments
        segments = {
            "entities": knowledge_data.get("entities", []),
            "events": knowledge_data.get("events", []),
            "tables": knowledge_data.get("tables", []),
            "relationships": knowledge_data.get("relationships", []),
            "metrics": knowledge_data.get("metrics", [])
        }
        
        # Create metadata
        metadata = {
            "document_type": knowledge_data.get("document_type"),
            "product": product,
            "page_id": page_id,
            "extracted_at": knowledge_data.get("extracted_at"),
            "confluence_url": f"https://slicepay.atlassian.net/wiki/spaces/{knowledge_data.get('space_key', '')}/pages/{page_id}"
        }
        
        # Add to knowledge register using syncer
        self.knowledge_register_syncer.add_knowledge_page(
            ref_id=ref_id,
            title=knowledge_data.get("title"),
            full_text=self._create_full_text(knowledge_data),
            keywords=self._extract_keywords(knowledge_data),
            segments=segments,
            metadata=metadata
        )
        
        # Also populate Python knowledge base
        self._populate_python_knowledge_base(knowledge_data, ref_id)
    
    def _populate_python_knowledge_base(self, knowledge_data: Dict, ref_id: str):
        """Populate Python knowledge base with extracted data."""
        # Add entities as tables
        for entity in knowledge_data.get("entities", []):
            entity_name = entity.get("name")
            if entity_name:
                self.kb_enricher.knowledge_base.setdefault("tables", {})[entity_name] = {
                    "name": entity_name,
                    "description": entity.get("description", ""),
                    "type": entity.get("type", "table"),
                    "source": knowledge_data.get("document_type"),
                    "product": knowledge_data.get("product"),
                    "ref_id": ref_id
                }
        
        # Add events
        for event in knowledge_data.get("events", []):
            event_name = event.get("event_name")
            if event_name:
                self.kb_enricher.knowledge_base.setdefault("events", {})[event_name] = {
                    "name": event_name,
                    "type": event.get("event_type", ""),
                    "description": event.get("description", ""),
                    "properties": event.get("properties", ""),
                    "source": "ARD",
                    "product": knowledge_data.get("product"),
                    "ref_id": ref_id
                }
        
        # Add relationships
        for rel in knowledge_data.get("relationships", []):
            rel_key = f"{rel.get('from_entity')}_{rel.get('to_entity')}"
            self.kb_enricher.knowledge_base.setdefault("relationships", {})[rel_key] = {
                "from_entity": rel.get("from_entity"),
                "to_entity": rel.get("to_entity"),
                "type": rel.get("type", "relationship"),
                "source": rel.get("source", "document"),
                "product": knowledge_data.get("product"),
                "ref_id": ref_id
            }
        
        # Save knowledge base
        kb_path = Path(self.kb_enricher.kb_path)
        with open(kb_path, 'w', encoding='utf-8') as f:
            json.dump(self.kb_enricher.knowledge_base, f, indent=2, ensure_ascii=False)
    
    def _create_full_text(self, knowledge_data: Dict) -> str:
        """Create full text for knowledge page."""
        parts = []
        
        parts.append(f"Title: {knowledge_data.get('title')}")
        parts.append(f"Product: {knowledge_data.get('product')}")
        parts.append(f"Document Type: {knowledge_data.get('document_type')}")
        
        # Add entities
        entities = knowledge_data.get("entities", [])
        if entities:
            parts.append("\nEntities:")
            for entity in entities:
                parts.append(f"  - {entity.get('name')}: {entity.get('description', '')}")
        
        # Add events
        events = knowledge_data.get("events", [])
        if events:
            parts.append("\nEvents:")
            for event in events:
                parts.append(f"  - {event.get('event_name')}: {event.get('description', '')}")
        
        # Add metrics
        metrics = knowledge_data.get("metrics", [])
        if metrics:
            parts.append("\nMetrics:")
            for metric in metrics:
                parts.append(f"  - {metric.get('name')}")
        
        return "\n".join(parts)
    
    def _extract_keywords(self, knowledge_data: Dict) -> List[str]:
        """Extract keywords from knowledge data."""
        keywords = []
        
        # Add product
        product = knowledge_data.get("product")
        if product:
            keywords.append(product.lower())
        
        # Add document type
        doc_type = knowledge_data.get("document_type")
        if doc_type:
            keywords.append(doc_type.lower())
        
        # Add entity names
        for entity in knowledge_data.get("entities", []):
            keywords.append(entity.get("name", "").lower())
        
        # Add event names
        for event in knowledge_data.get("events", []):
            event_name = event.get("event_name", "")
            if event_name:
                keywords.append(event_name.lower())
        
        return list(set([k for k in keywords if k]))
    
    
    def integrate_from_confluence(
        self,
        document_types: List[str] = ["ARD", "PRD", "TRD"],
        space_key: Optional[str] = None,
        limit: int = 100
    ) -> Dict:
        """
        Main integration method: Fetch from Confluence and populate Knowledge Base.
        
        Args:
            document_types: Document types to fetch
            space_key: Space key to search
            limit: Maximum pages per type
            
        Returns:
            Integration results
        """
        print("="*60)
        print("Confluence to Knowledge Base Integration")
        print("="*60)
        
        # Step 1: Fetch pages from Confluence
        print("\nStep 1: Fetching pages from Confluence...")
        pages = self.confluence_ingester.search_confluence_pages(
            document_types=document_types,
            space_key=space_key,
            limit=limit
        )
        
        if not pages:
            return {"success": False, "error": "No pages found"}
        
        print(f"Found {len(pages)} pages")
        
        # Step 2: Process each page
        print("\nStep 2: Processing pages and extracting knowledge...")
        results = {
            "success": True,
            "processed": 0,
            "failed": 0,
            "knowledge_pages": [],
            "products": set()
        }
        
        for page in pages:
            try:
                # Get full page content using direct API
                page_id = page.get("id")
                import requests
                from requests.auth import HTTPBasicAuth
                
                api_url = f"{self.confluence_ingester.api_base}/rest/api/content/{page_id}"
                params = {"expand": "body.storage,space,version"}
                
                response = requests.get(
                    api_url,
                    auth=HTTPBasicAuth(
                        self.confluence_ingester.username,
                        self.confluence_ingester.api_token
                    ),
                    headers={"Accept": "application/json"},
                    params=params,
                    timeout=30
                )
                
                if response.status_code != 200:
                    print(f"  ⚠ Could not fetch full content for page {page_id}: {response.status_code}")
                    continue
                
                full_page = response.json()
                
                # Extract knowledge
                knowledge = self.process_confluence_page_to_knowledge(full_page)
                
                # Add space key to knowledge
                space = full_page.get("space", {})
                knowledge["space_key"] = space.get("key", "")
                
                # Populate knowledge register
                self.populate_knowledge_register(knowledge)
                
                # Update product index
                product = knowledge.get("product")
                if product:
                    results["products"].add(product)
                    self.product_index.add_document_to_product(product, {
                        "file_name": f"confluence_page_{page_id}.html",
                        "page_id": page_id,
                        "title": knowledge.get("title"),
                        "document_type": knowledge.get("document_type"),
                        "reference_id": f"PROD-{product}-{page_id}"
                    })
                
                results["processed"] += 1
                results["knowledge_pages"].append({
                    "page_id": page_id,
                    "title": knowledge.get("title"),
                    "product": product,
                    "entities": len(knowledge.get("entities", [])),
                    "events": len(knowledge.get("events", [])),
                    "tables": len(knowledge.get("tables", []))
                })
                
                print(f"  ✓ Processed: {knowledge.get('title')} ({product})")
                print(f"    Entities: {len(knowledge.get('entities', []))}, "
                      f"Events: {len(knowledge.get('events', []))}, "
                      f"Tables: {len(knowledge.get('tables', []))}")
            
            except Exception as e:
                results["failed"] += 1
                print(f"  ✗ Error processing page {page.get('id')}: {e}")
                import traceback
                traceback.print_exc()
        
        # Save product index
        self.product_index.save()
        
        # Save knowledge register
        self.knowledge_register_syncer.save()
        
        # Step 3: Summary
        print("\n" + "="*60)
        print("Integration Complete")
        print("="*60)
        print(f"Processed: {results['processed']}/{len(pages)} pages")
        print(f"Products: {len(results['products'])}")
        print(f"Knowledge pages created: {len(results['knowledge_pages'])}")
        
        # Show statistics
        stats = self.knowledge_register_syncer.get_statistics()
        print(f"\nKnowledge Register Statistics:")
        print(f"  Total pages: {stats['total_pages']}")
        print(f"  Total keywords: {stats['total_keywords']}")
        print(f"  Total entities: {stats['total_entities']}")
        print(f"  Total events: {stats['total_events']}")
        print(f"  Total tables: {stats['total_tables']}")
        
        results["products"] = list(results["products"])
        results["statistics"] = stats
        
        return results


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Integrate Confluence with Knowledge Base")
    parser.add_argument("--url", type=str, help="Confluence URL")
    parser.add_argument("--username", type=str, help="Confluence username")
    parser.add_argument("--api-token", type=str, help="Confluence API token")
    parser.add_argument("--space-key", type=str, help="Confluence space key")
    parser.add_argument("--doc-types", nargs="+", default=["ARD", "PRD", "TRD"],
                       help="Document types to fetch")
    parser.add_argument("--limit", type=int, default=100, help="Max pages per type")
    
    args = parser.parse_args()
    
    integrator = ConfluenceKnowledgeBaseIntegrator(
        confluence_url=args.url,
        confluence_username=args.username,
        confluence_api_token=args.api_token
    )
    
    results = integrator.integrate_from_confluence(
        document_types=args.doc_types,
        space_key=args.space_key,
        limit=args.limit
    )
    
    if results.get("success"):
        print("\n✓ Knowledge Base populated!")
        print(f"✓ Knowledge Register: {integrator.knowledge_register_path}")
        print(f"✓ Product Index: {integrator.product_index.index_file}")
        print("\nNext: Documents are now searchable via vector search")


if __name__ == "__main__":
    main()

