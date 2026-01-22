"""
Debug script to see why tables aren't being extracted from the ARD page
"""

import sys
import os
from pathlib import Path
import re
from html.parser import HTMLParser
from html import unescape

# Set environment variables
# Set environment variables (use actual values from environment or config)
os.environ["CONFLUENCE_URL"] = os.getenv("CONFLUENCE_URL", "https://slicepay.atlassian.net/wiki")
os.environ["CONFLUENCE_USERNAME"] = os.getenv("CONFLUENCE_USERNAME", "")
os.environ["CONFLUENCE_API_TOKEN"] = os.getenv("CONFLUENCE_API_TOKEN", "")

sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.confluence_to_knowledge_base import ConfluenceKnowledgeBaseIntegrator

# Read the HTML file
html_file = Path("data/raw/confluence_page_2898362610.html")
if not html_file.exists():
    print(f"File not found: {html_file}")
    sys.exit(1)

html_content = html_file.read_text(encoding='utf-8')

print("="*80)
print("DEBUGGING TABLE EXTRACTION")
print("="*80)

# Check if table tags exist
table_count = len(re.findall(r'<table[^>]*>', html_content, re.IGNORECASE))
print(f"\n1. Number of <table> tags found: {table_count}")

# Show first table structure
table_pattern = r'<table[^>]*>(.*?)</table>'
table_matches = list(re.finditer(table_pattern, html_content, re.DOTALL | re.IGNORECASE))

if table_matches:
    print(f"\n2. Found {len(table_matches)} table(s)")
    
    for i, match in enumerate(table_matches[:2], 1):  # Show first 2 tables
        table_html = match.group(0)
        print(f"\n   Table {i} (first 500 chars):")
        print(f"   {table_html[:500]}...")
        
        # Check for headers
        header_pattern = r'<th[^>]*>(.*?)</th>'
        headers = re.findall(header_pattern, table_html, re.DOTALL | re.IGNORECASE)
        print(f"\n   Headers found: {len(headers)}")
        for j, header in enumerate(headers[:5], 1):
            cleaned = re.sub(r'<[^>]+>', '', header)
            cleaned = unescape(cleaned)
            print(f"     {j}. {cleaned.strip()[:100]}")
        
        # Check for rows
        row_pattern = r'<tr[^>]*>(.*?)</tr>'
        rows = re.findall(row_pattern, table_html, re.DOTALL | re.IGNORECASE)
        print(f"\n   Rows found: {len(rows)}")
        if rows:
            print(f"     First row (first 200 chars): {rows[0][:200]}...")

# Test the actual extraction
print("\n" + "="*80)
print("TESTING ACTUAL EXTRACTION FUNCTION")
print("="*80)

integrator = ConfluenceKnowledgeBaseIntegrator()
tables = integrator.extract_tables_from_html(html_content)

print(f"\nExtracted {len(tables)} table(s)")

if tables:
    for i, table in enumerate(tables, 1):
        print(f"\nTable {i}:")
        print(f"  Headers: {table.get('headers', [])}")
        print(f"  Row count: {table.get('row_count', 0)}")
        if table.get('rows'):
            print(f"  First row: {table['rows'][0]}")
else:
    print("\nNo tables extracted. Checking why...")
    
    # Check if the regex patterns match
    table_pattern = r'<table[^>]*>(.*?)</table>'
    matches = list(re.finditer(table_pattern, html_content, re.DOTALL | re.IGNORECASE))
    print(f"  Regex found {len(matches)} table(s)")
    
    if matches:
        print("  Checking first table structure...")
        first_table = matches[0].group(1)
        
        # Check headers
        header_pattern = r'<th[^>]*>(.*?)</th>'
        headers = re.findall(header_pattern, first_table, re.DOTALL | re.IGNORECASE)
        print(f"    Headers in first table: {len(headers)}")
        
        # Check rows
        row_pattern = r'<tr[^>]*>(.*?)</tr>'
        rows = re.findall(row_pattern, first_table, re.DOTALL | re.IGNORECASE)
        print(f"    Rows in first table: {len(rows)}")

# Test event extraction
print("\n" + "="*80)
print("TESTING EVENT EXTRACTION")
print("="*80)

events = integrator.extract_events_from_ard(html_content)
print(f"\nExtracted {len(events)} event(s)")

if events:
    for i, event in enumerate(events[:5], 1):
        print(f"\nEvent {i}:")
        print(f"  Name: {event.get('event_name')}")
        print(f"  Type: {event.get('event_type')}")
        print(f"  Description: {event.get('description', '')[:100]}")
else:
    print("\nNo events extracted.")
    print("This is because no tables were extracted (events are extracted from tables)")

