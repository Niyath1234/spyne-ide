#!/bin/bash

# Complete Workflow: Confluence → Knowledge Base → Vector Search
# 
# This script:
# 1. Fetches ARD/PRD/TRD from Confluence
# 2. Extracts structured information (entities, events, tables, relationships)
# 3. Populates Knowledge Register
# 4. Processes documents for vector search
# 5. Indexes in vector database

set -e

echo "=========================================="
echo "Confluence to Knowledge Base Workflow"
echo "=========================================="
echo ""

# Check environment variables
if [ -z "$CONFLUENCE_URL" ] || [ -z "$CONFLUENCE_USERNAME" ] || [ -z "$CONFLUENCE_API_TOKEN" ]; then
    echo "Error: Confluence credentials not set"
    echo "Set: CONFLUENCE_URL, CONFLUENCE_USERNAME, CONFLUENCE_API_TOKEN"
    exit 1
fi

# Step 1: Fetch from Confluence and populate Knowledge Base
echo "Step 1: Fetching from Confluence and populating Knowledge Base..."
echo "----------------------------------------"
python src/confluence_to_knowledge_base.py \
    --url "$CONFLUENCE_URL" \
    --username "$CONFLUENCE_USERNAME" \
    --api-token "$CONFLUENCE_API_TOKEN" \
    ${CONFLUENCE_SPACE_KEY:+--space-key "$CONFLUENCE_SPACE_KEY"}

if [ $? -ne 0 ]; then
    echo "Error: Failed to fetch from Confluence and populate Knowledge Base"
    exit 1
fi

echo ""
echo "✓ Knowledge Base populated"
echo ""

# Step 2: Process documents (ingest → chunk → index)
echo "Step 2: Processing documents for vector search..."
echo "----------------------------------------"
python src/pipeline.py --step all

if [ $? -ne 0 ]; then
    echo "Error: Document processing failed"
    exit 1
fi

echo ""
echo "=========================================="
echo "✓ Workflow Complete!"
echo "=========================================="
echo ""
echo "Knowledge Base Statistics:"
python -c "
from src.knowledge_register_sync import KnowledgeRegisterSyncer
syncer = KnowledgeRegisterSyncer()
stats = syncer.get_statistics()
print(f'  - Total pages: {stats[\"total_pages\"]}')
print(f'  - Total entities: {stats[\"total_entities\"]}')
print(f'  - Total events: {stats[\"total_events\"]}')
print(f'  - Total tables: {stats[\"total_tables\"]}')
print(f'  - Total keywords: {stats[\"total_keywords\"]}')
"
echo ""
echo "Products indexed:"
python -c "
import json
with open('data/processed/product_index.json') as f:
    index = json.load(f)
    for product, info in index.get('products', {}).items():
        print(f'  - {product}: {len(info.get(\"documents\", []))} documents')
"
echo ""
echo "Now you can search the Knowledge Base:"
echo "  python test_query.py --question 'What are the UPI Lite events?' --project 'UPI Lite'"

