#!/bin/bash

# Complete Workflow: Confluence → Product Index → Vector DB
# 
# This script:
# 1. Fetches ARD/PRD/TRD from Confluence
# 2. Extracts products from titles/metadata
# 3. Creates product indexes
# 4. Processes documents (ingest → chunk → index)

set -e

echo "=========================================="
echo "Confluence to Vector DB Workflow"
echo "=========================================="
echo ""

# Check environment variables
if [ -z "$CONFLUENCE_URL" ] || [ -z "$CONFLUENCE_USERNAME" ] || [ -z "$CONFLUENCE_API_TOKEN" ]; then
    echo "Error: Confluence credentials not set"
    echo "Set: CONFLUENCE_URL, CONFLUENCE_USERNAME, CONFLUENCE_API_TOKEN"
    exit 1
fi

# Step 1: Fetch from Confluence
echo "Step 1: Fetching documents from Confluence..."
echo "----------------------------------------"
python src/confluence_ingest.py \
    --url "$CONFLUENCE_URL" \
    --username "$CONFLUENCE_USERNAME" \
    --api-token "$CONFLUENCE_API_TOKEN" \
    ${CONFLUENCE_SPACE_KEY:+--space-key "$CONFLUENCE_SPACE_KEY"}

if [ $? -ne 0 ]; then
    echo "Error: Failed to fetch from Confluence"
    exit 1
fi

echo ""
echo "✓ Documents fetched and product indexes created"
echo ""

# Step 2: Process documents
echo "Step 2: Processing documents..."
echo "----------------------------------------"
python src/pipeline.py --step ingest

if [ $? -ne 0 ]; then
    echo "Error: Document ingestion failed"
    exit 1
fi

echo ""
echo "✓ Documents processed"
echo ""

# Step 3: Chunk documents
echo "Step 3: Chunking documents..."
echo "----------------------------------------"
python src/pipeline.py --step chunk

if [ $? -ne 0 ]; then
    echo "Error: Chunking failed"
    exit 1
fi

echo ""
echo "✓ Documents chunked"
echo ""

# Step 4: Index documents
echo "Step 4: Indexing documents..."
echo "----------------------------------------"
python src/pipeline.py --step index

if [ $? -ne 0 ]; then
    echo "Error: Indexing failed"
    exit 1
fi

echo ""
echo "=========================================="
echo "✓ Workflow Complete!"
echo "=========================================="
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
echo "Next: Add table relations to products as they are identified"

