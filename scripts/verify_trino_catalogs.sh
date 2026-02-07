#!/bin/bash
# Verify Trino catalogs are available

set -e

TRINO_HOST=${TRINO_HOST:-localhost}
TRINO_PORT=${TRINO_PORT:-8081}
TRINO_USER=${TRINO_USER:-admin}

echo "=========================================="
echo "Verifying Trino Catalogs"
echo "=========================================="
echo "Trino Host: ${TRINO_HOST}:${TRINO_PORT}"
echo ""

# Test 1: Check if Trino is accessible
echo "1. Testing Trino connectivity..."
if curl -s -f "http://${TRINO_HOST}:${TRINO_PORT}/v1/info" > /dev/null; then
    echo "   ✓ Trino is accessible"
else
    echo "   ✗ Cannot connect to Trino at http://${TRINO_HOST}:${TRINO_PORT}"
    echo ""
    echo "   To start Trino:"
    echo "   cd docker && docker-compose up trino"
    exit 1
fi

# Test 2: List all catalogs
echo ""
echo "2. Listing available catalogs..."
CATALOG_QUERY="SHOW CATALOGS"
CATALOG_RESPONSE=$(curl -s -X POST \
  -H "X-Trino-User: ${TRINO_USER}" \
  -H "Content-Type: text/plain" \
  "http://${TRINO_HOST}:${TRINO_PORT}/v1/statement" \
  -d "$CATALOG_QUERY")

CATALOG_URI=$(echo "$CATALOG_RESPONSE" | jq -r '.nextUri' 2>/dev/null)

if [ -z "$CATALOG_URI" ] || [ "$CATALOG_URI" = "null" ]; then
    echo "   ✗ Failed to get catalogs"
    echo "   Response: $CATALOG_RESPONSE"
    exit 1
fi

# Wait a moment for query to complete
sleep 1

CATALOG_DATA=$(curl -s "$CATALOG_URI" | jq -r '.data[] | .[0]' 2>/dev/null)

if [ -z "$CATALOG_DATA" ]; then
    echo "   ✗ No catalogs found"
    exit 1
fi

echo "   ✓ Available catalogs:"
echo "$CATALOG_DATA" | while read catalog; do
    echo "     - $catalog"
done

# Check for TPC-DS and TPCH
HAS_TPCDS=$(echo "$CATALOG_DATA" | grep -q "^tpcds$" && echo "yes" || echo "no")
HAS_TPCH=$(echo "$CATALOG_DATA" | grep -q "^tpch$" && echo "yes" || echo "no")

echo ""
if [ "$HAS_TPCDS" = "yes" ]; then
    echo "   ✓ TPC-DS catalog (tpcds) is available"
    
    # Test TPC-DS schemas
    echo ""
    echo "3. Checking TPC-DS schemas..."
    SCHEMA_QUERY="SHOW SCHEMAS FROM tpcds"
    SCHEMA_RESPONSE=$(curl -s -X POST \
      -H "X-Trino-User: ${TRINO_USER}" \
      -H "X-Trino-Catalog: tpcds" \
      -H "Content-Type: text/plain" \
      "http://${TRINO_HOST}:${TRINO_PORT}/v1/statement" \
      -d "$SCHEMA_QUERY")
    
    SCHEMA_URI=$(echo "$SCHEMA_RESPONSE" | jq -r '.nextUri' 2>/dev/null)
    if [ ! -z "$SCHEMA_URI" ] && [ "$SCHEMA_URI" != "null" ]; then
        sleep 1
        SCHEMA_DATA=$(curl -s "$SCHEMA_URI" | jq -r '.data[] | .[0]' 2>/dev/null | grep -v "^information_schema$")
        if [ ! -z "$SCHEMA_DATA" ]; then
            echo "   ✓ TPC-DS schemas:"
            echo "$SCHEMA_DATA" | head -5 | while read schema; do
                echo "     - $schema"
            done
        fi
    fi
else
    echo "   ✗ TPC-DS catalog (tpcds) is NOT available"
    echo ""
    echo "   To fix:"
    echo "   1. Verify tpcds.properties exists in config/trino/catalog/"
    echo "   2. Restart Trino: docker-compose restart trino"
    echo "   3. Check Trino logs: docker-compose logs trino"
fi

if [ "$HAS_TPCH" = "yes" ]; then
    echo ""
    echo "   ✓ TPC-H catalog (tpch) is available"
else
    echo ""
    echo "   ✗ TPC-H catalog (tpch) is NOT available"
fi

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo "TPC-DS catalog: $HAS_TPCDS"
echo "TPC-H catalog: $HAS_TPCH"
echo ""
echo "Example queries:"
if [ "$HAS_TPCDS" = "yes" ]; then
    echo "  SELECT * FROM tpcds.tiny.customer LIMIT 10;"
    echo "  SELECT * FROM tpcds.tiny.store_sales LIMIT 10;"
fi
if [ "$HAS_TPCH" = "yes" ]; then
    echo "  SELECT * FROM tpch.tiny.customer LIMIT 10;"
fi
