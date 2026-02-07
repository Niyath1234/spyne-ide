#!/bin/bash
# Test TPC-DS setup with Trino

set -e

TRINO_HOST=${TRINO_HOST:-localhost}
TRINO_PORT=${TRINO_PORT:-8081}
TRINO_USER=${TRINO_USER:-admin}
TRINO_CATALOG="tpcds"
TRINO_SCHEMA="tiny"

echo "=========================================="
echo "Testing TPC-DS Setup"
echo "=========================================="
echo "Trino Host: ${TRINO_HOST}:${TRINO_PORT}"
echo "Catalog: ${TRINO_CATALOG}"
echo "Schema: ${TRINO_SCHEMA}"
echo ""

# Test 1: Check if Trino is accessible
echo "1. Testing Trino connectivity..."
if curl -s -f "http://${TRINO_HOST}:${TRINO_PORT}/v1/info" > /dev/null; then
    echo "   ✓ Trino is accessible"
else
    echo "   ✗ Cannot connect to Trino at http://${TRINO_HOST}:${TRINO_PORT}"
    echo "   Make sure Trino is running: docker-compose up trino"
    exit 1
fi

# Test 2: Check if TPC-DS catalog is available
echo ""
echo "2. Checking if TPC-DS catalog is available..."
CATALOGS=$(curl -s -X POST \
  -H "X-Trino-User: ${TRINO_USER}" \
  -H "Content-Type: application/json" \
  "http://${TRINO_HOST}:${TRINO_PORT}/v1/statement" \
  -d "SHOW CATALOGS" | jq -r '.nextUri' 2>/dev/null)

if [ -z "$CATALOGS" ]; then
    echo "   ✗ Failed to get catalogs"
    exit 1
fi

CATALOG_RESULT=$(curl -s "$CATALOGS" | jq -r '.data[] | .[]' 2>/dev/null | grep -q "tpcds" && echo "found" || echo "not found")

if [ "$CATALOG_RESULT" = "found" ]; then
    echo "   ✓ TPC-DS catalog is available"
else
    echo "   ✗ TPC-DS catalog not found"
    echo "   Make sure tpcds.properties is configured in config/trino/catalog/"
    exit 1
fi

# Test 3: Check if tiny schema exists
echo ""
echo "3. Checking if ${TRINO_SCHEMA} schema exists..."
SCHEMA_QUERY="SHOW SCHEMAS FROM ${TRINO_CATALOG}"
SCHEMA_URI=$(curl -s -X POST \
  -H "X-Trino-User: ${TRINO_USER}" \
  -H "X-Trino-Catalog: ${TRINO_CATALOG}" \
  -H "Content-Type: application/json" \
  "http://${TRINO_HOST}:${TRINO_PORT}/v1/statement" \
  -d "$SCHEMA_QUERY" | jq -r '.nextUri' 2>/dev/null)

if [ -z "$SCHEMA_URI" ]; then
    echo "   ✗ Failed to query schemas"
    exit 1
fi

SCHEMA_RESULT=$(curl -s "$SCHEMA_URI" | jq -r '.data[] | .[]' 2>/dev/null | grep -q "${TRINO_SCHEMA}" && echo "found" || echo "not found")

if [ "$SCHEMA_RESULT" = "found" ]; then
    echo "   ✓ Schema ${TRINO_SCHEMA} exists"
else
    echo "   ✗ Schema ${TRINO_SCHEMA} not found"
    exit 1
fi

# Test 4: List tables
echo ""
echo "4. Listing tables in ${TRINO_CATALOG}.${TRINO_SCHEMA}..."
TABLE_QUERY="SHOW TABLES FROM ${TRINO_CATALOG}.${TRINO_SCHEMA}"
TABLE_URI=$(curl -s -X POST \
  -H "X-Trino-User: ${TRINO_USER}" \
  -H "X-Trino-Catalog: ${TRINO_CATALOG}" \
  -H "X-Trino-Schema: ${TRINO_SCHEMA}" \
  -H "Content-Type: application/json" \
  "http://${TRINO_HOST}:${TRINO_PORT}/v1/statement" \
  -d "$TABLE_QUERY" | jq -r '.nextUri' 2>/dev/null)

if [ -z "$TABLE_URI" ]; then
    echo "   ✗ Failed to query tables"
    exit 1
fi

TABLE_COUNT=$(curl -s "$TABLE_URI" | jq -r '.data | length' 2>/dev/null)
echo "   ✓ Found ${TABLE_COUNT} tables"

# Test 5: Query a sample table
echo ""
echo "5. Testing query on customer table..."
CUSTOMER_QUERY="SELECT COUNT(*) as customer_count FROM ${TRINO_CATALOG}.${TRINO_SCHEMA}.customer"
CUSTOMER_URI=$(curl -s -X POST \
  -H "X-Trino-User: ${TRINO_USER}" \
  -H "X-Trino-Catalog: ${TRINO_CATALOG}" \
  -H "X-Trino-Schema: ${TRINO_SCHEMA}" \
  -H "Content-Type: application/json" \
  "http://${TRINO_HOST}:${TRINO_PORT}/v1/statement" \
  -d "$CUSTOMER_QUERY" | jq -r '.nextUri' 2>/dev/null)

if [ -z "$CUSTOMER_URI" ]; then
    echo "   ✗ Failed to execute query"
    exit 1
fi

# Wait for query to complete and get results
sleep 1
CUSTOMER_COUNT=$(curl -s "$CUSTOMER_URI" | jq -r '.data[0][0]' 2>/dev/null)

if [ ! -z "$CUSTOMER_COUNT" ] && [ "$CUSTOMER_COUNT" != "null" ]; then
    echo "   ✓ Query successful - Customer count: ${CUSTOMER_COUNT}"
else
    echo "   ✗ Query failed or returned no results"
    exit 1
fi

# Test 6: Query store_sales
echo ""
echo "6. Testing query on store_sales table..."
SALES_QUERY="SELECT COUNT(*) as sales_count FROM ${TRINO_CATALOG}.${TRINO_SCHEMA}.store_sales LIMIT 1"
SALES_URI=$(curl -s -X POST \
  -H "X-Trino-User: ${TRINO_USER}" \
  -H "X-Trino-Catalog: ${TRINO_CATALOG}" \
  -H "X-Trino-Schema: ${TRINO_SCHEMA}" \
  -H "Content-Type: application/json" \
  "http://${TRINO_HOST}:${TRINO_PORT}/v1/statement" \
  -d "$SALES_QUERY" | jq -r '.nextUri' 2>/dev/null)

if [ -z "$SALES_URI" ]; then
    echo "   ✗ Failed to execute query"
    exit 1
fi

sleep 1
SALES_COUNT=$(curl -s "$SALES_URI" | jq -r '.data[0][0]' 2>/dev/null)

if [ ! -z "$SALES_COUNT" ] && [ "$SALES_COUNT" != "null" ]; then
    echo "   ✓ Query successful - Store sales count: ${SALES_COUNT}"
else
    echo "   ✗ Query failed or returned no results"
    exit 1
fi

echo ""
echo "=========================================="
echo "✓ All TPC-DS tests passed!"
echo "=========================================="
echo ""
echo "You can now use TPC-DS tables in your queries:"
echo "  SELECT * FROM tpcds.tiny.customer LIMIT 10;"
echo "  SELECT * FROM tpcds.tiny.store_sales LIMIT 10;"
echo "  SELECT * FROM tpcds.tiny.date_dim LIMIT 10;"
