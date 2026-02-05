#!/bin/bash

echo "Testing fixed SQL generation and execution..."
echo ""

# Test 1: Generate SQL via API
echo "1. Generating SQL via API..."
SQL_RESPONSE=$(curl -s -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{"query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"}')

SQL=$(echo "$SQL_RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('sql', ''))" 2>/dev/null)

if [ -z "$SQL" ]; then
  echo "ERROR: Could not extract SQL from API response"
  echo "Response: $SQL_RESPONSE"
  exit 1
fi

echo "Generated SQL:"
echo "$SQL"
echo ""

# Test 2: Execute SQL against Trino
echo "2. Executing SQL against Trino..."
RESPONSE=$(curl -s -X POST http://localhost:8081/v1/statement \
  -H "Content-Type: text/plain" \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: tpch" \
  -H "X-Trino-Schema: tiny" \
  -d "$SQL LIMIT 5")

NEXT_URI=$(echo "$RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('nextUri', ''))" 2>/dev/null)

if [ -z "$NEXT_URI" ]; then
  echo "ERROR: Could not get nextUri from Trino"
  echo "Response: $RESPONSE"
  exit 1
fi

echo "Query submitted. Polling for results..."
echo ""

# Poll until query completes
MAX_ITERATIONS=20
ITERATION=0

while [ $ITERATION -lt $MAX_ITERATIONS ]; do
  RESULT=$(curl -s "$NEXT_URI")
  STATE=$(echo "$RESULT" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('stats', {}).get('state', 'UNKNOWN'))" 2>/dev/null)
  
  echo "State: $STATE"
  
  if [ "$STATE" = "FINISHED" ]; then
    echo ""
    echo "✅ Query completed successfully!"
    echo ""
    echo "Results:"
    echo "$RESULT" | python3 -m json.tool | grep -A 30 '"data"' | head -40
    exit 0
  elif [ "$STATE" = "FAILED" ]; then
    echo ""
    echo "❌ Query failed!"
    ERROR_MSG=$(echo "$RESULT" | python3 -c "import sys, json; d=json.load(sys.stdin); err=d.get('error', {}); print(err.get('message', 'Unknown error'))" 2>/dev/null)
    echo "Error: $ERROR_MSG"
    echo ""
    echo "Full error details:"
    echo "$RESULT" | python3 -m json.tool | grep -A 20 '"error"'
    exit 1
  fi
  
  # Get next URI for next iteration
  NEXT_URI=$(echo "$RESULT" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('nextUri', ''))" 2>/dev/null)
  
  if [ -z "$NEXT_URI" ]; then
    echo "No nextUri found. Query may have completed."
    echo "$RESULT" | python3 -m json.tool | head -50
    break
  fi
  
  sleep 1
  ITERATION=$((ITERATION + 1))
done

if [ $ITERATION -eq $MAX_ITERATIONS ]; then
  echo "Timeout: Query did not complete within $MAX_ITERATIONS seconds"
  exit 1
fi
