#!/bin/bash

# SQL query to execute
SQL="SELECT c.c_custkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_price FROM customer c LEFT JOIN orders o ON o.o_custkey = c.c_custkey LEFT JOIN lineitem l ON l.l_orderkey = o.o_orderkey GROUP BY c.c_custkey LIMIT 10"

echo "Executing SQL query against Trino..."
echo "SQL: $SQL"
echo ""

# Submit query
RESPONSE=$(curl -s -X POST http://localhost:8081/v1/statement \
  -H "Content-Type: text/plain" \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: tpch" \
  -H "X-Trino-Schema: tiny" \
  -d "$SQL")

# Extract nextUri
NEXT_URI=$(echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('nextUri', ''))" 2>/dev/null)

if [ -z "$NEXT_URI" ]; then
  echo "Error: Could not get nextUri from response"
  echo "Response: $RESPONSE"
  exit 1
fi

echo "Query submitted. Polling for results..."
echo ""

# Poll until query completes
MAX_ITERATIONS=30
ITERATION=0

while [ $ITERATION -lt $MAX_ITERATIONS ]; do
  RESULT=$(curl -s "$NEXT_URI")
  STATE=$(echo "$RESULT" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('stats', {}).get('state', 'UNKNOWN'))" 2>/dev/null)
  
  echo "State: $STATE"
  
  if [ "$STATE" = "FINISHED" ]; then
    echo ""
    echo "✅ Query completed successfully!"
    echo ""
    echo "Results:"
    echo "$RESULT" | python3 -m json.tool | grep -A 100 '"data"' | head -50
    break
  elif [ "$STATE" = "FAILED" ]; then
    echo ""
    echo "❌ Query failed!"
    echo "$RESULT" | python3 -m json.tool
    exit 1
  fi
  
  # Get next URI for next iteration
  NEXT_URI=$(echo "$RESULT" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('nextUri', ''))" 2>/dev/null)
  
  if [ -z "$NEXT_URI" ]; then
    echo "No nextUri found. Query may have completed."
    echo "$RESULT" | python3 -m json.tool
    break
  fi
  
  sleep 1
  ITERATION=$((ITERATION + 1))
done

if [ $ITERATION -eq $MAX_ITERATIONS ]; then
  echo "Timeout: Query did not complete within $MAX_ITERATIONS seconds"
  exit 1
fi
