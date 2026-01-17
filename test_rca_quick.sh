#!/bin/bash

# Quick test script for RCA Engine
set -e

echo "🧪 Quick RCA Test"
echo "=================="
echo ""

# Load environment
export $(grep -v '^#' .env | xargs)

# Check if server is running
if ! curl -s http://localhost:8080/api/health > /dev/null 2>&1; then
    echo "❌ Server not running. Please start it first with: ./target/release/server"
    exit 1
fi

echo "✅ Server is running"
echo ""

# Test query
QUERY="Why is the outstanding balance different between khatabook and tb for loan L001?"

echo "🔍 Testing Graph Traversal RCA"
echo "   Query: $QUERY"
echo ""

# Make request with timeout
RESPONSE=$(curl -s -X POST http://localhost:8080/api/graph/traverse \
    -H "Content-Type: application/json" \
    -d "{
        \"query\": \"$QUERY\",
        \"metadata_dir\": \"metadata\",
        \"data_dir\": \"data\"
    }" \
    --max-time 300)

# Check response
if echo "$RESPONSE" | grep -q "error"; then
    echo "❌ Request failed"
    echo ""
    echo "Response:"
    echo "$RESPONSE" | head -20
    echo ""
    echo "📋 Server logs:"
    tail -20 /tmp/rca_server.log 2>/dev/null || echo "No server logs found"
    exit 1
fi

echo "✅ Request successful!"
echo ""
echo "📊 Results:"
echo "==========="
echo ""

# Try to parse with jq if available
if command -v jq &> /dev/null; then
    echo "$RESPONSE" | jq -r '
        if .result.root_cause_found then "🎯 Root Cause Found: " + (.result.root_cause_found | tostring) else "" end,
        if .result.current_hypothesis then "💡 Hypothesis: " + .result.current_hypothesis else "" end,
        if .result.findings then "🔍 Findings (" + (.result.findings | length | tostring) + "):" else "" end,
        (.result.findings[]? // empty | "   • " + .),
        if .result.visited_path then "🛤️  Visited " + (.result.visited_path | length | tostring) + " nodes" else "" end
    ' 2>/dev/null || echo "$RESPONSE" | head -50
else
    echo "$RESPONSE" | head -100
fi

echo ""
echo "✅ Test Complete!"

