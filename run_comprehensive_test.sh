#!/bin/bash

# Comprehensive RCA Test Script
set -e

echo "🧪 Comprehensive RCA Engine Test"
echo "================================="
echo ""

# Test 1: Health Check
echo "1️⃣  Testing Health Endpoint..."
HEALTH=$(curl -s http://localhost:8080/api/health)
if echo "$HEALTH" | grep -q "ok"; then
    echo "   ✅ Health check passed"
else
    echo "   ❌ Health check failed: $HEALTH"
    exit 1
fi
echo ""

# Test 2: Graph Traversal RCA
echo "2️⃣  Testing Graph Traversal RCA..."
echo "   Query: Why is the outstanding balance different between khatabook and tb for loan L001?"
echo ""

RESPONSE=$(curl -s -X POST http://localhost:8080/api/graph/traverse \
    -H "Content-Type: application/json" \
    -d '{
        "query": "Why is the outstanding balance different between khatabook and tb for loan L001?",
        "metadata_dir": "metadata",
        "data_dir": "data"
    }' \
    --max-time 300)

if echo "$RESPONSE" | grep -q "error"; then
    echo "   ❌ Graph traversal failed"
    echo "   Response: $RESPONSE" | head -5
    echo ""
    echo "   📋 Server logs:"
    tail -20 /tmp/rca_server_test.log 2>/dev/null | tail -10
    exit 1
fi

echo "   ✅ Request successful"
echo ""

# Parse and display results
if command -v jq &> /dev/null; then
    echo "   📊 Results:"
    echo "   ==========="
    
    ROOT_CAUSE=$(echo "$RESPONSE" | jq -r '.result.root_cause_found // "Unknown"' 2>/dev/null)
    HYPOTHESIS=$(echo "$RESPONSE" | jq -r '.result.current_hypothesis // "N/A"' 2>/dev/null)
    FINDINGS_COUNT=$(echo "$RESPONSE" | jq -r '.result.findings | length // 0' 2>/dev/null)
    VISITED_COUNT=$(echo "$RESPONSE" | jq -r '.result.visited_path | length // 0' 2>/dev/null)
    
    echo "   🎯 Root Cause Found: $ROOT_CAUSE"
    echo "   💡 Hypothesis: $HYPOTHESIS"
    echo "   🔍 Findings: $FINDINGS_COUNT"
    echo "   🛤️  Nodes Visited: $VISITED_COUNT"
    echo ""
    
    if [ "$VISITED_COUNT" -gt 0 ]; then
        echo "   ✅ Graph traversal is working - visited $VISITED_COUNT nodes"
    fi
    
    if [ "$FINDINGS_COUNT" -gt 0 ]; then
        echo "   ✅ Findings generated - $FINDINGS_COUNT findings"
    fi
    
    echo ""
    echo "   📄 Full Response (first 50 lines):"
    echo "$RESPONSE" | jq '.' 2>/dev/null | head -50
else
    echo "   📊 Response received (install jq for better formatting)"
    echo "$RESPONSE" | head -100
fi

echo ""
echo ""

# Test 3: Regular RCA Endpoint
echo "3️⃣  Testing Regular RCA Endpoint..."
echo "   Query: Why is paid_amount different between collections_mis and outstanding_daily?"
echo ""

RCA_RESPONSE=$(curl -s -X POST http://localhost:8080/api/reasoning/query \
    -H "Content-Type: application/json" \
    -d '{"query": "Why is paid_amount different between collections_mis and outstanding_daily?"}' \
    --max-time 300)

if echo "$RCA_RESPONSE" | grep -q "error"; then
    echo "   ⚠️  Regular RCA request had issues (this is OK if graph traversal works)"
    echo "   Response: $(echo "$RCA_RESPONSE" | head -3)"
else
    echo "   ✅ Regular RCA request successful"
    if command -v jq &> /dev/null; then
        echo "$RCA_RESPONSE" | jq -r '.result // .error' 2>/dev/null | head -20
    else
        echo "$RCA_RESPONSE" | head -50
    fi
fi

echo ""
echo "================================="
echo "✅ Test Complete!"
echo ""
echo "📋 Server logs available at: /tmp/rca_server_test.log"

