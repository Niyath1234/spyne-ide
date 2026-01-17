#!/bin/bash

# Simple End-to-End Test for Advanced RCA Engine
# Uses curl for testing (no Python dependencies)

set -e

echo "🧪 RCA Engine End-to-End Test"
echo "=============================="
echo ""

# Check for .env file
if [ ! -f .env ]; then
    echo "❌ .env file not found"
    echo "   Please create .env with OPENAI_API_KEY"
    exit 1
fi

# Load environment
export $(grep -v '^#' .env | xargs)

# Check if API key is set
if [ -z "$OPENAI_API_KEY" ]; then
    echo "❌ OPENAI_API_KEY not set in .env file"
    exit 1
fi

echo "✅ Environment loaded"
echo "   Model: ${OPENAI_MODEL:-gpt-4}"
echo ""

# Build server
echo "🔨 Building server..."
cargo build --bin server --release || {
    echo "❌ Build failed"
    exit 1
}

echo "✅ Build successful"
echo ""

# Start server in background
echo "🚀 Starting server..."
./target/release/server > /tmp/rca_server.log 2>&1 &
SERVER_PID=$!

# Wait for server
echo "⏳ Waiting for server to start..."
for i in {1..10}; do
    sleep 1
    if curl -s http://localhost:8080/api/health > /dev/null 2>&1; then
        echo "✅ Server started (PID: $SERVER_PID)"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "❌ Server failed to start"
        cat /tmp/rca_server.log
        kill $SERVER_PID 2>/dev/null || true
        exit 1
    fi
done

echo ""

# Test health
echo "🏥 Testing health endpoint..."
HEALTH=$(curl -s http://localhost:8080/api/health)
if echo "$HEALTH" | grep -q "ok"; then
    echo "✅ Health check passed"
else
    echo "❌ Health check failed: $HEALTH"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# Test query
TEST_QUERY="Why is the outstanding balance different between khatabook and tb for loan L001?"

echo "🔍 Testing Advanced Graph Traversal RCA"
echo "   Query: $TEST_QUERY"
echo ""

# Make request
echo "📡 Sending request to /api/graph/traverse..."
echo "   (This may take a few minutes with LLM calls...)"
echo ""

RESPONSE=$(curl -s -X POST http://localhost:8080/api/graph/traverse \
    -H "Content-Type: application/json" \
    -d "{
        \"query\": \"$TEST_QUERY\",
        \"metadata_dir\": \"metadata\",
        \"data_dir\": \"data\"
    }" \
    --max-time 600)

# Check if request succeeded
if echo "$RESPONSE" | grep -q "error"; then
    echo "❌ Request failed"
    echo "$RESPONSE" | head -20
    echo ""
    echo "📋 Server logs:"
    tail -30 /tmp/rca_server.log
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

echo "✅ Request successful!"
echo ""

# Display results
echo "📊 RCA Results:"
echo "==============="
echo ""

# Try to extract key info (basic parsing without jq)
if echo "$RESPONSE" | grep -q "root_cause_found"; then
    ROOT_CAUSE=$(echo "$RESPONSE" | grep -o '"root_cause_found":[^,}]*' | cut -d'"' -f4 || echo "Unknown")
    echo "🎯 Root Cause Found: $ROOT_CAUSE"
fi

if echo "$RESPONSE" | grep -q "current_hypothesis"; then
    HYPOTHESIS=$(echo "$RESPONSE" | grep -o '"current_hypothesis":"[^"]*' | cut -d'"' -f4 || echo "N/A")
    echo ""
    echo "💡 Current Hypothesis:"
    echo "   $HYPOTHESIS"
fi

if echo "$RESPONSE" | grep -q "visited_path"; then
    echo ""
    echo "🛤️  Visited Path (first 10 nodes):"
    echo "$RESPONSE" | grep -o '"visited_path":\[[^]]*\]' | head -1 | grep -o '"[^"]*"' | head -10 | sed 's/"//g' | nl
fi

if echo "$RESPONSE" | grep -q "findings"; then
    echo ""
    echo "🔍 Findings:"
    echo "$RESPONSE" | grep -o '"findings":\[[^]]*\]' | head -1 | grep -o '"[^"]*"' | head -5 | sed 's/"//g' | nl
fi

echo ""
echo "================================"
echo ""

# Show full JSON if jq is available
if command -v jq &> /dev/null; then
    echo "📄 Full Response (formatted):"
    echo "$RESPONSE" | jq '.' | head -100
    echo ""
    echo "... (truncated, see full response above)"
else
    echo "📄 Response (first 500 chars):"
    echo "$RESPONSE" | head -c 500
    echo "..."
    echo ""
    echo "💡 Install 'jq' for better JSON formatting: brew install jq"
fi

echo ""
echo "================================"
echo ""

# Stop server
echo "🛑 Stopping server..."
kill $SERVER_PID 2>/dev/null || true
sleep 1

if kill -0 $SERVER_PID 2>/dev/null; then
    kill -9 $SERVER_PID 2>/dev/null || true
fi

echo "✅ Server stopped"
echo ""
echo "📋 Full server logs: /tmp/rca_server.log"
echo ""
echo "✅ Test Complete!"

