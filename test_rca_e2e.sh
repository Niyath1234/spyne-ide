#!/bin/bash

# End-to-End Test Script for Advanced RCA Engine
# Tests the graph traversal-based RCA system with real LLM integration

set -e

echo "🧪 RCA Engine End-to-End Test"
echo "=============================="
echo ""

# Check for .env file
if [ ! -f .env ]; then
    echo "⚠️  Warning: .env file not found"
    echo "   Creating .env template..."
    echo ""
    echo "# OpenAI Configuration" > .env
    echo "OPENAI_API_KEY=your_api_key_here" >> .env
    echo "OPENAI_MODEL=gpt-4" >> .env
    echo "OPENAI_BASE_URL=https://api.openai.com/v1" >> .env
    echo ""
    echo "❌ Please add your OPENAI_API_KEY to .env file and run again"
    exit 1
fi

# Load environment variables
source .env

# Check if API key is set
if [ -z "$OPENAI_API_KEY" ] || [ "$OPENAI_API_KEY" == "your_api_key_here" ]; then
    echo "❌ Error: OPENAI_API_KEY not set in .env file"
    echo "   Please set your OpenAI API key in .env"
    exit 1
fi

echo "✅ Environment variables loaded"
echo "   Model: ${OPENAI_MODEL:-gpt-4}"
echo "   Base URL: ${OPENAI_BASE_URL:-https://api.openai.com/v1}"
echo ""

# Build the server if needed
echo "🔨 Building server..."
cargo build --bin server --release

if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi

echo "✅ Build successful"
echo ""

# Start server in background
echo "🚀 Starting server in background..."
./target/release/server > /tmp/rca_server.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo "⏳ Waiting for server to start..."
sleep 3

# Check if server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "❌ Server failed to start. Check /tmp/rca_server.log"
    exit 1
fi

echo "✅ Server started (PID: $SERVER_PID)"
echo ""

# Test health endpoint
echo "🏥 Testing health endpoint..."
HEALTH_RESPONSE=$(curl -s http://localhost:8080/api/health || echo "FAILED")
if [[ "$HEALTH_RESPONSE" == *"ok"* ]]; then
    echo "✅ Health check passed"
else
    echo "❌ Health check failed: $HEALTH_RESPONSE"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

# Test query - using a real RCA scenario
TEST_QUERY="Why is the outstanding balance different between system A and system B for loan L001?"

echo "🔍 Testing Graph Traversal RCA"
echo "   Query: $TEST_QUERY"
echo ""

# Make the graph traversal request
echo "📡 Sending request to /api/graph/traverse..."
TRAVERSE_RESPONSE=$(curl -s -X POST http://localhost:8080/api/graph/traverse \
    -H "Content-Type: application/json" \
    -d "{
        \"query\": \"$TEST_QUERY\",
        \"metadata_dir\": \"metadata\",
        \"data_dir\": \"data\"
    }" || echo "FAILED")

if [[ "$TRAVERSE_RESPONSE" == *"FAILED"* ]] || [[ "$TRAVERSE_RESPONSE" == *"error"* ]]; then
    echo "❌ Graph traversal request failed"
    echo "   Response: $TRAVERSE_RESPONSE"
    echo ""
    echo "📋 Server logs:"
    tail -20 /tmp/rca_server.log
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

echo "✅ Request successful"
echo ""

# Parse and display results
echo "📊 RCA Results:"
echo "==============="
echo ""

# Try to extract key information from JSON response
if command -v jq &> /dev/null; then
    echo "$TRAVERSE_RESPONSE" | jq -r '.result.root_cause_found // "Unknown"' | head -1 | while read -r root_cause; do
        echo "Root Cause Found: $root_cause"
    done
    
    echo ""
    echo "Current Hypothesis:"
    echo "$TRAVERSE_RESPONSE" | jq -r '.result.current_hypothesis // "N/A"' | head -1
    
    echo ""
    echo "Findings:"
    echo "$TRAVERSE_RESPONSE" | jq -r '.result.findings[]? // "N/A"' | head -5
    
    echo ""
    echo "Visited Path (nodes explored):"
    echo "$TRAVERSE_RESPONSE" | jq -r '.result.visited_path[]? // "N/A"' | head -10
    
    echo ""
    echo "Hints Used:"
    echo "$TRAVERSE_RESPONSE" | jq -r '.result.hints[]? // "N/A"' | head -5
else
    # Fallback: show raw JSON (formatted if possible)
    echo "$TRAVERSE_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$TRAVERSE_RESPONSE"
fi

echo ""
echo "================================"
echo ""

# Also test the regular RCA endpoint
echo "🔍 Testing Regular RCA Endpoint..."
echo "   Query: $TEST_QUERY"
echo ""

RCA_RESPONSE=$(curl -s -X POST http://localhost:8080/api/reasoning/query \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"$TEST_QUERY\"}" || echo "FAILED")

if [[ "$RCA_RESPONSE" == *"FAILED"* ]]; then
    echo "⚠️  Regular RCA request failed (this is OK if graph traversal works)"
else
    echo "✅ Regular RCA request successful"
    if command -v jq &> /dev/null; then
        echo "$RCA_RESPONSE" | jq -r '.result // .error' | head -20
    else
        echo "$RCA_RESPONSE" | head -50
    fi
fi

echo ""
echo "================================"
echo ""

# Stop server
echo "🛑 Stopping server..."
kill $SERVER_PID 2>/dev/null || true
sleep 1

if kill -0 $SERVER_PID 2>/dev/null; then
    echo "⚠️  Server still running, force killing..."
    kill -9 $SERVER_PID 2>/dev/null || true
fi

echo "✅ Server stopped"
echo ""
echo "📋 Full server logs available at: /tmp/rca_server.log"
echo ""
echo "✅ End-to-End Test Complete!"

