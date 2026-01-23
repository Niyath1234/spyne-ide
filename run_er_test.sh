#!/bin/bash
# Run ER Diagram Flow Test

cd "$(dirname "$0")"

echo "🚀 Starting ER Diagram Flow Test"
echo "================================"

# Check if Pipeline API is running
if ! curl -s http://localhost:8082/api/pipeline/health > /dev/null 2>&1; then
    echo "⚠️  Pipeline API not running. Starting it..."
    python pipeline_api_server.py > /tmp/pipeline_api.log 2>&1 &
    PIPELINE_PID=$!
    echo "   Started Pipeline API (PID: $PIPELINE_PID)"
    sleep 5
fi

# Check if Knowledge Base API is running
if ! curl -s http://localhost:8083/api/knowledge-base/health > /dev/null 2>&1; then
    echo "⚠️  Knowledge Base API not running. Starting it..."
    python knowledge_base_api.py > /tmp/kb_api.log 2>&1 &
    KB_PID=$!
    echo "   Started Knowledge Base API (PID: $KB_PID)"
    sleep 3
fi

# Wait a bit more for services to be ready
sleep 2

# Run the test
echo ""
echo "📊 Running ER Diagram Flow Test..."
echo "================================"
python test_er_diagram_flow.py

# Cleanup - kill background processes if we started them
if [ ! -z "$PIPELINE_PID" ]; then
    echo ""
    echo "🧹 Cleaning up Pipeline API (PID: $PIPELINE_PID)..."
    kill $PIPELINE_PID 2>/dev/null
fi

if [ ! -z "$KB_PID" ]; then
    echo "🧹 Cleaning up Knowledge Base API (PID: $KB_PID)..."
    kill $KB_PID 2>/dev/null
fi

echo ""
echo "✅ Test completed!"

