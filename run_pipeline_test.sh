#!/bin/bash
# Quick script to run pipeline tests

echo "🚀 Starting Pipeline Flow Test"
echo "================================"

# Check if pipeline API is running
if ! curl -s http://localhost:8082/api/pipeline/health > /dev/null; then
    echo "⚠️  Pipeline API server is not running."
    echo "   Starting pipeline API server..."
    python pipeline_api_server.py &
    PIPELINE_PID=$!
    echo "   Pipeline API started with PID: $PIPELINE_PID"
    sleep 3
    echo "   Waiting for server to be ready..."
    sleep 2
fi

# Run the test
echo ""
echo "Running complete flow test..."
python test_pipeline_flow.py

echo ""
echo "✅ Test completed!"
echo ""
echo "To run interactive menu:"
echo "  python test_pipeline_flow.py --interactive"

