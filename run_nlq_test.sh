#!/bin/bash
# Script to run natural language query tests

echo "🚀 Natural Language Query Test"
echo "================================"

# Check if pipeline API is running
if ! curl -s http://localhost:8082/api/pipeline/health > /dev/null; then
    echo "⚠️  Pipeline API server is not running."
    echo "   Starting pipeline API server..."
    python pipeline_api_server.py &
    PIPELINE_PID=$!
    echo "   Pipeline API started with PID: $PIPELINE_PID"
    sleep 3
fi

# Check if RCA API is running
if ! curl -s http://localhost:8080/api/health > /dev/null; then
    echo "⚠️  RCA API server is not running."
    echo "   Please start it with: cargo run --bin server"
    echo "   Or: cd src && cargo run --bin server"
    echo ""
    echo "   Waiting 5 seconds for you to start it..."
    sleep 5
fi

# Run the test
echo ""
echo "Running natural language query test..."
python test_natural_language_queries.py

echo ""
echo "✅ Test completed!"
echo ""
echo "Results saved to: natural_language_test_results.json"

