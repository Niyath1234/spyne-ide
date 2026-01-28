#!/bin/bash
# Production startup script for RCA Engine

set -e

# Default configuration
export RCA_HOST=${RCA_HOST:-0.0.0.0}
export RCA_PORT=${RCA_PORT:-8080}
export RCA_WORKERS=${RCA_WORKERS:-4}
export RCA_THREADS=${RCA_THREADS:-4}
export RCA_TIMEOUT=${RCA_TIMEOUT:-120}
export RCA_LOG_LEVEL=${RCA_LOG_LEVEL:-info}

# Directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Starting RCA Engine Production Server"
echo "   Host: $RCA_HOST:$RCA_PORT"
echo "   Workers: $RCA_WORKERS"
echo "   Threads: $RCA_THREADS"
echo "   Timeout: ${RCA_TIMEOUT}s"
echo "   Log Level: $RCA_LOG_LEVEL"

# Check if gunicorn is installed
if ! command -v gunicorn &> /dev/null; then
    echo "❌ gunicorn not found. Installing..."
    pip install gunicorn
fi

# Check if OPENAI_API_KEY is set
if [ -z "$OPENAI_API_KEY" ]; then
    echo "⚠️  OPENAI_API_KEY not set - LLM features will be disabled"
else
    echo "✅ OPENAI_API_KEY is set - LLM features enabled"
fi

# Start with gunicorn
echo ""
echo "Starting gunicorn..."
exec gunicorn \
    --config gunicorn.conf.py \
    --bind ${RCA_HOST}:${RCA_PORT} \
    --workers ${RCA_WORKERS} \
    --threads ${RCA_THREADS} \
    --timeout ${RCA_TIMEOUT} \
    --log-level ${RCA_LOG_LEVEL} \
    "app_production:app"

