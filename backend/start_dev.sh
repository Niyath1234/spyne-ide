#!/bin/bash
# Development startup script for RCA Engine

set -e

# Default configuration
export RCA_HOST=${RCA_HOST:-0.0.0.0}
export RCA_PORT=${RCA_PORT:-8080}
export RCA_DEBUG=${RCA_DEBUG:-true}
export RCA_LOG_LEVEL=${RCA_LOG_LEVEL:-DEBUG}

# Directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🔧 Starting RCA Engine Development Server"
echo "   Host: $RCA_HOST:$RCA_PORT"
echo "   Debug: $RCA_DEBUG"
echo "   Log Level: $RCA_LOG_LEVEL"

# Check if OPENAI_API_KEY is set
if [ -z "$OPENAI_API_KEY" ]; then
    echo "⚠️  OPENAI_API_KEY not set - LLM features will be disabled"
else
    echo "✅ OPENAI_API_KEY is set - LLM features enabled"
fi

# Start with Flask development server
echo ""
echo "Starting Flask development server..."
exec python3 app_production.py

