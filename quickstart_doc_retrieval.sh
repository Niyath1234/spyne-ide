#!/bin/bash

# Quick Start Script for Document Retrieval System
# This script guides you through the setup and usage

set -e

echo "=========================================="
echo "High-Fidelity Document Retrieval System"
echo "Quick Start Guide"
echo "=========================================="
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found. Creating from .env.example..."
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "✓ Created .env file. Please fill in your API keys!"
        echo ""
        echo "Required API keys:"
        echo "  - LLAMA_CLOUD_API_KEY (from https://cloud.llamaindex.ai/)"
        echo "  - PINECONE_API_KEY (from https://www.pinecone.io/)"
        echo "  - OPENAI_API_KEY (from https://platform.openai.com/)"
        echo "  - COHERE_API_KEY (from https://cohere.com/)"
        echo ""
        read -p "Press Enter after you've filled in the API keys..."
    else
        echo "✗ .env.example not found. Please create .env manually."
        exit 1
    fi
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -q -r requirements_doc_retrieval.txt

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Place your documents (PDF/Docx) in data/raw/"
echo "   Example: cp my_ard.pdf data/raw/"
echo ""
echo "2. Ingest documents:"
echo "   python src/ingest.py"
echo ""
echo "3. Chunk documents:"
echo "   python src/chunking.py --save"
echo ""
echo "4. Index documents:"
echo "   python src/vector_db.py --chunks-file data/processed/chunks.json"
echo ""
echo "5. Query documents:"
echo "   python test_query.py"
echo ""
echo "Or run all steps at once:"
echo "   python src/ingest.py && python src/chunking.py --save && python src/vector_db.py --chunks-file data/processed/chunks.json"
echo ""

