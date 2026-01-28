# Spyne IDE

A production-grade SQL query generation system using LLM and comprehensive metadata context.

## Features

- **LLM-based Query Generation**: Uses OpenAI GPT models to generate SQL from natural language
- **Comprehensive Context**: Builds context from tables, metrics, dimensions, relationships, and business rules
- **Metadata Caching**: Centralized metadata provider with process-level caching
- **Token Budget Management**: Intelligent token counting and truncation
- **Retry Logic**: Automatic retry with exponential backoff for transient failures
- **Query Caching**: Semantic caching to reduce API calls and costs
- **Parallel Knowledge Retrieval**: Parallel execution of RAG, graph, and rule searches
- **Dynamic Rule Discovery**: Automatically discovers relevant business rules
- **Context Compression**: Reduces token usage by 30-50% while maintaining accuracy

## Architecture

### Module Structure

```
spyne-ide/
├── backend/
├── __init__.py              # Module exports
├── metadata_provider.py     # Centralized metadata loading with caching
├── llm_query_generator.py  # LLM-based query generation
├── sql_builder.py          # SQL query builder
├── hybrid_knowledge_retriever.py  # Hybrid knowledge retrieval
├── knowledge_base_client.py # KnowledgeBase API client
├── query_regeneration_api.py # Query generation API
├── api/                     # REST API endpoints
├── planning/                # Query planning components
├── execution/               # Query execution components
├── observability/           # Logging and monitoring
├── security/                # Security features
└── ...
```

### Key Components

1. **MetadataProvider**: Single source of truth for metadata with caching
2. **LLMQueryGenerator**: Generates SQL intents using LLM with comprehensive context
3. **ContextBundle**: Bundles all context components (RAG, hybrid, structured, rules)
4. **HybridKnowledgeRetriever**: Combines RAG, graph, and rule-based retrieval

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export OPENAI_API_KEY=your_api_key
export OPENAI_MODEL=gpt-4  # or gpt-4o, gpt-5, etc.
export OPENAI_BASE_URL=https://api.openai.com/v1  # Optional
```

## Usage

### Basic Usage

```python
from backend import LLMQueryGenerator, MetadataProvider

# Load metadata (cached automatically)
metadata = MetadataProvider.load()

# Generate SQL from natural language
generator = LLMQueryGenerator()
intent, reasoning_steps = generator.generate_sql_intent(
    "show me khatabook customers",
    metadata
)

# Convert intent to SQL
sql, explain_plan, warnings = generator.intent_to_sql(intent, metadata)
```

### API Usage

```python
from backend.query_regeneration_api import generate_sql_from_query

result = generate_sql_from_query("show me khatabook customers", use_llm=True)
print(result["sql"])
```

## Code Standards

### Import Organization

1. **Standard library imports** (alphabetically sorted)
2. **Third-party imports** (alphabetically sorted)
3. **Local application imports** (using absolute imports with `backend.` prefix)

Example:
```python
import json
import logging
from pathlib import Path
from typing import Dict, Any

import requests
import tiktoken

from backend.metadata_provider import MetadataProvider
from backend.sql_builder import SQLBuilder
```

### Module Structure

- All modules use absolute imports: `from backend.module import Class`
- No `sys.path` manipulation (except in entry points)
- Proper `__init__.py` files for package structure
- Clear separation of concerns

### Error Handling

- All exceptions are logged with `exc_info=True`
- Graceful degradation for optional components
- Critical failures raise exceptions
- Non-critical failures log warnings and continue

### Logging

- Use module-level logger: `logger = logging.getLogger(__name__)`
- Log levels: DEBUG, INFO, WARNING, ERROR
- Include context in log messages

## Configuration

### Environment Variables

- `OPENAI_API_KEY`: OpenAI API key (required)
- `OPENAI_MODEL`: Model to use (default: gpt-4)
- `OPENAI_BASE_URL`: Base URL for API (default: https://api.openai.com/v1)

### Metadata Files

Place metadata files in `metadata/` directory:
- `semantic_registry.json`: Metrics and dimensions
- `tables.json`: Table schemas and relationships

## Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=backend tests/
```

## Performance Optimizations

1. **Metadata Caching**: Metadata loaded once per process
2. **Query Caching**: Repeated queries use cached results
3. **Parallel Retrieval**: Knowledge retrieval runs in parallel
4. **Context Compression**: Reduces token usage by 30-50%
5. **Token Budget**: Prevents context overflow errors

## Contributing

1. Follow PEP 8 style guide
2. Use type hints for function signatures
3. Write docstrings for all public functions/classes
4. Add tests for new features
5. Update README for significant changes

## License

[Your License Here]

