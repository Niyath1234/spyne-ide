# Repository Structure

## Overview

The Spyne IDE repository follows a modular, production-grade architecture with clear separation of concerns.

## Directory Structure

```
spyne-ide/
├── backend/                    # Core backend modules
│   ├── __init__.py            # Module exports
│   ├── metadata_provider.py   # Centralized metadata provider
│   ├── llm_query_generator.py # LLM query generation
│   ├── sql_builder.py         # SQL query builder
│   ├── hybrid_knowledge_retriever.py # Hybrid knowledge retrieval
│   ├── knowledge_base_client.py # KnowledgeBase API client
│   ├── query_regeneration_api.py # Query generation API
│   │
│   ├── api/                   # REST API endpoints
│   │   ├── __init__.py
│   │   ├── query.py
│   │   ├── health.py
│   │   └── metrics.py
│   │
│   ├── planning/              # Query planning components
│   │   ├── __init__.py
│   │   ├── schema_selector.py
│   │   ├── metric_resolver.py
│   │   └── ...
│   │
│   ├── execution/             # Query execution
│   ├── observability/         # Logging and monitoring
│   ├── security/              # Security features
│   ├── failure_handling/      # Error handling
│   └── ...
│
├── tests/                     # Test suite
├── metadata/                  # Metadata files (JSON)
├── config/                    # Configuration files
├── requirements.txt           # Python dependencies
├── README.md                  # Main documentation
├── CODE_STANDARDS.md          # Coding standards
└── ...
```

## Module Organization

### Core Modules

1. **metadata_provider.py**
   - Single source of truth for metadata
   - Process-level caching
   - Graceful fallback handling

2. **llm_query_generator.py**
   - LLM-based query generation
   - Context building and bundling
   - Token management and caching

3. **sql_builder.py**
   - SQL query construction
   - Table relationship resolution
   - Intent validation

4. **hybrid_knowledge_retriever.py**
   - Parallel knowledge retrieval
   - RAG + Graph + Rules
   - Knowledge validation

### Module Dependencies

```
metadata_provider.py (no dependencies)
    ↓
llm_query_generator.py
    ├── metadata_provider.py
    ├── sql_builder.py
    └── hybrid_knowledge_retriever.py
        └── knowledge_base_client.py
```

## Code Standards Compliance

### ✅ Import Organization
- Standard library imports first
- Third-party imports second
- Local imports last (absolute imports with `backend.` prefix)

### ✅ Module Structure
- Proper `__init__.py` files
- Clear separation of concerns
- Single responsibility principle

### ✅ Error Handling
- Specific exception catching
- Proper logging with `exc_info=True`
- Graceful degradation

### ✅ Documentation
- Module docstrings
- Function docstrings (Google style)
- Type hints for all functions

### ✅ Naming Conventions
- `snake_case` for functions/variables
- `PascalCase` for classes
- `UPPER_CASE` for constants
- Leading underscore for private members

## Modularity Checklist

- [x] **Clear Module Boundaries**: Each module has a single, well-defined purpose
- [x] **Proper Imports**: Using absolute imports (`backend.module`)
- [x] **No Circular Dependencies**: Dependency graph is acyclic
- [x] **Interface Segregation**: Clear public APIs via `__init__.py`
- [x] **Dependency Injection**: Dependencies passed as parameters
- [x] **Separation of Concerns**: Business logic separated from I/O
- [x] **Reusability**: Core modules can be used independently
- [x] **Testability**: Modules can be tested in isolation

## Entry Points

Entry points (allowed to use `sys.path`):
- `backend/app.py` - Flask application
- `backend/app_production.py` - Production server
- `src/pipeline.py` - Pipeline entry point
- `test_outstanding_daily_regeneration.py` - Test script

## Metadata Files

Metadata files should be placed in `metadata/` directory:
- `semantic_registry.json` - Metrics and dimensions
- `tables.json` - Table schemas

The system gracefully handles missing files and provides fallbacks.

## Testing

Tests are organized in `tests/` directory:
- Unit tests for individual modules
- Integration tests for component interactions
- End-to-end tests for full pipeline

## Configuration

- Environment variables for runtime configuration
- `.env` file for local development (gitignored)
- `config/` directory for application configs

## Documentation

- `README.md` - Main documentation
- `CODE_STANDARDS.md` - Coding standards and best practices
- `REPOSITORY_STRUCTURE.md` - This file
- Inline docstrings for all public APIs

