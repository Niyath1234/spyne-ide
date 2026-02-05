# AI SQL System - Execution-Grade NL→SQL Pipeline

**Better than Cursor. Self-corrects. Generalizes. Learns over time.**

## Overview

This is a **modular intelligence pipeline** that converts messy business language into perfect Trino SQL. Built with LangGraph orchestration, it provides CTO-grade architecture with enterprise-level accuracy.

## Architecture

```
                    USER QUERY
                         │
                         ▼
              ┌──────────────────┐
              │ API LAYER        │
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ LANGGRAPH BRAIN  │  ← orchestrates all
              └────────┬─────────┘
                       ▼
        ┌─────────────────────────────────┐
        │ INTELLIGENCE PIPELINE           │
        │                                 │
        │ 1. Intent Engine                │
        │ 2. Resolution Engine            │
        │ 3. Semantic Retrieval           │
        │ 4. Join Graph Planner           │
        │ 5. Query Plan Builder            │
        │ 6. SQL Generator                │
        │ 7. SQL Critic                   │
        │ 8. SQL Validator                │
        │ 9. Trino Validator              │
        │10. Learning Layer               │
        └─────────────────────────────────┘
                       ▼
                   FINAL SQL
```

## Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL with pgvector extension
- Trino instance (optional, for validation)
- OpenAI API key

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export OPENAI_API_KEY=your_key_here
export POSTGRES_CONNECTION_STRING=postgresql://user:pass@localhost:5432/dbname
export TRINO_HOST=localhost
export TRINO_PORT=8080
```

### Run API Server

```bash
python -m backend.ai_sql_system.main
```

Server runs on `http://localhost:8000`

### Test Query

```bash
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "revenue per customer"}'
```

## Module Structure

### `/trino` - Trino Intelligence
- `client.py` - Connection and query execution
- `schema_loader.py` - Extract table/column metadata
- `validator.py` - Validate SQL against Trino

### `/metadata` - Metadata + Vector Brain
- `ingestion.py` - Store tables, columns, metrics in Postgres
- `vector_store.py` - pgvector operations for semantic search
- `semantic_registry.py` - Unified metadata interface

### `/retrieval` - Semantic Retrieval
- `semantic_search.py` - Retrieve relevant metadata using embeddings

### `/planning` - Planning Modules
- `intent_engine.py` - Extract metric, grain, filters, time
- `resolution_engine.py` - Classify query (EXACT_MATCH, DERIVABLE, etc.)
- `join_graph.py` - Compute join paths using NetworkX
- `query_planner.py` - Build structured query plan

### `/sql` - SQL Generation & Validation
- `generator.py` - Convert plan → Trino SQL
- `critic.py` - Self-correct SQL in second LLM pass
- `validator.py` - Validate SQL AST using sqlglot

### `/learning` - Learning Layer
- `memory.py` - Store successful queries for future reference

### `/orchestration` - LangGraph Brain
- `graph.py` - Master pipeline connecting all nodes

### `/api` - API Layer
- `routes.py` - FastAPI endpoints

### `/evaluation` - Evaluation Suite
- `test_suite.py` - 200 test queries across categories

## Performance Targets

| Metric        | Target |
| ------------- | ------ |
| Latency       | <3.5s  |
| Accuracy      | >90%   |
| Hallucination | <3%    |

## Key Features

1. **Modular Design** - Each component has single responsibility
2. **Self-Correcting** - SQL critic loop fixes errors automatically
3. **Deterministic Joins** - NetworkX ensures correct join paths
4. **Semantic Retrieval** - Only relevant metadata sent to LLM
5. **Learning System** - Stores successful queries for improvement
6. **Enterprise-Grade** - Production-ready error handling and validation

## Environment Variables

```bash
# Required
OPENAI_API_KEY=sk-...
POSTGRES_CONNECTION_STRING=postgresql://...

# Optional
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_USER=admin
TRINO_CATALOG=tpch
TRINO_SCHEMA=tiny
LLM_MODEL=gpt-4
PORT=8000
```

## API Endpoints

- `POST /api/query` - Generate SQL from natural language
- `GET /api/metrics` - Get available metrics
- `GET /api/tables` - Get available tables
- `GET /health` - Health check

## Evaluation

Run evaluation suite:

```python
from backend.ai_sql_system.orchestration.graph import LangGraphOrchestrator
from backend.ai_sql_system.evaluation.test_suite import EvaluationSuite

orchestrator = LangGraphOrchestrator()
suite = EvaluationSuite(orchestrator)
results = suite.run_evaluation()
print(f"Accuracy: {results['accuracy']:.2%}")
print(f"Avg Latency: {results['avg_latency_ms']:.0f}ms")
```

## Architecture Principles

1. **Single Responsibility** - Each compiler phase has one job
2. **Fail Fast** - If semantic information is lost, fail immediately
3. **No Heuristics** - Enforce correctness structurally, not with hacks
4. **Deterministic** - Join paths computed algorithmically, not guessed

## License

MIT
