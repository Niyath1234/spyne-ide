# AI SQL System - Complete Architecture Document

## System Goal

Build a system that:
- Converts messy business language → perfect Trino SQL
- Better than Cursor
- Self-corrects
- Generalizes
- Learns over time

**We are building an AI query planning engine, not an NL→SQL script.**

## Overall System Layout

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
        │ 5. Query Plan Builder           │
        │ 6. SQL Generator                │
        │ 7. SQL Critic                   │
        │ 8. SQL Validator                │
        │ 9. Trino Validator              │
        │10. Learning Layer               │
        └─────────────────────────────────┘
                       ▼
                   FINAL SQL
```

## Core Tech Stack

### Backend
- **FastAPI** - Modern async web framework

### Orchestration
- **LangGraph** - Critical for pipeline orchestration

### Database
- **Postgres + pgvector** - Metadata storage with vector search

### Cache
- **Redis** - Optional caching layer

### SQL Parsing
- **sqlglot** - SQL parsing and validation

### Join Graph
- **networkx** - Graph algorithms for join path finding

### Retrieval
- **LlamaIndex** (optional) or custom pgvector search

### LLM
- **GPT-4** (main) or GPT-5.2
- Small local model optional

### Warehouse
- **Trino** - Your backend data warehouse

## LangGraph Pipeline - Node by Node

### State Object (Global Graph State)

```python
class GraphState(TypedDict):
    user_query: str
    
    intent: dict
    resolution: dict
    
    retrieved_context: dict
    join_path: list
    
    query_plan: dict
    
    generated_sql: str
    fixed_sql: str
    
    validation_errors: list
    final_sql: str
```

### Node 1: Intent Node

**Purpose**: Extract metric, grain, filters, time

**Input**: `user_query`

**Output**: 
```json
{
  "metric": "revenue",
  "grain": "customer",
  "filters": [],
  "time_range": "last_month",
  "top_n": null
}
```

**Stores**: `state["intent"]`

### Node 2: Resolution Node (CRITICAL)

**Purpose**: Decide how system should behave

**Classifies**:
- `EXACT_MATCH` - Metric exists
- `DERIVABLE` - Can compute from other columns
- `CLOSE_MATCH` - Similar metric exists
- `AMBIGUOUS` - Needs clarification
- `IMPOSSIBLE` - No data available

**Output**:
```json
{
  "type": "DERIVABLE",
  "reason": "profit can be computed as revenue - cost",
  "confidence": 0.82
}
```

**Stores**: `state["resolution"]`

**Early Exit**: If `IMPOSSIBLE` → exit with message

### Node 3: Semantic Retrieval Node

**Purpose**: Fetch only relevant metadata

**Uses**: pgvector + semantic search

**Retrieves**:
- Relevant tables
- Columns
- Metrics
- Business rules
- Past queries

**Output**: `state["retrieved_context"]`

**NO LLM** - Pure retrieval

### Node 4: Join Planner Node

**Purpose**: Compute join path deterministically

**Uses**: NetworkX graph

**Function**: `find_join_path(tableA, tableB)`

**Output**: `state["join_path"]` - List of (table1, table2) tuples

**No LLM** - Pure graph algorithm

### Node 5: Query Plan Node (MOST IMPORTANT)

**Purpose**: Build structured query plan

**Output**:
```json
{
  "base_table": "orders",
  "joins": [
    {"table": "customers", "type": "LEFT", "on": "orders.customer_id = customers.id"}
  ],
  "metric_sql": "SUM(order_amount)",
  "group_by": ["customers.id"],
  "filters": [],
  "time_filter": "order_date >= DATE '2024-01-01'"
}
```

**Stores**: `state["query_plan"]`

**This becomes source of truth** - Everything deterministic now

### Node 6: SQL Generation Node

**Purpose**: Convert plan → Trino SQL

**Prompt**: "Generate Trino SQL using ONLY this query plan. Do not invent columns or joins."

**Stores**: `state["generated_sql"]`

### Node 7: SQL Critic Node (VERY IMPORTANT)

**Purpose**: Self-correct SQL

**Checks**:
- Invalid joins
- Missing GROUP BY
- Wrong aggregation
- Invalid columns

**Fixes SQL** and stores: `state["fixed_sql"]`

**Massive accuracy boost**

### Node 8: SQL AST Validator Node

**Uses**: sqlglot

**Checks**:
- Syntax valid
- Columns exist
- GROUP BY correct

**Auto-fixes** small issues

**Stores errors**: `state["validation_errors"]`

### Node 9: Trino Validation Node

**Step 1**: Run `EXPLAIN <sql>`

If error → Send error back to LLM repair prompt

**Step 2**: Run `SELECT * FROM (...) LIMIT 1`

If works → `state["final_sql"]`

**Self-healing system**

### Node 10: Memory Node

**Stores**:
- `user_query`
- `final_sql`
- `success`
- `execution_time_ms`

**Next time**: Retrieve similar queries first

**System improves automatically**

## Expected Performance

| Stage      | Time  |
| ---------- | ----- |
| Intent     | 300ms |
| Resolution | 400ms |
| Retrieval  | 150ms |
| Planning   | 500ms |
| SQL Gen    | 1s    |
| Critic     | 800ms |
| Validation | 500ms |
| **Total**  | **~3-4 sec** |

## Performance Targets

| Metric        | Target |
| ------------- | ------ |
| Latency       | <3.5s  |
| Accuracy      | >90%   |
| Hallucination | <3%    |

## Build Order

### Week 1
- Trino connector ✓
- Metadata ingestion ✓
- Vector DB ✓

### Week 2
- Intent engine ✓
- Semantic retrieval ✓
- Join graph ✓

### Week 3
- Query planner ✓
- SQL generator ✓
- Critic loop ✓

### Week 4
- Execution validator ✓
- Memory system ✓
- Evaluation suite ✓

## Key Design Principles

1. **Single Responsibility** - Each compiler phase has one job
2. **Fail Fast** - If semantic information is lost, fail immediately
3. **No Heuristics** - Enforce correctness structurally
4. **Deterministic** - Join paths computed algorithmically

## What Makes This Better Than Cursor

1. **Multi-Stage Planning** - Not one-shot generation
2. **Self-Correction** - SQL critic loop fixes errors
3. **Deterministic Joins** - NetworkX ensures correctness
4. **Semantic Retrieval** - Only relevant context sent to LLM
5. **Learning System** - Improves over time
6. **Enterprise-Grade** - Production-ready validation

## Conclusion

If built properly, this is one of the most advanced NL→SQL systems anywhere.

Not exaggeration.
