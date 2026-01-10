# RCA-ENGINE Architecture

## Design Principles

1. **Deterministic**: All RCA decisions are based on metadata and deterministic execution, not LLM inference
2. **Three Root Causes**: Everything collapses into Population, Logic, or Data mismatch
3. **Thin LLM Layer**: LLM only translates, disambiguates (≤3 questions), and explains
4. **Hypergraph-Guided**: Business knowledge encoded as hypergraph of entities, tables, rules, and relationships

## Component Overview

### Core Modules

#### `metadata.rs`
- Loads and validates all JSON metadata files
- Builds indexes for fast lookup (tables_by_name, rules_by_id, etc.)
- Provides accessors for entities, tables, metrics, rules, lineage

#### `llm.rs`
- Thin wrapper around OpenAI API
- Three functions:
  - `interpret_query()`: Maps natural language to structured interpretation
  - `resolve_ambiguity()`: Generates questions for disambiguation
  - `explain_rca()`: Converts structured RCA result to business language
- Always returns JSON, never sees raw data

#### `graph.rs`
- Hypergraph traversal and subgraph extraction
- Finds rules and tables needed for reconciliation
- Identifies join paths between tables
- Determines final grain of rules

#### `identity.rs`
- Resolves canonical keys vs alternate keys
- Normalizes dataframes to common grain
- Handles key mappings between systems

#### `time.rs`
- Applies as-of date filtering
- Detects temporal misalignment
- Enforces time-based business rules

#### `operators.rs`
- Relational operations using Polars:
  - `scan`: Load table from Parquet
  - `join`: Join two dataframes
  - `filter`: Filter rows
  - `derive`: Compute new columns
  - `group`: Aggregate by groups
  - `select`: Select/rename columns
- Tracks row counts to detect join explosion

#### `rule_compiler.rs` & `executor.rs`
- Compiles rule pipelines into execution plans
- Executes plans step-by-step
- Supports step-by-step execution for drilldown

#### `diff.rs`
- Population diff: Missing/extra/duplicate entities
- Data diff: Value differences with precision handling
- Returns structured comparison results

#### `drilldown.rs`
- For mismatched keys, re-executes pipelines step-by-step
- Compares intermediate outputs
- Finds first divergence point

#### `rca.rs`
- Main orchestration module
- Coordinates all components
- Classifies mismatches into 3 root causes
- Produces final RCA result

#### `ambiguity.rs`
- Handles ambiguous interpretations
- Asks ≤3 multiple-choice questions
- Resolves to specific rule IDs, time columns, etc.

#### `explain.rs`
- Wrapper around LLM explanation
- Converts structured RCA to business language

## Data Flow

```
User Query (Natural Language)
    ↓
LLM Interpretation (JSON)
    ↓
Ambiguity Resolution (if needed)
    ↓
Rule Resolution (from metadata)
    ↓
Grain Normalization
    ↓
Pipeline Execution (Polars)
    ↓
Time Filtering
    ↓
Comparison (Population + Data Diff)
    ↓
Classification (3 Root Causes)
    ↓
Drill-down (for mismatches)
    ↓
LLM Explanation (Business Language)
```

## Metadata Structure

### entities.json
Defines business entities and their grains:
```json
{
  "id": "loan",
  "grain": ["loan_id"],
  "attributes": ["loan_id", "customer_id", ...]
}
```

### tables.json
Maps tables to entities:
```json
{
  "name": "khatabook_loans",
  "entity": "loan",
  "primary_key": ["loan_id"],
  "time_column": "disbursement_date",
  "system": "khatabook"
}
```

### metrics.json
Defines metrics with precision and null policies:
```json
{
  "id": "tos",
  "grain": ["loan_id"],
  "precision": 2,
  "null_policy": "zero"
}
```

### rules.json
Defines computation pipelines as operator graphs:
```json
{
  "id": "khatabook_tos",
  "system": "khatabook",
  "metric": "tos",
  "pipeline": [
    {"op": "scan", "table": "khatabook_loans"},
    {"op": "join", "table": "khatabook_emis", "on": ["loan_id"]},
    ...
  ]
}
```

### lineage.json
Defines hypergraph edges (possible joins):
```json
{
  "from": "khatabook_loans",
  "to": "khatabook_emis",
  "keys": {"loan_id": "loan_id"}
}
```

## Execution Model

### Pipeline Execution
1. Load tables from Parquet files
2. Apply as-of filtering if specified
3. Execute operations sequentially:
   - Joins are validated for explosion
   - Aggregations preserve grain
   - Derived columns computed deterministically

### Comparison Model
1. **Population Diff**:
   - Extract unique keys from both sides
   - Find missing/extra/duplicates
   
2. **Data Diff**:
   - Join on common keys
   - Compare metric values with precision
   - Identify mismatches

### Classification Model
All mismatches classified into:
- **Population Mismatch**: Different set of entities
  - Missing entities
  - Extra entities
  - Duplicates
  
- **Logic Mismatch**: Different transformations
  - Operation order
  - Join explosion
  - Aggregation differences
  
- **Data Mismatch**: Same entity, different values
  - Time misalignment
  - Null/default handling
  - Precision/rounding
  - Schema drift

## LLM Integration

### Query Interpretation
- Input: Natural language query
- Output: Structured JSON with system_a, system_b, metric, as_of_date
- Uses: business_labels.json, metrics.json

### Ambiguity Resolution
- Input: Ambiguity type, options
- Output: Questions (max 3)
- User answers → resolved values

### Explanation
- Input: Structured RCA result
- Output: Business-friendly explanation
- Never sees raw data, only structured output

## Error Handling

All errors are typed using `RcaError` enum:
- Metadata errors (invalid JSON, missing files)
- LLM errors (API failures, parsing)
- Graph errors (no rules found, invalid paths)
- Execution errors (join explosion, missing columns)
- Identity errors (key resolution failures)
- Time errors (date parsing, temporal issues)

## Performance Considerations

- Polars provides columnar, SIMD-optimized execution
- Metadata indexes for O(1) lookups
- Step-by-step execution only for mismatched keys (drilldown)
- LLM calls minimized (max 2 + 1 for ambiguity)
- Results cached where possible

## Extensibility

To add new metrics:
1. Add to `metrics.json`
2. Add business label to `business_labels.json`
3. Create rules in `rules.json`
4. Update tables if needed

To add new systems:
1. Add system label to `business_labels.json`
2. Add tables to `tables.json`
3. Create rules in `rules.json`
4. Update lineage if needed

