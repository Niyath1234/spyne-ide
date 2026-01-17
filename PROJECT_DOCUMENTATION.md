# RCA Engine - Complete Project Documentation

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Project Overview](#project-overview)
3. [System Architecture](#system-architecture)
4. [Core Components](#core-components)
5. [Data Flow & Processing](#data-flow--processing)
6. [Implementation Details](#implementation-details)
7. [User Interface](#user-interface)
8. [API & Integration](#api--integration)
9. [Metadata & Configuration](#metadata--configuration)
10. [Testing & Validation](#testing--validation)
11. [Deployment & Operations](#deployment--operations)
12. [Performance & Scalability](#performance--scalability)
13. [Security & Trust](#security--trust)
14. [Future Enhancements](#future-enhancements)

---

## Executive Summary

The **RCA (Root Cause Analysis) Engine** is a production-ready, intelligent system designed to automatically identify and explain discrepancies between data systems. It transforms the complex, manual process of root cause analysis into an automated, AI-powered investigation that provides row-level precision and complete auditability.

### Key Value Propositions

- **From Complex SQL to Natural Language**: Transforms 300-line SQL queries into simple 1-2 line natural language questions
- **Row-Level Precision**: Identifies exact rows (UUIDs) causing discrepancies, not just aggregate differences
- **Complete Lineage Tracing**: Tracks every data transformation from source to final metric
- **AI-Powered Reasoning**: Uses LLM to guide intelligent exploration and provide human-readable explanations
- **Mathematical Verification**: Proves that row-level differences match aggregate mismatches
- **Dual-Mode Architecture**: Supports both comprehensive fixed pipeline and adaptive graph traversal approaches

### Problem Solved

**Traditional RCA Challenges:**
- Manual investigation is time-consuming (hours to days)
- Aggregate-level comparisons don't reveal root causes
- No visibility into data transformation pipelines
- Difficult to trace where discrepancies originate
- Cannot verify reconciliation correctness
- Hard to reproduce analysis

**RCA Engine Solution:**
- Automated row-level analysis in minutes
- Complete lineage tracing with audit trail
- AI-powered root cause attribution
- Deterministic verification with mathematical proof
- Performance optimized for large-scale datasets
- Full reproducibility and trust layer

---

## Project Overview

### Technology Stack

**Backend (Rust):**
- **Data Processing**: Polars (DataFrames), Arrow (columnar format)
- **Async Runtime**: Tokio
- **HTTP Server**: Custom tokio-based server
- **LLM Integration**: OpenAI-compatible API client
- **Serialization**: Serde/JSON
- **Graph Processing**: Custom hypergraph implementation

**Frontend (TypeScript/React):**
- **Framework**: React with TypeScript
- **Build Tool**: Vite
- **HTTP Client**: Axios

**Data Storage:**
- **Format**: CSV and Parquet files
- **Location**: Local filesystem (`tables/` directory)
- **Metadata**: JSON files in `metadata/` directory

### Project Structure

```
RCA-ENGINE/
├── src/                    # Rust source code
│   ├── core/              # Core engine modules
│   │   ├── agent/         # Agent implementations (RCA Cursor, Graph Traversal)
│   │   ├── engine/        # Execution engine components
│   │   ├── lineage/       # Lineage tracing
│   │   ├── metrics/       # Metric normalization
│   │   ├── models/        # Data models
│   │   ├── observability/ # Tracing and logging
│   │   ├── performance/  # Performance optimizations
│   │   ├── rca/           # RCA-specific logic
│   │   ├── safety/        # Safety guardrails
│   │   └── trust/         # Trust layer (evidence, verification)
│   ├── ingestion/         # Data ingestion modules
│   ├── modules/           # Modular components
│   ├── bin/              # Binary executables
│   │   ├── server.rs     # HTTP server
│   │   └── build_rule.rs # Rule builder utility
│   └── lib.rs            # Library root
├── ui/                    # Frontend React application
├── metadata/              # Metadata definitions (tables, rules, lineage)
├── tables/                # Data files (CSV/Parquet)
├── Hypergraph/           # Hypergraph module
├── KnowledgeBase/        # Knowledge base module
├── WorldState/           # World state module
├── tests/                 # Test files
├── Cargo.toml            # Rust dependencies
└── package.json          # Node.js dependencies
```

### Key Features

1. **Natural Language Interface**: Users ask questions in plain English
2. **Intent Compilation**: LLM parses queries and extracts structured intent
3. **Task Grounding**: Maps abstract intent to concrete database tables/columns
4. **Dual Execution Modes**:
   - **Fixed Pipeline (RCA Cursor)**: Comprehensive, deterministic analysis
   - **Dynamic Graph Traversal**: Adaptive, intelligent exploration
5. **Row-Level Analysis**: Identifies exact rows causing discrepancies
6. **Lineage Tracing**: Complete audit trail of data transformations
7. **Root Cause Attribution**: AI-powered explanations of differences
8. **Mathematical Verification**: Proves correctness of analysis
9. **Performance Optimizations**: Chunked processing, hash-based diff, sampling
10. **Trust Layer**: Evidence storage, deterministic replay, verification

---

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interface (React)                    │
│              Natural Language Query Input                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                  HTTP API Server (Rust)                      │
│              RESTful endpoints for RCA operations            │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Intent Compiler (LLM-Powered)                    │
│  - Parses natural language query                            │
│  - Extracts systems, metrics, constraints                   │
│  - Generates IntentSpec                                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Task Grounder                                   │
│  - Maps intent to concrete tables/columns                  │
│  - Selects optimal rules using LLM reasoning                │
│  - Resolves grain and constraints                           │
│  - Builds knowledge graph                                   │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────────────┐   ┌──────────────────────────────┐
│  APPROACH 1:          │   │  APPROACH 2:                  │
│  Fixed Pipeline       │   │  Dynamic Graph Traversal      │
│  (RCA Cursor)         │   │  (Graph Traversal Agent)      │
│                       │   │                               │
│  Phase 1: Normalize   │   │  Traverse → Test → Observe   │
│  Phase 2: Materialize │   │  → Decide → Repeat            │
│  Phase 3: Canonicalize│   │                               │
│  Phase 4: Row Diff    │   │  - LLM-guided node selection │
│  Phase 5: Lineage     │   │  - Small SQL probes (LIMIT)  │
│  Phase 6: Attribution │   │  - Early termination         │
│  Phase 7: Narrative   │   │  - Adaptive exploration       │
│  Phase 8: Reconcile   │   │                               │
└───────────┬───────────┘   └───────────────┬──────────────┘
            │                               │
            └───────────────┬───────────────┘
                            ▼
        ┌──────────────────────────────────────┐
        │         Final RCA Result              │
        │  - Row-level differences              │
        │  - Root cause explanations            │
        │  - Human-readable narratives          │
        │  - Verification proof                │
        └──────────────────────────────────────┘
```

### Component Interaction Flow

```
User Query
    ↓
HTTP Server (bin/server.rs)
    ↓
RCA Engine (rca.rs)
    ↓
Intent Compiler (intent_compiler.rs)
    ↓
Task Grounder (task_grounder.rs)
    ↓
┌─────────────────┬─────────────────┐
│                 │                 │
RCA Cursor        Graph Traversal    Hybrid
(cursor.rs)       (graph_traversal.rs)
│                 │
│                 │
Execution Engine  SQL Engine
(execution_engine.rs) (sql_engine.rs)
│                 │
│                 │
Data Processing   Probe Results
(polars)          (SqlProbeResult)
│                 │
│                 │
Results           Findings
(RcaCursorResult) (TraversalState)
```

### Knowledge Graph Structure

The system maintains a rich knowledge graph representing:

**Node Types:**
- **Table Nodes**: Base data tables with column metadata
- **Rule Nodes**: Business rule calculations with formulas
- **Join Nodes**: Join relationships with keys and types
- **Filter Nodes**: Filter conditions and expressions
- **Metric Nodes**: Final metric calculations

**Edge Types:**
- Table → Join (via lineage)
- Join → Table (relationships)
- Table → Rule (used in computation)
- Rule → Metric (calculates metric)
- Filter → Table (applied to table)

**Metadata Enrichment:**
- Hypergraph statistics (row counts, selectivities, data quality scores)
- Column types, descriptions, labels
- Join feasibility, filter selectivity
- Business context and relationships

---

## Core Components

### 1. Intent Compiler (`intent_compiler.rs`)

**Purpose**: Converts natural language queries into structured intent specifications.

**Key Functions:**
- Parses user queries using LLM
- Extracts systems, metrics, entities, constraints
- Handles ambiguity resolution
- Generates `IntentSpec` with task type (RCA or Data Validation)

**Input Example:**
```
"Why is recovery mismatching between system A and B for Digital loans on 2026-01-08?"
```

**Output Example:**
```rust
IntentSpec {
    task_type: RCA,
    systems: ["system_a", "system_b"],
    target_metrics: ["recovery"],
    entities: ["loan"],
    grain: ["uuid", "paid_date", "mis_date", "current_bucket"],
    constraints: [
        FilterConstraint { column: "loan_type", operator: "=", value: "Digital" },
        FilterConstraint { column: "paid_date", operator: "=", value: "2026-01-08" }
    ]
}
```

**Implementation Details:**
- Uses LLM client (`llm.rs`) to parse queries
- Leverages metadata for context (available tables, metrics, systems)
- Handles complex date logic (FTD, CM, LMTD, LM)
- Resolves ambiguous terms using fuzzy matching

### 2. Task Grounder (`task_grounder.rs`)

**Purpose**: Maps abstract intent to concrete database tables, columns, and rules.

**Key Functions:**
- Fuzzy matching to find relevant tables
- LLM reasoning for optimal table/rule selection
- Considers labels, grain, and entity relationships
- Scores and ranks candidate tables
- Resolves column mappings and constraints

**Process:**
1. **Fuzzy Matching**: Finds candidate tables using string similarity
2. **LLM Reasoning**: Uses comprehensive reasoning about table relevance
3. **Label Matching**: Matches task labels with table/rule labels
4. **Rule Reasoning**: Selects best rules using chain-of-thought reasoning
5. **Column Inference**: Identifies relevant columns for metrics
6. **Validation**: Ensures all mappings are valid and reachable

**Example:**
```rust
// User says: "Exclude writeoff loans"
// System finds candidates:
Candidates = [
    { table: "writeoff_users", score: 0.95 },
    { table: "loan_summary", score: 0.60 }
]

// LLM reasoning selects: loan_summary.writeoff_flag
// System generates filter: WHERE writeoff_flag = false
```

### 3. RCA Cursor (`core/agent/rca_cursor/cursor.rs`)

**Purpose**: Fixed pipeline approach for comprehensive, deterministic analysis.

**Phases:**
1. **Normalize Metrics**: Converts business rules into standardized metric definitions
2. **Materialize Rows**: Extracts row-level data from both systems
3. **Canonicalize**: Maps dataframes to common format
4. **Row Diff**: Identifies exact differences between systems
5. **Lineage Trace**: Tracks data transformations
6. **Attribution**: Explains why rows differ
7. **Narrative**: Generates human-readable explanations
8. **Reconcile**: Verifies aggregate matches row-level differences

**Key Features:**
- Always runs all phases (comprehensive)
- Complete audit trail
- Deterministic results
- Best for production reconciliation

**Data Structures:**
```rust
RcaCursorResult {
    row_diff: RowDiffResult,
    lineage_traces: Vec<LineageTrace>,
    attributions: Vec<RowExplanation>,
    narrative: String,
    reconciliation: ReconciliationProof,
    confidence: f64
}
```

### 4. Graph Traversal Agent (`graph_traversal.rs`)

**Purpose**: Dynamic, adaptive root cause investigation.

**Core Pattern: Traverse → Test → Observe → Decide → Repeat**

**Process:**
1. **Traverse**: Choose next best node to visit (LLM-guided)
2. **Test**: Run small SQL probe at that node (LIMIT 100)
3. **Observe**: Analyze probe result (row count, nulls, join failures)
4. **Decide**: Determine next step based on observations
5. **Repeat**: Continue until root cause found

**Key Features:**
- Dynamic paths (not fixed pipeline)
- Early termination when root cause found
- Rich metadata at each node
- Small probes for fast iteration
- Adaptive decision making

**Node Selection Factors:**
- Relevance to findings (missing rows → probe joins)
- Information gain (which node eliminates most possibilities?)
- Proximity (nodes connected to visited nodes)
- LLM reasoning with full metadata context

**Example Flow:**
```
1. Probe table:payments_a
   → Result: 1000 rows
   → Observation: System A has data

2. Probe table:payments_b
   → Result: 950 rows
   → Observation: System B missing 50 rows
   → Decision: Probe joins (most likely cause)

3. Probe join:payments:orders
   → Result: 50 rows where join failed
   → Observation: Exact match - 50 missing rows = 50 join failures
   → Decision: ROOT CAUSE FOUND → STOP
```

### 5. SQL Engine (`sql_engine.rs`)

**Purpose**: Executes small SQL probe queries for dynamic graph traversal.

**Key Functions:**
- Executes SQL queries against tables (CSV/Parquet)
- Returns probe results with sample rows, statistics, warnings
- Supports table probes, join probes, filter probes, rule probes
- Fast iteration with LIMIT 100 for quick exploration

**Probe Types:**
- **Table Probe**: `SELECT * FROM table WHERE conditions LIMIT 100`
- **Join Probe**: `SELECT a.* FROM table_a a LEFT JOIN table_b b ON ... WHERE b.key IS NULL LIMIT 100`
- **Filter Probe**: `SELECT * FROM table WHERE filter_condition LIMIT 100`
- **Rule Probe**: `SELECT ... FROM ... WHERE ... GROUP BY ... LIMIT 100`

**Result Structure:**
```rust
SqlProbeResult {
    row_count: usize,
    sample_rows: Vec<HashMap<String, String>>,
    column_names: Vec<String>,
    null_counts: HashMap<String, usize>,
    value_ranges: HashMap<String, (String, String)>,
    execution_time_ms: u64,
    warnings: Vec<String>
}
```

### 6. Execution Engine (`execution_engine.rs`)

**Purpose**: Executes queries and processes data for RCA analysis.

**Key Functions:**
- Builds queries from metric definitions
- Applies joins, filters, transformations
- Converts aggregation queries to pre-aggregation form
- Outputs individual rows instead of aggregates
- Handles chunked processing for large datasets

**Query Building:**
```rust
// From MetricDefinition to SQL
MetricDefinition {
    base_tables: ["payments"],
    joins: [Join { left: "payments", right: "orders", on: "order_id" }],
    filters: [Filter { column: "paid_date", operator: "=", value: "2026-01-08" }],
    aggregation: Sum("paid_amount"),
    group_by: ["uuid"]
}
    ↓
SQL: SELECT uuid, paid_amount FROM payments p 
     LEFT JOIN orders o ON p.order_id = o.order_id
     WHERE p.paid_date = '2026-01-08'
```

### 7. Row Diff Engine (`core/engine/row_diff.rs`)

**Purpose**: Identifies exact differences between two canonical dataframes.

**Process:**
1. Compares rows by key (e.g., loan_id, uuid)
2. Categorizes differences:
   - `missing_left`: Rows only in left system
   - `missing_right`: Rows only in right system
   - `value_mismatch`: Rows with different values
   - `matches`: Rows that match exactly
3. Calculates precision-aware numeric differences

**Output:**
```rust
RowDiffResult {
    missing_left: DataFrame,      // Rows only in System A
    missing_right: DataFrame,    // Rows only in System B
    value_mismatch: DataFrame,    // Rows with different values
    matches: DataFrame,           // Matching rows
    summary: DiffSummary {
        total_left: 1000,
        total_right: 950,
        missing_left_count: 0,
        missing_right_count: 50,
        mismatch_count: 0,
        match_count: 950
    }
}
```

### 8. Lineage Tracing (`core/lineage/`)

**Purpose**: Tracks how each row was processed through the pipeline.

**Components:**

**a) Join Tracing (`join_trace.rs`):**
- Records which joins succeeded/failed per row
- Tracks join conditions and types
- Identifies missing join matches

**b) Filter Tracing (`filter_trace.rs`):**
- Records filter decisions (pass/fail) per row
- Captures filter expressions and values
- Explains why rows were filtered out

**c) Rule Tracing (`rule_trace.rs`):**
- Tracks rule execution per row
- Records input/output values
- Identifies which rules fired

**Example:**
```rust
LineageTrace {
    row_id: vec!["uuid_001".to_string()],
    join_traces: vec![
        JoinTrace {
            join_name: "payments:orders",
            succeeded: false,
            reason: "order_id not found in orders table"
        }
    ],
    filter_traces: vec![
        FilterTrace {
            filter_name: "status_active",
            passed: true,
            condition: "status = 'active'"
        }
    ],
    rule_traces: vec![
        RuleTrace {
            rule_name: "system_a_recovery",
            input_values: HashMap::new(),
            output_value: 1000.0
        }
    ]
}
```

### 9. Root Cause Attribution (`core/rca/attribution.rs`)

**Purpose**: Combines lineage traces to explain why rows differ.

**Process:**
1. Analyzes join traces for missing matches
2. Examines filter traces for dropped rows
3. Reviews rule traces for transformations
4. Combines evidence into structured explanations

**Output:**
```rust
RowExplanation {
    row_id: vec!["uuid_001".to_string()],
    difference_type: MissingInRight,
    explanations: vec![
        ExplanationItem {
            source: Join,
            explanation: "Row dropped due to failed join: payments → orders on order_id",
            evidence: {
                "join_name": "payments:orders",
                "failed_key": "order_id",
                "missing_value": "ORD123"
            }
        }
    ],
    confidence: 0.95
}
```

### 10. Narrative Builder (`core/rca/narrative.rs`)

**Purpose**: Converts structured explanations into human-readable narratives.

**Modes:**
- **Template-Based**: Always available, structured templates
- **LLM-Enhanced**: Optional LLM enhancement for natural language

**Example Output:**
```
Row [uuid_001]: Missing in System B

Summary: Row uuid_001 exists in System A but not in System B.

Details:
1. Row dropped due to failed join: payments → orders on order_id.
   Order ID ORD123 not found in orders table in System B.
2. Filter condition not met: status = 'active'. Row has status = 'closed'.

Impact:
- Missing value: 1,000.00
- Contributes to aggregate difference: 1,000.00

Recommendations:
- Check if order ORD123 exists in System B orders table
- Verify join logic for payments → orders
- Review status field mapping between systems
```

### 11. Aggregate Reconciliation Engine (`core/engine/aggregate_reconcile.rs`)

**Purpose**: Proves that row-level differences explain aggregate mismatch.

**Mathematical Proof:**
```
reported_mismatch = sum(System A) - sum(System B)

calculated_mismatch = 
    sum(missing_left values) -           // Rows only in A
    sum(missing_right values) +          // Rows only in B (subtract)
    sum(value_mismatch differences)      // Value differences

Verification: |reported_mismatch - calculated_mismatch| < tolerance
```

**Output:**
```rust
ReconciliationProof {
    reported_mismatch: 50000.0,
    calculated_mismatch: 50000.0,
    difference: 0.0,
    within_tolerance: true,
    breakdown: ReconciliationBreakdown {
        missing_left_contribution: 0.0,
        missing_right_contribution: 50000.0,
        mismatch_contribution: 0.0
    }
}
```

### 12. LLM Client (`llm.rs`)

**Purpose**: Interfaces with OpenAI-compatible LLM APIs.

**Key Functions:**
- Sends prompts to LLM API
- Handles API responses and errors
- Manages rate limiting and retries
- Parses structured responses (JSON)

**Configuration:**
- API key from environment variable `OPENAI_API_KEY`
- Model selection (default: `gpt-4`)
- Temperature and other parameters
- Fallback responses when API unavailable

### 13. Metadata System (`metadata.rs`)

**Purpose**: Manages metadata about tables, rules, lineage, and relationships.

**Metadata Files:**
- `tables.json`: Table definitions with columns, types, descriptions
- `rules.json`: Business rules with formulas and source entities
- `lineage.json`: Join relationships and data flow
- `entities.json`: Entity definitions and relationships
- `metrics.json`: Metric definitions
- `identity.json`: Identity resolution rules
- `time.json`: Time-related metadata
- `business_labels.json`: Business labels and mappings

**Structure:**
```rust
Metadata {
    tables: Vec<Table>,
    rules: Vec<Rule>,
    lineage: Vec<Lineage>,
    entities: Vec<Entity>,
    metrics: Vec<Metric>,
    identity: IdentityConfig,
    time: TimeConfig,
    business_labels: BusinessLabels
}
```

### 14. Performance Optimizations (`core/performance/`)

**Components:**

**a) Chunked Extraction (`chunked_extraction.rs`):**
- Processes large datasets in chunks
- Reduces memory footprint
- Enables streaming processing

**b) Hash-Based Diff (`hash_diff.rs`):**
- Fast comparison using row hashes
- O(n) comparison instead of O(n²)
- Returns key-level differences efficiently

**c) Sampling (`sampling.rs`):**
- Random sampling for quick analysis
- Stratified sampling for representative subsets
- Top-N sampling for high-value rows

**d) Pushdown Predicates (`pushdown.rs`):**
- Pushes filters to data source
- Reduces data transfer
- Optimizes query execution

**e) Parallel Execution (`parallel_executor.rs`):**
- Executes queries in parallel
- Processes multiple systems concurrently
- Optimizes resource usage

**f) Parquet Cache (`parquet_cache.rs`):**
- Caches parquet file reads
- Reduces I/O operations
- Improves query performance

### 15. Trust Layer (`core/trust/`)

**Components:**

**a) Evidence Storage (`evidence.rs`):**
- Stores complete execution records
- Tracks inputs, outputs, intermediates
- Enables auditability

**b) Deterministic Replay (`replay.rs`):**
- Replays executions from evidence
- Verifies outputs match original
- Enables reproducibility

**c) Verification (`verification.rs`):**
- Aggregate reconciliation proof
- Consistency checks
- Integrity validation

### 16. Safety Guardrails (`safety_guardrails.rs`)

**Purpose**: Prevents unsafe operations and ensures system reliability.

**Checks:**
- Resource limits (memory, execution time)
- Query complexity limits
- Data size limits
- Rate limiting for LLM calls
- Error recovery and graceful degradation

### 17. Observability (`core/observability/`)

**Components:**

**a) Trace Collector (`trace_collector.rs`):**
- Collects execution traces
- Records timing and performance metrics
- Tracks LLM calls and costs

**b) Trace Store (`trace_store.rs`):**
- Stores traces for analysis
- Enables debugging and optimization
- Provides audit trail

**c) Execution Trace (`execution_trace.rs`):**
- Detailed execution traces
- Step-by-step operation logs
- Performance profiling

---

## Data Flow & Processing

### Complete RCA Flow

```
1. User Query (Natural Language)
   "Why is recovery mismatching between system A and B for Digital loans on 2026-01-08?"
   ↓
2. HTTP Server Receives Request
   POST /api/rca/analyze
   ↓
3. Intent Compilation
   IntentSpec {
       systems: ["system_a", "system_b"],
       metric: "recovery",
       filters: ["loan_type=Digital", "paid_date=2026-01-08"]
   }
   ↓
4. Task Grounding
   GroundedTask {
       tables: ["payments_a", "payments_b"],
       rules: ["system_a_recovery_rule", "system_b_recovery_rule"],
       grain: ["uuid", "paid_date", "mis_date", "current_bucket"]
   }
   ↓
5. Execution Mode Selection
   - Fixed Pipeline (RCA Cursor) OR
   - Dynamic Graph Traversal
   ↓
6. Data Extraction
   - System A: Query payments_a table
   - System B: Query payments_b table
   - Apply filters and joins
   - Extract row-level data
   ↓
7. Canonicalization
   - Map both systems to common format
   - Normalize column names
   - Align data types
   ↓
8. Row-Level Diff
   - Compare rows by key (uuid)
   - Identify missing rows
   - Identify value mismatches
   ↓
9. Lineage Tracing
   - Trace joins for each row
   - Trace filters for each row
   - Trace rule execution
   ↓
10. Root Cause Attribution
    - Analyze lineage traces
    - Identify root causes
    - Generate explanations
    ↓
11. Narrative Generation
    - Format results
    - Generate human-readable explanations
    ↓
12. Aggregate Reconciliation
    - Verify row-level matches aggregate
    - Calculate proof
    ↓
13. Response to User
    - Formatted results
    - Root cause explanations
    - Verification proof
```

### Fixed Pipeline Flow (RCA Cursor)

```
Phase 1: Normalize Metrics
  Input: Business rules
  Output: MetricDefinition
  Process: Parse formulas, extract tables, joins, filters

Phase 2: Materialize Rows
  Input: MetricDefinition
  Output: Row-level dataframes (System A & B)
  Process: Execute queries, extract rows before aggregation

Phase 3: Canonicalize
  Input: Raw dataframes
  Output: Canonical dataframes
  Process: Map columns, normalize types, align grain

Phase 4: Row Diff
  Input: Canonical dataframes
  Output: RowDiffResult
  Process: Compare rows, categorize differences

Phase 5: Lineage Trace
  Input: RowDiffResult, execution context
  Output: LineageTrace for each row
  Process: Trace joins, filters, rules

Phase 6: Attribution
  Input: LineageTrace, RowDiffResult
  Output: RowExplanation for each difference
  Process: Analyze traces, generate explanations

Phase 7: Narrative
  Input: RowExplanation, RowDiffResult
  Output: Human-readable narrative
  Process: Format results, generate explanations

Phase 8: Reconcile
  Input: RowDiffResult, aggregate values
  Output: ReconciliationProof
  Process: Verify mathematical correctness
```

### Dynamic Graph Traversal Flow

```
1. Build Knowledge Graph
   - Load metadata
   - Create nodes (tables, rules, joins, filters, metrics)
   - Add hypergraph statistics
   ↓
2. Initialize Traversal State
   - visited_path: []
   - findings: []
   - current_hypothesis: None
   - nodes: KnowledgeGraph
   ↓
3. Loop: Traverse → Test → Observe → Decide
   
   a) Traverse: Choose Next Node
      - LLM receives all candidate nodes with metadata
      - LLM reasons about which node to probe
      - Selects node based on:
        * Relevance to findings
        * Information gain
        * Proximity to visited nodes
      ↓
   b) Test: Run SQL Probe
      - Generate SQL query for selected node
      - Execute with LIMIT 100
      - Return SqlProbeResult
      ↓
   c) Observe: Interpret Result
      - Analyze row count
      - Check for nulls, join failures
      - Identify patterns
      - Record findings
      ↓
   d) Decide: Determine Next Step
      - Update hypothesis
      - Check if root cause found
      - If found: STOP
      - If not: Continue to (a)
   ↓
4. Generate Final Report
   - Aggregate findings
   - Form root cause conclusion
   - Generate explanation
   - Return TraversalState
```

### Data Processing Pipeline

```
CSV/Parquet Files
    ↓
Polars DataFrame (Lazy)
    ↓
Query Execution (with filters, joins)
    ↓
Row-Level Extraction
    ↓
Canonical Mapping
    ↓
Row Comparison (Hash-based)
    ↓
Difference Categorization
    ↓
Lineage Tracing
    ↓
Attribution Analysis
    ↓
Result Formatting
```

---

## Implementation Details

### Key Data Structures

**IntentSpec:**
```rust
pub struct IntentSpec {
    pub task_type: TaskType,           // RCA or DataValidation
    pub systems: Vec<String>,          // ["system_a", "system_b"]
    pub target_metrics: Vec<String>,   // ["recovery", "tos"]
    pub entities: Vec<String>,         // ["loan", "payment"]
    pub grain: Vec<String>,            // ["loan_id"] or ["uuid", "paid_date"]
    pub constraints: Vec<Constraint>, // Filters, date ranges, etc.
}
```

**GroundedTask:**
```rust
pub struct GroundedTask {
    pub candidate_tables: Vec<TableCandidate>,  // With confidence scores
    pub selected_rules: Vec<Rule>,              // Best matching rules
    pub resolved_columns: HashMap<String, String>, // Column mappings
    pub grain: Vec<String>,                      // Resolved grain
    pub filters: Vec<Filter>,                    // Resolved filters
    pub confidence: f64,                         // Overall confidence
}
```

**MetricDefinition:**
```rust
pub struct MetricDefinition {
    pub base_tables: Vec<String>,      // Source tables
    pub joins: Vec<Join>,              // Join relationships
    pub filters: Vec<Filter>,          // Filter conditions
    pub aggregation: Aggregation,      // Sum, Count, Avg, etc.
    pub group_by: Vec<String>,        // Grouping columns
    pub formula: String,               // Calculation formula
}
```

**RowDiffResult:**
```rust
pub struct RowDiffResult {
    pub missing_left: DataFrame,      // Rows only in System A
    pub missing_right: DataFrame,      // Rows only in System B
    pub value_mismatch: DataFrame,     // Rows with different values
    pub matches: DataFrame,            // Matching rows
    pub summary: DiffSummary,          // Statistics
}
```

**TraversalState:**
```rust
pub struct TraversalState {
    pub nodes: HashMap<String, TraversalNode>, // All nodes in graph
    pub visited_path: Vec<String>,             // Nodes visited
    pub findings: Vec<Finding>,                // Discoveries with evidence
    pub current_hypothesis: Option<Hypothesis>, // Current understanding
    pub root_cause_found: bool,                 // Termination flag
    pub confidence: f64,                        // Overall confidence
}
```

**TraversalNode:**
```rust
pub struct TraversalNode {
    pub id: String,                              // Node identifier
    pub node_type: NodeType,                     // Table, Rule, Join, Filter, Metric
    pub visited: bool,                           // Visit status
    pub visit_count: usize,                      // Number of visits
    pub last_probe_result: Option<SqlProbeResult>, // Last probe result
    pub score: f64,                              // Relevance score
    pub reasons: Vec<String>,                   // Why this node is relevant
    pub metadata: NodeMetadata,                 // Rich metadata
}
```

### Error Handling

**Error Types:**
```rust
pub enum RcaError {
    IntentCompilationError(String),
    TaskGroundingError(String),
    ExecutionError(String),
    ValidationError(String),
    LlmError(String),
    DataError(String),
    // ... more error types
}
```

**Error Recovery:**
- Graceful degradation when LLM unavailable
- Fallback to template formatting
- Validation errors provide specific guidance
- Resource limits prevent system overload

### Validation System

**Input Validation:**
- IntentSpec validation (required fields, types)
- GroundedTask validation (table existence, column existence)
- Query validation (SQL syntax, safety)

**Output Validation:**
- Formatter input contract validation
- Formatter output contract validation
- Reconciliation proof validation

**Data Validation:**
- Schema validation
- Type checking
- Constraint validation
- Referential integrity

---

## User Interface

### Frontend Architecture

**Technology Stack:**
- React with TypeScript
- Vite for building
- Axios for HTTP requests

**Key Components:**
- Query input form
- Results display
- Visualization components
- Error handling UI

### User Flow

```
1. User opens UI (http://localhost:8080)
   ↓
2. User enters natural language query
   "Why is recovery mismatching between system A and B?"
   ↓
3. UI sends POST request to /api/rca/analyze
   ↓
4. Server processes query (5-10 seconds)
   ↓
5. UI receives results and displays:
   - Summary of differences
   - Root cause explanations
   - Detailed breakdowns
   - Verification proof
   ↓
6. User can drill down into specific differences
```

### API Endpoints

**Health Check:**
```
GET /api/health
Response: { "status": "ok" }
```

**RCA Analysis:**
```
POST /api/rca/analyze
Request: {
    "query": "Why is recovery mismatching...",
    "metadata_dir": "metadata/multi_grain_test"
}
Response: {
    "result": RcaCursorResult,
    "execution_time_ms": 5000
}
```

**Graph Traversal:**
```
POST /api/graph/traverse
Request: {
    "query": "Why is recovery mismatching...",
    "metadata_dir": "metadata/multi_grain_test"
}
Response: {
    "result": TraversalState,
    "execution_time_ms": 3000
}
```

---

## API & Integration

### HTTP Server (`bin/server.rs`)

**Features:**
- Simple tokio-based HTTP server
- RESTful API endpoints
- CORS support for UI
- Error handling and logging

**Routes:**
- `GET /api/health`: Health check
- `POST /api/rca/analyze`: Fixed pipeline RCA
- `POST /api/graph/traverse`: Graph traversal RCA
- `GET /api/metadata/tables`: List tables
- `GET /api/metadata/rules`: List rules

### CLI Interface

**Commands:**
```bash
# Run fixed pipeline RCA
./target/release/rca-engine rca "Why is recovery mismatching..." \
    --metadata-dir metadata/multi_grain_test

# Run graph traversal RCA
./target/release/rca-engine agentic "Why is recovery mismatching..." \
    --metadata-dir metadata/multi_grain_test

# Build rule
./target/release/rca-engine build-rule --input rule.json
```

### Integration Points

**Inputs:**
- Metadata files (JSON)
- Data files (CSV/Parquet)
- Natural language queries
- Configuration (environment variables)

**Outputs:**
- RCA results (JSON)
- Human-readable narratives
- Evidence records
- Verification proofs

---

## Metadata & Configuration

### Metadata Structure

**Tables (`tables.json`):**
```json
{
  "tables": [
    {
      "name": "payments_a",
      "columns": [
        { "name": "uuid", "type": "string", "description": "Payment UUID" },
        { "name": "paid_amount", "type": "float", "description": "Amount paid" }
      ],
      "grain": ["uuid"],
      "labels": ["payment", "system_a"],
      "system": "system_a"
    }
  ]
}
```

**Rules (`rules.json`):**
```json
{
  "rules": [
    {
      "id": "system_a_recovery",
      "system": "system_a",
      "metric": "recovery",
      "target_entity": "payment",
      "target_grain": ["uuid"],
      "computation": {
        "description": "Recovery = paid_amount",
        "source_entities": ["payment"],
        "formula": "paid_amount",
        "aggregation_grain": ["uuid"]
      }
    }
  ]
}
```

**Lineage (`lineage.json`):**
```json
{
  "lineage": [
    {
      "from": "payments_a",
      "to": "orders",
      "join_type": "left",
      "join_keys": [
        { "left": "order_id", "right": "order_id" }
      ]
    }
  ]
}
```

### Configuration

**Environment Variables:**
- `OPENAI_API_KEY`: LLM API key
- `RCA_METADATA_DIR`: Default metadata directory
- `RCA_DATA_DIR`: Default data directory
- `RCA_MAX_ROWS`: Maximum rows to process
- `RCA_TIMEOUT_MS`: Execution timeout

---

## Testing & Validation

### Test Structure

**Unit Tests:**
- Component-level tests
- Data structure validation
- Algorithm correctness

**Integration Tests:**
- End-to-end RCA flows
- API endpoint tests
- Metadata loading tests

**Real-World Tests:**
- Actual data reconciliation
- Complex query scenarios
- Performance benchmarks

### Test Files

- `tests/`: Test directory
- `REAL_WORLD_WORKFLOW_TEST_RESULTS.md`: Test results documentation

### Validation Mechanisms

**Input Validation:**
- IntentSpec validation
- Task validation
- Query validation

**Output Validation:**
- Formatter contracts
- Reconciliation proof
- Consistency checks

**Data Validation:**
- Schema validation
- Type checking
- Constraint validation

---

## Deployment & Operations

### Building the Project

**Backend:**
```bash
# Build release binary
cargo build --release

# Build server
cargo build --bin server --release
```

**Frontend:**
```bash
cd ui
npm install
npm run build
```

### Running the System

**Start Server:**
```bash
./start_server.sh
# Or
./target/release/server
```

**Run CLI:**
```bash
./target/release/rca-engine rca "query" --metadata-dir metadata/
```

### Scripts

**`start_server.sh`**: Starts HTTP server
**`run_agentic_rca.sh`**: Runs graph traversal RCA
**`load_tables.sh`**: Loads data files
**`load_scf.sh`**: Loads SCF data

---

## Performance & Scalability

### Performance Characteristics

**Scalability:**
- Chunked processing for large datasets
- Hash-based diff (O(n) instead of O(n²))
- Pushdown predicates reduce data transfer
- Parallel processing for multiple systems

**Optimization Techniques:**
- Sampling for quick analysis
- Caching for repeated queries
- Lazy evaluation with Polars
- Resource limits prevent overload

### Benchmarks

**Typical Performance:**
- Small datasets (< 10K rows): < 5 seconds
- Medium datasets (10K-100K rows): 5-30 seconds
- Large datasets (100K+ rows): 30+ seconds (with chunking)

**Factors Affecting Performance:**
- Dataset size
- Number of joins
- Complexity of rules
- LLM API latency
- System resources

---

## Security & Trust

### Trust Layer Components

**Evidence Storage:**
- Complete execution records
- Input/output tracking
- Intermediate state storage

**Deterministic Replay:**
- Reproduce exact analysis
- Verify outputs match
- Enable debugging

**Verification:**
- Mathematical proof of correctness
- Aggregate reconciliation
- Consistency checks

### Safety Mechanisms

**Resource Limits:**
- Memory limits
- Execution time limits
- Query complexity limits

**Error Recovery:**
- Graceful degradation
- Fallback mechanisms
- Clear error messages

**Validation:**
- Input validation
- Output validation
- Data validation

---

## Future Enhancements

### Planned Features

1. **Real-time RCA**: Stream processing for live data
2. **Anomaly Detection**: Automatic discrepancy detection
3. **Predictive RCA**: ML models for root cause prediction
4. **Visualization**: Interactive dashboards
5. **Collaboration**: Multi-user workflows
6. **DuckDB Integration**: Full SQL support
7. **Parallel Probes**: Execute multiple probes simultaneously
8. **Probe Caching**: Cache probe results
9. **Learning**: Learn from past traversals
10. **Hypergraph Stats Updates**: Real-time stats from queries

### Extensibility

**Custom Rules:**
- Plugin system for custom rule types
- Natural language rule definitions

**Data Sources:**
- Connectors for various data sources
- Database integration (PostgreSQL, MySQL, etc.)

**Export Formats:**
- Multiple output formats (JSON, CSV, PDF)
- Custom report templates

**Integration:**
- APIs for external tool integration
- Webhook support
- Event-driven architecture

---

## Conclusion

The RCA Engine is a **comprehensive, production-ready system** that transforms manual root cause analysis into an automated, intelligent investigation. It provides:

- ✅ **Natural Language Interface**: Simple queries, complex analysis
- ✅ **Row-Level Precision**: Exact rows causing discrepancies
- ✅ **Complete Lineage**: Full audit trail of transformations
- ✅ **AI-Powered Reasoning**: Intelligent exploration and explanations
- ✅ **Mathematical Verification**: Proof of correctness
- ✅ **Performance Optimized**: Handles large-scale datasets
- ✅ **Dual-Mode Architecture**: Fixed pipeline and dynamic traversal
- ✅ **Trust Layer**: Evidence, replay, verification

The system is designed for **production use** with comprehensive error handling, performance optimizations, and trust mechanisms that ensure reliability and reproducibility for critical business use cases.

---

## Appendix

### Key Files Reference

**Core Implementation:**
- `src/lib.rs`: Library root
- `src/rca.rs`: Main RCA engine
- `src/intent_compiler.rs`: Intent compilation
- `src/task_grounder.rs`: Task grounding
- `src/core/agent/rca_cursor/cursor.rs`: Fixed pipeline
- `src/graph_traversal.rs`: Dynamic traversal
- `src/sql_engine.rs`: SQL execution
- `src/llm.rs`: LLM client

**Metadata:**
- `metadata/tables.json`: Table definitions
- `metadata/rules.json`: Business rules
- `metadata/lineage.json`: Data lineage
- `metadata/entities.json`: Entity definitions

**Documentation:**
- `SUMMARY.md`: System summary
- `user_knowledge.md`: User guide
- `PROJECT_DOCUMENTATION.md`: This document

### Glossary

**RCA**: Root Cause Analysis
**TOS**: Total Outstanding
**FTD**: First Time Default
**CM**: Current Month
**LMTD**: Last Month to Date
**LM**: Last Month
**Grain**: Level of aggregation (loan_id, uuid, etc.)
**Lineage**: Data transformation path
**Attribution**: Explanation of why differences occur
**Canonical**: Standardized format across systems

---

*Document Version: 1.0*  
*Last Updated: 2026-01-08*

