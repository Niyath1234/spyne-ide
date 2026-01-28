# End-to-End Pipeline: Complex Query Processing

## Overview

This document describes the complete end-to-end flow when a user submits a complex natural language query to Spyne IDE. The system processes queries through multiple planes (Ingress → Planning → Execution → Presentation) with sophisticated reasoning, metadata retrieval, and intelligent engine selection.

## ⚠️ Implementation Status

**Current State:** The Python backend pipeline works end-to-end, but some advanced features (Rust execution engines, NodeRegistry search) are not fully integrated. See `PIPELINE_REALITY_CHECK.md` for details.

**What Works:**
- ✅ Query → Intent → Schema → SQL → Execution → Results (Python backend)
- ✅ Node-level metadata isolation
- ✅ Knowledge retrieval (RAG, graph, rules)
- ✅ Schema selection based on metrics/intent

**What's Partially Integrated:**
- ⚠️ NodeRegistry search (exists in Rust, not called from Python)
- ⚠️ Execution engine selection (exists in Rust, Python uses traditional DB executor)

This document describes the **ideal/complete architecture**. The actual implementation may differ in some areas.

## Example Complex Query

**User Query:**
```
"Show me the top 10 customers by total order value in the last 30 days, 
excluding cancelled orders, grouped by customer segment, and include their 
average order frequency. Also show how this compares to the previous period."
```

This query involves:
- Multiple tables (customers, orders)
- Aggregations (sum, count, average)
- Time-based filtering (last 30 days, previous period comparison)
- Filtering (exclude cancelled orders)
- Grouping (by customer segment)
- Ranking (top 10)
- Comparative analysis (current vs previous period)

---

## Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    USER QUERY INPUT                             │
│  "Show me top 10 customers by total order value..."            │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGRESS PLANE                                │
│  • Request validation                                           │
│  • Authentication/authorization                                  │
│  • Rate limiting                                                │
│  • Request ID generation                                       │
│  • Correlation ID creation                                     │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PLANNING PLANE                                │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Stage 1: Intent Extraction                              │  │
│  │ • Extract entities (tables, columns, metrics)          │  │
│  │ • Identify query type (relational/metric/modification)   │  │
│  │ • Extract time references                                │  │
│  │ • Identify aggregations                                  │  │
│  │                                                          │  │
│  │ Output: {                                                │  │
│  │   query_type: "metric",                                 │  │
│  │   entities: {                                            │  │
│  │     tables: ["customers", "orders"],                     │  │
│  │     metrics: ["total_order_value", "order_frequency"],  │  │
│  │     dimensions: ["customer_segment"]                    │  │
│  │   },                                                      │  │
│  │   time_references: ["last_30_days", "previous_period"],  │  │
│  │   aggregations: ["sum", "count", "average"]              │  │
│  │ }                                                         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Stage 2: Node & Table Discovery                          │  │
│  │                                                          │  │
│  │ Step 2.1: Extract Query Terms                           │  │
│  │ • Extract keywords from query:                          │  │
│  │   - "customers" → table name                            │  │
│  │   - "orders" → table name                                │  │
│  │   - "total order value" → metric                         │  │
│  │   - "customer segment" → dimension                       │  │
│  │                                                          │  │
│  │ Step 2.2: Node Registry Search                          │  │
│  │ • Search Knowledge Register (text search):               │  │
│  │   1. Fast path: Search index lookup (O(1))              │  │
│  │      - Check search_index["customers"] → page_ids       │  │
│  │   2. Optimized search: Full-text search engine          │  │
│  │      - Search "customers" in full_text + keywords        │  │
│  │      - Returns matching page IDs                         │  │
│  │   3. Fallback: Linear scan if index empty                │  │
│  │                                                          │  │
│  │ • Map Page IDs → Nodes:                                 │  │
│  │   - Page ID = Node ref_id (same identifier)             │  │
│  │   - Get Node by ref_id → Node{name: "customers", ...}  │  │
│  │   - Get Knowledge Page → human-readable descriptions    │  │
│  │   - Get Metadata Page → technical schema info           │  │
│  │                                                          │  │
│  │ Example:                                                 │  │
│  │   Search "customers" → ["ref_abc123"]                   │  │
│  │   ref_abc123 → Node{name: "khatabook_customers", ...}  │  │
│  │   ref_abc123 → KnowledgePage{full_text: "Customer...",  │  │
│  │                              keywords: ["customer", ...]}│  │
│  │   ref_abc123 → MetadataPage{schema: {...}, ...}        │  │
│  │                                                          │  │
│  │ Step 2.3: Schema Selection                              │  │
│  │ • SchemaSelector.select(intent, context):                │  │
│  │   1. If metric specified:                               │  │
│  │      - Find metric in semantic_registry                  │  │
│  │      - Get metric.base_table → "orders"                  │  │
│  │      - Find table metadata for "orders"                 │  │
│  │      - Check product_specific tables (if applicable)     │  │
│  │      - Get required joins from metric definition        │  │
│  │                                                          │  │
│  │   2. If no metric, use keyword matching:                 │  │
│  │      - Match query keywords to table names              │  │
│  │      - Match query keywords to table descriptions       │  │
│  │      - Match query keywords to table labels              │  │
│  │      - Match dimensions to table columns                │  │
│  │                                                          │  │
│  │   3. Infer product type (if needed):                     │  │
│  │      - Check context.product                             │  │
│  │      - Infer from keywords: "khatabook" → "khatabook"   │  │
│  │      - Select product-specific tables                    │  │
│  │                                                          │  │
│  │ Output: Selected tables:                                 │  │
│  │   ["khatabook_customers", "khatabook_orders"]           │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Stage 3: Knowledge Retrieval                             │  │
│  │                                                          │  │
│  │ Step 3.1: Node-Level Metadata Access                    │  │
│  │ • NodeLevelMetadataAccessor.get_tables_for_query():     │  │
│  │   - Extract table names from query                      │  │
│  │   - Load only mentioned tables (lazy loading)           │  │
│  │   - Build isolated context (only relevant nodes)         │  │
│  │                                                          │  │
│  │ • Get relevant metadata:                                │  │
│  │   - get_tables_for_query() → only "customers", "orders" │  │
│  │   - get_joins_for_tables() → joins between these tables │  │
│  │   - get_metrics_for_query() → metrics mentioning these  │  │
│  │   - get_dimensions_for_query() → dimensions for tables  │  │
│  │   - get_rules_for_tables() → business rules for tables  │  │
│  │   - get_knowledge_base_terms_for_query() → KB terms     │  │
│  │                                                          │  │
│  │ Step 3.2: Parallel Knowledge Retrieval                 │  │
│  │                                                          │  │
│  │ • RAG Search (KnowledgeBase Vector Store):              │  │
│  │   1. KnowledgeBaseClient.rag_retrieve(query)            │  │
│  │   2. Vector similarity search in KnowledgeBase           │  │
│  │   3. Returns top_k similar concepts                     │  │
│  │   4. Concepts include:                                  │  │
│  │      - Business term definitions                        │  │
│  │      - Domain-specific knowledge                        │  │
│  │      - Related concepts                                 │  │
│  │                                                          │  │
│  │ • Graph Search (Hypergraph Traversal):                  │  │
│  │   1. Build knowledge graph from metadata                │  │
│  │   2. Traverse from identified nodes                     │  │
│  │   3. Find related entities (metrics, dimensions)       │  │
│  │   4. Discover join paths                               │  │
│  │   5. Find relationship chains                          │  │
│  │                                                          │  │
│  │ • Rule Search (Metadata-Based):                         │  │
│  │   1. Search semantic_registry for metrics               │  │
│  │   2. Find metric definitions                            │  │
│  │   3. Get dimension hierarchies                          │  │
│  │   4. Retrieve business rules from rules.json            │  │
│  │   5. Extract rules mentioning selected tables          │  │
│  │                                                          │  │
│  │ Output: RetrievedKnowledge[]                             │  │
│  │   - Relevant metrics definitions                         │  │
│  │   - Join relationships                                   │  │
│  │   - Business rules (e.g., "cancelled orders have        │  │
│  │     status='CANCELLED'")                                │  │
│  │   - Knowledge base concepts                             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Stage 3: Context Assembly                                │  │
│  │ • Load metadata (tables, schemas, relationships)         │  │
│  │ • Enhance with retrieved knowledge                       │  │
│  │ • Build comprehensive context bundle                     │  │
│  │ • Compress context (reduce token usage)                 │  │
│  │                                                          │  │
│  │ Context Includes:                                        │  │
│  │ • Table schemas with column types                        │  │
│  │ • Foreign key relationships                              │  │
│  │ • Metric definitions and formulas                        │  │
│  │ • Dimension hierarchies                                  │  │
│  │ • Business rules                                          │  │
│  │ • Similar query examples                                 │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Stage 4: SQL Intent Generation (LLM)                    │  │
│  │                                                          │  │
│  │ LLM Prompt:                                              │  │
│  │ "Given this query: [user_query]                         │  │
│  │  And this context: [comprehensive_context]               │  │
│  │  Generate SQL intent specification..."                   │  │
│  │                                                          │  │
│  │ LLM Reasoning (Chain of Thought):                        │  │
│  │ 1. "Query requires joining customers and orders"         │  │
│  │ 2. "Need to filter orders.status != 'CANCELLED'"        │  │
│  │ 3. "Calculate total_order_value = SUM(orders.amount)"   │  │
│  │ 4. "Group by customer_segment"                           │  │
│  │ 5. "Order by total_order_value DESC, LIMIT 10"          │  │
│  │ 6. "Calculate average order frequency"                   │  │
│  │ 7. "Compare with previous period"                       │  │
│  │                                                          │  │
│  │ Output: IntentSpec {                                     │  │
│  │   tables: ["customers", "orders"],                       │  │
│  │   joins: [{                                              │  │
│  │     type: "INNER",                                       │  │
│  │     left: "customers",                                   │  │
│  │     right: "orders",                                     │  │
│  │     condition: "customers.id = orders.customer_id"      │  │
│  │   }],                                                     │  │
│  │   filters: [{                                            │  │
│  │     column: "orders.status",                             │  │
│  │     operator: "!=",                                      │  │
│  │     value: "CANCELLED"                                   │  │
│  │   }, {                                                    │  │
│  │     column: "orders.created_at",                         │  │
│  │     operator: ">=",                                      │  │
│  │     value: "CURRENT_DATE - INTERVAL '30 days'"           │  │
│  │   }],                                                     │  │
│  │   aggregations: [{                                       │  │
│  │     metric: "total_order_value",                         │  │
│  │     function: "SUM",                                     │  │
│  │     column: "orders.amount"                              │  │
│  │   }, {                                                    │  │
│  │     metric: "order_frequency",                           │  │
│  │     function: "AVG",                                     │  │
│  │     column: "COUNT(orders.id)"                           │  │
│  │   }],                                                     │  │
│  │   group_by: ["customers.segment"],                       │  │
│  │   order_by: [{                                          │  │
│  │     column: "total_order_value",                          │  │
│  │     direction: "DESC"                                    │  │
│  │   }],                                                     │  │
│  │   limit: 10,                                             │  │
│  │   time_scope: {                                          │  │
│  │     current: "last_30_days",                              │  │
│  │     compare: "previous_30_days"                          │  │
│  │   }                                                       │  │
│  │ }                                                         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Stage 5: SQL Generation                                   │  │
│  │ • Convert intent to SQL                                  │  │
│  │ • Apply query optimizations                              │  │
│  │ • Validate SQL syntax                                    │  │
│  │ • Generate explain plan                                  │  │
│  │                                                          │  │
│  │ Generated SQL:                                            │  │
│  │ WITH current_period AS (                                 │  │
│  │   SELECT                                                  │  │
│  │     c.id AS customer_id,                                │  │
│  │     c.segment AS customer_segment,                       │  │
│  │     SUM(o.amount) AS total_order_value,                  │  │
│  │     AVG(order_count) AS avg_order_frequency              │  │
│  │   FROM customers c                                        │  │
│  │   INNER JOIN orders o ON c.id = o.customer_id           │  │
│  │   WHERE o.status != 'CANCELLED'                         │  │
│  │     AND o.created_at >= CURRENT_DATE - INTERVAL '30 days'│  │
│  │   GROUP BY c.id, c.segment                               │  │
│  │ ),                                                         │  │
│  │ previous_period AS (                                      │  │
│  │   SELECT ... [similar for previous period]              │  │
│  │ )                                                          │  │
│  │ SELECT                                                    │  │
│  │   cp.customer_segment,                                   │  │
│  │   cp.total_order_value,                                  │  │
│  │   cp.avg_order_frequency,                                │  │
│  │   pp.total_order_value AS prev_total_order_value,        │  │
│  │   (cp.total_order_value - pp.total_order_value) AS change│  │
│  │ FROM current_period cp                                    │  │
│  │ LEFT JOIN previous_period pp                              │  │
│  │   ON cp.customer_id = pp.customer_id                     │  │
│  │ ORDER BY cp.total_order_value DESC                        │  │
│  │ LIMIT 10                                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Stage 6: Query Profile & Engine Selection                 │  │
│  │                                                          │  │
│  │ Query Profile Extraction:                                │  │
│  │ • Parse SQL AST                                          │  │
│  │ • Extract characteristics:                               │  │
│  │   - uses_ctes: true                                      │  │
│  │   - uses_window_functions: false                         │  │
│  │   - join_count: 2                                        │  │
│  │   - estimated_scan_gb: 15                              │  │
│  │   - requires_federation: false                          │  │
│  │   - complexity_score: 65                                │  │
│  │                                                          │  │
│  │ Agent Engine Selection (Chain of Thought):               │  │
│  │ 1. "Analyzing query characteristics..."                  │  │
│  │ 2. "Query uses CTEs (Common Table Expressions)"          │  │
│  │ 3. "Estimated data scan: 15GB"                          │  │
│  │ 4. "Small scan detected (<5GB) - comparing engine speeds"│  │
│  │ 5. "Evaluated 3 engines:"                               │  │
│  │    - "polars: 950ms estimated (CTE penalty: +500ms)"     │  │
│  │    - "duckdb: 1750ms estimated"                          │  │
│  │    - "trino: 4500ms estimated (network overhead)"         │  │
│  │ 6. "Selecting duckdb (fastest: 1750ms)"                  │  │
│  │    - "DuckDB handles CTEs efficiently"                   │  │
│  │    - "Good balance of capabilities and speed"           │  │
│  │                                                          │  │
│  │ Engine Selection:                                        │  │
│  │ {                                                         │  │
│  │   engine_name: "duckdb",                                 │  │
│  │   reasoning: [                                           │  │
│  │     "Query uses CTEs",                                   │  │
│  │     "Estimated scan: 15GB",                              │  │
│  │     "DuckDB handles CTEs efficiently",                   │  │
│  │     "Selecting DuckDB (fastest: 1750ms)"                 │  │
│  │   ],                                                      │  │
│  │   fallback_available: false                               │  │
│  │ }                                                         │  │
│  └──────────────────────────────────────────────────────────┘  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    EXECUTION PLANE                                │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Query Validation & Security                             │  │
│  │ • Query firewall (check for dangerous operations)        │  │
│  │ • SQL injection detection                                │  │
│  │ • Access control validation                              │  │
│  │ • Resource limits (timeout, row limits)                 │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Execution Router                                          │  │
│  │ • Validate engine selection                               │  │
│  │ • Check engine capabilities                               │  │
│  │ • Execute with selected engine                           │  │
│  │                                                          │  │
│  │ Execution Flow:                                          │  │
│  │ 1. Router receives SQL + EngineSelection                  │  │
│  │ 2. Validates DuckDB can handle query (CTEs ✓)            │  │
│  │ 3. Creates ExecutionContext:                            │  │
│  │    {                                                      │  │
│  │      user: { user_id: "...", roles: [...] },            │  │
│  │      timeout_ms: 30000,                                  │  │
│  │      row_limit: None,                                    │  │
│  │      preview: false                                      │  │
│  │    }                                                      │  │
│  │ 4. Calls DuckDBEngine.execute()                          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ DuckDB Engine Execution                                   │  │
│  │ • Connect to DuckDB instance                             │  │
│  │ • Execute SQL query                                      │  │
│  │ • Stream results                                          │  │
│  │ • Measure execution time                                 │  │
│  │                                                          │  │
│  │ Execution Metrics:                                       │  │
│  │ • Execution time: 1.8s                                   │  │
│  │ • Rows scanned: 2.5M                                     │  │
│  │ • Rows returned: 10                                      │  │
│  │ • Memory used: 450MB                                     │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Result Processing                                         │  │
│  │ • Convert to DataFrame (Polars)                         │  │
│  │ • Apply formatting                                       │  │
│  │ • Add metadata (execution time, engine used)             │  │
│  │ • Include agent reasoning                                │  │
│  │                                                          │  │
│  │ QueryResult:                                             │  │
│  │ {                                                         │  │
│  │   success: true,                                         │  │
│  │   data: DataFrame([                                      │  │
│  │     {                                                    │  │
│  │       customer_segment: "Enterprise",                   │  │
│  │       total_order_value: 1250000.00,                     │  │
│  │       avg_order_frequency: 12.5,                        │  │
│  │       prev_total_order_value: 1180000.00,               │  │
│  │       change: 70000.00                                   │  │
│  │     },                                                    │  │
│  │     ... (9 more rows)                                    │  │
│  │   ]),                                                     │  │
│  │   execution_time_ms: 1800,                               │  │
│  │   engine_metadata: {                                     │  │
│  │     agent_reasoning: [                                   │  │
│  │       "Query uses CTEs",                                 │  │
│  │       "Estimated scan: 15GB",                            │  │
│  │       "Selecting DuckDB (fastest: 1750ms)"               │  │
│  │     ],                                                    │  │
│  │     agent_selected_engine: "duckdb"                      │  │
│  │   },                                                      │  │
│  │   warnings: []                                            │  │
│  │ }                                                         │  │
│  └──────────────────────────────────────────────────────────┘  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PRESENTATION PLANE                              │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Result Formatting                                        │  │
│  │ • Format based on requested format (JSON/CSV/Table)      │  │
│  │ • Generate natural language explanation                  │  │
│  │ • Add visualizations (if applicable)                      │  │
│  │ • Include metadata and reasoning                          │  │
│  │                                                          │  │
│  │ Explanation Generation:                                 │  │
│  │ "This query shows the top 10 customer segments by       │  │
│  │  total order value in the last 30 days, excluding        │  │
│  │  cancelled orders. The results are grouped by customer   │  │
│  │  segment and include average order frequency. The query   │  │
│  │  also compares current period performance with the        │  │
│  │  previous 30-day period to show growth trends."         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                                         │
│                        ▼                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Response Assembly                                         │  │
│  │ • Combine data, explanation, metadata                     │  │
│  │ • Add correlation IDs for tracing                        │  │
│  │ • Include performance metrics                             │  │
│  │                                                          │  │
│  │ Final Response:                                          │  │
│  │ {                                                         │  │
│  │   success: true,                                         │  │
│  │   data: [...],                                           │  │
│  │   explanation: "...",                                    │  │
│  │   metadata: {                                            │  │
│  │     execution_time_ms: 1800,                            │  │
│  │     engine: "duckdb",                                    │  │
│  │     rows_returned: 10,                                   │  │
│  │     agent_reasoning: [...]                               │  │
│  │   },                                                      │  │
│  │   correlation_id: {                                      │  │
│  │     request_id: "...",                                   │  │
│  │     planning_id: "...",                                  │  │
│  │     execution_id: "..."                                  │  │
│  │   }                                                       │  │
│  │ }                                                         │  │
│  └──────────────────────────────────────────────────────────┘  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    USER RESPONSE                                 │
│  Formatted results with explanation and metadata                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Node Discovery & Knowledge Base Selection

### How the System Identifies Which Tables/Nodes to Query

The system uses a multi-step process to identify relevant tables and nodes:

#### Step 1: Query Term Extraction

**Process:**
- Extract keywords from user query
- Identify potential table names, metrics, dimensions
- Example: Query "customers" → potential table name

**Implementation:**
- `NodeLevelMetadataAccessor._extract_table_names_from_query()`
- Keyword matching against known table names
- Case-insensitive matching

#### Step 2: Node Registry Search

**Architecture:**
The system uses a **Node Registry** (`src/node_registry.rs`) that contains:
- **Nodes**: Registered entities (tables, metrics, etc.) with unique ref_ids
- **Knowledge Register**: Human-readable information stored in pages (page_id = ref_id)
- **Metadata Register**: Technical metadata stored in pages (page_id = ref_id)

**Search Flow:**

1. **Knowledge Register Search** (`NodeRegistry.search_knowledge()`):
   ```
   Query: "customers"
   ↓
   Fast Path: Search index lookup (O(1))
   - Check search_index["customers"] → ["ref_abc123", "ref_def456"]
   ↓
   Optimized Search: Full-text search engine
   - Search "customers" in full_text + keywords
   - Returns matching page IDs
   ↓
   Fallback: Linear scan (if index empty)
   - Scan all pages for keyword matches
   ```

2. **Node Resolution** (`NodeRegistry.search_all()`):
   ```
   Page IDs: ["ref_abc123"]
   ↓
   For each page_id:
   - Get Node by ref_id → Node{name: "khatabook_customers", type: "table", ...}
   - Get Knowledge Page → {full_text: "Customer master table...", keywords: [...]}
   - Get Metadata Page → {schema: {columns: [...]}, ...}
   ```

3. **Result**:
   - Returns tuple: `(nodes, knowledge_pages, metadata_pages)`
   - All three are linked by the same ref_id

#### Step 3: Schema Selection

**Process** (`backend/planning/schema_selector.py`):

1. **Metric-Based Selection** (if metric specified):
   ```
   Intent: {metric: "total_order_value"}
   ↓
   Find metric in semantic_registry.json
   ↓
   Get metric.base_table → "orders"
   ↓
   Find table metadata for "orders"
   ↓
   Check product_specific tables (if applicable)
   ↓
   Get required_joins → ["customers"]
   ↓
   Result: ["orders", "customers"]
   ```

2. **Keyword-Based Selection** (if no metric):
   ```
   Query: "customers by segment"
   ↓
   Match keywords to:
   - Table names: "customers" → "khatabook_customers"
   - Table descriptions: "customer" in description
   - Table labels: "customer" in labels
   - Column names: "segment" → column in "customers" table
   ↓
   Result: ["khatabook_customers"]
   ```

3. **Product Inference**:
   ```
   Query: "khatabook customers"
   ↓
   Infer product: "khatabook" → product_type = "khatabook"
   ↓
   Select product-specific tables
   ↓
   Result: ["khatabook_customers", "khatabook_orders"]
   ```

#### Step 4: Node-Level Metadata Access

**Isolated Loading** (`backend/node_level_metadata_accessor.py`):

Only loads metadata for identified tables (not all tables):

```python
# Get only relevant tables
relevant_tables = get_tables_for_query(query, ["customers", "orders"])
# Returns: {"customers": {...}, "orders": {...}}

# Get only relevant joins
relevant_joins = get_joins_for_tables(["customers", "orders"])
# Returns: [{from: "customers", to: "orders", ...}]

# Get only relevant metrics
relevant_metrics = get_metrics_for_query(query, ["total_order_value"])
# Returns: [{name: "total_order_value", base_table: "orders", ...}]

# Get only relevant dimensions
relevant_dimensions = get_dimensions_for_query(query, ["segment"], ["customers"])
# Returns: [{name: "segment", base_table: "customers", ...}]

# Get only relevant rules
relevant_rules = get_rules_for_tables(["customers", "orders"])
# Returns: [{computation: {source_table: "orders", ...}, ...}]
```

**Benefits:**
- **Performance**: Only loads what's needed
- **Memory**: Reduces memory footprint
- **Speed**: Faster context assembly

### How the System Selects Knowledge Base

#### KnowledgeBase Architecture

**Single KnowledgeBase Instance:**
- **REST API Server**: Runs on port 8080 (`KnowledgeBase/src/main.rs`)
- **Vector Store**: Stores business concepts with embeddings
- **Client**: `KnowledgeBaseClient` connects to API

**KnowledgeBase Content:**
- Loaded from `metadata/knowledge_base.json`
- Contains business concepts, terms, definitions
- Vector store indexes all concepts for semantic search

#### KnowledgeBase Search Flow

**RAG Retrieval** (`backend/knowledge_base_client.py`):

```
Query: "customers by total order value"
↓
KnowledgeBaseClient.rag_retrieve(query, top_k=10)
↓
POST /rag
{
  "query": "customers by total order value",
  "top_k": 10
}
↓
Vector Store: Semantic similarity search
- Compare query embedding with all concept embeddings
- Return top_k most similar concepts
↓
Results:
[
  {
    "concept": "customer_segment",
    "similarity": 0.85,
    "definition": "Customer grouping by value..."
  },
  {
    "concept": "total_order_value",
    "similarity": 0.92,
    "definition": "Sum of all order amounts..."
  },
  ...
]
```

**No Pre-Selection Needed:**
- Searches **entire KnowledgeBase** (all concepts)
- Uses vector embeddings for semantic similarity
- No need to know which concepts exist beforehand
- Returns most relevant concepts automatically

#### KnowledgeBase Integration Points

1. **RAG Search** (Primary):
   - `KnowledgeBaseClient.rag_retrieve()` → vector similarity
   - Returns top_k similar concepts
   - Used in `HybridKnowledgeRetriever._rag_search()`

2. **Concept Search** (Secondary):
   - `KnowledgeBaseClient.search_concepts()` → text search
   - Searches concept names and definitions
   - Used for exact matches

3. **Knowledge Base Terms** (Metadata):
   - `NodeLevelMetadataAccessor.get_knowledge_base_terms_for_query()`
   - Loads from `metadata/knowledge_base.json`
   - Matches query keywords to term names/aliases
   - Used for business term resolution

### Complete Flow Example

**Query:** "Show me top 10 customers by total order value"

**Step-by-Step:**

1. **Extract Terms**:
   - Tables: ["customers", "orders"]
   - Metrics: ["total_order_value"]
   - Dimensions: []

2. **Node Registry Search**:
   ```
   Search "customers" → ["ref_customers_123"]
   Search "orders" → ["ref_orders_456"]
   Search "total_order_value" → ["ref_metric_789"]
   ```

3. **Get Nodes**:
   ```
   ref_customers_123 → Node{name: "khatabook_customers", type: "table"}
   ref_orders_456 → Node{name: "khatabook_orders", type: "table"}
   ref_metric_789 → Node{name: "total_order_value", type: "metric"}
   ```

4. **Schema Selection**:
   ```
   Metric "total_order_value" → base_table: "orders"
   Required joins: ["customers"]
   Selected tables: ["khatabook_orders", "khatabook_customers"]
   ```

5. **Node-Level Metadata**:
   ```
   Load only: khatabook_orders, khatabook_customers
   Get joins between these tables
   Get metrics mentioning these tables
   Get dimensions for these tables
   Get rules for these tables
   ```

6. **KnowledgeBase RAG**:
   ```
   rag_retrieve("top 10 customers by total order value")
   → Returns: ["customer_segment", "total_order_value", "order_ranking", ...]
   ```

7. **Context Assembly**:
   ```
   Combine:
   - Selected tables metadata
   - Relevant joins
   - Relevant metrics/dimensions
   - Relevant rules
   - KnowledgeBase concepts
   → Comprehensive context bundle
   ```

---

## Detailed Component Interactions

### 1. Ingress Plane (`backend/planes/ingress.py`)

**Responsibilities:**
- Request validation and sanitization
- Authentication/authorization
- Rate limiting
- Request ID generation
- Correlation ID creation

**Key Operations:**
```python
# Validate request
validated_input = validate_request(request)
# Authenticate user
user_id = authenticate(request.token)
# Generate correlation ID
correlation_id = CorrelationID.create()
```

### 2. Planning Plane (`backend/planes/planning.py`)

**Multi-Stage Pipeline:**

#### Stage 1: Intent Extraction
- Uses LLM to extract structured intent
- Identifies entities, query type, time references
- Handles conversational context (modifications to previous queries)

#### Stage 2: Node & Table Discovery

**How the System Identifies Which Tables/Nodes to Query:**

1. **Query Term Extraction**:
   - Extract keywords from user query
   - Identify potential table names, metrics, dimensions
   - Example: "customers" → potential table name

2. **Node Registry Search** (`src/node_registry.rs`):
   - **Knowledge Register Search**:
     - Fast path: Search index lookup (O(1)) for exact keyword matches
     - Optimized search: Full-text search engine for fuzzy matches
     - Fallback: Linear scan if index empty
     - Returns: Matching page IDs (which are also node ref IDs)
   
   - **Node Resolution**:
     - Page ID = Node ref_id (same identifier)
     - Use ref_id to get:
       - **Node**: Entity information (name, type, metadata)
       - **Knowledge Page**: Human-readable descriptions, keywords
       - **Metadata Page**: Technical schema, column info
   
   - **Example Flow**:
     ```
     Query: "customers"
     ↓
     Search Knowledge Register → ["ref_abc123"]
     ↓
     ref_abc123 → Node{name: "khatabook_customers", type: "table"}
     ref_abc123 → KnowledgePage{full_text: "Customer master table...", keywords: ["customer", "khatabook"]}
     ref_abc123 → MetadataPage{schema: {columns: [...]}, ...}
     ```

3. **Schema Selection** (`backend/planning/schema_selector.py`):
   - **Metric-Based Selection**:
     - If metric specified in intent:
       - Find metric in `semantic_registry.json`
       - Get `metric.base_table` → identifies primary table
       - Check `product_specific` tables if product inferred
       - Get `requires_join` → identifies join tables
   
   - **Keyword-Based Selection**:
     - Match query keywords to:
       - Table names
       - Table descriptions
       - Table labels
       - Column names (for dimensions)
   
   - **Product Inference**:
     - Check context for product type
     - Infer from keywords: "khatabook" → "khatabook", "bank" → "bank"
     - Select product-specific tables

4. **Node-Level Metadata Access** (`backend/node_level_metadata_accessor.py`):
   - **Isolated Loading**: Only load metadata for identified tables
   - **Lazy Loading**: Tables loaded on-demand, not all at once
   - **Relevant Metadata Retrieval**:
     - `get_tables_for_query()` → only mentioned tables
     - `get_joins_for_tables()` → joins between selected tables
     - `get_metrics_for_query()` → metrics related to tables
     - `get_dimensions_for_query()` → dimensions for tables
     - `get_rules_for_tables()` → business rules for tables
     - `get_knowledge_base_terms_for_query()` → KB terms

**How the System Selects Knowledge Base:**

1. **KnowledgeBase Selection**:
   - **Single KnowledgeBase**: System uses one KnowledgeBase instance
   - **RAG Vector Store**: KnowledgeBase REST API server (port 8080)
   - **Client**: `KnowledgeBaseClient` connects to API

2. **KnowledgeBase Search Flow**:
   - **RAG Retrieval** (`backend/knowledge_base_client.py`):
     - `rag_retrieve(query, top_k)` → vector similarity search
     - Searches all concepts in KnowledgeBase
     - Returns top_k most similar concepts
     - Concepts include business terms, definitions, rules
   
   - **Concept Search**:
     - Searches across all registered concepts
     - Uses vector embeddings for semantic similarity
     - No need to pre-select - searches entire KB

3. **KnowledgeBase Content**:
   - Loaded from `metadata/knowledge_base.json`
   - Contains business concepts, terms, definitions
   - Vector store indexes all concepts for fast search

#### Stage 3: Knowledge Retrieval
- **Parallel execution** of three retrieval methods:
  - **RAG Search**: Vector similarity search in KnowledgeBase
  - **Graph Search**: Hypergraph traversal for relationships
  - **Rule Search**: Metadata-based rule discovery
- Combines results with relevance scoring

#### Stage 4: Context Assembly
- Loads metadata (tables, schemas, relationships)
- Enhances with retrieved knowledge
- Builds comprehensive context bundle
- Compresses context (30-50% token reduction)

#### Stage 5: SQL Intent Generation
- LLM generates structured intent specification
- Chain of thought reasoning captured
- Intent includes tables, joins, filters, aggregations, etc.

#### Stage 6: SQL Generation
- Converts intent to SQL
- Applies optimizations
- Validates syntax
- Generates explain plan

#### Stage 7: Query Profile & Engine Selection
- Extracts query profile from SQL
- Agent reasons about engine selection
- Selects optimal engine based on:
  - Query characteristics (CTEs, window functions, joins)
  - Scan size
  - Speed preferences
  - Engine capabilities

### 3. Execution Plane (`backend/planes/execution.py`)

**Security & Validation:**
- Query firewall (dangerous operations check)
- SQL injection detection
- Access control validation
- Resource limits (timeout, row limits)

**Engine Execution:**
- Router validates engine selection
- Executes with selected engine (DuckDB/Trino/Polars)
- Handles errors and fallbacks
- Measures performance metrics

**Result Processing:**
- Converts to DataFrame
- Adds metadata (execution time, engine used, agent reasoning)
- Includes warnings if any

### 4. Presentation Plane (`backend/planes/presentation.py`)

**Formatting:**
- Formats results (JSON/CSV/Table)
- Generates natural language explanation
- Adds visualizations (if applicable)

**Response Assembly:**
- Combines data, explanation, metadata
- Adds correlation IDs for tracing
- Includes performance metrics

---

## Performance Characteristics

### Timing Breakdown (Example)

| Stage | Duration | Notes |
|-------|----------|-------|
| Ingress | 5-10ms | Request validation, auth |
| Intent Extraction | 200-500ms | LLM call |
| Knowledge Retrieval | 100-300ms | Parallel RAG/graph/rule search |
| Context Assembly | 50-100ms | Metadata loading, compression |
| SQL Intent Generation | 500-1500ms | LLM call with context |
| SQL Generation | 50-100ms | Intent to SQL conversion |
| Engine Selection | 50-100ms | Profile analysis, agent reasoning |
| Query Validation | 10-20ms | Security checks |
| Execution | 1800ms | DuckDB query execution |
| Presentation | 50-100ms | Formatting, explanation |
| **Total** | **~3-4 seconds** | End-to-end latency |

### Optimization Features

1. **Metadata Caching**: Metadata loaded once per process
2. **Query Caching**: Repeated queries use cached results
3. **Parallel Retrieval**: Knowledge retrieval runs in parallel
4. **Context Compression**: 30-50% token reduction
5. **Intelligent Engine Selection**: Chooses fastest engine for query
6. **Connection Pooling**: Database connections reused

---

## Error Handling & Resilience

### Error Recovery Strategies

1. **Intent Extraction Failure**:
   - Falls back to rule-based extraction
   - Uses keyword matching

2. **Knowledge Retrieval Failure**:
   - Continues with empty knowledge
   - Uses metadata only

3. **SQL Generation Failure**:
   - Retries with simplified context
   - Falls back to template-based generation

4. **Engine Execution Failure**:
   - Automatic fallback to DuckDB (if available)
   - Error includes agent reasoning for debugging

5. **Timeout Handling**:
   - Query timeout enforced
   - Partial results returned if available

---

## Observability & Monitoring

### Logging

- **Structured Logging**: All stages log with correlation IDs
- **Request Tracing**: Full request lifecycle tracked
- **Performance Metrics**: Timing for each stage
- **Error Tracking**: Errors logged with context

### Metrics

- **Golden Signals**:
  - Latency (planning, execution, total)
  - Throughput (queries per second)
  - Error rate
  - Resource usage (rows scanned, memory)

- **Business Metrics**:
  - Query complexity distribution
  - Engine selection distribution
  - Cache hit rate
  - LLM token usage

---

## Example: Complete Request/Response

### Request
```json
{
  "query": "Show me the top 10 customers by total order value in the last 30 days, excluding cancelled orders, grouped by customer segment",
  "user_id": "user_123",
  "context": {
    "format": "json",
    "prefer_speed": true
  }
}
```

### Response
```json
{
  "success": true,
  "data": [
    {
      "customer_segment": "Enterprise",
      "total_order_value": 1250000.00,
      "avg_order_frequency": 12.5,
      "prev_total_order_value": 1180000.00,
      "change": 70000.00
    },
    ...
  ],
  "explanation": "This query shows the top 10 customer segments by total order value in the last 30 days, excluding cancelled orders. The results are grouped by customer segment and include average order frequency. The query also compares current period performance with the previous 30-day period.",
  "metadata": {
    "execution_time_ms": 1800,
    "engine": "duckdb",
    "rows_returned": 10,
    "rows_scanned": 2500000,
    "agent_reasoning": [
      "Query uses CTEs",
      "Estimated scan: 15GB",
      "DuckDB handles CTEs efficiently",
      "Selecting DuckDB (fastest: 1750ms)"
    ]
  },
  "correlation_id": {
    "request_id": "req_abc123",
    "planning_id": "plan_xyz789",
    "execution_id": "exec_def456"
  }
}
```

---

## Key Design Principles

1. **Separation of Concerns**: Each plane handles a specific responsibility
2. **Chain of Thought**: Agent reasoning captured at each decision point
3. **Intelligent Caching**: Reduces redundant computation
4. **Parallel Processing**: Knowledge retrieval runs in parallel
5. **Graceful Degradation**: System continues with reduced functionality on errors
6. **Observability**: Full tracing and metrics for debugging
7. **Security First**: Validation and security checks at multiple layers
8. **Performance Optimization**: Speed-aware engine selection, context compression

---

## Future Enhancements

1. **Learning System**: Learn from successful queries to improve future generations
2. **Query Optimization**: Apply more sophisticated SQL optimizations
3. **Predictive Caching**: Pre-cache likely queries
4. **Multi-Engine Execution**: Run on multiple engines and use fastest result
5. **Incremental Results**: Stream results as they become available
6. **Query Suggestions**: Suggest query improvements based on metadata

