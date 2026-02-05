# Complete LLM-Based SQL Generation Pipeline

## Overview

This document explains the complete end-to-end pipeline for generating SQL queries from natural language using LLM (Large Language Model). The system transforms a user's natural language query into executable Trino SQL through multiple stages.

---

## Architecture Flow

```
User Query (Natural Language)
    ↓
[1] Flask API Endpoint (/api/reasoning/query)
    ↓
[2] Query Router (LLM vs Rule-Based)
    ↓
[3] Metadata Loading (MetadataProvider)
    ↓
[4] LLM Query Generator (LLMQueryGenerator)
    ├─ Context Building
    ├─ LLM API Call
    └─ Intent Generation (JSON)
    ↓
[5] Intent Validation (IntentValidator)
    ├─ Table Resolution
    ├─ Join Validation
    └─ Intent Fixing
    ↓
[6] SQL Builder (SQLBuilder)
    ├─ SELECT Clause
    ├─ FROM/JOIN Clauses
    ├─ WHERE Clause
    ├─ GROUP BY Clause
    └─ ORDER BY Clause
    ↓
[7] Final SQL Output
```

---

## Stage-by-Stage Breakdown

### Stage 1: API Endpoint (`app_production.py`)

**Location**: `backend/app_production.py::reasoning_query()`

**What Happens**:
1. Receives POST request to `/api/reasoning/query` with JSON body:
   ```json
   {
     "query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
   }
   ```

2. Checks if `OPENAI_API_KEY` is configured:
   ```python
   use_llm = bool(os.getenv('OPENAI_API_KEY', ''))
   ```

3. Calls `generate_sql_from_query(query_text, use_llm=use_llm)`

4. Returns JSON response with:
   - `sql`: Generated SQL query
   - `intent`: JSON intent structure
   - `method`: "llm_with_full_context" or "rule_based"
   - `steps`: Array of reasoning steps for UI

**Key Decision Point**: If `OPENAI_API_KEY` is missing, falls back to rule-based generation.

---

### Stage 2: Query Router (`llm_query_generator.py`)

**Location**: `backend/llm_query_generator.py::generate_sql_with_llm()`

**What Happens**:
1. Loads metadata using `MetadataProvider.load()`
2. If `use_llm=True`:
   - Creates `LLMQueryGenerator` instance
   - Calls `generator.generate_sql_intent(query, metadata)` → Returns intent JSON
   - Calls `generator.intent_to_sql(intent, metadata, query_text=query)` → Returns SQL
3. If `use_llm=False`:
   - Falls back to rule-based SQL generation (pattern matching)

**Key Files**:
- `backend/llm_query_generator.py` - Main LLM orchestration
- `backend/metadata_provider.py` - Metadata loading

---

### Stage 3: Metadata Loading (`metadata_provider.py`)

**Location**: `backend/metadata_provider.py::MetadataProvider.load()`

**What Happens**:
1. Loads JSON files from `metadata/` directory:
   - `tables.json` - Table schemas, columns, descriptions
   - `semantic_registry.json` - Pre-defined metrics and dimensions
   - `lineage.json` - Table relationships and join paths
   - `knowledge_base.json` - Business terms and definitions
   - `rules.json` - Business rules

2. Returns combined metadata dictionary:
   ```python
   {
     "tables": {...},
     "semantic_registry": {...},
     "lineage": {...},
     "knowledge_base": {...},
     "rules": [...]
   }
   ```

3. Uses `@lru_cache` for process-level caching (loaded once per process)

**Key Point**: Metadata is READ-ONLY. All mutations go through CKO client.

---

### Stage 4: LLM Query Generator (`llm_query_generator.py`)

**Location**: `backend/llm_query_generator.py::LLMQueryGenerator.generate_sql_intent()`

**This is the core LLM interaction stage. Here's what happens:**

#### 4.1 Context Building

**Method**: `build_comprehensive_context()`

Builds a massive context string containing:

1. **Tables Metadata**:
   ```
   Table: tpch.tiny.lineitem
     Columns: l_extendedprice, l_discount, l_orderkey, ...
     Description: Line item details
   ```

2. **Semantic Registry** (Metrics & Dimensions):
   ```
   Metric: discount
     SQL Expression: SUM(extendedprice * (1 - discount))
   ```

3. **Lineage/Relationships**:
   ```
   lineitem -> orders ON lineitem.l_orderkey = orders.o_orderkey
   orders -> customer ON orders.o_custkey = customer.c_custkey
   ```

4. **Knowledge Base Terms**:
   ```
   Term: discount
     Definition: Total discount amount
     Related Tables: lineitem
   ```

5. **Business Rules**:
   ```
   Rule: discount_calculation
     Formula: SUM(extendedprice * (1 - discount))
   ```

**Token Management**: Context is truncated if > 18,000 tokens to fit within model limits.

#### 4.2 LLM Prompt Construction

**System Prompt** (`system_prompt`):
- Instructions on how to analyze queries
- Rules for metric vs relational queries
- Format requirements for JSON output
- Examples of correct intent structures

**User Prompt** (`user_prompt`):
- The actual user query
- All the context built above
- Instructions to generate JSON intent

**Example User Prompt**:
```
Query: "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"

CONTEXT:
[TABLES METADATA]
[RELATIONSHIPS]
[METRICS]
[DIMENSIONS]
[RULES]

Generate a JSON object with this structure:
{
  "intent": {
    "query_type": "metric",
    "base_table": "tpch.tiny.lineitem",
    "metric": {"name": "discount", "sql_expression": "SUM(extendedprice * (1 - discount))"},
    "columns": ["customer.c_custkey"],
    "joins": [...],
    "group_by": ["customer.c_custkey"]
  }
}
```

#### 4.3 LLM API Call

**Method**: `call_llm()`

1. Makes HTTP POST to OpenAI API:
   ```python
   POST https://api.openai.com/v1/chat/completions
   {
     "model": "gpt-4",
     "messages": [
       {"role": "system", "content": system_prompt},
       {"role": "user", "content": user_prompt}
     ],
     "temperature": 0.1,
     "max_tokens": 3000
   }
   ```

2. Receives JSON response from LLM

3. Parses JSON (with error handling for malformed responses):
   - Strips markdown code fences (```json)
   - Removes trailing commas
   - Extracts JSON from text if embedded

#### 4.4 Intent Extraction

**Method**: `generate_sql_intent()`

Extracts from LLM response:
```python
{
  "reasoning": {...},  # LLM's chain of thought
  "intent": {
    "query_type": "metric",
    "base_table": "tpch.tiny.lineitem",
    "metric": {
      "name": "discount",
      "sql_expression": "SUM(l_extendedprice * (1 - l_discount))"
    },
    "columns": ["customer.c_custkey", "customer.c_name"],
    "joins": [
      {
        "table": "tpch.tiny.orders",
        "type": "LEFT",
        "on": "lineitem.l_orderkey = orders.o_orderkey"
      },
      {
        "table": "tpch.tiny.customer",
        "type": "LEFT",
        "on": "orders.o_custkey = customer.c_custkey"
      }
    ],
    "group_by": ["customer.c_custkey", "customer.c_name", "region", "product_group"]
  }
}
```

**Current Issue**: LLM sometimes generates incorrect intent:
- Adds unnecessary columns to `columns` field
- Adds hardcoded computed dimensions (`region`, `product_group`) to `group_by`
- Uses full table names in JOIN ON clauses instead of aliases

---

### Stage 5: Intent Validation (`sql_builder.py`)

**Location**: `backend/sql_builder.py::IntentValidator.validate()`

**What Happens**:

1. **Table Resolution**:
   - Creates `TableRelationshipResolver` with metadata
   - Loads all tables into `resolver.tables` dictionary
   - Maps table names (handles schema prefixes like `tpch.tiny.lineitem`)

2. **Base Table Validation**:
   - Checks if `base_table` exists in metadata
   - Performs fuzzy matching (case-insensitive, with/without schema)

3. **Join Validation**:
   - Validates all tables in JOINs exist
   - Checks JOIN ON clauses reference valid tables
   - Validates column references in ON clauses

4. **Column Validation**:
   - Checks columns exist in referenced tables
   - Validates table references in expressions

5. **Intent Fixing** (`fix_intent()`):
   - Adds missing intermediate joins
   - Fixes JOIN ON clauses
   - Adds missing GROUP BY columns
   - Returns confidence level (SAFE, AMBIGUOUS, UNSAFE)

**Current Issue**: 
- `TableRelationshipResolver.tables` was empty during live requests (now fixed by disabling node-level isolation)
- JOIN ON clauses use full table names (`orders.o_custkey`) instead of aliases (`t3.o_custkey`)

---

### Stage 6: SQL Builder (`sql_builder.py`)

**Location**: `backend/sql_builder.py::SQLBuilder.build()`

**This stage converts the validated intent into actual SQL:**

#### 6.1 Alias Assignment

```python
base_table = "tpch.tiny.lineitem"
table_alias = "t1"
join_aliases = {
  "tpch.tiny.lineitem": "t1",
  "tpch.tiny.orders": "t2",
  "tpch.tiny.customer": "t3"
}
```

#### 6.2 SELECT Clause Building (`_build_select()`)

**For Metric Queries**:

1. Processes `columns` (dimensions for GROUP BY):
   ```python
   columns = ["customer.c_custkey", "customer.c_name"]
   # Becomes: "customer.c_custkey, customer.c_name"
   ```

2. Processes `computed_dimensions`:
   ```python
   computed_dimensions = [
     {"name": "region", "sql_expression": "'OS'"},
     {"name": "product_group", "sql_expression": "'Credit Card'"}
   ]
   # Becomes: "'OS' AS region, 'Credit Card' AS product_group"
   ```

3. Adds `metric`:
   ```python
   metric = {"name": "discount", "sql_expression": "SUM(l_extendedprice * (1 - l_discount))"}
   # Becomes: "SUM(l_extendedprice * (1 - l_discount)) AS discount"
   ```

**Current Issue**: 
- SELECT includes all columns from `columns` field, even if they shouldn't be there
- Computed dimensions (`region`, `product_group`) are added even when not needed

#### 6.3 FROM Clause

```python
from_clause = f"FROM {base_table} {table_alias}"
# Result: "FROM tpch.tiny.lineitem t1"
```

#### 6.4 JOIN Clauses (`_build_joins()`)

**Processes each join from intent**:

```python
joins = [
  {
    "table": "tpch.tiny.orders",
    "type": "LEFT",
    "on": "lineitem.l_orderkey = orders.o_orderkey"
  }
]
```

**Current Issue**: 
- JOIN ON clauses use full table names (`lineitem.l_orderkey`, `orders.o_orderkey`)
- Should use aliases (`t1.l_orderkey`, `t2.o_orderkey`)
- The `_replace_table_names()` method should handle this but doesn't always work correctly

**Expected Output**:
```sql
LEFT JOIN tpch.tiny.orders t2 ON t1.l_orderkey = t2.o_orderkey
```

**Actual Output**:
```sql
LEFT JOIN tpch.tiny.orders t2 ON lineitem.l_orderkey = orders.o_orderkey
```

#### 6.5 GROUP BY Clause (`_build_group_by()`)

**Processes `group_by` from intent**:

```python
group_by = ["customer.c_custkey", "customer.c_name", "region", "product_group"]
```

**Current Issue**:
- Includes computed dimensions (`region`, `product_group`) that shouldn't be there
- LLM generates these because of knowledge base rules that add hardcoded filters

**Expected Output**:
```sql
GROUP BY customer.c_custkey, customer.c_name
```

**Actual Output**:
```sql
GROUP BY customer.c_custkey, customer.c_name, region, product_group
```

---

## Current Issues in Your SQL

### Issue 1: JOIN ON Clauses Use Full Table Names

**Problem**:
```sql
LEFT JOIN tpch.tiny.customer t2 ON orders.o_custkey = customer.c_custkey
```

**Should Be**:
```sql
LEFT JOIN tpch.tiny.customer t2 ON t3.o_custkey = t2.c_custkey
```

**Root Cause**: `_replace_table_names()` in `sql_builder.py` doesn't properly replace table names in JOIN ON clauses with aliases.

**Fix Location**: `backend/sql_builder.py::_build_joins()` - Need to ensure `_replace_table_names()` correctly maps:
- `orders` → `t3`
- `customer` → `t2`
- `lineitem` → `t1`

---

### Issue 2: GROUP BY Includes Unnecessary Computed Dimensions

**Problem**:
```sql
GROUP BY customer.c_custkey, customer.c_name, region, product_group
```

**Should Be**:
```sql
GROUP BY customer.c_custkey, customer.c_name
```

**Root Cause**: 
1. LLM is generating `region` and `product_group` in `group_by` because knowledge base rules add hardcoded filters
2. These computed dimensions are being included in GROUP BY even though they're constants

**Fix Location**: 
- `backend/llm_query_generator.py` - Prompt needs to explicitly say: "When user says 'at customer level', ONLY group by customer columns, NOT by computed dimensions"
- `backend/sql_builder.py::_build_group_by()` - Filter out computed dimensions that are constants

---

### Issue 3: SELECT Includes Too Many Columns

**Problem**:
```sql
SELECT customer.c_custkey, customer.c_name, customer.c_address, customer.c_phone, customer.c_acctbal, customer.c_mktsegment, SUM(...) AS discount
```

**Should Be**:
```sql
SELECT customer.c_custkey, SUM(...) AS discount
```

**Root Cause**: LLM is generating all customer columns in `columns` field, but for "at customer level" queries, we only need the customer key.

**Fix Location**: `backend/llm_query_generator.py` - Prompt needs to say: "When user says 'at X level', only include the key column(s) for X, not all columns"

---

## Data Flow Summary

```
User Query: "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
    ↓
[API] Receives query, checks OPENAI_API_KEY
    ↓
[Metadata] Loads tables.json, semantic_registry.json, lineage.json, knowledge_base.json
    ↓
[Context Builder] Combines all metadata into 15,000+ token context string
    ↓
[LLM] Sends context + query to OpenAI GPT-4, receives JSON intent
    ↓
[Intent] {
  "query_type": "metric",
  "base_table": "tpch.tiny.lineitem",
  "metric": {"name": "discount", "sql_expression": "SUM(l_extendedprice * (1 - l_discount))"},
  "columns": ["customer.c_custkey", "customer.c_name", ...],  ← Too many columns
  "joins": [
    {"table": "tpch.tiny.orders", "on": "lineitem.l_orderkey = orders.o_orderkey"},  ← Wrong format
    {"table": "tpch.tiny.customer", "on": "orders.o_custkey = customer.c_custkey"}  ← Wrong format
  ],
  "group_by": ["customer.c_custkey", "customer.c_name", "region", "product_group"]  ← Extra dims
}
    ↓
[Validator] Validates tables exist, fixes joins, adds intermediate tables
    ↓
[SQL Builder] Converts intent to SQL:
  - SELECT: customer.c_custkey, customer.c_name, ..., SUM(...) AS discount
  - FROM: tpch.tiny.lineitem t1
  - JOIN: LEFT JOIN tpch.tiny.orders t2 ON lineitem.l_orderkey = orders.o_orderkey  ← Wrong
  - JOIN: LEFT JOIN tpch.tiny.customer t3 ON orders.o_custkey = customer.c_custkey  ← Wrong
  - GROUP BY: customer.c_custkey, customer.c_name, region, product_group  ← Extra dims
    ↓
[Output] Final SQL (with issues)
```

---

## Key Components

### 1. `LLMQueryGenerator` (`backend/llm_query_generator.py`)
- Orchestrates LLM interaction
- Builds comprehensive context
- Calls OpenAI API
- Parses LLM JSON response
- Converts intent to SQL

### 2. `TableRelationshipResolver` (`backend/sql_builder.py`)
- Resolves table relationships
- Finds join paths between tables
- Maps table names to metadata

### 3. `IntentValidator` (`backend/sql_builder.py`)
- Validates intent structure
- Checks tables/columns exist
- Fixes common intent issues
- Returns confidence levels

### 4. `SQLBuilder` (`backend/sql_builder.py`)
- Converts intent to SQL
- Builds SELECT, FROM, JOIN, WHERE, GROUP BY, ORDER BY
- Handles table aliases
- Processes computed dimensions

### 5. `MetadataProvider` (`backend/metadata_provider.py`)
- Loads metadata from JSON files
- Provides cached access
- READ-ONLY (no mutations)

---

## Why Issues Occur

1. **LLM Prompt Limitations**: The prompt doesn't explicitly forbid certain behaviors (like adding extra columns or computed dimensions)

2. **Table Name Replacement**: The `_replace_table_names()` method doesn't consistently replace full table names with aliases in JOIN ON clauses

3. **Knowledge Base Rules**: Hardcoded rules add `region` and `product_group` filters, which the LLM incorrectly interprets as GROUP BY dimensions

4. **Intent Structure**: The intent structure allows flexibility, but the SQL builder doesn't filter out unnecessary fields

---

## Recommendations

1. **Improve LLM Prompt**: Add explicit examples showing what NOT to include
2. **Fix Table Name Replacement**: Ensure JOIN ON clauses always use aliases
3. **Filter GROUP BY**: Remove constant computed dimensions from GROUP BY
4. **Simplify Columns**: When user says "at X level", only include key columns
5. **Add Post-Processing**: Clean up SQL after generation to fix common issues

---

## Testing the Pipeline

To test each stage:

```bash
# 1. Test API endpoint
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{"query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"}'

# 2. Check logs for intent
docker logs rca-backend | grep "Intent passed to SQLBuilder"

# 3. Check generated SQL
# Response will contain "sql" field with the generated query
```

---

## Conclusion

The pipeline works end-to-end but has several issues:
1. JOIN ON clauses use full table names instead of aliases
2. GROUP BY includes unnecessary computed dimensions
3. SELECT includes too many columns

These are fixable through:
- Better LLM prompts
- Improved SQL builder logic
- Post-processing cleanup

The core architecture is sound - the LLM generates correct intent structure, and the SQL builder converts it to SQL. The issues are in the details of how table names are replaced and which fields are included.
