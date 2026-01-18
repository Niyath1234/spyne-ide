# SQL Query Planner Metadata Requirements - Gap Analysis

## What a SQL Query Planner Needs

### Current State: What We Have ✅

#### 1. Table Schema ✅
```sql
SELECT name, entity_id, system, primary_key FROM tables;
```
- Table names
- System/source
- Entity mappings
- Primary keys (as JSON array)

#### 2. Column Metadata ✅
```sql
SELECT column_name, data_type, nullable FROM dataset_columns;
```
- Column names
- Data types (integer, float, string, date, datetime, boolean)
- Nullability

#### 3. Statistics ✅ (Basic)
```sql
SELECT table_name, row_count FROM dataset_versions;
```
- Row counts per table
- Unique value counts per column (NDV - Number of Distinct Values)
- Null counts per column

#### 4. Relationships ✅
```sql
SELECT from_table, to_table, keys, relationship FROM lineage_edges;
```
- Join relationships
- Foreign key mappings
- Lineage (upstream/downstream)

---

## What's Missing ❌ (Critical for Query Planner)

### 1. Indexes ❌
**What's Missing**:
- Which columns are indexed?
- Index types (B-tree, hash, bitmap)
- Composite indexes (multi-column)
- Index selectivity

**Why Planner Needs It**:
- Decide between index scan vs full table scan
- Choose best join algorithm
- Estimate query cost

**Example**:
```sql
-- Planner needs to know:
INDEX ON disbursements(cif)  -- Fast lookup by customer
INDEX ON disbursements(disbursement_date)  -- Fast date filtering
INDEX ON disbursements(branch_code, account_no)  -- Composite for joins
```

### 2. Data Distribution / Histograms ❌
**What's Missing**:
- Value distribution (how data is spread)
- Most common values (MCV)
- Histogram buckets
- Skew detection

**Why Planner Needs It**:
- Estimate result set size
- Choose between hash join vs nested loop
- Detect data skew (some branches have 1000x more records)

**Example**:
```
branch_code distribution:
  Code 333: 150 rows (common)
  Code 10: 80 rows
  Code 52: 3 rows (rare)
  
Planner knows: filtering by code=333 returns many rows (use hash join)
                filtering by code=52 returns few rows (use index scan)
```

### 3. Column Statistics ❌ (Advanced)
**What's Missing**:
- Min/max values (for range queries)
- Average value length (for string columns)
- Correlation between columns
- Data clustering/ordering

**Why Planner Needs It**:
```sql
WHERE disbursement_date BETWEEN '2025-10-01' AND '2025-10-31'
-- Planner needs: min_date, max_date to estimate % of rows matching
```

### 4. Partitioning Information ❌
**What's Missing**:
- How table is partitioned (by date, region, etc.)
- Partition pruning opportunities

**Why Planner Needs It**:
- Skip reading irrelevant partitions
- Parallel query execution

**Example**:
```sql
disbursements PARTITIONED BY (disbursement_date)
  - 2025-10: 544 rows
  - 2025-09: 1200 rows
  - 2025-08: 980 rows

Query: WHERE disbursement_date = '2025-10-15'
Planner: Only scan 2025-10 partition (partition pruning)
```

### 5. Table Size Estimates ❌
**What's Missing**:
- Table size in bytes
- Average row size
- Block/page count

**Why Planner Needs It**:
- Memory allocation
- Buffer pool sizing
- I/O cost estimation

### 6. Constraint Information ❌ (Partial)
**What's Missing**:
- UNIQUE constraints (per column)
- CHECK constraints (value ranges)
- NOT NULL constraints (we have nullability ✓)

**Why Planner Needs It**:
- Eliminate impossible conditions
- Join elimination
- Constraint exclusion

**Example**:
```sql
CONSTRAINT disbursement_amount > 0
CONSTRAINT branch_code IN (2, 10, 13, ...)
CONSTRAINT cif IS UNIQUE

Planner knows: JOIN on cif is 1:1 (not 1:many)
```

---

## Current Metadata Schema

### What We Store Now
```sql
-- tables
name, entity_id, system, primary_key, time_column, path, columns, labels

-- dataset_columns
column_name, data_type, nullable, unique_values_count, null_count

-- dataset_versions
row_count, checksum

-- lineage_edges
from_table, to_table, keys, relationship
```

### What Planner Actually Needs
```sql
-- Column-level stats (MISSING)
column_name, data_type, 
  ndv (distinct values),           ✓ (have unique_values_count)
  null_count,                       ✓
  min_value,                        ❌ REMOVED (but planner needs!)
  max_value,                        ❌ REMOVED (but planner needs!)
  avg_width (bytes),                ❌
  histogram (value distribution),   ❌
  most_common_values                ❌

-- Table-level stats (PARTIAL)
row_count,                          ✓
table_size_bytes,                   ❌
avg_row_length,                     ❌
clustering_factor                   ❌

-- Index metadata (MISSING)
index_name, columns[], type,        ❌
selectivity, leaf_pages             ❌

-- Partition metadata (MISSING)
partition_key, partition_bounds     ❌
```

---

## Recommendation: Add Query Planner Metadata

### Option 1: Minimal (Good Enough for Most Queries)

Add to `dataset_columns`:
```sql
ALTER TABLE dataset_columns 
ADD COLUMN avg_value_length INTEGER,  -- For string columns
ADD COLUMN most_common_values JSONB,  -- Top 10 most frequent values
ADD COLUMN value_distribution JSONB;  -- Histogram buckets
```

Add new table `table_indexes`:
```sql
CREATE TABLE table_indexes (
    id UUID PRIMARY KEY,
    table_name TEXT REFERENCES tables(name),
    index_name TEXT,
    columns TEXT[],  -- Array of column names
    index_type TEXT,  -- 'btree', 'hash', 'bitmap'
    is_unique BOOLEAN,
    selectivity FLOAT,  -- % of unique values
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Option 2: Full SQL Planner Stats (Production Grade)

```sql
-- Column statistics (like PostgreSQL pg_stats)
CREATE TABLE column_statistics (
    table_name TEXT,
    column_name TEXT,
    data_type TEXT,
    
    -- Cardinality
    null_frac FLOAT,  -- Fraction of nulls (0.0 to 1.0)
    avg_width INTEGER,  -- Average storage width in bytes
    n_distinct INTEGER,  -- Number of distinct values (-1 = unique)
    
    -- Value ranges
    min_value TEXT,  -- For range queries
    max_value TEXT,
    
    -- Distribution
    most_common_vals TEXT[],  -- Most frequent values
    most_common_freqs FLOAT[],  -- Their frequencies
    histogram_bounds TEXT[],  -- Histogram bucket boundaries
    
    -- Correlation
    correlation FLOAT,  -- Correlation with physical row order
    
    last_analyzed TIMESTAMP,
    PRIMARY KEY (table_name, column_name)
);

-- Index metadata
CREATE TABLE index_metadata (
    table_name TEXT,
    index_name TEXT,
    index_columns TEXT[],
    index_type TEXT,
    is_unique BOOLEAN,
    is_primary BOOLEAN,
    where_clause TEXT,  -- For partial indexes
    
    -- Index statistics
    num_rows BIGINT,
    num_pages INTEGER,
    avg_leaf_density FLOAT,
    clustering_factor FLOAT,
    
    PRIMARY KEY (table_name, index_name)
);

-- Table statistics
CREATE TABLE table_statistics (
    table_name TEXT PRIMARY KEY,
    row_count BIGINT,
    table_size_bytes BIGINT,
    avg_row_length INTEGER,
    num_pages INTEGER,
    
    -- Partitioning
    is_partitioned BOOLEAN,
    partition_key TEXT,
    partition_strategy TEXT,  -- 'range', 'list', 'hash'
    
    -- Clustering
    clustering_column TEXT,
    clustering_factor FLOAT,
    
    last_analyzed TIMESTAMP
);
```

---

## Critical Gap: Min/Max Values

### The Dilemma
❌ We removed `min_value`, `max_value` for privacy
✅ But query planner NEEDS them for range queries

### Solution: Store Ranges Without Exposing Data

**Option A: Numeric Ranges Only (Safe)**
```sql
ALTER TABLE dataset_columns
ADD COLUMN min_value_numeric FLOAT,  -- Only for numbers/dates
ADD COLUMN max_value_numeric FLOAT;

-- Example:
disbursement_amount: min=1.0, max=7819347.0
sanction_date: min=2022-03-24, max=2025-10-31
```
These are **aggregate statistics**, not individual data points.

**Option B: Bucketed Ranges (Privacy-Preserving)**
```sql
disbursement_amount distribution:
  [0-100K]: 234 rows
  [100K-500K]: 189 rows
  [500K-1M]: 89 rows
  [1M-5M]: 28 rows
  [5M+]: 4 rows
```
Planner gets useful info without exposing exact values.

---

## Actionable Next Steps

### Immediate (Critical for Basic Queries)
1. **Re-add min/max for numeric/date columns** (as aggregates, not individual values)
2. **Add index metadata** (which columns are indexed)
3. **Add most common values** (frequency distribution)

### Short-term (Better Query Plans)
4. **Add histogram buckets** (value distribution)
5. **Add correlation statistics** (data ordering)
6. **Track table sizes** (bytes, pages)

### Long-term (Production Grade)
7. **Partition metadata** (pruning opportunities)
8. **Join selectivity** (cardinality estimates)
9. **Auto-update statistics** (when data changes)

---

## Example: Before vs After

### Current Metadata (Insufficient)
```json
{
  "table": "disbursements",
  "columns": [
    {"name": "cif", "type": "integer", "nulls": 0},
    {"name": "amount", "type": "float", "nulls": 0}
  ],
  "row_count": 544
}
```

**Planner can't answer**:
- Is `cif` unique? (1:1 join or 1:many?)
- What's the range of `amount`? (how many rows match `amount > 1000000`?)
- Are there indexes? (use index scan or full table scan?)

### Complete Metadata (Planner-Ready)
```json
{
  "table": "disbursements",
  "columns": [
    {
      "name": "cif",
      "type": "integer",
      "nulls": 0,
      "n_distinct": 434,  // ← Planner: ~1.25 rows per cif (not unique)
      "avg_width": 12
    },
    {
      "name": "amount",
      "type": "float",
      "nulls": 0,
      "min_value": 1.0,  // ← Planner: range is 1 to 7.8M
      "max_value": 7819347.0,
      "histogram": [1, 50000, 200000, 500000, 1000000, 7819347],
      "most_common_vals": [540000, 450000, 800000]  // Popular amounts
    }
  ],
  "row_count": 544,
  "indexes": [
    {"columns": ["cif"], "type": "btree", "selectivity": 0.8},  // ← Use index!
    {"columns": ["disbursement_date"], "type": "btree"}
  ]
}
```

**Now planner can**:
- Choose index scan for `WHERE cif = X` (high selectivity)
- Estimate `WHERE amount > 1000000` returns ~5% of rows
- Use hash join for cif (not unique, many matches)

---

## Bottom Line

### Current Status
✅ Have: Basic schema, nullability, row counts
❌ Missing: Indexes, distribution, ranges, histograms

### For Query Planner to Work Well
**Must Have**:
1. Min/max values (ranges) ← **CRITICAL**
2. Index metadata ← **CRITICAL**
3. N_distinct (cardinality) ← **Have it ✓**

**Should Have**:
4. Histograms (distribution)
5. Most common values
6. Table sizes

**Nice to Have**:
7. Correlation stats
8. Partition info
9. Clustering factor

### Recommendation
**Add back min/max as aggregate statistics** (not individual data), plus index metadata. This gives 80% of planner benefit with minimal privacy risk.

