# RCA-ENGINE: Complete Logic Documentation

## Overview

RCA-ENGINE is a **deterministic Root Cause Analysis engine** for data reconciliation. It uses a hypergraph-based knowledge model where all business logic is encoded as metadata. The system autonomously decides **how** to execute reconciliations based on **what** is specified in the metadata.

**Key Principle**: The LLM is only a thin translator/disambiguator/explainer. All execution decisions are made deterministically by the engine based on metadata, not by the LLM.

---

## Core Architecture: Three Root Causes

All reconciliation mismatches collapse into exactly **three root causes**:

1. **Population Mismatch** - Different set of entities
   - Missing entities in one system
   - Extra entities in one system
   - Duplicate entities

2. **Logic Mismatch** - Different transformations/aggregations
   - Different operation order
   - Join explosion
   - Aggregation differences
   - Missing/extra operations

3. **Data Mismatch** - Same entity, same logic, different source values
   - Time misalignment
   - Null/default handling differences
   - Precision/rounding differences
   - Schema drift

---

## Complete End-to-End Flow: Example

Let's trace through a complete example: **"Khatabook vs TB TOS reconciliation as of 2025-12-31"**

### Input

```bash
cargo run -- "Khatabook vs TB TOS recon as of 2025-12-31"
```

---

## Step-by-Step Execution

### Step 1: Main Entry Point (`main.rs`)

```rust
// 1. Parse command-line arguments
let args = Args::parse();

// 2. Load all metadata from JSON files
let metadata = metadata::Metadata::load(&args.metadata_dir)?;

// 3. Initialize LLM client (with dummy API key for testing)
let llm = llm::LlmClient::new(api_key, model, base_url);

// 4. Create RCA engine and run
let engine = rca::RcaEngine::new(metadata, llm, args.data_dir);
let result = engine.run(&args.query).await?;
```

**What happens:**
- Loads 9 JSON metadata files (entities, tables, metrics, rules, lineage, etc.)
- Builds indexes for fast O(1) lookups
- Initializes components

---

### Step 2: Query Interpretation (`llm.rs`)

The LLM translates natural language to structured JSON. It **never makes execution decisions** - only translates labels.

**Input Query:**
```
"Khatabook vs TB TOS recon as of 2025-12-31"
```

**LLM Prompt Context:**
- Systems: `["Khatabook", "TB", ...]` from `business_labels.json`
- Metrics: `["TOS", "POS", ...]` from `metrics.json`
- Business aliases and labels

**LLM Output (JSON):**
```json
{
  "system_a": "khatabook",
  "system_b": "tb",
  "metric": "tos",
  "as_of_date": "2025-12-31",
  "confidence": 0.95
}
```

**Why this works:**
- LLM only maps natural language → metadata IDs
- Uses `business_labels.json` to resolve "Khatabook" → `"khatabook"` system ID
- Uses `metrics.json` to resolve "TOS" → `"tos"` metric ID
- **Never sees raw data, never makes execution decisions**

---

### Step 3: Ambiguity Resolution (`ambiguity.rs`)

If multiple interpretations exist (multiple rules, time columns, etc.), ask at most **3 multiple-choice questions**.

**Example Scenario:**
Suppose there are 2 rules for computing `khatabook` TOS:
- `khatabook_tos_v1` - Legacy calculation
- `khatabook_tos_v2` - New calculation

**System Behavior:**
```rust
// Check for ambiguities
let rules_a = metadata.get_rules_for_system_metric("khatabook", "tos");
if rules_a.len() > 1 {
    // Build question from rules
    let question = build_rule_question("khatabook", rules_a)?;
    // Ask user (max 3 questions total)
    // User selects: "khatabook_tos_v2"
}
```

**Question Format:**
```
Question 1: Which rule version for khatabook?
  1. khatabook_tos_v1 - Legacy calculation
  2. khatabook_tos_v2 - New calculation with updated formula
Your choice (1-2): 2
```

**Output:**
```rust
ResolvedInterpretation {
    system_a: "khatabook",
    system_b: "tb",
    metric: "tos",
    as_of_date: Some("2025-12-31"),
    rule_a: Some("khatabook_tos_v2"),
    rule_b: Some("tb_tos")
}
```

**Key Point:** System asks precise questions only when business knowledge is ambiguous. Human provides the business knowledge, system makes all execution decisions.

---

### Step 4: Rule Resolution & Subgraph Extraction (`graph.rs`)

The system finds which rules compute the metric for both systems and extracts the relevant subgraph.

**Process:**
```rust
// Find rules for both systems
let rules_a = metadata.get_rules_for_system_metric("khatabook", "tos");
// Returns: [Rule { id: "khatabook_tos", ... }]

let rules_b = metadata.get_rules_for_system_metric("tb", "tos");
// Returns: [Rule { id: "tb_tos", ... }]

// Get reconciliation subgraph
let subgraph = graph.get_reconciliation_subgraph("khatabook", "tb", "tos")?;
```

**Subgraph Output:**
```rust
ReconciliationSubgraph {
    system_a: "khatabook",
    system_b: "tb",
    metric: "tos",
    rules_a: ["khatabook_tos"],
    rules_b: ["tb_tos"],
    tables_a: ["khatabook_loans", "khatabook_emis", "khatabook_transactions"],
    tables_b: ["tb_loans", "tb_loan_summary"]
}
```

**How tables are derived:**
- System reads `rule.computation.source_entities` = `["loan", "emi", "transaction"]`
- Looks up `tables.json` to find tables where `entity IN ["loan", "emi", "transaction"]` AND `system == "khatabook"`
- **No hardcoded pipeline - all derived from metadata!**

---

### Step 5: Pipeline Construction (`rule_compiler.rs`)

**This is where the autonomous decision-making happens!** The system automatically constructs the execution pipeline from the rule specification + metadata.

**Rule Specification (`rules.json`):**
```json
{
  "id": "khatabook_tos",
  "system": "khatabook",
  "metric": "tos",
  "target_entity": "loan",
  "target_grain": ["loan_id"],
  "computation": {
    "description": "Total Outstanding = Sum of (EMI Amount - Transaction Amount) per loan",
    "source_entities": ["loan", "emi", "transaction"],
    "attributes_needed": {
      "emi": ["loan_id", "emi_number", "amount"],
      "transaction": ["loan_id", "emi_number", "amount"]
    },
    "formula": "SUM(emi_amount - COALESCE(transaction_amount, 0))",
    "aggregation_grain": ["loan_id"]
  }
}
```

**Autonomous Pipeline Construction Process:**

#### 5.1: Map Entities to Tables

```rust
// For each entity in source_entities
for entity in ["loan", "emi", "transaction"] {
    // Find tables for this entity in this system
    let tables = metadata.tables
        .iter()
        .filter(|t| t.entity == entity && t.system == "khatabook")
        .collect();
    // Result:
    //   "loan" → ["khatabook_loans"]
    //   "emi" → ["khatabook_emis"]
    //   "transaction" → ["khatabook_transactions"]
}
```

#### 5.2: Determine Root Table

```rust
// Root entity is target_entity: "loan"
let root_table = "khatabook_loans";  // First table for root entity
```

#### 5.3: Find Join Paths (Using `lineage.json`)

```rust
// System uses BFS to find join paths from root to all other tables
// Lineage edges:
[
  {
    "from": "khatabook_loans",
    "to": "khatabook_emis",
    "keys": {"loan_id": "loan_id"},
    "relationship": "one_to_many"
  },
  {
    "from": "khatabook_emis",
    "to": "khatabook_transactions",
    "keys": {"loan_id": "loan_id", "emi_number": "emi_number"},
    "relationship": "one_to_many"
  }
]

// BFS finds path: khatabook_loans → khatabook_emis → khatabook_transactions
```

#### 5.4: Determine Join Types

```rust
// Join type determined from lineage relationship
// "one_to_many" → LEFT JOIN (preserve all loans, even without EMIs)
// "many_to_one" → INNER JOIN
// "one_to_one" → LEFT JOIN

// khatabook_loans → khatabook_emis: one_to_many → LEFT JOIN
// khatabook_emis → khatabook_transactions: one_to_many → LEFT JOIN
```

#### 5.5: Parse Formula and Construct Operations

```rust
// Formula: "SUM(emi_amount - COALESCE(transaction_amount, 0))"

// Step 1: Extract inner expression (inside SUM)
let inner_expr = "emi_amount - COALESCE(transaction_amount, 0)";

// Step 2: Add DERIVE operation
PipelineOp::Derive {
    expr: "emi_amount - COALESCE(transaction_amount, 0)",
    as: "computed_value"  // Temporary column
}

// Step 3: Add GROUP operation
PipelineOp::Group {
    by: ["loan_id"],  // From aggregation_grain
    agg: {
        "tos": "SUM(computed_value)"  // Aggregate the derived column
    }
}
```

#### 5.6: Final Pipeline Construction

**Complete Automatically Generated Pipeline:**

```rust
vec![
    // Step 1: Scan root table
    PipelineOp::Scan { table: "khatabook_loans" },
    
    // Step 2: Join EMIs (found via lineage)
    PipelineOp::Join {
        table: "khatabook_emis",
        on: ["loan_id"],  // From lineage keys
        join_type: "left"  // From relationship: one_to_many
    },
    
    // Step 3: Join Transactions (found via lineage)
    PipelineOp::Join {
        table: "khatabook_transactions",
        on: ["loan_id", "emi_number"],  // From lineage keys
        join_type: "left"  // From relationship: one_to_many
    },
    
    // Step 4: Derive intermediate calculation
    PipelineOp::Derive {
        expr: "emi_amount - COALESCE(transaction_amount, 0)",
        as: "computed_value"
    },
    
    // Step 5: Aggregate by loan_id
    PipelineOp::Group {
        by: ["loan_id"],
        agg: {
            "tos": "SUM(computed_value)"
        }
    },
    
    // Step 6: Select final columns
    PipelineOp::Select {
        columns: ["loan_id", "tos"]
    }
]
```

**Key Insight:** The system **decided**:
- Which tables to scan (from entities)
- Which joins to perform (from lineage edges)
- Join order (BFS shortest path)
- Join types (from relationship types)
- Derive operation (from formula parsing)
- Aggregation (from formula + aggregation_grain)

**Human only provided:** What entities are needed, what formula to compute, what grain to aggregate to.

---

### Step 6: Pipeline Execution (`rule_compiler.rs` + `operators.rs`)

Execute both pipelines deterministically using Polars.

#### 6.1: Execute Khatabook Pipeline

```rust
// For each operation in the pipeline
for step in pipeline {
    match step {
        Scan { table } => {
            // Load table from Parquet using path from tables.json
            let df = scan_with_metadata("khatabook_loans")?;
            // Apply as-of filtering if needed
            df = apply_as_of(df, "2025-12-31")?;
        }
        Join { table, on, join_type } => {
            // Load right table
            let right = scan_with_metadata(table)?;
            // Perform join
            df = join(df, right, on, join_type)?;
            // Check for join explosion (>10x row increase)
        }
        Derive { expr, as } => {
            // Parse and execute expression
            df = derive(df, expr, as)?;
        }
        Group { by, agg } => {
            // Aggregate by columns
            df = group_by(df, by, agg)?;
        }
        Select { columns } => {
            // Select final columns
            df = select(df, columns)?;
        }
    }
}
```

**Intermediate Results:**

```
After Scan(khatabook_loans):
  loan_id | customer_id | disbursement_date
  1001    | C001        | 2025-01-15
  1002    | C002        | 2025-02-20
  (2 rows)

After Join(khatabook_emis):
  loan_id | emi_number | emi_amount | ...
  1001    | 1          | 5000.00    | ...
  1001    | 2          | 5000.00    | ...
  1002    | 1          | 3000.00    | ...
  (3 rows)

After Join(khatabook_transactions):
  loan_id | emi_number | emi_amount | transaction_amount | ...
  1001    | 1          | 5000.00    | 4500.00           | ...
  1001    | 2          | 5000.00    | NULL              | ...
  1002    | 1          | 3000.00    | 3000.00           | ...
  (3 rows)

After Derive:
  loan_id | emi_number | computed_value
  1001    | 1          | 500.00        (5000 - 4500)
  1001    | 2          | 5000.00       (5000 - 0, COALESCE)
  1002    | 1          | 0.00          (3000 - 3000)
  (3 rows)

After Group:
  loan_id | tos
  1001    | 5500.00    (SUM: 500 + 5000)
  1002    | 0.00       (SUM: 0)
  (2 rows)
```

#### 6.2: Execute TB Pipeline

Similarly, for `tb_tos` rule:
```json
{
  "source_entities": ["loan"],
  "formula": "total_outstanding",
  "aggregation_grain": ["loan_id"]
}
```

**Generated Pipeline:**
```rust
[
    Scan { table: "tb_loan_summary" },  // Only one entity, one table
    Select { columns: ["loan_id", "total_outstanding"] }  // Direct column reference
]
```

**Result:**
```
loan_id | tos
1001    | 6000.00
1002    | 0.00
(2 rows)
```

---

### Step 7: Grain Normalization (`identity.rs`)

Ensure both results are at the same grain level for comparison.

```rust
let grain_a = graph.get_rule_grain("khatabook_tos")?;  // ["loan_id"]
let grain_b = graph.get_rule_grain("tb_tos")?;         // ["loan_id"]

// If grains differ, use identity.json to normalize keys
if grain_a != grain_b {
    // Find key mappings and normalize
    df_a = normalize_keys(df_a, grain_a, common_grain)?;
    df_b = normalize_keys(df_b, grain_b, common_grain)?;
}
```

**In this example:** Both are already at `loan_id` grain, so no normalization needed.

---

### Step 8: Time Logic (`time.rs`)

Apply as-of date filtering and detect temporal misalignment.

```rust
// As-of filtering was already applied during scan
// Now check for temporal misalignment

let temporal_misalignment = detect_temporal_misalignment(
    &df_a, "khatabook_loans",
    &df_b, "tb_loan_summary",
    Some("2025-12-31")
)?;

// Checks:
// - Are data ranges aligned?
// - Are there missing time periods?
// - Do time columns match?
```

---

### Step 9: Comparison (`diff.rs`)

Compare results at final grain using population diff and data diff.

#### 9.1: Population Diff

```rust
// Extract unique keys from both sides
let keys_a: HashSet<Vec<String>> = extract_keys(df_a, ["loan_id"]);
// {["1001"], ["1002"]}

let keys_b: HashSet<Vec<String>> = extract_keys(df_b, ["loan_id"]);
// {["1001"], ["1002"]}

// Find differences
let missing_in_b = keys_a - keys_b;  // []
let extra_in_b = keys_b - keys_a;    // []
let common_keys = keys_a ∩ keys_b;   // {["1001"], ["1002"]}
```

**Result:**
```
Population Diff:
  Missing in B: 0
  Extra in B: 0
  Common: 2
```

#### 9.2: Data Diff

```rust
// Join on common keys and compare metric values
let comparison = join_and_compare(df_a, df_b, ["loan_id"], "tos", precision=2)?;
```

**Comparison:**
```
loan_id | tos_khatabook | tos_tb | difference | match
1001    | 5500.00       | 6000.00| -500.00    | false
1002    | 0.00          | 0.00   | 0.00       | true
```

**Result:**
```
Data Diff:
  Matches: 1
  Mismatches: 1
  Precision: 2 decimal places
```

---

### Step 10: Classification (`rca.rs`)

Classify mismatches into three root causes.

```rust
fn classify_mismatches(comparison: &ComparisonResult) -> Vec<RootCauseClassification> {
    let mut classifications = Vec::new();
    
    // Population Mismatch
    if !comparison.population_diff.missing_in_b.is_empty() {
        classifications.push(RootCauseClassification {
            root_cause: "Population Mismatch",
            subtype: "Missing Entities",
            description: "1 entity missing in system B",
            count: 1
        });
    }
    
    // Data Mismatch
    if comparison.data_diff.mismatches > 0 {
        classifications.push(RootCauseClassification {
            root_cause: "Data Mismatch",
            subtype: "Value Difference",
            description: "1 entity has different metric value (diff: 500.00)",
            count: 1
        });
    }
    
    // Logic Mismatch (detected in drilldown)
    // ...
}
```

**Classification Result:**
```rust
[
    RootCauseClassification {
        root_cause: "Data Mismatch",
        subtype: "Value Difference",
        description: "loan_id=1001: khatabook=5500.00, tb=6000.00, diff=-500.00",
        count: 1
    }
]
```

---

### Step 11: Drill-down (`drilldown.rs`)

For mismatched keys, re-execute pipelines step-by-step to find the first divergence point.

```rust
// For mismatched key: ["1001"]
let divergence = drilldown.find_divergence(
    "khatabook_tos",
    "tb_tos",
    &[vec!["1001"]],
    Some("2025-12-31")
).await?;
```

**Drill-down Process:**

```rust
// Execute khatabook_tos step-by-step for loan_id=1001
Step 1 (Scan khatabook_loans): 1 row
Step 2 (Join khatabook_emis): 2 rows
Step 3 (Join khatabook_transactions): 2 rows
Step 4 (Derive): 2 rows, computed_value = [500.00, 5000.00]
Step 5 (Group): 1 row, tos = 5500.00

// Execute tb_tos step-by-step for loan_id=1001
Step 1 (Scan tb_loan_summary): 1 row, total_outstanding = 6000.00
Step 2 (Select): 1 row, tos = 6000.00

// Compare intermediate results
// Divergence at: Final aggregation step
// khatabook: SUM(500 + 5000) = 5500
// tb: Direct value = 6000
```

**Divergence Result:**
```rust
DivergencePoint {
    step_index: 5,  // Group/aggregation step
    divergence_type: "Aggregation Logic",
    description: "khatabook uses SUM of EMI-transaction differences, tb uses pre-computed value",
    intermediate_values_a: { "tos": 5500.00 },
    intermediate_values_b: { "tos": 6000.00 }
}
```

**This is a Logic Mismatch!** The systems use different computation methods.

**Updated Classification:**
```rust
[
    RootCauseClassification {
        root_cause: "Logic Mismatch",
        subtype: "Aggregation Difference",
        description: "khatabook computes TOS from EMI-transaction differences, tb uses pre-computed total_outstanding column",
        count: 1
    }
]
```

---

### Step 12: Explanation (`explain.rs` + `llm.rs`)

LLM explains the structured RCA result in business language. It **never sees raw data**, only structured output.

**Input to LLM:**
```json
{
  "system_a": "khatabook",
  "system_b": "tb",
  "metric": "tos",
  "classifications": [
    {
      "root_cause": "Logic Mismatch",
      "subtype": "Aggregation Difference",
      "description": "...",
      "count": 1
    }
  ],
  "divergence": {
    "step": "Group/aggregation",
    "type": "Aggregation Logic"
  }
}
```

**LLM Output (Business Explanation):**
```
Root Cause Analysis for Khatabook vs TB TOS Reconciliation

Issue Summary:
The systems use different methods to calculate Total Outstanding (TOS) for loan 1001.

Root Cause: Logic Mismatch
- Khatabook: Computes TOS by summing (EMI Amount - Transaction Amount) for each EMI, resulting in 5,500.00
- TB: Uses a pre-computed total_outstanding column directly, showing 6,000.00

Difference: 500.00

Recommendation:
Review the business rule definition. If both methods are valid, align on a single calculation method. If TB's pre-computed value should match Khatabook's calculation, investigate why TB shows a higher value (possible missing transaction or timing issue).
```

---

## Summary: Autonomous Decision-Making

### What the System Decides Autonomously:

1. **Which tables to use** - From `source_entities` → `tables.json`
2. **Which joins to perform** - From `lineage.json` edges
3. **Join order** - BFS shortest path algorithm
4. **Join types** - From `relationship` field in lineage
5. **Pipeline operations** - From formula parsing
6. **Aggregation logic** - From `aggregation_grain` + formula
7. **Execution plan** - All of the above combined

### What Humans Provide (Metadata):

1. **Business entities** - What entities exist (`entities.json`)
2. **Tables mapping** - Which tables represent which entities (`tables.json`)
3. **Business rules** - What needs to be computed (`rules.json`):
   - Source entities needed
   - Formula/calculation
   - Target grain
4. **Lineage/relationships** - What joins are possible (`lineage.json`)
5. **Business labels** - Natural language → metadata IDs (`business_labels.json`)

### What LLM Does (Thin Layer):

1. **Translates** natural language → metadata IDs (1 call)
2. **Asks questions** if ambiguous (≤3 questions, 1 call)
3. **Explains** structured results → business language (1 call)

**Total LLM calls: 2-3 per run**

---

## Metadata-Driven Example: Khatabook TOS Rule

### Rule Definition (`rules.json`):

```json
{
  "id": "khatabook_tos",
  "system": "khatabook",
  "metric": "tos",
  "target_entity": "loan",
  "target_grain": ["loan_id"],
  "computation": {
    "description": "Total Outstanding = Sum of (EMI Amount - Transaction Amount) per loan",
    "source_entities": ["loan", "emi", "transaction"],
    "attributes_needed": {
      "emi": ["loan_id", "emi_number", "amount"],
      "transaction": ["loan_id", "emi_number", "amount"]
    },
    "formula": "SUM(emi_amount - COALESCE(transaction_amount, 0))",
    "aggregation_grain": ["loan_id"]
  }
}
```

### System's Autonomous Decisions:

#### Decision 1: Map Entities to Tables
```
Input: source_entities = ["loan", "emi", "transaction"]
Query: tables.json WHERE entity IN [...] AND system = "khatabook"
Output:
  "loan" → ["khatabook_loans"]
  "emi" → ["khatabook_emis"]
  "transaction" → ["khatabook_transactions"]
```

#### Decision 2: Find Root Table
```
Input: target_entity = "loan"
Output: root_table = "khatabook_loans"
```

#### Decision 3: Find Join Paths
```
Input: root = "khatabook_loans", targets = ["khatabook_emis", "khatabook_transactions"]
Query: lineage.json edges
BFS Algorithm:
  1. From "khatabook_loans": 
     → Found edge to "khatabook_emis" (keys: loan_id)
  2. From "khatabook_emis":
     → Found edge to "khatabook_transactions" (keys: loan_id, emi_number)
Output: Path = [loans → emis → transactions]
```

#### Decision 4: Determine Join Types
```
Input: lineage relationships
  loans → emis: "one_to_many"
  emis → transactions: "one_to_many"
Logic:
  one_to_many → LEFT JOIN (preserve parent)
Output:
  Join(emis): type = "left"
  Join(transactions): type = "left"
```

#### Decision 5: Parse Formula
```
Input: formula = "SUM(emi_amount - COALESCE(transaction_amount, 0))"
Parse:
  - Has aggregation: YES (SUM)
  - Inner expression: "emi_amount - COALESCE(transaction_amount, 0)"
  - Output column: "tos" (from metric)
  - Aggregation grain: ["loan_id"]
Output:
  Derive: expr = inner_expr, as = "computed_value"
  Group: by = ["loan_id"], agg = {"tos": "SUM(computed_value)"}
```

#### Decision 6: Construct Final Pipeline
```
Combine all decisions:
  1. Scan(root_table)
  2. Join(path[0])
  3. Join(path[1])
  4. Derive(formula_inner)
  5. Group(aggregation_grain, formula_agg)
  6. Select([target_grain, metric])
```

**Result:** Complete execution plan generated autonomously!

---

## Complete Execution Trace Example

### Input Query
```
"Khatabook vs TB TOS reconciliation as of 2025-12-31"
```

### Step-by-Step Execution

```
[STEP 1] LLM Interpretation
  Input: Natural language query + business_labels + metrics
  Output: {
    "system_a": "khatabook",
    "system_b": "tb",
    "metric": "tos",
    "as_of_date": "2025-12-31"
  }

[STEP 2] Ambiguity Resolution
  Input: interpretation
  Check: Multiple rules for khatabook+tos? No → Skip
  Check: Multiple rules for tb+tos? No → Skip
  Output: resolved (same as interpretation)

[STEP 3] Rule Resolution
  Input: system_a="khatabook", system_b="tb", metric="tos"
  Query: rules.json WHERE (system="khatabook" AND metric="tos") OR (system="tb" AND metric="tos")
  Output:
    rules_a = ["khatabook_tos"]
    rules_b = ["tb_tos"]

[STEP 4] Subgraph Extraction
  Input: rules_a, rules_b
  Derive tables from rule.computation.source_entities:
    khatabook_tos: ["loan", "emi", "transaction"] → ["khatabook_loans", "khatabook_emis", "khatabook_transactions"]
    tb_tos: ["loan"] → ["tb_loan_summary"]
  Output:
    tables_a = ["khatabook_loans", "khatabook_emis", "khatabook_transactions"]
    tables_b = ["tb_loan_summary"]

[STEP 5] Pipeline Construction (khatabook_tos)
  Input: rule.computation specification
  Process:
    - Map entities → tables: ✓
    - Find root table: "khatabook_loans" ✓
    - BFS join paths: loans → emis → transactions ✓
    - Join types: left, left ✓
    - Parse formula: SUM(...) → Derive + Group ✓
  Output Pipeline:
    [
      Scan("khatabook_loans"),
      Join("khatabook_emis", on=["loan_id"], type="left"),
      Join("khatabook_transactions", on=["loan_id", "emi_number"], type="left"),
      Derive("emi_amount - COALESCE(transaction_amount, 0)", as="computed_value"),
      Group(by=["loan_id"], agg={"tos": "SUM(computed_value)"}),
      Select(["loan_id", "tos"])
    ]

[STEP 6] Pipeline Construction (tb_tos)
  Input: rule.computation specification
  Process:
    - Map entities → tables: ["loan"] → ["tb_loan_summary"] ✓
    - Root table: "tb_loan_summary" ✓
    - No joins needed ✓
    - Formula: "total_outstanding" (direct column) ✓
  Output Pipeline:
    [
      Scan("tb_loan_summary"),
      Select(["loan_id", "total_outstanding"])
    ]

[STEP 7] Pipeline Execution (khatabook_tos)
  Execute each operation sequentially:
    Scan: 2 rows (loan_id: 1001, 1002)
    Join(emis): 3 rows (loan_id: 1001×2, 1002×1)
    Join(transactions): 3 rows
    Derive: 3 rows (computed_value: [500, 5000, 0])
    Group: 2 rows (loan_id: [1001, 1002], tos: [5500, 0])
    Select: 2 rows
  Final Result:
    loan_id | tos
    1001    | 5500.00
    1002    | 0.00

[STEP 8] Pipeline Execution (tb_tos)
  Execute each operation sequentially:
    Scan: 2 rows (loan_id: 1001, 1002, total_outstanding: [6000, 0])
    Select: 2 rows
  Final Result:
    loan_id | tos
    1001    | 6000.00
    1002    | 0.00

[STEP 9] Grain Normalization
  Check: grain_a = ["loan_id"], grain_b = ["loan_id"]
  Result: Same grain, no normalization needed ✓

[STEP 10] Time Logic
  As-of filtering: Applied during scan (disbursement_date <= 2025-12-31)
  Temporal alignment: Check date ranges...
  Result: Aligned ✓

[STEP 11] Population Diff
  Extract keys from both sides:
    keys_a: {["1001"], ["1002"]}
    keys_b: {["1001"], ["1002"]}
  Result:
    Missing in B: 0
    Extra in B: 0
    Common: 2

[STEP 12] Data Diff
  Join on loan_id and compare tos values:
    loan_id | tos_a | tos_b | diff | match
    1001    | 5500  | 6000  | -500 | false
    1002    | 0     | 0     | 0    | true
  Result:
    Matches: 1
    Mismatches: 1

[STEP 13] Classification
  Analyze mismatches:
    - Population: All keys match ✓
    - Data: 1 value mismatch (loan_id=1001, diff=-500)
    - Logic: Need drilldown to determine

[STEP 14] Drill-down
  For mismatched key: ["1001"]
  Re-execute step-by-step and compare:
    Step 1-4: Same row counts
    Step 5 (Group): 
      khatabook: SUM(500 + 5000) = 5500
      tb: Direct value = 6000
  Divergence: Aggregation step
  Type: Logic Mismatch - Different computation methods

[STEP 15] Final Classification
  Root Cause: Logic Mismatch
  Subtype: Aggregation Difference
  Description: khatabook computes from EMI-transactions, tb uses pre-computed value

[STEP 16] Explanation
  LLM Input: Structured RCA result
  LLM Output: Business-friendly explanation
  "The systems use different methods to calculate TOS. Khatabook sums 
   EMI-transaction differences (5,500), while TB uses a pre-computed 
   column (6,000). Difference: 500. Review business rules to align 
   calculation method."

[STEP 17] Output Results
  Print formatted RCA result with classifications, comparisons, and explanation
```

---

## Key Takeaways

1. **Autonomous Decision-Making**: System decides HOW to execute based on WHAT is specified in metadata
2. **Deterministic**: Same metadata + same data = same result (no LLM inference in execution)
3. **Metadata-Driven**: All business knowledge encoded as structured JSON, not hardcoded
4. **Thin LLM Layer**: LLM only translates, disambiguates, and explains - never executes
5. **Three Root Causes**: All mismatches collapse into Population/Logic/Data mismatch
6. **Provable Results**: Every step is traceable and deterministic

---

## File Structure Summary

```
RCA-Engine/
├── src/
│   ├── main.rs              # CLI entry point
│   ├── rca.rs               # Main orchestration (Steps 1-17)
│   ├── llm.rs               # LLM translation (Step 1, 16)
│   ├── ambiguity.rs         # Ambiguity resolution (Step 2)
│   ├── graph.rs             # Rule/subgraph resolution (Step 3-4)
│   ├── rule_compiler.rs     # Pipeline construction (Step 5-6)
│   ├── operators.rs         # Relational operations (Step 7-8)
│   ├── identity.rs          # Grain normalization (Step 9)
│   ├── time.rs              # Time logic (Step 10)
│   ├── diff.rs              # Comparison (Step 11-12)
│   ├── drilldown.rs         # Step-by-step tracing (Step 14)
│   └── explain.rs           # Business explanation (Step 16)
├── metadata/                # Business knowledge
│   ├── entities.json        # Business entities
│   ├── tables.json          # Table → entity mapping
│   ├── metrics.json         # Metric definitions
│   ├── business_labels.json # Natural language labels
│   ├── rules.json           # Rule specifications (WHAT to compute)
│   ├── lineage.json         # Join relationships (possible edges)
│   ├── time.json            # Time rules
│   ├── identity.json        # Key mappings
│   └── exceptions.json      # Business exceptions
└── data/                    # Data files (Parquet)
    ├── khatabook/
    └── tb/
```

---

## Conclusion

The RCA-ENGINE is a **fully autonomous, metadata-driven reconciliation system**. It makes all execution decisions based on structured metadata, with the LLM serving only as a thin translation layer. The system can handle any reconciliation scenario as long as the metadata accurately describes the business knowledge.

**Human provides:** Business knowledge (metadata)  
**System decides:** How to execute (pipeline construction, joins, aggregations)  
**LLM translates:** Natural language ↔ Structured data

This architecture ensures **provable, deterministic, traceable** root cause analysis for data reconciliation.