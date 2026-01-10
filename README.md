# RCA-ENGINE

A deterministic Root Cause Analysis engine for data reconciliation, guided by a hypergraph of data, rules, and business meaning. Uses LLM only as a thin translator and explainer — never as the decision engine.

## Core Principles

RCA is always reducible to only three root causes:

1. **Population mismatch** – different set of entities
2. **Logic mismatch** – different transformations/aggregations
3. **Data mismatch** – same entity, same logic, different source values

Everything in the system must ultimately classify into one of these.

## Architecture

```
RCA-ENGINE/
├── src/
│   ├── main.rs          # CLI + orchestration flow
│   ├── llm.rs           # OpenAI calls (thin layer)
│   ├── metadata.rs      # Load/validate knowledge
│   ├── graph.rs         # Hypergraph traversal
│   ├── identity.rs       # Key resolution
│   ├── time.rs          # As-of logic
│   ├── operators.rs     # Relational ops (Polars)
│   ├── rule_compiler.rs # Compile rules → plans
│   ├── executor.rs      # Execute plans (via rule_compiler)
│   ├── diff.rs          # Population & data diff
│   ├── drilldown.rs     # Stepwise tracing
│   ├── rca.rs           # Orchestration
│   ├── ambiguity.rs     # 3-question logic
│   └── explain.rs       # Business explanation
├── metadata/            # Hardcoded business knowledge
│   ├── entities.json
│   ├── tables.json
│   ├── metrics.json
│   ├── business_labels.json
│   ├── rules.json
│   ├── lineage.json
│   ├── time.json
│   ├── identity.json
│   └── exceptions.json
└── data/                # Data files (parquet)
```

## Usage

```bash
# Set OpenAI API key (or use dummy for testing)
export OPENAI_API_KEY="your-api-key"

# Run reconciliation
cargo run -- "Khatabook vs TB TOS recon as of 2025-12-31"

# With custom paths
cargo run -- \
  --metadata-dir ./metadata \
  --data-dir ./data \
  "Khatabook vs TB TOS reconciliation"
```

## Execution Flow

1. **Query Interpretation**: LLM maps natural language to business labels (systemA, systemB, metric)
2. **Ambiguity Resolution**: If needed, ask ≤3 multiple-choice questions
3. **Rule Resolution**: Find rules that compute the metrics for both systems
4. **Grain Normalization**: Detect and normalize to common grain
5. **Pipeline Execution**: Execute both pipelines deterministically using Polars
6. **Time Logic**: Apply as-of rules and detect temporal misalignment
7. **Comparison**: Population diff + data diff at final grain
8. **Classification**: Classify mismatches into 3 root causes
9. **Drill-down**: For mismatched keys, trace step-by-step to find divergence
10. **Explanation**: LLM explains structured RCA result in business language

## LLM Usage

- **Max 2 calls per run** (plus 1 only if ambiguity)
- **Never sees raw data** - only labels, column names, structured RCA output
- **Always JSON in/out**
- **Roles**: Translator, Disambiguator (≤3 questions), Narrator
- **NOT**: Executor, Judge, Data analyzer

## Example: Khatabook vs TB TOS Reconciliation

The system includes a hardcoded example for reconciling TOS (Total Outstanding) between Khatabook and TB systems:

- **Khatabook TOS**: Computed from loans → EMIs → transactions
- **TB TOS**: Computed from loan_summary table
- **Grain**: Both normalized to `loan_id` level
- **Comparison**: Population diff + value diff with precision handling

## Metadata Structure

All business knowledge is stored in JSON files under `metadata/`:

- **entities.json**: Business entities and their grains
- **tables.json**: Table → entity mapping, primary keys, time columns
- **metrics.json**: Metric definitions, precision, null policies
- **business_labels.json**: Natural language labels → system/metric IDs
- **rules.json**: Metric computation pipelines as operator graphs
- **lineage.json**: Hypergraph edges defining possible joins
- **time.json**: As-of rules and lateness policies
- **identity.json**: Canonical keys and alternates
- **exceptions.json**: Special business overrides

## Non-Negotiable Principles

- All meaning comes from metadata, not the LLM
- RCA must be provable, not guessed
- Everything collapses into 3 root causes
- LLM is a thin layer, never the brain
- If business meaning is ambiguous, ask at most 3 precise questions

## Dependencies

- **polars**: Columnar data processing and execution engine
- **reqwest**: HTTP client for OpenAI API
- **tokio**: Async runtime
- **serde/serde_json**: JSON serialization
- **clap**: CLI argument parsing
- **chrono**: Date/time handling

## Development

```bash
# Check compilation
cargo check

# Build
cargo build

# Run tests (when implemented)
cargo test

# Run with example
cargo run -- "Khatabook vs TB TOS recon as of 2025-12-31"
```

## Notes

- Currently uses dummy API key if `OPENAI_API_KEY` is not set
- Data files should be in Parquet format under `data/` directory
- Metadata files must be valid JSON matching the expected schemas

# RCA-Engine
