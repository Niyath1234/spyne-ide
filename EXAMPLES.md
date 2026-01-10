# RCA-ENGINE Examples

## Example: Khatabook vs TB TOS Reconciliation

This example demonstrates a complete reconciliation workflow for Total Outstanding (TOS) between two systems.

### Setup

1. **Metadata Configuration**: All metadata files are in `metadata/` directory
2. **Data Files**: Place Parquet files in `data/` directory:
   - `khatabook/loans.parquet`
   - `khatabook/emis.parquet`
   - `khatabook/transactions.parquet`
   - `tb/loan_summary.parquet`

### Execution

```bash
cargo run -- "Khatabook vs TB TOS recon as of 2025-12-31"
```

### What Happens

1. **Query Interpretation**: LLM extracts:
   - system_a: "khatabook"
   - system_b: "tb"
   - metric: "tos"
   - as_of_date: "2025-12-31"

2. **Rule Resolution**: Finds rules:
   - `khatabook_tos`: Computes TOS from loans → EMIs → transactions
   - `tb_tos`: Reads TOS from loan_summary table

3. **Grain Normalization**: Both normalized to `loan_id` level

4. **Execution**: 
   - Khatabook: Join loans, EMIs, transactions; compute outstanding; aggregate
   - TB: Read loan_summary directly

5. **Comparison**:
   - Population diff: Missing/extra loan_ids
   - Data diff: Value differences (with precision handling)

6. **Classification**: Mismatches classified into:
   - Population Mismatch (missing/extra entities)
   - Data Mismatch (value differences)
   - Logic Mismatch (if drilldown reveals different transformations)

7. **Drill-down**: For mismatched keys, trace execution step-by-step to find first divergence

8. **Explanation**: LLM generates business-friendly explanation

### Expected Output

```
RCA Result for: Khatabook vs TB TOS recon as of 2025-12-31
System A: khatabook | System B: tb | Metric: tos
As-of Date: 2025-12-31

=== Classifications ===
- Population Mismatch (Missing Entities)
  5 entities missing in system B
- Data Mismatch (Value Difference)
  12 entities have different metric values

=== Population Diff ===
Missing in B: 5
Extra in B: 2
Common: 100

=== Data Diff ===
Matches: 88
Mismatches: 12

=== Divergence Point ===
Step: 2 | Type: value_mismatch
```

### Customizing Rules

Edit `metadata/rules.json` to modify computation logic:

```json
{
  "id": "khatabook_tos",
  "pipeline": [
    {"op": "scan", "table": "khatabook_loans"},
    {"op": "join", "table": "khatabook_emis", "on": ["loan_id"], "type": "left"},
    {"op": "derive", "expr": "emi_amount - COALESCE(transaction_amount, 0)", "as": "outstanding"},
    {"op": "group", "by": ["loan_id"], "agg": {"tos": "SUM(outstanding)"}}
  ]
}
```

### Adding New Metrics

1. Add metric definition to `metadata/metrics.json`
2. Add business label to `metadata/business_labels.json`
3. Create rules in `metadata/rules.json` for each system
4. Update table mappings if needed

