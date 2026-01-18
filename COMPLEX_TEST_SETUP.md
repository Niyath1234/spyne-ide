# Complex Multi-Table Reconciliation Test Setup

## Test Scenario Overview

This test validates the RCA Engine's ability to handle highly complex reconciliation scenarios:

### Requirements:
1. **System A**: 5 tables, customer-level data (no loan-level), needs aggregation
2. **System B**: 4 tables, loan-level data, needs to build grain  
3. **Complex TOS formulas**: Each system has different formulas requiring multiple tables
4. **Multi-loan customers**: Each customer can have multiple loans

## Test Structure Created

### Metadata Files (`metadata/complex_multi_table_test/`)

1. **tables.json** - Defines 9 tables:
   - System A (5 tables):
     - `customer_master_a` - Customer master data
     - `customer_accounts_a` - Account-level data
     - `customer_transactions_a` - Transaction-level data
     - `customer_balances_a` - Balance snapshots
     - `customer_summary_a` - Aggregated customer summaries
   - System B (4 tables):
     - `loan_master_b` - Loan master data
     - `loan_disbursements_b` - Disbursement records
     - `loan_repayments_b` - Repayment records
     - `loan_outstanding_b` - Outstanding balances

2. **rules.json** - Defines complex TOS rules:
   - **System A TOS**: `SUM(account_balance) + SUM(transaction_amount) - SUM(writeoff_amount) WHERE account_status = 'active'`
     - Requires joining: customer_accounts_a, customer_transactions_a, customer_summary_a
   - **System B TOS**: `SUM(disbursed_amount) - SUM(repaid_amount) + SUM(interest_accrued) - SUM(penalty_waived) WHERE loan_status IN ('active', 'overdue')`
     - Requires joining: loan_disbursements_b, loan_repayments_b, loan_outstanding_b

3. **entities.json** - Defines customer and loan entities

4. **lineage.json** - Defines join relationships between tables

5. **business_labels.json** - System and metric labels

6. **exceptions.json** - Business exceptions (empty)

### Test Data Files (`data/complex_multi_table_test/`)

Created CSV files for all 9 tables with realistic test data:
- 5 customers (CUST001-CUST005)
- Multiple accounts per customer
- Multiple transactions per customer
- 7 loans across the customers
- Multiple disbursements and repayments per loan
- Outstanding balances with interest and penalties

## Current Status

✅ Metadata structure created
✅ Test data files created  
✅ Test script created (`tests/test_complex_multi_table_reconciliation.rs`)
⚠️  Metadata needs `path` fields added to tables.json (in progress)

## Next Steps to Complete Test

1. Add `path` fields to tables.json pointing to CSV files
2. Fix any remaining metadata format issues
3. Run the test to verify:
   - Metadata loading
   - Intent compilation
   - Task grounding
   - RCA execution

## Expected Capabilities Demonstrated

This test will demonstrate that the RCA Engine can:
1. ✅ Handle multi-table systems (5 and 4 tables respectively)
2. ✅ Process different grain levels (customer vs loan)
3. ✅ Understand complex formulas requiring multiple table joins
4. ✅ Aggregate customer-level data to compare with loan-level data
5. ✅ Build grain from loan-level to customer-level for reconciliation
6. ✅ Handle one-to-many relationships (customers with multiple loans)

## Test Query

```
"Why is TOS (Total Outstanding) different between system A and system B?"
```

This query will trigger:
- Intent compilation to extract systems and metric
- Task grounding to find relevant tables and rules
- Execution planning to join multiple tables
- Data extraction and aggregation
- Row-level comparison
- Root cause analysis

