#!/bin/bash

# RCA Engine Metadata Loader
# This script loads all metadata files (tables, rules, lineage, entities, labels, metrics) for the RCA Engine

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     RCA Engine Metadata Loader                            ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

METADATA_DIR="$SCRIPT_DIR/metadata"

# Step 1: Create metadata directory if it doesn't exist
echo -e "${YELLOW}[1/7]${NC} Setting up metadata directory..."
mkdir -p "$METADATA_DIR"
echo -e "${GREEN}✅ Metadata directory ready${NC}"
echo ""

# Step 2: Load tables metadata
echo -e "${YELLOW}[2/7]${NC} Loading tables metadata..."
cat > "$METADATA_DIR/tables.json" << 'TABLES_EOF'
{
  "tables": [
    {
      "name": "repayments",
      "entity": "payment",
      "primary_key": ["repay_id"],
      "time_column": "created_at",
      "system": "system_a",
      "path": "data/repayments/repayments.csv",
      "description": "Repayment transactions from System A. Contains all repayment records with user_uuid, repay_id, total_amount, payment status, and transaction details. Used for recovery reconciliation.",
      "grain": ["repay_id"],
      "labels": ["payments", "recovery", "reconciliation", "transactions"],
      "columns": [
        {"name": "repay_id", "data_type": "string", "description": "Unique repayment transaction ID"},
        {"name": "user_uuid", "data_type": "string", "description": "User UUID identifier for the customer"},
        {"name": "total_amount", "data_type": "decimal", "description": "Total payment amount in base currency"},
        {"name": "repayable_amount", "data_type": "decimal", "description": "Amount that was repayable"},
        {"name": "created_at", "data_type": "timestamp", "description": "Payment creation timestamp"},
        {"name": "status", "data_type": "string", "description": "Payment status (PENDING, IN_PROGRESS, SUCCESS, FAILED)"},
        {"name": "repayment_type", "data_type": "string", "description": "Type of repayment (ONLINE, OFFLINE)"},
        {"name": "payment_order_id", "data_type": "string", "description": "Payment gateway order ID"},
        {"name": "__is_deleted", "data_type": "boolean", "description": "Soft delete flag"}
      ]
    },
    {
      "name": "lmsdata_emi_payment_view",
      "entity": "payment",
      "primary_key": ["id"],
      "time_column": "payment_date",
      "system": "system_b",
      "path": "data/emi_payments/lmsdata_emi_payment_view.csv",
      "description": "EMI payment data from System B (LMS). Contains detailed EMI payment records with uuid, repayment_txn_id, paid_amount, principal, interest, and late penalty components. Used for recovery reconciliation.",
      "grain": ["id"],
      "labels": ["payments", "recovery", "reconciliation", "emi", "lms"],
      "columns": [
        {"name": "uuid", "data_type": "string", "description": "User UUID identifier"},
        {"name": "repayment_txn_id", "data_type": "string", "description": "Repayment transaction ID (maps to repayments.repay_id)"},
        {"name": "paid_amount", "data_type": "decimal", "description": "Total amount paid"},
        {"name": "paidAmountMicro", "data_type": "decimal", "description": "Amount paid in micro units"},
        {"name": "payment_date", "data_type": "date", "description": "Payment date"},
        {"name": "principal_paid", "data_type": "decimal", "description": "Principal amount paid"},
        {"name": "interest_paid", "data_type": "decimal", "description": "Interest amount paid"},
        {"name": "late_interest_paid", "data_type": "decimal", "description": "Late interest paid"},
        {"name": "order_id", "data_type": "string", "description": "Order ID associated with payment"},
        {"name": "is_deleted", "data_type": "boolean", "description": "Soft delete flag"}
      ]
    },
    {
      "name": "current_month_collection_report",
      "entity": "collection",
      "primary_key": ["uuid"],
      "time_column": "paid_date",
      "system": "system_a",
      "path": "data/collections_mis/current_month_collection_report.csv",
      "description": "Collection report from System A for current month. Contains uuid, paid_amount, paid_date, mis_date, bucket information, and collection team details. Used for collections MIS and recovery tracking.",
      "grain": ["uuid"],
      "labels": ["collections", "recovery", "reconciliation", "mis", "bucket"],
      "columns": [
        {"name": "uuid", "data_type": "string", "description": "User UUID identifier"},
        {"name": "paid_amount", "data_type": "decimal", "description": "Amount paid"},
        {"name": "paid_date", "data_type": "date", "description": "Payment date"},
        {"name": "mis_date", "data_type": "date", "description": "MIS date for reporting"},
        {"name": "current_bucket", "data_type": "string", "description": "Current delinquency bucket"},
        {"name": "dpd", "data_type": "integer", "description": "Days past due"},
        {"name": "emi", "data_type": "decimal", "description": "EMI amount"},
        {"name": "pos", "data_type": "decimal", "description": "Principal outstanding"}
      ]
    },
    {
      "name": "outstanding_daily",
      "entity": "loan",
      "primary_key": ["uuid", "order_id", "last_day"],
      "time_column": "last_day",
      "system": "system_a",
      "path": "data/outstanding_daily/outstanding_daily.csv",
      "description": "Daily outstanding snapshot from System A. Contains loan-level outstanding amounts, DPD, arrears, NPA flags, and securitization information. Used for outstanding reconciliation and NPA tracking.",
      "grain": ["uuid", "order_id", "last_day"],
      "labels": ["outstanding", "loan", "npa", "securitization", "arrears"],
      "columns": [
        {"name": "uuid", "data_type": "string", "description": "User UUID identifier"},
        {"name": "order_id", "data_type": "string", "description": "Order ID"},
        {"name": "loan_id", "data_type": "string", "description": "Loan ID"},
        {"name": "last_day", "data_type": "date", "description": "Snapshot date"},
        {"name": "principal_outstanding", "data_type": "decimal", "description": "Principal outstanding amount"},
        {"name": "interest_outstanding", "data_type": "decimal", "description": "Interest outstanding amount"},
        {"name": "dpd", "data_type": "integer", "description": "Days past due"},
        {"name": "writeoff_flag", "data_type": "boolean", "description": "Writeoff flag"},
        {"name": "securitization_flag", "data_type": "boolean", "description": "Securitization flag"}
      ]
    },
    {
      "name": "da_orders",
      "entity": "order",
      "primary_key": ["order_id", "uuid"],
      "time_column": "da_date",
      "system": "system_a",
      "path": "data/da_orders/da_orders.csv",
      "description": "DA (Delegation of Authority) orders from System A. Contains order_id, uuid, securitization_nbfc, da_date, and POS on DA date. Used for securitization tracking and order management.",
      "grain": ["order_id", "uuid"],
      "labels": ["orders", "securitization", "da", "delegation"],
      "columns": [
        {"name": "order_id", "data_type": "string", "description": "Order ID"},
        {"name": "uuid", "data_type": "string", "description": "User UUID identifier"},
        {"name": "securitization_nbfc", "data_type": "string", "description": "Securitization NBFC name"},
        {"name": "da_date", "data_type": "date", "description": "Delegation of Authority date"},
        {"name": "pos_on_da_date", "data_type": "decimal", "description": "Principal outstanding on DA date"}
      ]
    },
    {
      "name": "provisional_writeoff",
      "entity": "writeoff",
      "primary_key": ["order_id", "uuid"],
      "time_column": "writeoff_date",
      "system": "system_a",
      "path": "data/provisional_writeoff/provisional_writeoff.csv",
      "description": "Provisional writeoff records from System A. Contains order_id, uuid, colending_flag, NPA start date, outstanding amounts, and writeoff date. Used for writeoff tracking and NPA management.",
      "grain": ["order_id", "uuid"],
      "labels": ["writeoff", "npa", "provisional", "colending"],
      "columns": [
        {"name": "order_id", "data_type": "string", "description": "Order ID"},
        {"name": "uuid", "data_type": "string", "description": "User UUID identifier"},
        {"name": "colending_flag", "data_type": "boolean", "description": "Co-lending flag"},
        {"name": "npa_start_date_irac", "data_type": "date", "description": "NPA start date as per IRAC norms"},
        {"name": "principal_outstanding", "data_type": "decimal", "description": "Principal outstanding at writeoff"},
        {"name": "interest_outstanding", "data_type": "decimal", "description": "Interest outstanding at writeoff"},
        {"name": "writeoff_date", "data_type": "date", "description": "Writeoff date"}
      ]
    },
    {
      "name": "writeoff_users",
      "entity": "writeoff",
      "primary_key": ["uuid", "order_id"],
      "time_column": "writeoff_date",
      "system": "system_a",
      "path": "data/writeoff_users/writeoff_users.csv",
      "description": "User-level writeoff records from System A. Contains uuid, order_id, NBFC name, DPD, writeoff amount, request date, flag, and writeoff date. Used for writeoff user tracking and reporting.",
      "grain": ["uuid", "order_id"],
      "labels": ["writeoff", "users", "nbfc", "dpd"],
      "columns": [
        {"name": "uuid", "data_type": "string", "description": "User UUID identifier"},
        {"name": "order_id", "data_type": "string", "description": "Order ID"},
        {"name": "nbfc_name", "data_type": "string", "description": "NBFC name"},
        {"name": "dpd", "data_type": "integer", "description": "Days past due"},
        {"name": "amt", "data_type": "decimal", "description": "Writeoff amount"},
        {"name": "req_date", "data_type": "date", "description": "Writeoff request date"},
        {"name": "flag", "data_type": "string", "description": "Writeoff flag"},
        {"name": "writeoff_date", "data_type": "date", "description": "Writeoff date"}
      ]
    }
  ]
}
TABLES_EOF
echo -e "${GREEN}✅ Tables metadata loaded (7 tables)${NC}"
echo ""

# Step 3: Load business rules
echo -e "${YELLOW}[3/7]${NC} Loading business rules..."
cat > "$METADATA_DIR/rules.json" << 'RULES_EOF'
[
  {
    "id": "system_a_recovery_rule",
    "system": "system_a",
    "metric": "recovery",
    "target_entity": "payment",
    "target_grain": ["uuid"],
    "description": "Recovery calculation from System A repayments. Always exclude deleted records (__is_deleted = false). Only include successful or in-progress payments. Exclude failed/cancelled payments. Maps user_uuid to uuid, total_amount to paid_amount, created_at to paid_date.",
    "computation": {
      "description": "Recovery calculation from System A repayments table",
      "source_entities": ["payment"],
      "source_table": "repayments",
      "attributes_needed": {
        "payment": ["user_uuid", "total_amount", "created_at"]
      },
      "formula": "sum(total_amount)",
      "aggregation_grain": ["user_uuid"],
      "filter_conditions": {
        "created_at": "IS NOT NULL",
        "total_amount": "IS NOT NULL",
        "__is_deleted": "= false",
        "status": "IN ('SUCCESS', 'IN_PROGRESS')"
      },
      "note": "Maps user_uuid to uuid, total_amount to paid_amount, created_at to paid_date. Excludes deleted and failed payments."
    },
    "labels": ["recovery", "payments", "system_a"]
  },
  {
    "id": "system_b_recovery_rule",
    "system": "system_b",
    "metric": "recovery",
    "target_entity": "payment",
    "target_grain": ["uuid"],
    "description": "Recovery calculation from System B EMI payments. Always exclude deleted records (is_deleted = false). Only include records with valid payment_date and paid_amount. Direct mapping: uuid, paid_amount, payment_date.",
    "computation": {
      "description": "Recovery calculation from System B EMI payments",
      "source_entities": ["payment"],
      "source_table": "lmsdata_emi_payment_view",
      "attributes_needed": {
        "payment": ["uuid", "paid_amount", "payment_date"]
      },
      "formula": "sum(paid_amount)",
      "aggregation_grain": ["uuid"],
      "filter_conditions": {
        "payment_date": "IS NOT NULL",
        "paid_amount": "IS NOT NULL",
        "is_deleted": "= false"
      },
      "note": "Direct mapping: uuid, paid_amount, payment_date. Excludes deleted records."
    },
    "labels": ["recovery", "payments", "system_b"]
  },
  {
    "id": "system_a_collections_recovery_rule",
    "system": "system_a",
    "metric": "recovery",
    "target_entity": "collection",
    "target_grain": ["uuid"],
    "description": "Recovery calculation from System A collections MIS report. Only include records with valid paid_date and paid_amount. This is the collections team's view of recovery. Uses uuid, paid_amount, paid_date.",
    "computation": {
      "description": "Recovery calculation from System A collections report",
      "source_entities": ["collection"],
      "source_table": "current_month_collection_report",
      "attributes_needed": {
        "collection": ["uuid", "paid_amount", "paid_date"]
      },
      "formula": "sum(paid_amount)",
      "aggregation_grain": ["uuid"],
      "filter_conditions": {
        "paid_date": "IS NOT NULL",
        "paid_amount": "IS NOT NULL"
      },
      "note": "Collections MIS recovery calculation. Uses uuid, paid_amount, paid_date."
    },
    "labels": ["recovery", "collections", "system_a", "mis"]
  },
  {
    "id": "system_a_outstanding_rule",
    "system": "system_a",
    "metric": "outstanding",
    "target_entity": "loan",
    "target_grain": ["uuid", "order_id"],
    "description": "Outstanding calculation from System A daily snapshot. Always exclude provisional writeoff (check provisional_writeoff table). Always exclude writeoff users (check writeoff_users table). Only include records with valid outstanding amounts. Total outstanding = principal_outstanding + interest_outstanding. Snapshot at last_day grain.",
    "computation": {
      "description": "Outstanding calculation from System A daily snapshot",
      "source_entities": ["loan"],
      "source_table": "outstanding_daily",
      "attributes_needed": {
        "loan": ["uuid", "order_id", "principal_outstanding", "interest_outstanding", "last_day"]
      },
      "formula": "sum(principal_outstanding + interest_outstanding)",
      "aggregation_grain": ["uuid", "order_id"],
      "filter_conditions": {
        "last_day": "IS NOT NULL",
        "principal_outstanding": "IS NOT NULL",
        "interest_outstanding": "IS NOT NULL"
      },
      "exclusions": [
        "LEFT JOIN provisional_writeoff ON outstanding_daily.uuid = provisional_writeoff.uuid AND outstanding_daily.order_id = provisional_writeoff.order_id WHERE provisional_writeoff.uuid IS NULL",
        "LEFT JOIN writeoff_users ON outstanding_daily.uuid = writeoff_users.uuid AND outstanding_daily.order_id = writeoff_users.order_id WHERE writeoff_users.uuid IS NULL"
      ],
      "note": "Total outstanding = principal + interest. Snapshot at last_day grain. Excludes provisional writeoff and writeoff users."
    },
    "labels": ["outstanding", "loan", "system_a"]
  },
  {
    "id": "system_a_writeoff_provisional_rule",
    "system": "system_a",
    "metric": "writeoff",
    "target_entity": "writeoff",
    "target_grain": ["uuid", "order_id"],
    "description": "Provisional writeoff calculation from System A. This represents loans marked for writeoff but not yet finalized. Provisional writeoff amount = principal_outstanding + interest_outstanding at writeoff date. Only include records with valid writeoff_date and outstanding amounts.",
    "computation": {
      "description": "Provisional writeoff calculation from System A",
      "source_entities": ["writeoff"],
      "source_table": "provisional_writeoff",
      "attributes_needed": {
        "writeoff": ["uuid", "order_id", "principal_outstanding", "interest_outstanding", "writeoff_date"]
      },
      "formula": "sum(principal_outstanding + interest_outstanding)",
      "aggregation_grain": ["uuid", "order_id"],
      "filter_conditions": {
        "writeoff_date": "IS NOT NULL",
        "principal_outstanding": "IS NOT NULL",
        "interest_outstanding": "IS NOT NULL"
      },
      "note": "Provisional writeoff amount = principal + interest outstanding at writeoff date."
    },
    "labels": ["writeoff", "provisional", "system_a"]
  },
  {
    "id": "system_a_writeoff_users_rule",
    "system": "system_a",
    "metric": "writeoff",
    "target_entity": "writeoff",
    "target_grain": ["uuid", "order_id"],
    "description": "User writeoff calculation from System A. This represents finalized writeoff amounts at user level. Always exclude writeoff users from outstanding calculations. Only include records with valid writeoff_date and amount. User-level writeoff amount from writeoff_users table.",
    "computation": {
      "description": "User writeoff calculation from System A",
      "source_entities": ["writeoff"],
      "source_table": "writeoff_users",
      "attributes_needed": {
        "writeoff": ["uuid", "order_id", "amt", "writeoff_date"]
      },
      "formula": "sum(amt)",
      "aggregation_grain": ["uuid", "order_id"],
      "filter_conditions": {
        "writeoff_date": "IS NOT NULL",
        "amt": "IS NOT NULL"
      },
      "note": "User-level writeoff amount from writeoff_users table. Always exclude from outstanding calculations."
    },
    "labels": ["writeoff", "users", "system_a"]
  },
  {
    "id": "system_a_da_orders_rule",
    "system": "system_a",
    "metric": "da_pos",
    "target_entity": "order",
    "target_grain": ["order_id", "uuid"],
    "description": "DA (Delegation of Authority) orders POS calculation from System A. Always exclude provisional writeoff. Always exclude writeoff users. Principal outstanding on delegation of authority date. Used for securitization tracking. Only include records with valid da_date and pos_on_da_date.",
    "computation": {
      "description": "DA orders POS calculation from System A",
      "source_entities": ["order"],
      "source_table": "da_orders",
      "attributes_needed": {
        "order": ["order_id", "uuid", "pos_on_da_date", "da_date"]
      },
      "formula": "sum(pos_on_da_date)",
      "aggregation_grain": ["order_id", "uuid"],
      "filter_conditions": {
        "da_date": "IS NOT NULL",
        "pos_on_da_date": "IS NOT NULL"
      },
      "exclusions": [
        "LEFT JOIN provisional_writeoff ON da_orders.uuid = provisional_writeoff.uuid AND da_orders.order_id = provisional_writeoff.order_id WHERE provisional_writeoff.uuid IS NULL",
        "LEFT JOIN writeoff_users ON da_orders.uuid = writeoff_users.uuid AND da_orders.order_id = writeoff_users.order_id WHERE writeoff_users.uuid IS NULL"
      ],
      "note": "Principal outstanding on delegation of authority date. Excludes provisional writeoff and writeoff users."
    },
    "labels": ["da", "orders", "securitization", "system_a"]
  },
  {
    "id": "system_b_principal_paid_rule",
    "system": "system_b",
    "metric": "principal_recovery",
    "target_entity": "payment",
    "target_grain": ["uuid"],
    "description": "Principal recovery from System B EMI payments. Always exclude deleted records (is_deleted = false). Only include records with valid payment_date and principal_paid amount. Principal amount recovered from EMI payments.",
    "computation": {
      "description": "Principal recovery from System B EMI payments",
      "source_entities": ["payment"],
      "source_table": "lmsdata_emi_payment_view",
      "attributes_needed": {
        "payment": ["uuid", "principal_paid", "payment_date"]
      },
      "formula": "sum(principal_paid)",
      "aggregation_grain": ["uuid"],
      "filter_conditions": {
        "payment_date": "IS NOT NULL",
        "principal_paid": "IS NOT NULL",
        "is_deleted": "= false"
      },
      "note": "Principal amount recovered from EMI payments. Excludes deleted records."
    },
    "labels": ["recovery", "principal", "payments", "system_b"]
  },
  {
    "id": "system_b_interest_paid_rule",
    "system": "system_b",
    "metric": "interest_recovery",
    "target_entity": "payment",
    "target_grain": ["uuid"],
    "description": "Interest recovery from System B EMI payments. Always exclude deleted records (is_deleted = false). Only include records with valid payment_date and interest_paid amount. Interest amount recovered from EMI payments.",
    "computation": {
      "description": "Interest recovery from System B EMI payments",
      "source_entities": ["payment"],
      "source_table": "lmsdata_emi_payment_view",
      "attributes_needed": {
        "payment": ["uuid", "interest_paid", "payment_date"]
      },
      "formula": "sum(interest_paid)",
      "aggregation_grain": ["uuid"],
      "filter_conditions": {
        "payment_date": "IS NOT NULL",
        "interest_paid": "IS NOT NULL",
        "is_deleted": "= false"
      },
      "note": "Interest amount recovered from EMI payments. Excludes deleted records."
    },
    "labels": ["recovery", "interest", "payments", "system_b"]
  }
]
RULES_EOF
echo -e "${GREEN}✅ Business rules loaded (9 rules)${NC}"
echo ""

# Step 4: Load exceptions (required by metadata loader)
echo -e "${YELLOW}[4/7]${NC} Loading exceptions..."
cat > "$METADATA_DIR/exceptions.json" << 'EXCEPTIONS_EOF'
{
  "exceptions": []
}
EXCEPTIONS_EOF
echo -e "${GREEN}✅ Exceptions loaded${NC}"
echo ""

# Step 5: Load lineage (joins)
echo -e "${YELLOW}[5/7]${NC} Loading lineage and join relationships..."
cat > "$METADATA_DIR/lineage.json" << 'LINEAGE_EOF'
[
  {
    "type": "edge",
    "from": "repayments",
    "to": "lmsdata_emi_payment_view",
    "keys": {
      "repay_id": "repayment_txn_id"
    },
    "relationship": "one_to_many",
    "description": "Join repayments to EMI payments via repayment transaction ID"
  },
  {
    "type": "edge",
    "from": "repayments",
    "to": "current_month_collection_report",
    "keys": {
      "user_uuid": "uuid"
    },
    "relationship": "many_to_one",
    "description": "Join repayments to collections report via user UUID"
  },
  {
    "type": "edge",
    "from": "outstanding_daily",
    "to": "da_orders",
    "keys": {
      "order_id": "order_id",
      "uuid": "uuid"
    },
    "relationship": "one_to_one",
    "description": "Join outstanding daily to DA orders via order_id and uuid"
  },
  {
    "type": "edge",
    "from": "outstanding_daily",
    "to": "provisional_writeoff",
    "keys": {
      "order_id": "order_id",
      "uuid": "uuid"
    },
    "relationship": "one_to_one",
    "description": "Join outstanding daily to provisional writeoff via order_id and uuid"
  },
  {
    "type": "edge",
    "from": "outstanding_daily",
    "to": "writeoff_users",
    "keys": {
      "order_id": "order_id",
      "uuid": "uuid"
    },
    "relationship": "one_to_one",
    "description": "Join outstanding daily to writeoff users via order_id and uuid"
  },
  {
    "type": "edge",
    "from": "provisional_writeoff",
    "to": "writeoff_users",
    "keys": {
      "order_id": "order_id",
      "uuid": "uuid"
    },
    "relationship": "one_to_one",
    "description": "Join provisional writeoff to writeoff users via order_id and uuid"
  },
  {
    "type": "edge",
    "from": "lmsdata_emi_payment_view",
    "to": "outstanding_daily",
    "keys": {
      "uuid": "uuid",
      "order_id": "order_id"
    },
    "relationship": "many_to_one",
    "description": "Join EMI payments to outstanding daily via uuid and order_id"
  },
  {
    "type": "edge",
    "from": "current_month_collection_report",
    "to": "outstanding_daily",
    "keys": {
      "uuid": "uuid"
    },
    "relationship": "many_to_one",
    "description": "Join collections report to outstanding daily via uuid"
  }
]
LINEAGE_EOF
echo -e "${GREEN}✅ Lineage loaded (8 joins)${NC}"
echo ""

# Step 6: Load entities (column mappings)
echo -e "${YELLOW}[6/7]${NC} Loading entity mappings..."
cat > "$METADATA_DIR/entities.json" << 'ENTITIES_EOF'
{
  "entities": [
    {
      "name": "payment",
      "canonical_columns": {
        "uuid": ["user_uuid", "uuid"],
        "paid_amount": ["total_amount", "paid_amount", "paidAmountMicro"],
        "paid_date": ["created_at", "payment_date", "paid_date"],
        "repayment_id": ["repay_id", "repayment_txn_id", "repaymentTxnId"],
        "principal_paid": ["principal_paid", "principalPaidMicro"],
        "interest_paid": ["interest_paid", "interestPaidMicro"],
        "status": ["status", "transient_status"]
      },
      "description": "Payment entity with canonical column mappings across systems"
    },
    {
      "name": "loan",
      "canonical_columns": {
        "uuid": ["uuid"],
        "order_id": ["order_id"],
        "loan_id": ["loan_id"],
        "principal_outstanding": ["principal_outstanding", "pos_on_da_date"],
        "interest_outstanding": ["interest_outstanding"],
        "total_outstanding": ["principal_outstanding + interest_outstanding"],
        "dpd": ["dpd"],
        "snapshot_date": ["last_day", "da_date"]
      },
      "description": "Loan entity with outstanding and DPD information"
    },
    {
      "name": "writeoff",
      "canonical_columns": {
        "uuid": ["uuid"],
        "order_id": ["order_id"],
        "writeoff_amount": ["amt", "principal_outstanding + interest_outstanding"],
        "writeoff_date": ["writeoff_date"],
        "npa_start_date": ["npa_start_date_irac"],
        "principal_outstanding": ["principal_outstanding"],
        "interest_outstanding": ["interest_outstanding"]
      },
      "description": "Writeoff entity with provisional and user-level writeoff data"
    },
    {
      "name": "collection",
      "canonical_columns": {
        "uuid": ["uuid"],
        "paid_amount": ["paid_amount"],
        "paid_date": ["paid_date"],
        "mis_date": ["mis_date"],
        "bucket": ["current_bucket", "source_bucket"],
        "dpd": ["dpd"]
      },
      "description": "Collection entity with MIS and bucket information"
    },
    {
      "name": "order",
      "canonical_columns": {
        "order_id": ["order_id"],
        "uuid": ["uuid"],
        "da_date": ["da_date"],
        "pos_on_da_date": ["pos_on_da_date"],
        "securitization_nbfc": ["securitization_nbfc"]
      },
      "description": "Order entity with DA and securitization information"
    }
  ]
}
ENTITIES_EOF
echo -e "${GREEN}✅ Entity mappings loaded${NC}"
echo ""

# Step 7: Load business labels
echo -e "${YELLOW}[7/7]${NC} Loading business labels..."
cat > "$METADATA_DIR/business_labels.json" << 'LABELS_EOF'
{
  "labels": [
    {
      "name": "recovery",
      "description": "Recovery/payment amounts",
      "applies_to": ["tables", "rules", "metrics"]
    },
    {
      "name": "payments",
      "description": "Payment transactions",
      "applies_to": ["tables", "rules"]
    },
    {
      "name": "collections",
      "description": "Collection reports and data",
      "applies_to": ["tables"]
    },
    {
      "name": "digital",
      "description": "Digital loan products",
      "applies_to": ["tables", "rules"]
    },
    {
      "name": "reconciliation",
      "description": "Reconciliation between systems",
      "applies_to": ["tables", "rules", "metrics"]
    },
    {
      "name": "outstanding",
      "description": "Outstanding loan amounts",
      "applies_to": ["tables", "rules", "metrics"]
    },
    {
      "name": "writeoff",
      "description": "Writeoff records",
      "applies_to": ["tables", "rules"]
    },
    {
      "name": "npa",
      "description": "Non-performing assets",
      "applies_to": ["tables", "rules"]
    },
    {
      "name": "securitization",
      "description": "Securitization data",
      "applies_to": ["tables", "rules"]
    }
  ]
}
LABELS_EOF
echo -e "${GREEN}✅ Business labels loaded${NC}"
echo ""

# Step 8: Load metrics
echo -e "${YELLOW}[8/8]${NC} Loading metrics..."
cat > "$METADATA_DIR/metrics.json" << 'METRICS_EOF'
[
  {
    "id": "recovery",
    "name": "Recovery Amount",
    "description": "Total recovery amount (paid_amount) from payments",
    "grain": ["uuid"],
    "precision": 2,
    "null_policy": "zero",
    "unit": "currency",
    "labels": ["recovery", "payments", "reconciliation"],
    "versions": [
      {
        "version": "v1",
        "description": "Standard recovery calculation as sum of paid_amount"
      }
    ]
  },
  {
    "id": "outstanding",
    "name": "Outstanding Amount",
    "description": "Total outstanding amount (principal + interest)",
    "grain": ["uuid", "order_id"],
    "precision": 2,
    "null_policy": "zero",
    "unit": "currency",
    "labels": ["outstanding", "loan"],
    "versions": [
      {
        "version": "v1",
        "description": "Standard outstanding calculation excluding writeoffs"
      }
    ]
  },
  {
    "id": "writeoff",
    "name": "Writeoff Amount",
    "description": "Total writeoff amount",
    "grain": ["uuid", "order_id"],
    "precision": 2,
    "null_policy": "zero",
    "unit": "currency",
    "labels": ["writeoff", "npa"],
    "versions": [
      {
        "version": "v1",
        "description": "Writeoff calculation from provisional and user tables"
      }
    ]
  }
]
METRICS_EOF
echo -e "${GREEN}✅ Metrics loaded${NC}"
echo ""

# Summary
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    Summary                                ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}✅ Metadata loaded successfully!${NC}"
echo ""
echo -e "📊 Tables: ${GREEN}7${NC}"
echo -e "📋 Rules: ${GREEN}9${NC}"
echo -e "🔗 Joins: ${GREEN}8${NC}"
echo -e "📝 Entities: ${GREEN}5${NC}"
echo -e "🏷️  Labels: ${GREEN}9${NC}"
echo -e "📈 Metrics: ${GREEN}3${NC}"
echo ""
echo -e "${YELLOW}Metadata files created in:${NC} ${BLUE}$METADATA_DIR${NC}"
echo ""
echo -e "${GREEN}Metadata is ready! 🚀${NC}"
