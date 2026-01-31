# CKO Invariants - System-Wide Rules

This document defines the **non-negotiable invariants** that prevent the three critical risks from resurfacing.

## The Five Invariants

### 1. WorldState is the only authority on meaning

**What this means:**
- Only `components/WorldState/` may define or mutate:
  - table schema
  - table lifecycle state
  - joins
  - contracts
  - global rules
  - lineage

**Enforcement:**
- All Python code MUST use `backend/cko_client.py` to request changes
- Direct writes to metadata stores are forbidden
- `backend/stores/table_store.py` → becomes read-only adapter
- `backend/stores/contract_store.py` → becomes CKO client wrapper
- `backend/metadata_provider.py` → becomes read-only cache

**Code locations:**
- `backend/cko_client.py` - Single write API
- `components/WorldState/` - Canonical truth

---

### 2. Python never generates executable SQL

**What this means:**
- Python (`backend/`) may only extract intent
- Rust (`rust/core/`) owns planning, validation, and SQL generation
- There is exactly one logical plan format: `rust/core/engine/logical_plan.rs`
- There is exactly one SQL renderer: Rust

**Enforcement:**
- `backend/sql_builder.py` → DELETE or mark as preview-only (never executed)
- `backend/llm_query_generator.py` → Output intent only (no SQL)
- Python planning outputs intent JSON only:
  ```json
  {
    "intent": "top customers by revenue",
    "entities": ["customer", "order"],
    "constraints": ["last 30 days"],
    "preferences": ["left join"]
  }
  ```

**Code locations:**
- `rust/core/engine/logical_plan.rs` - Single logical plan format
- `rust/compiler/` - Single SQL renderer
- `rust/execution_loop/` - Single execution path

---

### 3. Ingestion never writes to ACTIVE tables

**What this means:**
- All ingestion MUST land in `SHADOW` state
- There is NO code path that allows ingestion → ACTIVE
- Promotion is the ONLY bridge to user-visible data

**Enforcement:**
- `backend/api/ingestion.py` → Requires admin + `INGESTION_ENABLED=true`
- `rust/ingestion/orchestrator.rs` → Enforces SHADOW state
- `backend/stores/contract_store.py` → Creates contracts in SHADOW state
- Production default: `INGESTION_ENABLED=false`

**Code locations:**
- `backend/api/ingestion.py` - Admin-only gate
- `rust/ingestion/orchestrator.rs` - SHADOW enforcement
- `backend/api/table_state.py` - Promotion API (separate from ingestion)

---

### 4. Promotion is the only path to user-visible change

**What this means:**
- Promotion requires admin role
- Promotion requires explicit confirmation
- Promotion is logged and auditable
- Promotion is reversible (within 24 hours)

**Enforcement:**
- `POST /api/tables/{table_id}/promote` → Admin-only endpoint
- Requires validation summary and diff preview
- Not callable from ingestion code
- Creates audit trail

**Code locations:**
- `backend/api/table_state.py` - Promotion endpoint
- `backend/stores/table_store.py` - Promotion logic

---

### 5. All irreversible actions are explicit and auditable

**What this means:**
- Every state change is logged
- Every promotion is logged with user, timestamp, reason
- Rollback information is preserved
- Audit trail is queryable

**Enforcement:**
- All state changes go through `table_store.promote_table()`
- All changes record `changed_by` and `created_at`
- State history table tracks all transitions
- Logs include warnings for dangerous operations

**Code locations:**
- `backend/stores/table_store.py` - State history tracking
- `backend/api/table_state.py` - Audit logging

---

## How These Fixes Work Together

| Risk | Structural Fix | Invariant |
|------|---------------|-----------|
| Too many truths | Single CKO (WorldState) | #1 |
| Duplicate planning | Intent (Python) / Plan (Rust) | #2 |
| Accidental ingestion | Shadow + Promotion gate | #3, #4 |

They reinforce each other. None work alone.

---

## Verification Checklist

To verify these invariants are enforced:

- [ ] `grep -r "register_table\|register_contract" backend/` → Only in `cko_client.py`
- [ ] `grep -r "SELECT\|INSERT\|UPDATE" backend/sql_builder.py` → None (or preview-only)
- [ ] `grep -r "state.*ACTIVE\|ACTIVE.*state" rust/ingestion/` → None (only SHADOW)
- [ ] `grep -r "INGESTION_ENABLED" backend/api/ingestion.py` → Present and checked
- [ ] `grep -r "can_promote\|ADMIN" backend/api/table_state.py` → Present and checked

---

## Breaking These Invariants

If you find code that violates these invariants:

1. **DO NOT** "fix" it by adding more code
2. **DO** delete or route the violating code through the correct path
3. **DO** add a test that prevents regression

These invariants are **structural**, not **advisory**.

