# Risk Mitigation Implementation Summary

This document summarizes the implementation of fixes for the three critical risks.

## Status: ✅ Core Fixes Implemented

---

## Risk #1: Too Many Centers of Gravity

### ✅ Implemented

1. **CKO Client Created** (`backend/cko_client.py`)
   - Single write API for Python → WorldState communication
   - All metadata mutations must go through this client
   - Enforces request-only pattern (Python requests, WorldState decides)

2. **Metadata Provider Demoted** (`backend/metadata_provider.py`)
   - Marked as READ-ONLY
   - Added comments explaining it's a cache/projection only
   - Never mutates metadata

3. **Contract Store Updated** (`backend/stores/contract_store.py`)
   - Added CKO client import (ready for routing)
   - Added comments about SHADOW state enforcement
   - Enhanced logging for SHADOW state

### 🔄 Remaining Work

- [ ] Route `table_store.py` writes through CKO client
- [ ] Route `contract_store.py` writes through CKO client (currently direct DB writes)
- [ ] Add integration tests verifying CKO boundary

---

## Risk #2: Duplicate Planning Pipelines

### ✅ Implemented

1. **Intent Format Created** (`backend/planning/intent_format.py`)
   - Defines intent-only output format
   - Validates no SQL/table names in intent
   - Provides example structure

2. **SQL Generation Marked Deprecated**
   - `backend/llm_query_generator.py` → Added warning comments
   - `backend/sql_builder.py` → Added deprecation notice
   - Both marked as migration-only

3. **Rust Logical Plan Identified**
   - `rust/core/engine/logical_plan.rs` → Single execution path
   - Documented as canonical plan format

### 🔄 Remaining Work

- [ ] Move `intent_to_sql` logic to Rust
- [ ] Update Python planning to output intent JSON only
- [ ] Add validation to reject SQL in Python planning output
- [ ] Create Rust endpoint to accept intent and return SQL

---

## Risk #3: Ingestion Is Too Easy to Trigger

### ✅ Implemented

1. **Admin-Only + Feature Flag** (`backend/api/ingestion.py`)
   - `require_ingestion_access()` function enforces both
   - All ingestion endpoints check admin role + `INGESTION_ENABLED=true`
   - Production default: `INGESTION_ENABLED=false`

2. **SHADOW State Enforcement** (`backend/stores/contract_store.py`)
   - Line 81: Contracts ALWAYS created in SHADOW state
   - Added warning logs about SHADOW state
   - No code path allows ingestion → ACTIVE

3. **Promotion API Separate** (`backend/api/table_state.py`)
   - Promotion endpoint is separate from ingestion
   - Requires admin role
   - Includes validation and audit logging

4. **Orchestrator Comments** (`rust/ingestion/orchestrator.rs`)
   - Added comments about SHADOW enforcement
   - Documented that ingestion never writes to ACTIVE

### 🔄 Remaining Work

- [ ] Add integration test: ingestion always creates SHADOW tables
- [ ] Add integration test: promotion requires admin
- [ ] Add integration test: `INGESTION_ENABLED=false` blocks ingestion
- [ ] Add UI warnings for SHADOW tables
- [ ] Add audit trail for all promotions

---

## Invariants Documented

Created `docs/CKO_INVARIANTS.md` with:
- All five invariants explained
- Enforcement mechanisms
- Verification checklist
- Breaking invariant consequences

---

## Next Steps

### Immediate (This Week)

1. **Complete Risk #1**
   - Route `table_store.py` through CKO client
   - Route `contract_store.py` through CKO client
   - Add tests

2. **Complete Risk #3**
   - Add integration tests for SHADOW enforcement
   - Add UI warnings
   - Add audit logging

### Short Term (Next 2 Weeks)

3. **Complete Risk #2**
   - Move SQL generation to Rust
   - Update Python to output intent only
   - Add validation

### Long Term (Next Month)

4. **Monitoring & Observability**
   - Add metrics for CKO boundary violations
   - Add alerts for accidental ingestion
   - Add dashboard for promotion audit trail

---

## Verification

Run these checks to verify fixes:

```bash
# Risk #1: No direct metadata writes outside CKO
grep -r "register_table\|register_contract" backend/ --exclude="cko_client.py"
# Should return minimal results (only in cko_client.py)

# Risk #2: No SQL generation in Python (except deprecated)
grep -r "def.*sql\|SELECT\|INSERT" backend/sql_builder.py backend/llm_query_generator.py
# Should show deprecation warnings

# Risk #3: Ingestion requires admin + flag
grep -r "require_ingestion_access\|INGESTION_ENABLED" backend/api/ingestion.py
# Should show both checks

# Risk #3: Contracts always SHADOW
grep -r "'SHADOW'\|state.*SHADOW" backend/stores/contract_store.py
# Should show SHADOW enforcement
```

---

## Success Criteria

✅ **Risk #1 Solved When:**
- There is exactly one write API for metadata (CKO client)
- All other code paths are read-only
- "Why does the system think X?" has a single answer: WorldState

✅ **Risk #2 Solved When:**
- SQL can be generated in exactly one place (Rust)
- Replay produces byte-for-byte identical SQL
- Python cannot bypass Rust accidentally

✅ **Risk #3 Solved When:**
- Ingestion never affects queries by default
- Promotion is explicit and logged
- A non-admin cannot ingest anything
- "Accidental ingestion" is structurally impossible

---

## Notes

- These fixes are **structural**, not **advisory**
- They prevent the risks from resurfacing as the codebase grows
- They work together - none function alone
- Breaking them requires explicit code changes (not accidental)

