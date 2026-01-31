# Complete Execution Plan: Enterprise-Safe Spyne IDE

**Status:** 🟢 **READY FOR IMPLEMENTATION**  
**Version:** 1.0  
**Date:** January 2024

---

## 0. North-Star Principles (Non-Negotiable)

These guide **every** decision. If a feature violates one, it doesn't ship.

1. **Read-only by default**
2. **No automatic writes without explicit promotion**
3. **Nothing irreversible without preview + rollback**
4. **Magic becomes suggestion, never action**
5. **Admins move fast, users stay safe**
6. **Everything is versioned**
7. **Trust > Power**

---

## 1. Core System Model

### 1.1 Table Lifecycle States

Every table in Spyne exists in **exactly one state**:

```
READ_ONLY     → existing external tables
SHADOW        → ingestion-ready, isolated
ACTIVE        → canonical, user-facing
DEPRECATED    → legacy, safe fallback
```

#### State Definitions

| State      | Description       | Queryable   | Writable | Visible    | Auto-Joined |
| ---------- | ----------------- | ----------- | -------- | ---------- | ----------- |
| READ_ONLY  | External DB table | ✅           | ❌        | ✅          | ✅           |
| SHADOW     | Ingestion staging | ❌ (default) | ✅        | Admin-only | ❌           |
| ACTIVE     | Canonical table   | ✅           | ✅        | ✅          | ✅           |
| DEPRECATED | Old canonical     | ✅ (opt-in)  | ❌        | Hidden     | ❌           |

#### State Transitions

```
READ_ONLY → (no transition, external)
SHADOW → ACTIVE (promotion, admin-only)
ACTIVE → DEPRECATED (deprecation, admin-only)
DEPRECATED → ACTIVE (restore, admin-only)
```

**Rules:**
- Only `SHADOW` → `ACTIVE` requires promotion
- `ACTIVE` → `DEPRECATED` is reversible
- `READ_ONLY` tables never change state (they're external)

---

### 1.2 Role Model (Hard Boundaries)

| Role     | Query | Create Contracts | Ingest | Promote | Deprecate | View Shadow |
| -------- | ----- | ---------------- | ------ | ------- | --------- | ----------- |
| Viewer   | ✅     | ❌                | ❌      | ❌       | ❌         | ❌           |
| Analyst  | ✅     | ❌                | ❌      | ❌       | ❌         | ❌           |
| Engineer | ✅     | ✅                | ✅      | ❌       | ❌         | ✅           |
| Admin    | ✅     | ✅                | ✅      | ✅       | ✅         | ✅           |

**Enforcement:**
- API endpoints check role before operation
- UI hides controls based on role
- No role can bypass this
- **Promotion requires Admin. Always.**

---

### 1.3 System Modes

Spyne runs in **two explicit modes**:

```
MODE=READ_ONLY        (default)
MODE=INGESTION_READY  (admin-enabled)
```

**Behavior:**
- Users never see ingestion unless `INGESTION_READY`
- APIs enforce mode checks
- UI hides ingestion controls in `READ_ONLY` mode
- Mode change requires admin privilege

**Default:** `READ_ONLY` - safest for Day 1 users

---

## 2. Architecture Changes

### 2.1 Metadata Becomes Versioned + Stateful

Every metadata object has:

```json
{
  "id": "customers",
  "version": "v3",
  "state": "ACTIVE",
  "owner": "admin@company.com",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-20T14:30:00Z",
  "supersedes": "v2",
  "deprecated_at": null
}
```

**Applies to:**
- Tables
- Columns
- Joins
- Contracts
- Rules
- Metrics

**Version Format:** `v{N}` where N increments on breaking changes

---

### 2.2 Query Resolution Engine (Critical)

When resolving a query:

1. **Prefer `ACTIVE`** - canonical tables
2. **Fallback to `READ_ONLY`** - external tables
3. **Ignore `SHADOW`** - never auto-queried
4. **Include `DEPRECATED`** only if explicitly pinned

**Query Resolution Logic:**
```python
def resolve_tables(query_tables: List[str]) -> List[Table]:
    resolved = []
    for table_name in query_tables:
        # Try ACTIVE first
        active = get_table(table_name, state="ACTIVE")
        if active:
            resolved.append(active)
            continue
        
        # Fallback to READ_ONLY
        read_only = get_table(table_name, state="READ_ONLY")
        if read_only:
            resolved.append(read_only)
            continue
        
        # SHADOW never auto-resolved
        # DEPRECATED only if pinned
        raise TableNotFoundError(f"Table {table_name} not available")
    
    return resolved
```

**This prevents accidental shadow usage.**

---

### 2.3 Shadow Tables Are Physically Isolated

**Implementation Options:**

**Option A: Separate Schema (Recommended)**
```sql
CREATE SCHEMA spyne_shadow;
CREATE TABLE spyne_shadow.customers (...);
```

**Option B: Naming Convention**
```sql
CREATE TABLE __spyne_shadow__customers (...);
```

**Rules:**
- Never auto-joined
- Never auto-queried
- Only visible to Admins and Engineers
- Clear visual distinction in UI

---

## 3. Ingestion Safety Model (P0 Fix)

### 3.1 Explicit Ingestion Semantics (Required)

**No ingestion without this block:**

```json
{
  "ingestion_semantics": {
    "mode": "append | upsert",
    "idempotency_key": ["api_id", "event_id"],
    "event_time_column": "event_time",
    "processing_time_column": "ingested_at",
    "dedupe_window": "24h",
    "conflict_resolution": "latest_wins | error"
  }
}
```

**Validation:**
- If missing → ingestion rejected with error
- If invalid → ingestion rejected with error
- Must be provided on contract registration

---

### 3.2 Time Semantics (No More Lies)

| Column Type     | Source           | Example                    |
| --------------- | ---------------- | -------------------------- |
| event_time      | From API payload | `{"event_time": "2024-01-15T10:00:00Z"}` |
| processing_time | System clock     | `NOW()` at ingestion time   |
| user_time       | Explicit input   | User-provided timestamp     |

**Rules:**
- ❌ `NOW()` in global rules by default
- ✅ Explicit time source required
- ✅ Event time preferred over processing time

**Unsafe Usage:**
```json
{
  "column": "ingested_at",
  "default_value": "NOW()",
  "unsafe_allow_processing_time": true,
  "warning": "This creates non-reproducible state during replays"
}
```

**With warnings shown to admin.**

---

### 3.3 Replay & Backfill Are Explicit Operations

#### Replay API

```bash
POST /api/ingestion/replay
Content-Type: application/json

{
  "contract_id": "customers_v2",
  "time_range": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-01-15T23:59:59Z"
  },
  "dedupe_strategy": "idempotency_key",
  "dry_run": true,
  "preview_rows": 100
}
```

**Response:**
```json
{
  "dry_run": true,
  "estimated_rows": 15000,
  "estimated_duplicates": 200,
  "preview": [...],
  "confirmation_required": true
}
```

#### Backfill API

```bash
POST /api/ingestion/backfill
Content-Type: application/json

{
  "contract_id": "customers_v2",
  "source": "api_archive",
  "time_range": {
    "start": "2023-01-01T00:00:00Z",
    "end": "2023-12-31T23:59:59Z"
  },
  "batch_size": 1000,
  "dedupe_strategy": "idempotency_key",
  "dry_run": true
}
```

**Both require:**
- Time range (explicit)
- Dedupe strategy (explicit)
- Dry-run preview (mandatory)
- Confirmation (admin-only)

---

### 3.4 Idempotency Guarantees

**Idempotency Key:**
- Must be unique per event
- Used for deduplication
- Required for upsert mode

**Example:**
```json
{
  "idempotency_key": ["api_id", "event_id"],
  "mode": "upsert",
  "dedupe_window": "24h"
}
```

**Behavior:**
- Same idempotency key within dedupe window → skip or update
- Different idempotency key → insert
- Outside dedupe window → insert (new event)

---

## 4. Join Model (No Silent Betrayal)

### 4.1 Join Candidates, Not Auto-Joins

**System produces suggestions:**

```json
{
  "join_candidate": {
    "id": "candidates_customers_orders_001",
    "tables": ["customers", "orders"],
    "join_type": "left",
    "condition": "customers.customer_id = orders.customer_id",
    "confidence": 0.68,
    "reason": "shared_pk + same_api",
    "assumptions": [
      "same_grain",
      "same_semantics",
      "stable_api"
    ],
    "risk_level": "MEDIUM",
    "validation_stats": {
      "cardinality_check": "one_to_many",
      "null_percentage": 0.02,
      "fan_out_multiplier": 3.5,
      "sample_rows": 1000
    }
  }
}
```

**Confidence Levels:**
- `HIGH` (0.8+): Strong evidence, low risk
- `MEDIUM` (0.5-0.8): Some evidence, review recommended
- `LOW` (<0.5): Weak evidence, manual review required

---

### 4.2 Explicit Acceptance Required

**Accept Join API:**

```bash
POST /api/joins/accept
Content-Type: application/json

{
  "candidate_id": "candidates_customers_orders_001",
  "owner": "admin@company.com",
  "rationale": "Verified: same grain, stable API, tested in staging",
  "version": "v1"
}
```

**Accepted joins get:**
- Owner (who accepted)
- Rationale (why it's safe)
- Version (for tracking)
- Validation stats (for monitoring)
- Created timestamp

**No join is active without explicit acceptance.**

---

### 4.3 Join Validation (Before Activation)

**Mandatory checks:**

1. **Cardinality Check:**
   ```sql
   SELECT COUNT(DISTINCT customers.customer_id) as customers,
          COUNT(DISTINCT orders.customer_id) as orders_with_customers
   FROM customers
   LEFT JOIN orders ON customers.customer_id = orders.customer_id
   ```
   - Expected: `orders_with_customers <= customers`
   - Fail if: `orders_with_customers > customers` (data quality issue)

2. **Null Percentage:**
   - Check % of nulls in join columns
   - Warn if > 10%

3. **Fan-Out Multiplier:**
   - Average rows per parent
   - Warn if > 100 (explosive join)

4. **Sample Rows:**
   - Return sample of joined data
   - Admin reviews before acceptance

**Fail → join blocked or flagged for review**

---

### 4.4 Join Versioning

**Every accepted join has:**
```json
{
  "join_id": "customers_orders_v1",
  "version": "v1",
  "state": "ACTIVE",
  "tables": ["customers", "orders"],
  "condition": "customers.customer_id = orders.customer_id",
  "accepted_by": "admin@company.com",
  "accepted_at": "2024-01-15T10:00:00Z",
  "validation_stats": {...},
  "supersedes": null
}
```

**When API changes:**
- Create new join candidate
- If accepted → new version
- Old version marked `DEPRECATED`
- Queries use latest `ACTIVE` version

---

## 5. Metadata Drift & Evolution

### 5.1 Drift Detection Engine

**On schema refresh:**

```python
def detect_drift(current_schema: Schema, new_schema: Schema) -> DriftReport:
    changes = []
    
    # Detect additions
    for col in new_schema.columns - current_schema.columns:
        changes.append({
            "type": "ADD",
            "column": col.name,
            "severity": "COMPATIBLE"
        })
    
    # Detect removals
    for col in current_schema.columns - new_schema.columns:
        changes.append({
            "type": "REMOVE",
            "column": col.name,
            "severity": "BREAKING"
        })
    
    # Detect renames
    for old_col, new_col in detect_renames(current_schema, new_schema):
        changes.append({
            "type": "RENAME",
            "old_column": old_col.name,
            "new_column": new_col.name,
            "severity": "BREAKING"
        })
    
    # Detect type changes
    for col in current_schema.columns & new_schema.columns:
        if col.type != new_schema[col.name].type:
            changes.append({
                "type": "TYPE_CHANGE",
                "column": col.name,
                "old_type": col.type,
                "new_type": new_schema[col.name].type,
                "severity": "WARNING" if is_widening(col.type, new_schema[col.name].type) else "BREAKING"
            })
    
    return DriftReport(changes=changes, severity=max(c.severity for c in changes))
```

---

### 5.2 Drift States

| State      | Meaning          | Action Required              |
| ---------- | ---------------- | ---------------------------- |
| COMPATIBLE | Safe             | Auto-apply                   |
| WARNING    | Needs review     | Notify admin, allow override |
| BREAKING   | Blocks promotion | Require new version          |

**Severity Rules:**
- **COMPATIBLE:** Add nullable column, widen type
- **WARNING:** Add non-nullable column (with default), narrow type (with validation)
- **BREAKING:** Remove column, rename column, change semantics

---

### 5.3 Contract Evolution Rules

**Allowed without migration:**

✅ Add nullable column  
✅ Widen type (INT → BIGINT)  
✅ Add optional field to API response  

**Requires new version:**

❌ Rename column  
❌ Drop column  
❌ Change semantics  
❌ Narrow type (BIGINT → INT)  
❌ Remove field from API response  

**Version Bump:**
- Compatible changes → patch version (v1.0 → v1.1)
- Breaking changes → major version (v1.0 → v2.0)

---

### 5.4 Diff Visibility

**API:**

```bash
GET /api/contracts/{contract_id}/diff?from_version=v1&to_version=v2
```

**Response:**
```json
{
  "from_version": "v1",
  "to_version": "v2",
  "changes": [
    {
      "type": "ADD_COLUMN",
      "column": "status",
      "data_type": "string",
      "nullable": true
    },
    {
      "type": "RENAME_COLUMN",
      "old_column": "customer_id",
      "new_column": "id",
      "severity": "BREAKING"
    }
  ],
  "migration_guide": "To migrate from v1 to v2: ..."
}
```

---

## 6. NL → SQL Trust Layer

### 6.1 Mandatory Query Preview

**Default Flow:**

```
NL Query → Intent Extraction → Logical Plan → SQL Generation → Preview → Execute
```

**Preview API:**

```bash
POST /api/query/preview
Content-Type: application/json

{
  "query": "show me top 10 customers by revenue",
  "skip_preview": false  # default: false
}
```

**Response:**
```json
{
  "preview": {
    "sql": "SELECT ...",
    "tables_used": ["customers", "orders"],
    "join_versions": ["customers_orders_v1"],
    "filters": ["orders.status != 'CANCELLED'"],
    "aggregations": ["SUM(orders.amount) AS revenue"],
    "assumptions": [
      "Revenue = sum of order amounts",
      "Excludes cancelled orders"
    ],
    "confidence": 0.85,
    "estimated_rows": 10,
    "estimated_execution_time_ms": 1200
  },
  "requires_confirmation": true
}
```

**Skip preview requires explicit flag:**
```json
{
  "query": "...",
  "skip_preview": true,
  "skip_reason": "automated_testing"
}
```

---

### 6.2 Explainability Payload

**Every query returns:**

```json
{
  "result": {...},
  "explanation": {
    "plain_english": "This query shows the top 10 customers by total revenue in the last 30 days, excluding cancelled orders.",
    "tables_used": [
      {
        "name": "customers",
        "state": "ACTIVE",
        "version": "v2",
        "rows_scanned": 50000
      },
      {
        "name": "orders",
        "state": "ACTIVE",
        "version": "v1",
        "rows_scanned": 200000
      }
    ],
    "joins": [
      {
        "join_id": "customers_orders_v1",
        "type": "left",
        "condition": "customers.customer_id = orders.customer_id",
        "accepted_by": "admin@company.com",
        "accepted_at": "2024-01-15T10:00:00Z"
      }
    ],
    "filters": [
      {
        "table": "orders",
        "column": "status",
        "operator": "!=",
        "value": "CANCELLED",
        "applied": true
      }
    ],
    "aggregations": [
      {
        "function": "SUM",
        "column": "orders.amount",
        "alias": "revenue"
      }
    ],
    "assumptions": [
      "Revenue = sum of order amounts (gross revenue)",
      "Time range: last 30 days from current date"
    ],
    "confidence": 0.85
  }
}
```

---

### 6.3 Correction Loop

**User Feedback API:**

```bash
POST /api/query/correct
Content-Type: application/json

{
  "query_id": "query_123",
  "corrections": [
    {
      "type": "JOIN_TYPE",
      "current": "inner",
      "requested": "left",
      "reason": "Include customers with no orders"
    },
    {
      "type": "FILTER",
      "action": "add",
      "filter": "customers.status = 'active'",
      "reason": "Only show active customers"
    },
    {
      "type": "METRIC",
      "current": "gross_revenue",
      "requested": "net_revenue",
      "reason": "Use net revenue after discounts"
    }
  ]
}
```

**System updates logical plan, regenerates SQL (not SQL hacks)**

---

### 6.4 Guardrails

**Hard Stops On:**

1. **Explosive Joins:**
   - Fan-out multiplier > 1000
   - Estimated result rows > 10M
   - Requires explicit override

2. **Ambiguous Aggregates:**
   - Multiple aggregation levels
   - Missing GROUP BY
   - Requires clarification

3. **Missing Filters on Large Tables:**
   - Table > 1M rows
   - No time filter
   - No WHERE clause
   - Warns user

**Guardrail API:**

```bash
POST /api/query/validate
Content-Type: application/json

{
  "sql": "...",
  "tables": ["customers", "orders"]
}
```

**Response:**
```json
{
  "valid": false,
  "warnings": [
    {
      "type": "EXPLOSIVE_JOIN",
      "message": "Join may produce >10M rows",
      "requires_override": true
    }
  ],
  "errors": []
}
```

---

## 7. Operational Model (SRE-Safe)

### 7.1 Failure Domains

| Component         | Failure Impact   | Mitigation                    |
| ----------------- | ---------------- | ----------------------------- |
| Contract registry | Ingestion paused | Read-only mode, cached contracts |
| Query engine      | Queries fail     | Fallback to direct SQL        |
| Metadata store    | Cached fallback  | Versioned backups            |
| Ingestion engine  | Data lag         | Queue, retry, alert           |

**Isolation:**
- Each component can fail independently
- Queries work even if ingestion is down
- Ingestion queues if query engine is down

---

### 7.2 Observability (Required)

**Metrics:**

```prometheus
# Ingestion
spyne_ingestion_lag_seconds{contract_id="customers_v2"}
spyne_ingestion_rows_total{contract_id="customers_v2", status="success|error"}
spyne_replay_count_total{contract_id="customers_v2"}
spyne_backfill_rows_total{contract_id="customers_v2"}

# Joins
spyne_join_usage_total{join_id="customers_orders_v1"}
spyne_join_candidate_rejected_total{reason="low_confidence|validation_failed"}

# Drift
spyne_drift_detected_total{contract_id="customers_v2", severity="COMPATIBLE|WARNING|BREAKING"}
spyne_drift_resolved_total{contract_id="customers_v2"}

# Query
spyne_query_latency_seconds{query_type="nl_to_sql"}
spyne_query_preview_viewed_total
spyne_query_corrections_total
spyne_query_guardrail_triggered_total{type="explosive_join|ambiguous_aggregate"}
```

**Alerts:**

- Ingestion lag > 1 hour
- Drift detected (BREAKING severity)
- Join validation failures > 10%
- Query latency P95 > 5s

---

### 7.3 Rollback Guarantees

**Table Promotion:**
- Reversible within 24 hours
- Creates backup before promotion
- Rollback restores previous state

**Joins:**
- Versioned, can revert to previous version
- Deprecation doesn't delete, just hides

**Metadata Changes:**
- Append-only (never delete)
- Can restore any version
- Audit trail for all changes

---

### 7.4 Data Freshness SLAs

**Configurable per contract:**

```json
{
  "contract_id": "customers_v2",
  "sla": {
    "max_lag_seconds": 3600,
    "alert_threshold_seconds": 1800,
    "ingestion_schedule": "*/5 * * * *"  # Every 5 minutes
  }
}
```

**Monitoring:**
- Tracks actual lag vs SLA
- Alerts when threshold exceeded
- Dashboard shows lag per contract

---

## 8. Documentation Refactor (Critical)

### New Structure

```
1. Quick Start (Read-Only) ⭐ START HERE
   - Connect Database (2 minutes)
   - Register Metadata (3 minutes)
   - Query (instant)
   - First Query Example

2. How Spyne Thinks (Mental Model)
   - Table States
   - Role Model
   - System Modes
   - Query Resolution

3. Query Trust & Explainability
   - SQL Preview
   - Query Explanation
   - Correction Loop
   - Guardrails

4. Advanced: Ingestion (Admins Only)
   - When to Use Ingestion
   - Shadow Tables
   - Safety & Replay
   - Promotion Workflow
   - Production Safety

5. Metadata & Drift
   - Versioning
   - Drift Detection
   - Contract Evolution
   - Migration Guide

6. Operations Guide (SRE Perspective)
   - Architecture
   - Failure Domains
   - Monitoring
   - Disaster Recovery
   - Scaling

7. Who This Is / Is NOT For
   - Ideal Customer Profile
   - Use Cases
   - Anti-Patterns
```

**First ingestion mention happens after querying.**

---

## 9. Rollout Plan

### Week 1 – Safety & Defaults

**Goals:**
- Read-only enforced
- Shadow tables implemented
- Promotion API ready
- Ban unsafe globals

**Tasks:**
- [ ] Implement table state model
- [ ] Add role-based access control
- [ ] Create shadow table isolation
- [ ] Build promotion API
- [ ] Add ingestion semantics validation
- [ ] Remove unsafe global rule defaults

**Deliverables:**
- Table state API working
- Promotion workflow tested
- Ingestion requires explicit semantics

---

### Week 2 – Trust

**Goals:**
- SQL preview mandatory
- Join candidates (not auto-joins)
- Drift detection working

**Tasks:**
- [ ] Implement query preview API
- [ ] Build join candidate system
- [ ] Add join validation checks
- [ ] Create drift detection engine
- [ ] Add explainability payload

**Deliverables:**
- Preview before execution
- Join suggestions with acceptance
- Drift alerts working

---

### Week 3 – Scale

**Goals:**
- Ops docs complete
- Metrics dashboard
- ICP messaging clear

**Tasks:**
- [ ] Write operations guide
- [ ] Implement metrics collection
- [ ] Create monitoring dashboard
- [ ] Refine ICP messaging
- [ ] Update marketing materials

**Deliverables:**
- SRE-ready documentation
- Full observability
- Clear positioning

---

## 10. Success Criteria (Hard)

### Adoption Metrics

- ✅ **Time to first query:** < 5 minutes (Workflow 1)
- ✅ **% users starting with Workflow 1:** > 80%
- ✅ **% users progressing to Workflow 2:** < 20% (expected)

**Measurement:**
- Track user onboarding flow
- Log first query timestamp
- Monitor workflow selection

---

### Trust Metrics

- ✅ **% queries with SQL preview viewed:** > 60%
- ✅ **% suggested joins accepted:** < 50% (shows users are reviewing)
- ✅ **Support tickets about "wrong results":** < 5% of total tickets

**Measurement:**
- Log preview views
- Track join acceptance rate
- Monitor support tickets

---

### Safety Metrics

- ✅ **Zero silent joins** (all joins require acceptance)
- ✅ **Zero temporal corruption** (no `NOW()` in unsafe contexts)
- ✅ **Zero irreversible ingestion** (all promotions reversible)

**Measurement:**
- Audit log for all joins
- Monitor temporal column usage
- Track promotion rollbacks

---

## 11. API Specifications

### 11.1 Table State Management

```bash
# Get table state
GET /api/tables/{table_id}/state

# Promote shadow to active
POST /api/tables/{table_id}/promote
{
  "from_state": "SHADOW",
  "to_state": "ACTIVE",
  "dry_run": false
}

# Deprecate table
POST /api/tables/{table_id}/deprecate
{
  "reason": "Replaced by customers_v2"
}
```

### 11.2 Ingestion Semantics

```bash
# Register contract with semantics
POST /api/contracts/register
{
  "endpoint": "/api/v1/customers",
  "table_name": "customers",
  "ingestion_semantics": {
    "mode": "upsert",
    "idempotency_key": ["api_id", "event_id"],
    "event_time_column": "event_time",
    "processing_time_column": "ingested_at",
    "dedupe_window": "24h"
  }
}
```

### 11.3 Join Candidates

```bash
# Get join candidates
GET /api/joins/candidates?table1=customers&table2=orders

# Accept join candidate
POST /api/joins/accept
{
  "candidate_id": "candidates_customers_orders_001",
  "owner": "admin@company.com",
  "rationale": "..."
}
```

---

## 12. Implementation Checklist

### Phase 1: Core Safety (Week 1)

- [ ] Table state model (READ_ONLY, SHADOW, ACTIVE, DEPRECATED)
- [ ] Role-based access control (Viewer, Analyst, Engineer, Admin)
- [ ] System modes (READ_ONLY, INGESTION_READY)
- [ ] Shadow table isolation
- [ ] Promotion API
- [ ] Ingestion semantics validation
- [ ] Remove unsafe global rule defaults

### Phase 2: Trust Layer (Week 2)

- [ ] Query preview API
- [ ] Explainability payload
- [ ] Join candidate system
- [ ] Join validation checks
- [ ] Join acceptance workflow
- [ ] Drift detection engine
- [ ] Correction loop API

### Phase 3: Operations (Week 3)

- [ ] Metrics collection
- [ ] Monitoring dashboard
- [ ] Alerting system
- [ ] Operations documentation
- [ ] Disaster recovery procedures
- [ ] ICP messaging

---

## Final Statement

This execution plan transforms Spyne IDE from a **powerful but risky** system into an **enterprise-safe platform** that:

✅ **Respects org reality** - Works with existing infrastructure  
✅ **Earns trust gradually** - Read-only first, optional ingestion  
✅ **Scales power safely** - Explicit approvals, versioning, rollbacks  

**This is not a toy. This is not a demo. This is production infrastructure.**

Every decision in this plan prioritizes **safety and trust** over **power and magic**.

**Ready for implementation.**

