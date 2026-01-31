# Data Entry Guide: How Organizations Can Use Spyne IDE

**⭐ START HERE** - This guide shows you how to integrate Spyne IDE with your existing data infrastructure.

---

## Quick Start: Query Your Existing Database (5 Minutes)

**Most organizations start here.** Connect to your existing database and start querying in minutes.

### Step 1: Connect Database (2 minutes)

Edit `.env` file:

```bash
# Database Configuration
RCA_DB_TYPE=postgresql          # or mysql, sqlite
RCA_DB_HOST=your-db-host        # e.g., db.yourcompany.com
RCA_DB_PORT=5432                # PostgreSQL default
RCA_DB_NAME=your_database       # Your existing database name
RCA_DB_USER=your_db_user        # Database user (read-only recommended)
RCA_DB_PASSWORD=your_password   # Database password
```

**Verify connection:**
```bash
curl http://localhost:8080/api/v1/health/detailed
```

### Step 2: Register Metadata (3 minutes)

Tell Spyne IDE about your tables:

```bash
# Register customers table
curl -X POST http://localhost:8080/api/metadata/ingest/table \
  -H "Content-Type: application/json" \
  -d '{
    "table_description": "Table: customers - Customer master data with customer_id, name, email, created_at, status columns"
  }'

# Register orders table
curl -X POST http://localhost:8080/api/metadata/ingest/table \
  -H "Content-Type: application/json" \
  -d '{
    "table_description": "Table: orders - Order transactions with order_id, customer_id, order_date, total_amount, status columns"
  }'

# Register relationships
curl -X POST http://localhost:8080/api/metadata/ingest/join \
  -H "Content-Type: application/json" \
  -d '{
    "join_text": "orders.customer_id = customers.customer_id (one-to-many)"
  }'
```

### Step 3: Query (Instant)

```bash
curl -X POST http://localhost:8080/api/agent/run \
  -H "Content-Type: application/json" \
  -d '{
    "query": "show me top 10 customers by total order value"
  }'
```

**That's it!** You're now querying your existing database with natural language.

---

## How Spyne IDE Works (Mental Model)

### Core Concept

Spyne IDE is a **read-only query layer** that sits on top of your existing data infrastructure.

```
Your Existing Infrastructure:
├── Databases (PostgreSQL, MySQL, etc.)
│   ├── Tables (customers, orders, products, etc.)
│   └── Data (populated by your APIs)
│
└── APIs (that write to tables)
    └── Continue working as normal

Spyne IDE Layer (Read-Only):
├── Metadata Registry (describes your tables)
├── Query Engine (generates SQL from natural language)
└── Execution Engine (runs queries against your databases)
```

**Key Points:**
- ✅ **No data migration** - Uses your existing tables
- ✅ **APIs unchanged** - Your APIs continue working normally
- ✅ **Read-only** - Spyne IDE never modifies your data
- ✅ **Optional ingestion** - Advanced feature for power users (see below)

---

### Table States

Every table in Spyne exists in one of these states:

| State      | Description       | Queryable   | Visible    | When Used              |
| ---------- | ----------------- | ----------- | ---------- | ---------------------- |
| READ_ONLY  | External DB table | ✅           | ✅          | Your existing tables    |
| SHADOW     | Ingestion staging | ❌           | Admin-only | During ingestion setup |
| ACTIVE     | Canonical table   | ✅           | ✅          | After promotion         |
| DEPRECATED | Old canonical     | ✅ (opt-in)  | Hidden     | Legacy tables          |

**Default:** All your existing tables are `READ_ONLY` - safe and queryable.

---

### System Modes

Spyne runs in two modes:

- **`READ_ONLY`** (default) - Query existing tables only
- **`INGESTION_READY`** (admin-enabled) - Allows data ingestion

**You start in `READ_ONLY` mode** - the safest option.

---

## Query Trust & Explainability

### SQL Preview (Mandatory)

Before executing any query, Spyne shows you the SQL it will run:

```bash
POST /api/query/preview
{
  "query": "show me top 10 customers by revenue"
}
```

**Response:**
```json
{
  "preview": {
    "sql": "SELECT customers.name, SUM(orders.amount) AS revenue FROM customers LEFT JOIN orders ON customers.customer_id = orders.customer_id WHERE orders.status != 'CANCELLED' GROUP BY customers.customer_id ORDER BY revenue DESC LIMIT 10",
    "tables_used": ["customers", "orders"],
    "joins": [
      {
        "join_id": "customers_orders_v1",
        "type": "left",
        "condition": "customers.customer_id = orders.customer_id"
      }
    ],
    "filters": ["orders.status != 'CANCELLED'"],
    "aggregations": ["SUM(orders.amount) AS revenue"],
    "assumptions": [
      "Revenue = sum of order amounts",
      "Excludes cancelled orders"
    ],
    "confidence": 0.85
  },
  "requires_confirmation": true
}
```

**Review the SQL before execution** - you're in control.

---

### Query Explanation

Every query result includes a plain-English explanation:

```json
{
  "result": [...],
  "explanation": {
    "plain_english": "This query shows the top 10 customers by total revenue in the last 30 days, excluding cancelled orders.",
    "tables_used": [
      {
        "name": "customers",
        "state": "READ_ONLY",
        "rows_scanned": 50000
      }
    ],
    "joins": [...],
    "filters": [...],
    "assumptions": [...]
  }
}
```

---

### Correction Loop

If the query isn't quite right, provide feedback:

```bash
POST /api/query/correct
{
  "query_id": "query_123",
  "corrections": [
    {
      "type": "JOIN_TYPE",
      "current": "inner",
      "requested": "left",
      "reason": "Include customers with no orders"
    }
  ]
}
```

The system updates the logical plan and regenerates SQL (not SQL hacks).

---

### Guardrails

Spyne automatically prevents dangerous queries:

- **Explosive joins** - Warns if join may produce >10M rows
- **Ambiguous aggregates** - Asks for clarification
- **Missing filters** - Warns on large tables without WHERE clause

---

## Advanced: Ingestion Workflow (Admins Only)

**⚠️ This is for power users only.** Most organizations don't need this initially.

### When to Use Ingestion

Use ingestion when:
- ✅ You want to standardize table formats across your organization
- ✅ You need automatic data ingestion from APIs
- ✅ You're building a new data pipeline
- ✅ You have admin privileges

**Don't use ingestion if:**
- ❌ You already have working tables
- ❌ You're just getting started
- ❌ You don't have admin access

---

### Shadow Tables (Safety First)

Ingestion creates **shadow tables** first - isolated and invisible to users:

```
SHADOW → (promotion) → ACTIVE
```

**Shadow tables:**
- ✅ Created in isolated schema (`spyne_shadow` or `__spyne_shadow__`)
- ✅ Never auto-queried
- ✅ Never auto-joined
- ✅ Only visible to Admins and Engineers

**Promotion to ACTIVE requires explicit admin approval.**

---

### Production Safety

**⚠️ Critical:** Ingestion requires explicit safety configuration.

#### Ingestion Semantics (Required)

Every contract must define:

```json
{
  "ingestion_semantics": {
    "mode": "append | upsert",
    "idempotency_key": ["api_id", "event_id"],
    "event_time_column": "event_time",
    "processing_time_column": "ingested_at",
    "dedupe_window": "24h",
    "conflict_resolution": "latest_wins"
  }
}
```

**Without this, ingestion is rejected.**

---

#### Time Semantics (No More Lies)

| Column Type     | Source           | Safe? |
| --------------- | ---------------- | ----- |
| event_time      | From API payload | ✅     |
| processing_time | System clock     | ⚠️     |
| user_time       | Explicit input   | ✅     |

**Rules:**
- ❌ `NOW()` in global rules by default
- ✅ Explicit time source required
- ✅ Event time preferred over processing time

**Unsafe usage requires explicit flag:**
```json
{
  "column": "ingested_at",
  "default_value": "NOW()",
  "unsafe_allow_processing_time": true,
  "warning": "This creates non-reproducible state during replays"
}
```

---

#### Replay & Backfill

Both are **explicit operations** with mandatory previews:

```bash
# Replay (with preview)
POST /api/ingestion/replay
{
  "contract_id": "customers_v2",
  "time_range": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2024-01-15T23:59:59Z"
  },
  "dedupe_strategy": "idempotency_key",
  "dry_run": true  # Mandatory preview
}
```

**Both require:**
- Time range (explicit)
- Dedupe strategy (explicit)
- Dry-run preview (mandatory)
- Admin confirmation

---

### Join Suggestions (Not Auto-Joins)

**⚠️ Important:** Spyne **suggests** joins, never creates them automatically.

#### Join Candidates

When tables share the same API and matching primary keys, Spyne suggests a join:

```json
{
  "join_candidate": {
    "tables": ["customers", "orders"],
    "confidence": 0.68,
    "reason": "shared_pk + same_api",
    "assumptions": ["same_grain", "same_semantics"],
    "risk_level": "MEDIUM",
    "validation_stats": {
      "cardinality_check": "one_to_many",
      "null_percentage": 0.02,
      "fan_out_multiplier": 3.5
    }
  }
}
```

**⚠️ Always review suggested joins** - same API doesn't guarantee same grain or semantics.

---

#### Explicit Acceptance Required

```bash
POST /api/joins/accept
{
  "candidate_id": "candidates_customers_orders_001",
  "owner": "admin@company.com",
  "rationale": "Verified: same grain, stable API, tested in staging"
}
```

**No join is active without explicit admin acceptance.**

---

### Promotion Workflow

**Shadow → ACTIVE promotion:**

```bash
POST /api/tables/{table_id}/promote
{
  "from_state": "SHADOW",
  "to_state": "ACTIVE",
  "dry_run": false
}
```

**Promotion:**
- ✅ Creates backup before promotion
- ✅ Reversible within 24 hours
- ✅ Requires admin approval
- ✅ Logs all changes

---

## Metadata & Drift

### Versioning

Every metadata object is versioned:

```json
{
  "id": "customers",
  "version": "v3",
  "state": "ACTIVE",
  "supersedes": "v2"
}
```

**Applies to:** tables, columns, joins, contracts, rules

---

### Drift Detection

When APIs change, Spyne detects drift:

| Change Type    | Severity    | Action Required              |
| -------------- | ----------- | ---------------------------- |
| Add column     | COMPATIBLE  | Auto-apply                   |
| Remove column  | BREAKING    | Require new version          |
| Rename column  | BREAKING    | Require new version          |
| Widen type     | COMPATIBLE  | Auto-apply                   |
| Narrow type    | BREAKING    | Require new version          |

**Drift alerts notify admins automatically.**

---

### Contract Evolution

**Allowed without migration:**
- ✅ Add nullable column
- ✅ Widen type (INT → BIGINT)

**Requires new version:**
- ❌ Rename column
- ❌ Drop column
- ❌ Change semantics

---

## Operations Guide (SRE Perspective)

### Architecture

```
┌─────────────────────────────────────────┐
│         Spyne IDE Components            │
├─────────────────────────────────────────┤
│  Contract Registry  →  Ingestion     │
│  Metadata Store      →  Query Engine   │
│  Execution Engine    →  Results       │
└─────────────────────────────────────────┘
```

### Failure Domains

| Component         | Failure Impact   | Mitigation                    |
| ----------------- | ---------------- | ----------------------------- |
| Contract registry | Ingestion paused | Read-only mode, cached contracts |
| Query engine      | Queries fail     | Fallback to direct SQL        |
| Metadata store    | Cached fallback  | Versioned backups            |

**Isolation:** Each component can fail independently.

---

### Monitoring

**Key Metrics:**

- `spyne_ingestion_lag_seconds` - Data freshness
- `spyne_query_latency_seconds` - Query performance
- `spyne_drift_detected_total` - Schema changes
- `spyne_join_usage_total` - Join popularity

**Alerts:**
- Ingestion lag > 1 hour
- Drift detected (BREAKING severity)
- Query latency P95 > 5s

---

### Scaling Considerations

- **Metadata Store:** Versioned, append-only (never delete)
- **Query Engine:** Stateless, horizontally scalable
- **Ingestion:** Queue-based, can scale workers
- **Contract Registry:** Single source of truth, replicated

---

## Who This Is / Is NOT For

### Ideal Customer Profile

**Best for:**
- ✅ Mid-to-large organizations (100+ employees)
- ✅ Multiple APIs and fragmented data ownership
- ✅ Existing databases with tables
- ✅ Data teams needing natural language queries
- ✅ Organizations wanting read-only query layer

**Not ideal for:**
- ❌ Startups with simple needs
- ❌ Single API, single database
- ❌ Organizations requiring strict governance (initially)
- ❌ Teams without existing data infrastructure

---

### Use Cases

**Primary Use Case:**
> "We have existing databases with tables. We want analysts to query them using natural language without learning SQL."

**Secondary Use Case:**
> "We want to standardize table formats across multiple APIs and automate data ingestion."

---

## Complete Example: E-commerce Platform

**Existing Infrastructure:**
- PostgreSQL database: `ecommerce_prod`
- Tables: `users`, `products`, `orders`, `order_items`
- REST APIs that write to these tables

**Integration (Read-Only):**

```bash
# 1. Connect database
cat >> .env << EOF
RCA_DB_TYPE=postgresql
RCA_DB_HOST=ecommerce-db.prod.company.com
RCA_DB_PORT=5432
RCA_DB_NAME=ecommerce_prod
RCA_DB_USER=spyne_readonly
RCA_DB_PASSWORD=secure_readonly_password
EOF

# 2. Register tables
curl -X POST http://localhost:8080/api/metadata/ingest/table \
  -d '{"table_description": "Table: users - User accounts with user_id, email, created_at"}'

curl -X POST http://localhost:8080/api/metadata/ingest/table \
  -d '{"table_description": "Table: orders - Order transactions with order_id, user_id, total_amount"}'

# 3. Register relationships
curl -X POST http://localhost:8080/api/metadata/ingest/join \
  -d '{"join_text": "orders.user_id = users.user_id (one-to-many)"}'

# 4. Query
curl -X POST http://localhost:8080/api/agent/run \
  -d '{"query": "show me top 10 customers by total order value in last 30 days"}'
```

**Result:**
- ✅ APIs continue working unchanged
- ✅ Analysts query using natural language
- ✅ No data migration needed
- ✅ Read-only access (safe)

---

## Security Considerations

### Database Access

- ✅ Use read-only credentials for Spyne IDE
- ✅ Limit access to specific schemas/tables
- ✅ Enable SSL/TLS for database connections
- ✅ Use connection pooling

### API Security

- ✅ Rate limiting configured
- ✅ Authentication (add as needed)
- ✅ Input validation
- ✅ SQL injection protection

---

## Troubleshooting

### Cannot connect to database

```bash
# Test connection
psql -h $RCA_DB_HOST -U $RCA_DB_USER -d $RCA_DB_NAME

# Check environment variables
echo $RCA_DB_HOST
echo $RCA_DB_NAME
```

### Tables not found

```bash
# Verify tables are registered
curl http://localhost:8080/api/metadata/tables

# Check metadata files
ls -la metadata/
```

### Queries return no results

- Check database connection
- Verify tables exist in database
- Confirm metadata matches actual schema
- Verify user has SELECT permissions

---

## Next Steps

### For Read-Only Workflow (Most Users):

1. ✅ **Connect database** - Configure `.env` with credentials
2. ✅ **Register tables** - Use metadata ingestion API
3. ✅ **Test queries** - Start with simple queries
4. ✅ **Scale up** - Register more tables as needed

### For Ingestion Workflow (Admins Only):

1. ⚠️ **Enable ingestion mode** - Requires admin privileges
2. ⚠️ **Register contracts** - With explicit ingestion semantics
3. ⚠️ **Create shadow tables** - Test in isolation
4. ⚠️ **Promote to active** - After validation
5. ⚠️ **Monitor drift** - Set up alerts

---

## Key Takeaways

### Read-Only First (Default)

**Spyne IDE is a read-only query layer.** Your APIs continue working unchanged, and Spyne IDE queries the same tables your APIs write to.

**Start simple:** Connect → Register → Query (5 minutes)

### Ingestion is Optional (Advanced)

**Ingestion is for power users only.** Most organizations don't need it initially.

**When ready:** Shadow tables → Promotion → Active (with safety checks)

### Trust Through Transparency

- ✅ SQL preview before execution
- ✅ Query explanations in plain English
- ✅ Join suggestions (not auto-joins)
- ✅ Explicit approvals required

---

## Support

For questions or issues:
- Check [SETUP.md](./SETUP.md) for installation
- Review [PRODUCTION_READINESS.md](./PRODUCTION_READINESS.md) for deployment
- See [EXECUTION_PLAN.md](./EXECUTION_PLAN.md) for architecture details
- See [CLARIFICATION_API_GUIDE.md](./CLARIFICATION_API_GUIDE.md) for API reference

---

**Status:** Production Ready  
**Version:** 2.0.0  
**Last Updated:** January 2024
