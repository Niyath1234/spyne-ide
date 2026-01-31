# Enterprise Safety Integration Guide

This guide shows how to integrate the enterprise safety features into your Spyne IDE application.

---

## 🗄️ Step 1: Database Setup

### 1.1 Run Schema Migration

```bash
# Connect to your PostgreSQL database
psql -U your_user -d spyne_db

# Run the enterprise safety schema
\i database/schema_enterprise_safety.sql

# Verify tables were created
\dt
```

### 1.2 Verify Default Data

```sql
-- Check role permissions were inserted
SELECT * FROM role_permissions;

-- Check system mode
SELECT * FROM system_config WHERE key = 'system_mode';
```

---

## 🔌 Step 2: Initialize Stores

Create a database connection and initialize stores:

```python
# backend/stores/db_connection.py
import psycopg2
from psycopg2.extras import RealDictCursor
from backend.stores.table_store import TableStore
from backend.stores.contract_store import ContractStore

class DatabaseConnection:
    def __init__(self, config):
        self.conn = psycopg2.connect(
            host=config.DATABASE_HOST,
            port=config.DATABASE_PORT,
            database=config.DATABASE_NAME,
            user=config.DATABASE_USER,
            password=config.DATABASE_PASSWORD
        )
        self.conn.autocommit = False
    
    def get_cursor(self):
        return self.conn.cursor(cursor_factory=RealDictCursor)

# Initialize stores
db = DatabaseConnection(ProductionConfig)
table_store = TableStore(db)
contract_store = ContractStore(db)
```

---

## 🔐 Step 3: Add Authentication Middleware

Add middleware to extract user context from requests:

```python
# backend/middleware/auth.py
from functools import wraps
from flask import request, g, jsonify
from backend.models.table_state import UserRole

def extract_user_context():
    """Extract user context from request headers or JWT."""
    # In production, decode JWT token
    # For now, use headers
    
    g.user_email = request.headers.get('X-User-Email', 'unknown@example.com')
    g.user_role_str = request.headers.get('X-User-Role', 'VIEWER')
    
    try:
        g.user_role = UserRole(g.user_role_str.upper())
    except ValueError:
        g.user_role = UserRole.VIEWER

def require_role(role: UserRole):
    """Decorator to require specific role."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            extract_user_context()
            if g.user_role != role:
                return jsonify({
                    'success': False,
                    'error': f'Requires {role.value} role'
                }), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator
```

---

## 🚀 Step 4: Wire Up APIs in app_production.py

Update `app_production.py` to initialize and register all APIs:

```python
# In app_production.py, after creating Flask app

from backend.api import api_router
from backend.stores.db_connection import DatabaseConnection
from backend.stores.table_store import TableStore
from backend.stores.contract_store import ContractStore
from backend.middleware.auth import extract_user_context

# Initialize database connection
db = DatabaseConnection(ProductionConfig)

# Initialize stores
table_store = TableStore(db)
contract_store = ContractStore(db)

# Initialize system config (simple dict for now)
system_config = {'system_mode': 'READ_ONLY'}

# Initialize API endpoints
from backend.api.table_state import init_table_state_api
from backend.api.ingestion import init_ingestion_api
from backend.api.joins import init_joins_api
from backend.api.query_preview import init_query_preview_api
from backend.api.drift import init_drift_api

init_table_state_api(table_store)
init_ingestion_api(contract_store, system_config)
init_joins_api(join_store)  # Create join_store similarly
init_query_preview_api(query_preview_store, query_executor)  # Create these
init_drift_api(contract_store, drift_store)  # Create drift_store

# Register API router
app.register_blueprint(api_router)

# Add middleware
@app.before_request
def before_request():
    extract_user_context()
```

---

## 🧪 Step 5: Test the Integration

### Test Table State API

```bash
# Get table state
curl http://localhost:8080/api/tables/customers/state \
  -H "X-User-Role: VIEWER" \
  -H "X-User-Email: viewer@example.com"

# Promote table (requires ADMIN)
curl -X POST http://localhost:8080/api/tables/customers/promote \
  -H "Content-Type: application/json" \
  -H "X-User-Role: ADMIN" \
  -H "X-User-Email: admin@example.com" \
  -d '{
    "from_state": "SHADOW",
    "to_state": "ACTIVE",
    "dry_run": true
  }'
```

### Test Ingestion API

```bash
# Register contract (requires ENGINEER or ADMIN)
curl -X POST http://localhost:8080/api/contracts/register \
  -H "Content-Type: application/json" \
  -H "X-User-Role: ENGINEER" \
  -H "X-User-Email: engineer@example.com" \
  -d '{
    "endpoint": "/api/v1/customers",
    "table_name": "customers",
    "ingestion_semantics": {
      "mode": "upsert",
      "idempotency_key": ["api_id", "event_id"],
      "event_time_column": "event_time",
      "processing_time_column": "ingested_at",
      "dedupe_window": "24h",
      "conflict_resolution": "latest_wins"
    }
  }'
```

### Test Query Preview API

```bash
# Generate preview
curl -X POST http://localhost:8080/api/query/preview \
  -H "Content-Type: application/json" \
  -d '{
    "query": "show me top 10 customers by revenue"
  }'
```

---

## 🔧 Step 6: Implement Store Methods

The stores are currently stubs. Implement actual database queries:

### TableStore Example

```python
# backend/stores/table_store.py

def get_table_by_id(self, table_id: str) -> Optional[Dict[str, Any]]:
    """Get table by ID."""
    with self.db.get_cursor() as cur:
        cur.execute("""
            SELECT name, state, version, owner, created_at, updated_at
            FROM tables
            WHERE name = %s
        """, (table_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def promote_table(self, table_id: str, from_state: TableState, 
                  to_state: TableState, changed_by: str) -> Dict[str, Any]:
    """Promote table with transaction."""
    try:
        with self.db.get_cursor() as cur:
            # Check if another ACTIVE version exists
            if to_state == TableState.ACTIVE:
                cur.execute("""
                    SELECT name FROM tables 
                    WHERE name = %s AND state = 'ACTIVE'
                """, (table_id,))
                if cur.fetchone():
                    raise ValueError("Another ACTIVE version exists")
            
            # Update table state
            cur.execute("""
                UPDATE tables 
                SET state = %s, updated_at = NOW()
                WHERE name = %s
            """, (to_state.value, table_id))
            
            # Record in history
            cur.execute("""
                INSERT INTO table_state_history 
                (table_name, from_state, to_state, changed_by)
                VALUES (%s, %s, %s, %s)
            """, (table_id, from_state.value, to_state.value, changed_by))
            
            self.db.conn.commit()
            
            # Return updated table
            return self.get_table_by_id(table_id)
    except Exception as e:
        self.db.conn.rollback()
        raise
```

---

## 📊 Step 7: Add Observability

### Metrics Collection

```python
# backend/services/metrics.py
from backend.stores.db_connection import DatabaseConnection

class MetricsCollector:
    def __init__(self, db: DatabaseConnection):
        self.db = db
    
    def record_metric(self, metric_name: str, value: float, labels: Dict[str, str]):
        """Record a metric."""
        with self.db.get_cursor() as cur:
            cur.execute("""
                INSERT INTO spyne_metrics (metric_name, metric_value, labels)
                VALUES (%s, %s, %s)
            """, (metric_name, value, json.dumps(labels)))
            self.db.conn.commit()
    
    def record_ingestion_lag(self, contract_id: str, lag_seconds: float):
        """Record ingestion lag."""
        self.record_metric(
            'spyne_ingestion_lag_seconds',
            lag_seconds,
            {'contract_id': contract_id}
        )
```

---

## ✅ Verification Checklist

- [ ] Database schema created successfully
- [ ] Stores initialized with database connection
- [ ] APIs registered in Flask app
- [ ] Authentication middleware added
- [ ] User context extracted from requests
- [ ] Table state API working
- [ ] Ingestion API working
- [ ] Query preview API working
- [ ] Join API working
- [ ] Drift detection API working
- [ ] Metrics collection working
- [ ] All role permissions enforced

---

## 🐛 Troubleshooting

### Issue: "Service not initialized"
**Solution:** Make sure you've called the init functions for each API:
```python
init_table_state_api(table_store)
init_ingestion_api(contract_store, system_config)
# etc.
```

### Issue: "Permission denied"
**Solution:** Check that you're sending the correct `X-User-Role` header:
```bash
-H "X-User-Role: ADMIN"  # For admin operations
```

### Issue: "Table not found"
**Solution:** Ensure tables exist in database and have correct state:
```sql
SELECT name, state FROM tables;
```

### Issue: "Invalid ingestion_semantics"
**Solution:** Check that all required fields are present:
- mode (append or upsert)
- idempotency_key (array)
- event_time_column (string)
- processing_time_column (string)
- dedupe_window (string like "24h")
- conflict_resolution (latest_wins or error)

---

## 📚 Additional Resources

- [EXECUTION_PLAN.md](./EXECUTION_PLAN.md) - Full execution plan
- [IMPLEMENTATION_STATUS.md](./IMPLEMENTATION_STATUS.md) - Implementation status
- [ENTERPRISE_SAFETY_IMPLEMENTATION.md](./ENTERPRISE_SAFETY_IMPLEMENTATION.md) - Implementation details

---

## 🎉 Next Steps

1. **Complete Store Implementations** - Implement all database queries
2. **Add Authentication** - Integrate JWT or session-based auth
3. **Add Tests** - Write comprehensive test suite
4. **Add Documentation** - Create user and admin guides
5. **Add Monitoring** - Set up dashboards and alerts

