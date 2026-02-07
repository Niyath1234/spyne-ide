# TPC-DS Troubleshooting Guide

## Common Issues and Solutions

### Issue 1: Catalog 'tpds' not found

**Error:**
```
Catalog 'tpds' not found
```

**Cause:** Typo in catalog name - you typed `tpds` instead of `tpcds`

**Solution:**
- Use `tpcds` (with 'c') not `tpds`
- Correct query: `SELECT * FROM tpcds.tiny.customer LIMIT 10;`
- Wrong query: `SELECT * FROM tpds.tiny.customer LIMIT 10;`

### Issue 2: Catalog 'tpcds' not found

**Error:**
```
Catalog 'tpcds' not found
```

**Possible Causes:**

1. **Trino container not running**
   ```bash
   # Check if Trino is running
   docker-compose ps trino
   
   # Start Trino
   cd docker
   docker-compose up trino
   ```

2. **TPC-DS catalog not configured**
   ```bash
   # Verify tpcds.properties exists
   ls config/trino/catalog/tpcds.properties
   
   # Should contain:
   # connector.name=tpcds
   # tpcds.splits-per-node=4
   ```

3. **Trino not restarted after adding catalog**
   ```bash
   # Restart Trino to load new catalog
   docker-compose restart trino
   
   # Check logs for errors
   docker-compose logs trino | tail -50
   ```

### Issue 3: Schema 'tiny' does not exist

**Error:**
```
Schema 'tiny' does not exist
```

**Possible Causes:**

1. **Wrong catalog specified**
   - Make sure you're using `tpcds.tiny` not `tpch.tiny` for TPC-DS tables
   - TPC-DS tables: `tpcds.tiny.customer`, `tpcds.tiny.store_sales`
   - TPC-H tables: `tpch.tiny.customer`, `tpch.tiny.orders`

2. **Schema name typo**
   - Use `tiny` (lowercase) not `Tiny` or `TINY`
   - Correct: `SELECT * FROM tpcds.tiny.customer;`
   - Wrong: `SELECT * FROM tpcds.Tiny.customer;`

### Issue 4: Table does not exist

**Error:**
```
Table 'tpch.tiny.catalog_sales' does not exist
```

**Cause:** Using wrong catalog - `catalog_sales` is a TPC-DS table, not TPC-H

**Solution:**
- TPC-DS tables: `tpcds.tiny.catalog_sales`, `tpcds.tiny.store_sales`, `tpcds.tiny.web_sales`
- TPC-H tables: `tpch.tiny.orders`, `tpch.tiny.lineitem`, `tpch.tiny.customer`

**Correct query:**
```sql
SELECT * FROM tpcds.tiny.catalog_sales LIMIT 10;
```

### Issue 5: Backend defaults to wrong catalog

**Problem:** Backend always uses `tpch` catalog even for TPC-DS queries

**Solution:** The backend now auto-detects catalog from SQL query:
- If SQL contains `tpcds.` → uses `tpcds` catalog
- If SQL contains `tpch.` → uses `tpch` catalog
- Otherwise defaults to `tpcds`

You can also set environment variable:
```bash
export TRINO_CATALOG=tpcds
export TRINO_SCHEMA=tiny
```

## Verification Steps

### Step 1: Verify Trino is Running

```bash
# Check Trino container status
cd docker
docker-compose ps trino

# Check Trino health
curl http://localhost:8081/v1/info
```

### Step 2: Verify Catalogs are Available

Run the verification script:
```bash
./scripts/verify_trino_catalogs.sh
```

Or manually:
```bash
curl -X POST \
  -H "X-Trino-User: admin" \
  -H "Content-Type: text/plain" \
  http://localhost:8081/v1/statement \
  -d "SHOW CATALOGS"
```

Should show: `tpcds`, `tpch`, `postgres` (if configured)

### Step 3: Verify TPC-DS Schema

```bash
curl -X POST \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: tpcds" \
  -H "Content-Type: text/plain" \
  http://localhost:8081/v1/statement \
  -d "SHOW SCHEMAS FROM tpcds"
```

Should show: `tiny`, `sf1`, `sf10`, `sf100`, etc.

### Step 4: Verify Tables Exist

```bash
curl -X POST \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: tpcds" \
  -H "X-Trino-Schema: tiny" \
  -H "Content-Type: text/plain" \
  http://localhost:8081/v1/statement \
  -d "SHOW TABLES FROM tpcds.tiny"
```

Should show all 24 TPC-DS tables.

### Step 5: Test a Simple Query

```bash
curl -X POST \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: tpcds" \
  -H "X-Trino-Schema: tiny" \
  -H "Content-Type: text/plain" \
  http://localhost:8081/v1/statement \
  -d "SELECT COUNT(*) FROM tpcds.tiny.customer"
```

## Quick Reference

### Correct Catalog Names
- ✅ `tpcds` - TPC-DS benchmark (24 tables)
- ✅ `tpch` - TPC-H benchmark (8 tables)
- ❌ `tpds` - Typo, does not exist

### Correct Table Names

**TPC-DS Tables (use `tpcds` catalog):**
- `tpcds.tiny.customer`
- `tpcds.tiny.store_sales`
- `tpcds.tiny.catalog_sales`
- `tpcds.tiny.web_sales`
- `tpcds.tiny.inventory`
- `tpcds.tiny.date_dim`
- `tpcds.tiny.item`
- `tpcds.tiny.store`
- `tpcds.tiny.warehouse`
- And 15 more dimension tables...

**TPC-H Tables (use `tpch` catalog):**
- `tpch.tiny.customer`
- `tpch.tiny.orders`
- `tpch.tiny.lineitem`
- `tpch.tiny.part`
- `tpch.tiny.supplier`
- `tpch.tiny.nation`
- `tpch.tiny.region`
- `tpch.tiny.partsupp`

### Example Queries

**TPC-DS:**
```sql
-- List customers
SELECT * FROM tpcds.tiny.customer LIMIT 10;

-- Store sales
SELECT * FROM tpcds.tiny.store_sales LIMIT 10;

-- Multi-channel sales
SELECT 
    'store' as channel,
    COUNT(*) as sales_count
FROM tpcds.tiny.store_sales
UNION ALL
SELECT 
    'web' as channel,
    COUNT(*) as sales_count
FROM tpcds.tiny.web_sales;
```

**TPC-H:**
```sql
-- List customers
SELECT * FROM tpch.tiny.customer LIMIT 10;

-- Orders
SELECT * FROM tpch.tiny.orders LIMIT 10;
```

## Still Having Issues?

1. **Check Trino logs:**
   ```bash
   docker-compose logs trino | tail -100
   ```

2. **Verify configuration files:**
   ```bash
   cat config/trino/catalog/tpcds.properties
   ```

3. **Restart Trino:**
   ```bash
   docker-compose restart trino
   ```

4. **Check Trino Web UI:**
   - Open http://localhost:8081
   - Navigate to "Catalogs" to see available catalogs

5. **Run test script:**
   ```bash
   ./scripts/test_tpcds.sh
   ```

## Environment Variables

You can set these environment variables to override defaults:

```bash
export TRINO_HOST=localhost
export TRINO_PORT=8081
export TRINO_USER=admin
export TRINO_CATALOG=tpcds  # Default catalog
export TRINO_SCHEMA=tiny    # Default schema
```
