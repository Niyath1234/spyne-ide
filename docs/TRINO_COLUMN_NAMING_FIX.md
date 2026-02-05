# Trino TPCH Column Naming Fix

## Problem
Trino was throwing error: `Column 'c_custkey' cannot be resolved` even though the table `tpch.tiny.customer` exists.

## Root Cause
Trino TPCH connector has two column naming modes:
1. **SIMPLIFIED (default)**: Uses `custkey`, `orderkey`, `name` (no prefixes)
2. **STANDARD**: Uses `c_custkey`, `o_orderkey`, `c_name` (with table prefixes)

The connector was using SIMPLIFIED mode, but:
- Our metadata files use STANDARD names (`c_custkey`, `o_orderkey`, etc.)
- Our SQL generation uses STANDARD names
- This mismatch caused column resolution failures

## Solution
Updated `/config/trino/catalog/tpch.properties` to use STANDARD column naming:

```properties
connector.name=tpch
tpch.splits-per-node=4
tpch.column-naming=STANDARD  # ← Added this line
```

## Impact
After restarting Trino, column names will match:
- ✅ `c_custkey` (not `custkey`)
- ✅ `o_orderkey` (not `orderkey`)
- ✅ `l_extendedprice` (not `extendedprice`)
- ✅ `c_name` (not `name`)

This matches:
- TPC-H specification
- Our metadata files (`metadata/tables.json`)
- Our SQL generation code

## Next Steps

**Restart Trino container:**
```bash
docker-compose restart trino
```

**Verify the fix:**
```bash
# This should now work:
curl -X POST http://localhost:8081/v1/statement \
  -H "Content-Type: text/plain" \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: tpch" \
  -H "X-Trino-Schema: tiny" \
  -d "SELECT c_custkey FROM tpch.tiny.customer LIMIT 5"
```

## References
- [Trino TPCH Connector Documentation](https://trino.io/docs/current/connector/tpch.html)
- TPC-H Benchmark Specification
