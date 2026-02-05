# Trino JOIN Clause Fix - Summary

## Problem
Trino was throwing error: `Column 'o.o_custkey' cannot be resolved` when executing SQL queries with JOIN clauses.

## Root Cause
Trino requires that JOIN ON clauses reference tables that are **already in scope** first. When you have:
```sql
FROM customer c
LEFT JOIN orders o ON o.o_custkey = c.c_custkey
```

Trino cannot resolve `o.o_custkey` because the alias `o` is being defined in the same JOIN clause. The condition must be:
```sql
FROM customer c
LEFT JOIN orders o ON c.c_custkey = o.o_custkey
```

Where `c` (already in scope from FROM) comes first.

## Solution
Fixed the `_normalize_join_condition` method in `deterministic_builder.py` to:
1. Always put the already-scoped table (table1) first in JOIN conditions
2. Swap conditions if they come in the wrong order
3. Handle both fully qualified and short table names
4. Use proper alias mapping

## Changes Made

### 1. `backend/ai_sql_system/sql/deterministic_builder.py`
- Enhanced `_normalize_join_condition()` to detect and swap JOIN condition order
- Added safety check in JOIN clause building to ensure correct order
- Improved table name normalization with word boundaries

### 2. `backend/sql_builder.py`
- Improved `_replace_table_names()` with better case-insensitive matching
- Enhanced table name replacement logic

## Testing
All tests pass:
- ✅ `orders.o_custkey = customer.c_custkey` → `c.c_custkey = o.o_custkey`
- ✅ `customer.c_custkey = orders.o_custkey` → `c.c_custkey = o.o_custkey`
- ✅ `o.o_custkey = c.c_custkey` → `c.c_custkey = o.o_custkey`
- ✅ `c.c_custkey = o.o_custkey` → `c.c_custkey = o.o_custkey` (already correct)

## Next Steps

**IMPORTANT: Restart the backend server to load the changes**

After restart, test with:
```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{"query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"}'
```

The generated SQL should now have JOIN conditions in the correct order:
```sql
LEFT JOIN orders o ON c.c_custkey = o.o_custkey
```

Instead of:
```sql
LEFT JOIN orders o ON o.o_custkey = c.c_custkey
```

## Verification
Run the test script to verify the fix:
```bash
python3 test_join_fix.py
```

All tests should pass ✅
