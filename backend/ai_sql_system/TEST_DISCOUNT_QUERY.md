# Testing Discount Query

## Query
```
given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level
```

## Expected SQL

The system should generate:

```sql
SELECT 
    customer.c_custkey,
    SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS discount
FROM tpch.tiny.lineitem
LEFT JOIN tpch.tiny.orders 
    ON lineitem.l_orderkey = orders.o_orderkey
LEFT JOIN tpch.tiny.customer 
    ON orders.o_custkey = customer.c_custkey
GROUP BY customer.c_custkey
```

## Key Requirements

1. **Formula**: `SUM(l_extendedprice * (1 - l_discount))`
   - Maps "extendedprice" → `l_extendedprice`
   - Maps "discount" → `l_discount`

2. **Grain**: "at customer level" → `GROUP BY customer.c_custkey`

3. **Joins**:
   - `lineitem` → `orders` (via `l_orderkey = o_orderkey`)
   - `orders` → `customer` (via `o_custkey = c_custkey`)

4. **Base Table**: `lineitem` (contains the discount columns)

## Testing via API

Once Docker is running, test with:

```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
  }'
```

Or use the test script:

```bash
chmod +x backend/ai_sql_system/test_discount_query.sh
./backend/ai_sql_system/test_discount_query.sh
```

## What the System Should Do

1. **Intent Extraction**: 
   - metric: "discount" with formula "SUM(extendedprice * (1 - discount))"
   - grain: "customer"
   - filters: []

2. **Resolution**: 
   - Should classify as DERIVABLE (formula provided)

3. **Semantic Retrieval**: 
   - Find lineitem table (has l_extendedprice, l_discount)
   - Find customer table
   - Find join path: lineitem → orders → customer

4. **Join Planning**: 
   - Use NetworkX to find path: lineitem → orders → customer

5. **Query Plan**: 
   - base_table: "lineitem"
   - joins: [orders, customer]
   - metric_sql: "SUM(l_extendedprice * (1 - l_discount))"
   - group_by: ["customer.c_custkey"]

6. **SQL Generation**: 
   - Generate Trino SQL with proper aliases

7. **SQL Critic**: 
   - Check for missing GROUP BY
   - Verify column references

8. **Validation**: 
   - AST validation
   - Trino EXPLAIN validation

## Column Mapping

- `extendedprice` → `l_extendedprice` (in lineitem table)
- `discount` → `l_discount` (in lineitem table)
- `customer` → `customer.c_custkey` (for grouping)

## Notes

- The query explicitly provides the formula, so the system should use it directly
- "at customer level" means GROUP BY customer
- The system should use table aliases properly (t1, t2, t3 or explicit names)
