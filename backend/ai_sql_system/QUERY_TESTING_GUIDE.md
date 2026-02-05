# Query Testing Guide - Discount Query

## Test Query
```
given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level
```

## Expected Output

### Correct SQL Should Be:
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

## Testing Steps

### 1. Start Docker Services
```bash
cd /Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine
docker-compose up -d
```

### 2. Test via API Endpoint
```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
  }' | python3 -m json.tool
```

### 3. Check Response

The response should include:
- `success: true`
- `sql`: Generated SQL query
- `intent`: Extracted intent with metric formula
- `method: "langgraph_pipeline"`

### 4. Validate SQL Structure

Check that the SQL has:
- ✅ `SUM(l_extendedprice * (1 - l_discount))` formula
- ✅ `FROM tpch.tiny.lineitem` (base table)
- ✅ `JOIN tpch.tiny.orders` (via l_orderkey = o_orderkey)
- ✅ `JOIN tpch.tiny.customer` (via o_custkey = c_custkey)
- ✅ `GROUP BY customer.c_custkey`
- ✅ Proper table aliases (if used)

## Key Mapping Requirements

1. **Column Mapping**:
   - `extendedprice` → `l_extendedprice` (lineitem table)
   - `discount` → `l_discount` (lineitem table)

2. **Table Mapping**:
   - Base table: `lineitem` (has the discount columns)
   - Join path: `lineitem` → `orders` → `customer`

3. **Grain Mapping**:
   - "at customer level" → `GROUP BY customer.c_custkey`

## Pipeline Flow

1. **Intent Engine**: Should extract:
   ```json
   {
     "metric": "discount",
     "metric_formula": "SUM(extendedprice * (1 - discount))",
     "grain": "customer",
     "filters": []
   }
   ```

2. **Resolution Engine**: Should classify as `DERIVABLE` (formula provided)

3. **Semantic Retrieval**: Should find:
   - `lineitem` table (has l_extendedprice, l_discount)
   - `customer` table
   - Join relationships

4. **Join Planner**: Should find path:
   - `lineitem` → `orders` → `customer`

5. **Query Planner**: Should build plan with:
   - base_table: "lineitem"
   - metric_sql: "SUM(l_extendedprice * (1 - l_discount))"
   - group_by: ["customer.c_custkey"]

6. **SQL Generator**: Should generate Trino SQL

7. **SQL Critic**: Should validate and fix if needed

8. **Validators**: Should pass AST and Trino validation

## Troubleshooting

### If SQL is incorrect:

1. **Check Intent Extraction**:
   - Does it extract the formula correctly?
   - Does it identify "customer" as grain?

2. **Check Join Path**:
   - Is the join graph loaded correctly?
   - Does it find the path from lineitem to customer?

3. **Check Column Mapping**:
   - Does it map "extendedprice" to "l_extendedprice"?
   - Does it map "discount" to "l_discount"?

4. **Check SQL Generation**:
   - Does it use the correct table aliases?
   - Does it include GROUP BY?

### Common Issues:

1. **Wrong table aliases**: Should use proper aliases or full table names
2. **Missing GROUP BY**: Should always include GROUP BY for aggregations
3. **Wrong column names**: Should use l_extendedprice and l_discount, not extendedprice/discount
4. **Missing joins**: Should join through orders to get to customer

## Expected Performance

- **Latency**: < 3.5 seconds
- **Accuracy**: Should generate correct SQL on first try
- **Validation**: Should pass Trino EXPLAIN validation

## Next Steps After Testing

1. If SQL is correct: ✅ System is working!
2. If SQL has issues: Check logs and improve prompts/modules
3. Run evaluation suite: Test with 200+ queries
