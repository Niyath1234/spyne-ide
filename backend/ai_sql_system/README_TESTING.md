# Testing the New AI SQL System

## Quick Test Command

Once Docker is running, test the discount query:

```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"
  }'
```

## Expected SQL

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

## What to Check

1. ✅ SQL uses correct columns: `l_extendedprice` and `l_discount`
2. ✅ SQL joins through: `lineitem` → `orders` → `customer`
3. ✅ SQL has `GROUP BY customer.c_custkey`
4. ✅ SQL uses correct formula: `SUM(l_extendedprice * (1 - l_discount))`
5. ✅ Response includes `method: "langgraph_pipeline"`

## System Status

The new AI SQL System is now active and should:
- Use LangGraph 10-node pipeline
- Load join graph from metadata/lineage.json
- Use semantic retrieval with embeddings
- Self-correct SQL with critic loop
- Validate with Trino EXPLAIN

## Files Changed

- `/api/reasoning/query` → Now uses LangGraph pipeline
- `/api/v1/notebooks/<id>/cells/<cell_id>/generate-sql` → Now uses LangGraph pipeline

Old system (`llm_query_generator.py`) is no longer used by these endpoints.
