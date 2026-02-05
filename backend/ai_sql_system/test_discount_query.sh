#!/bin/bash
# Test script for discount query via API

echo "Testing discount query via API endpoint..."
echo ""

QUERY="given the formula for discount is SUM(extendedprice * (1 - discount)), give me discount at customer level"

curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"$QUERY\"}" \
  | python3 -m json.tool

echo ""
echo "Expected SQL should:"
echo "1. SELECT customer.c_custkey, SUM(l_extendedprice * (1 - l_discount)) AS discount"
echo "2. FROM tpch.tiny.lineitem"
echo "3. JOIN tpch.tiny.orders ON lineitem.l_orderkey = orders.o_orderkey"
echo "4. JOIN tpch.tiny.customer ON orders.o_custkey = customer.c_custkey"
echo "5. GROUP BY customer.c_custkey"
