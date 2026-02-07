# Both TPCH and TPCDS Catalogs Work!

Yes, you're absolutely right! **Both TPCH and TPCDS catalogs should work** in your Trino setup.

## Configuration Status

Both catalogs are configured and available:

### TPCH Catalog (`tpch`)
- **Config file:** `config/trino/catalog/tpch.properties`
- **Tables:** 8 tables (customer, orders, lineitem, part, supplier, partsupp, nation, region)
- **Schemas:** tiny, sf1, sf10, sf100, etc.

### TPCDS Catalog (`tpcds`)
- **Config file:** `config/trino/catalog/tpcds.properties`
- **Tables:** 24 tables (customer, store_sales, catalog_sales, web_sales, inventory, etc.)
- **Schemas:** tiny, sf1, sf10, sf100, sf300, sf1000, etc.

## How It Works

The backend **auto-detects** which catalog to use based on your SQL query:

1. **If your query contains `tpch.`** → Uses `tpch` catalog
2. **If your query contains `tpcds.`** → Uses `tpcds` catalog
3. **If neither is specified** → Defaults to `tpcds` (or uses `TRINO_CATALOG` env var)

## Example Queries

### TPCH Queries
```sql
-- ✅ Works - TPCH customer table
SELECT * FROM tpch.tiny.customer LIMIT 10;

-- ✅ Works - TPCH orders
SELECT * FROM tpch.tiny.orders LIMIT 10;

-- ✅ Works - TPCH with joins
SELECT 
    c.c_name,
    COUNT(o.o_orderkey) as order_count
FROM tpch.tiny.customer c
LEFT JOIN tpch.tiny.orders o ON c.c_custkey = o.o_custkey
GROUP BY c.c_name
LIMIT 10;
```

### TPCDS Queries
```sql
-- ✅ Works - TPCDS customer table
SELECT * FROM tpcds.tiny.customer LIMIT 10;

-- ✅ Works - TPCDS store sales
SELECT * FROM tpcds.tiny.store_sales LIMIT 10;

-- ✅ Works - TPCDS multi-channel
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

### Mixed Queries (if needed)
```sql
-- You can't mix TPCH and TPCDS in a single query
-- But you can run them separately:

-- First query: TPCH
SELECT COUNT(*) FROM tpch.tiny.customer;

-- Second query: TPCDS
SELECT COUNT(*) FROM tpcds.tiny.customer;
```

## Verification

To verify both catalogs are working:

```bash
# Run the verification script
./scripts/verify_trino_catalogs.sh

# Or manually check:
curl -X POST \
  -H "X-Trino-User: admin" \
  -H "Content-Type: text/plain" \
  http://localhost:8081/v1/statement \
  -d "SHOW CATALOGS"
```

Should show: `tpch`, `tpcds`, `postgres` (if configured)

## Key Differences

| Feature | TPCH | TPCDS |
|---------|------|-------|
| **Purpose** | Transaction processing benchmark | Decision support benchmark |
| **Tables** | 8 tables | 24 tables |
| **Focus** | Order processing | Multi-channel retail |
| **Channels** | Single channel | Store, Catalog, Web |
| **Use Case** | OLTP workloads | OLAP/BI workloads |

## Common Tables Comparison

| TPCH | TPCDS | Notes |
|------|-------|-------|
| `customer` | `customer` | Both have customer tables but different schemas |
| `orders` | `store_sales`, `catalog_sales`, `web_sales` | TPCDS has 3 sales channels |
| `lineitem` | N/A | TPCH has line items, TPCDS has separate sales tables |
| `part` | `item` | Similar concept, different names |
| `supplier` | N/A | TPCH has suppliers |
| N/A | `inventory` | TPCDS tracks inventory |
| N/A | `date_dim` | TPCDS has date dimension table |

## Troubleshooting

If one catalog doesn't work:

1. **Check Trino is running:**
   ```bash
   docker-compose ps trino
   ```

2. **Verify catalog files exist:**
   ```bash
   ls config/trino/catalog/tpch.properties
   ls config/trino/catalog/tpcds.properties
   ```

3. **Restart Trino:**
   ```bash
   docker-compose restart trino
   ```

4. **Check Trino logs:**
   ```bash
   docker-compose logs trino | tail -50
   ```

5. **Test both catalogs:**
   ```bash
   # Test TPCH
   curl -X POST \
     -H "X-Trino-User: admin" \
     -H "X-Trino-Catalog: tpch" \
     -H "X-Trino-Schema: tiny" \
     -H "Content-Type: text/plain" \
     http://localhost:8081/v1/statement \
     -d "SELECT COUNT(*) FROM tpch.tiny.customer"
   
   # Test TPCDS
   curl -X POST \
     -H "X-Trino-User: admin" \
     -H "X-Trino-Catalog: tpcds" \
     -H "X-Trino-Schema: tiny" \
     -H "Content-Type: text/plain" \
     http://localhost:8081/v1/statement \
     -d "SELECT COUNT(*) FROM tpcds.tiny.customer"
   ```

## Summary

✅ **Both TPCH and TPCDS work!**

- TPCH: Use `tpch.tiny.*` tables
- TPCDS: Use `tpcds.tiny.*` tables
- Backend auto-detects catalog from your SQL
- Both are configured and ready to use

Just make sure:
1. Trino container is running
2. Both catalog files exist
3. Use correct catalog name in queries (`tpch` or `tpcds`, not `tpds`)
