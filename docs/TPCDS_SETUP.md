# TPC-DS Setup Guide

This document describes the TPC-DS (Transaction Processing Performance Council - Decision Support) benchmark setup for Trino testing.

## Overview

TPC-DS is a decision support benchmark that models a retail product supplier. It includes:
- **24 tables total**: 7 fact tables and 17 dimension tables
- **Multiple scale factors**: From `tiny` (0.01GB) to `sf100000` (100TB)
- **On-the-fly data generation**: Trino generates data deterministically when queries are executed

## Configuration

The TPC-DS connector is configured in `/config/trino/catalog/tpcds.properties`:

```properties
connector.name=tpcds
tpcds.splits-per-node=4
```

## Available Schemas

TPC-DS provides multiple schemas with different scale factors:

- `tiny` - Scale factor 0.01 (~10MB) - **Recommended for testing**
- `sf1` - Scale factor 1 (~1GB)
- `sf10` - Scale factor 10 (~10GB)
- `sf100` - Scale factor 100 (~100GB)
- `sf300` - Scale factor 300 (~300GB)
- `sf1000` - Scale factor 1000 (~1TB)
- `sf3000` - Scale factor 3000 (~3TB)
- `sf10000` - Scale factor 10000 (~10TB)
- `sf30000` - Scale factor 30000 (~30TB)
- `sf100000` - Scale factor 100000 (~100TB)

## Tables

### Fact Tables (7)
1. **store_sales** - Sales transactions through store channel
2. **store_returns** - Returns for store channel sales
3. **catalog_sales** - Sales transactions through catalog channel
4. **catalog_returns** - Returns for catalog channel sales
5. **web_sales** - Sales transactions through web channel
6. **web_returns** - Returns for web channel sales
7. **inventory** - Product quantities on-hand at warehouses by week

### Dimension Tables (17)
1. **customer** - Customer information
2. **customer_address** - Customer addresses
3. **customer_demographics** - Customer demographic combinations
4. **date_dim** - Calendar dates
5. **household_demographics** - Household demographics
6. **item** - Product/item information
7. **store** - Store details
8. **call_center** - Call center details
9. **catalog_page** - Catalog page details
10. **web_site** - Website details
11. **web_page** - Web page details
12. **warehouse** - Warehouse information
13. **promotion** - Promotion information
14. **reason** - Return reason codes
15. **ship_mode** - Shipping mode information
16. **time_dim** - Time dimension
17. **income_band** - Income band information

## Usage

### Starting Trino

```bash
cd docker
docker-compose up trino
```

Or start all services:
```bash
docker-compose up
```

### Verifying Setup

Run the test script:
```bash
./scripts/test_tpcds.sh
```

Or manually verify:
```bash
# Check if TPC-DS catalog is available
curl -X POST \
  -H "X-Trino-User: admin" \
  -H "Content-Type: application/json" \
  http://localhost:8081/v1/statement \
  -d "SHOW CATALOGS"

# List schemas
curl -X POST \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: tpcds" \
  -H "Content-Type: application/json" \
  http://localhost:8081/v1/statement \
  -d "SHOW SCHEMAS FROM tpcds"

# List tables
curl -X POST \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: tpcds" \
  -H "X-Trino-Schema: tiny" \
  -H "Content-Type: application/json" \
  http://localhost:8081/v1/statement \
  -d "SHOW TABLES FROM tpcds.tiny"
```

### Example Queries

#### Basic Queries

```sql
-- Query customer table
SELECT * FROM tpcds.tiny.customer LIMIT 10;

-- Query store sales
SELECT * FROM tpcds.tiny.store_sales LIMIT 10;

-- Query date dimension
SELECT * FROM tpcds.tiny.date_dim LIMIT 10;
```

#### Join Queries

```sql
-- Customer sales analysis
SELECT 
    c.c_customer_id,
    c.c_first_name,
    c.c_last_name,
    COUNT(ss.ss_item_sk) as purchase_count,
    SUM(ss.ss_sales_price) as total_spent
FROM tpcds.tiny.customer c
LEFT JOIN tpcds.tiny.store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name
LIMIT 10;
```

#### Multi-Channel Analysis

```sql
-- Compare sales across channels
SELECT 
    'store' as channel,
    COUNT(*) as sales_count,
    SUM(ss_sales_price) as total_revenue
FROM tpcds.tiny.store_sales
UNION ALL
SELECT 
    'catalog' as channel,
    COUNT(*) as sales_count,
    SUM(cs_sales_price) as total_revenue
FROM tpcds.tiny.catalog_sales
UNION ALL
SELECT 
    'web' as channel,
    COUNT(*) as sales_count,
    SUM(ws_sales_price) as total_revenue
FROM tpcds.tiny.web_sales;
```

#### Time-Based Analysis

```sql
-- Sales by date
SELECT 
    d.d_date,
    d.d_day_name,
    COUNT(ss.ss_item_sk) as sales_count,
    SUM(ss.ss_sales_price) as total_revenue
FROM tpcds.tiny.store_sales ss
JOIN tpcds.tiny.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
GROUP BY d.d_date, d.d_day_name
ORDER BY d.d_date
LIMIT 20;
```

## Metadata

TPC-DS table schemas are included in `/metadata/tables.json`. The following tables are currently documented:

- `tpcds.tiny.customer`
- `tpcds.tiny.store_sales`
- `tpcds.tiny.date_dim`
- `tpcds.tiny.item`
- `tpcds.tiny.store`
- `tpcds.tiny.warehouse`
- `tpcds.tiny.web_sales`
- `tpcds.tiny.catalog_sales`
- `tpcds.tiny.inventory`

To extract additional table schemas from Trino, use:
```bash
python scripts/extract_tpcds_schema.py tiny
```

## Integration with RCA Engine

TPC-DS tables are available for testing with the RCA Engine's natural language to SQL system:

1. **Query Generation**: The system can generate SQL queries referencing TPC-DS tables
2. **Validation**: Trino validator can validate queries against TPC-DS schemas
3. **Testing**: Use TPC-DS for testing complex multi-table queries and joins

### Example Natural Language Query

```
"Show me the top 10 customers by total sales across all channels"
```

This can be translated to SQL using TPC-DS tables:
```sql
SELECT 
    c.c_customer_id,
    c.c_first_name,
    c.c_last_name,
    COALESCE(store_sales.total, 0) + 
    COALESCE(catalog_sales.total, 0) + 
    COALESCE(web_sales.total, 0) as total_sales
FROM tpcds.tiny.customer c
LEFT JOIN (
    SELECT ss_customer_sk, SUM(ss_sales_price) as total
    FROM tpcds.tiny.store_sales
    GROUP BY ss_customer_sk
) store_sales ON c.c_customer_sk = store_sales.ss_customer_sk
LEFT JOIN (
    SELECT cs_bill_customer_sk, SUM(cs_sales_price) as total
    FROM tpcds.tiny.catalog_sales
    GROUP BY cs_bill_customer_sk
) catalog_sales ON c.c_customer_sk = catalog_sales.cs_bill_customer_sk
LEFT JOIN (
    SELECT ws_bill_customer_sk, SUM(ws_sales_price) as total
    FROM tpcds.tiny.web_sales
    GROUP BY ws_bill_customer_sk
) web_sales ON c.c_customer_sk = web_sales.ws_bill_customer_sk
ORDER BY total_sales DESC
LIMIT 10;
```

## Troubleshooting

### TPC-DS Catalog Not Found

1. Verify `tpcds.properties` exists in `config/trino/catalog/`
2. Restart Trino: `docker-compose restart trino`
3. Check Trino logs: `docker-compose logs trino`

### Tables Not Accessible

1. Verify schema exists: `SHOW SCHEMAS FROM tpcds;`
2. Check table list: `SHOW TABLES FROM tpcds.tiny;`
3. Ensure you're using the correct schema name (e.g., `tiny`, `sf1`)

### Query Performance

- Start with `tiny` schema for testing
- Use `LIMIT` clauses for initial testing
- Scale up to larger schemas (`sf1`, `sf10`) for performance testing

## References

- [Trino TPC-DS Connector Documentation](https://trino.io/docs/current/connector/tpcds.html)
- [TPC-DS Benchmark Specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf)
- [Trino Configuration Guide](../config/trino/README.md)
