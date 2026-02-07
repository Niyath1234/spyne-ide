# Trino Configuration

This directory contains the configuration files for Trino, a distributed SQL query engine.

## Configuration Files

- **config.properties**: Main Trino configuration (coordinator settings, memory limits, etc.)
- **node.properties**: Node-specific settings (environment, data directory)
- **jvm.config**: JVM settings for Trino
- **catalog/**: Catalog configurations for data sources

## Catalogs

### PostgreSQL (`postgres.properties`)
Connects to the PostgreSQL database service defined in docker-compose.yml.

**Note**: Update the `connection-password` in `postgres.properties` to match your `RCA_DB_PASSWORD` environment variable if you've changed it from the default.

### TPCH (`tpch.properties`)
TPC-H benchmark connector for testing and demos.

**Configuration**:
- `connector.name=tpch` - TPCH connector
- `tpch.splits-per-node=4` - Parallel processing splits

**Available Schemas**:
- `tiny` - Smallest dataset (~1MB)
- `sf1` - Scale factor 1 (~1GB)
- `sf10` - Scale factor 10 (~10GB)
- `sf100` - Scale factor 100 (~100GB)

**Example Queries**:
```sql
-- List available schemas
SHOW SCHEMAS FROM tpch;

-- List tables in tiny schema
SHOW TABLES FROM tpch.tiny;

-- Query customer table
SELECT * FROM tpch.tiny.customer LIMIT 10;

-- Query orders
SELECT * FROM tpch.tiny.orders LIMIT 10;

-- Join customer and orders
SELECT 
    c.custkey,
    c.name,
    COUNT(o.orderkey) as order_count
FROM tpch.tiny.customer c
LEFT JOIN tpch.tiny.orders o ON c.custkey = o.custkey
GROUP BY c.custkey, c.name
LIMIT 10;
```

**Verifying TPCH is Enabled**:
```sql
-- Check if TPCH catalog is available
SHOW CATALOGS;
-- Should show: tpch, postgres, tpcds (if enabled)

-- Check TPCH schemas
SHOW SCHEMAS FROM tpch;
-- Should show: information_schema, tiny, sf1, sf10, sf100, etc.
```

### TPCDS (`tpcds.properties`)
TPC-DS benchmark connector for testing and demos.

**Configuration**:
- `connector.name=tpcds` - TPC-DS connector
- `tpcds.splits-per-node=4` - Parallel processing splits

**Available Schemas**:
- `tiny` - Smallest dataset (scale factor 0.01, ~10MB) - useful for testing
- `sf1` - Scale factor 1 (~1GB)
- `sf10` - Scale factor 10 (~10GB)
- `sf100` - Scale factor 100 (~100GB)
- `sf300` - Scale factor 300 (~300GB)
- `sf1000` - Scale factor 1000 (~1TB)
- `sf3000` - Scale factor 3000 (~3TB)
- `sf10000` - Scale factor 10000 (~10TB)
- `sf30000` - Scale factor 30000 (~30TB)
- `sf100000` - Scale factor 100000 (~100TB)

**TPC-DS Tables (24 total)**:
- **Fact Tables (7)**: `store_sales`, `store_returns`, `catalog_sales`, `catalog_returns`, `web_sales`, `web_returns`, `inventory`
- **Dimension Tables (17)**: `store`, `call_center`, `catalog_page`, `web_site`, `web_page`, `warehouse`, `customer`, `customer_address`, `customer_demographics`, `date_dim`, `household_demographics`, `item`, `promotion`, `reason`, `ship_mode`, `time_dim`, `income_band`

**Example Queries**:
```sql
-- List available schemas
SHOW SCHEMAS FROM tpcds;

-- List tables in tiny schema
SHOW TABLES FROM tpcds.tiny;

-- Query customer table
SELECT * FROM tpcds.tiny.customer LIMIT 10;

-- Query store sales
SELECT * FROM tpcds.tiny.store_sales LIMIT 10;

-- Join customer and store sales
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

-- Multi-channel sales analysis
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

**Verifying TPC-DS is Enabled**:
```sql
-- Check if TPC-DS catalog is available
SHOW CATALOGS;
-- Should show: tpch, postgres, tpcds

-- Check TPC-DS schemas
SHOW SCHEMAS FROM tpcds;
-- Should show: information_schema, tiny, sf1, sf10, sf100, sf300, sf1000, etc.

-- List tables in tiny schema
SHOW TABLES FROM tpcds.tiny;
-- Should show all 24 TPC-DS tables
```

## Usage

### Starting Trino

```bash
docker-compose up trino
```

Or start with all services:
```bash
docker-compose up
```

### Accessing Trino

- **Web UI**: http://localhost:8081
- **JDBC URL**: `jdbc:trino://localhost:8081`
- **CLI**: Use the Trino CLI or any JDBC-compatible client

### Connecting to Trino

#### Using Trino CLI
```bash
docker exec -it rca-trino trino --server http://localhost:8080
```

#### Example Queries

**Check available catalogs**:
```sql
SHOW CATALOGS;
-- Should show: tpch, postgres, tpcds (if enabled)
```

**TPCH testing**:
```sql
-- List TPCH schemas
SHOW SCHEMAS FROM tpch;

-- Query TPCH data
SELECT * FROM tpch.tiny.customer LIMIT 10;
SELECT * FROM tpch.tiny.orders LIMIT 10;
```

**PostgreSQL queries**:
```sql
SHOW SCHEMAS FROM postgres;
SELECT * FROM postgres.rca_engine.your_table LIMIT 10;
```

## Customization

To add more catalogs or modify settings:

1. Add catalog properties files in `catalog/` directory
2. Modify `config.properties` for server settings
3. Restart the Trino container: `docker-compose restart trino`

## Troubleshooting

- Check logs: `docker-compose logs trino`
- Verify health: `curl http://localhost:8081/v1/info`
- Ensure PostgreSQL is running if using the postgres catalog: `docker-compose ps postgres`
