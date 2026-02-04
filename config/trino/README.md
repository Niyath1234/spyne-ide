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
