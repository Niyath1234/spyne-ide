# Quick Commands - PostgreSQL Migration

## Fix Vendoring Issue

```bash
cd /Users/niyathnair/Desktop/Task/RCA-ENGINE

# Option 1: Disable vendoring (recommended for now)
cat > .cargo/config.toml <<'EOF'
# Vendoring disabled for PostgreSQL development
EOF

# Now build will download dependencies from crates.io
cargo build --bin test_db_connection
cargo build --bin migrate_metadata
```

## Test Database Connection

```bash
cargo run --bin test_db_connection
```

## Run Migration

```bash
# This will load all JSON metadata into PostgreSQL
cargo run --bin migrate_metadata
```

## Verify Migration in PostgreSQL

```bash
psql -d rca_engine -c "SELECT COUNT(*) FROM entities;"
psql -d rca_engine -c "SELECT COUNT(*) FROM tables;"
psql -d rca_engine -c "SELECT COUNT(*) FROM rules;"
psql -d rca_engine -c "SELECT COUNT(*) FROM metrics;"
```

## Start Server with PostgreSQL

```bash
# Make sure USE_POSTGRES=true in .env
cargo run --bin server
```

## Test RCA Query

```bash
curl -X POST http://localhost:8080/api/reasoning/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Compare scf_v1 and scf_v2 ledger balance"}'
```

## Re-enable Vendoring Later (if needed)

```bash
# Restore original vendor config
mv .cargo/config.toml.backup .cargo/config.toml

# Vendor all dependencies including sqlx
cargo vendor

# Build with vendored dependencies
cargo build
```

## Troubleshooting

### "error: no matching package found"
- Vendoring is still enabled, run Option 1 above

### "DATABASE_URL not set"
- Check `.env` file has: `DATABASE_URL=postgresql://niyathnair@localhost:5432/rca_engine`

### "connection refused"
- PostgreSQL not running: `brew services start postgresql@14`

### "role postgres does not exist"
- Use `niyathnair` as username, not `postgres`

