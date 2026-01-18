# Quick Start: PostgreSQL Setup for RCA Engine

## 🚀 Quick Setup (5 minutes)

### 1. Create Database in pgAdmin
1. Open pgAdmin 4
2. Right-click "Servers" → "Create" → "Server"
3. **General Tab**: Name = `RCA Engine`
4. **Connection Tab**:
   - **Host name/address**: `localhost` (⚠️ **Only the hostname, NOT the port!**)
   - **Port**: `5432` (separate field)
   - Username: `postgres`
   - Password: (your PostgreSQL password)
   - Save password?: ✅ Check this
5. Click "Save"
6. Expand server → Right-click "Databases" → "Create" → "Database"
7. Database name: `rca_engine`
8. Click "Save"

### 2. Run Schema SQL
1. Right-click `rca_engine` database → "Query Tool"
2. Copy and paste the SQL from `POSTGRES_SETUP_GUIDE.md` (Step 3)
3. Click "Execute" (F5)
4. You should see "Success" messages

### 3. Configure Environment
Add to your `.env` file:
```bash
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/rca_engine
USE_POSTGRES=true
```

### 4. Test Connection
```bash
cargo run --bin test_db_connection
```

You should see:
```
✅ Connected successfully!
📊 Tables in database: 20
```

## ✅ Verification Checklist

- [ ] Database `rca_engine` created
- [ ] Schema SQL executed successfully
- [ ] `.env` file has `DATABASE_URL`
- [ ] `test_db_connection` runs successfully
- [ ] All tables show 0 records (ready for migration)

## 🎯 Next Steps

Once verified, you can:
1. **Migrate Metadata**: Move JSON files to PostgreSQL
2. **Update Code**: Enable PostgreSQL in your application
3. **Test**: Run your existing workflows

## 📚 Full Documentation

See `POSTGRES_SETUP_GUIDE.md` for detailed instructions and troubleshooting.

