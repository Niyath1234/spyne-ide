# pgAdmin Connection Fix

## ❌ The Problem
You're trying to connect with username `postgres`, but that user doesn't exist in your PostgreSQL installation.

## ✅ The Solution
Use your macOS username: `niyathnair`

## 📝 Step-by-Step Connection in pgAdmin

### Step 1: Create New Server Connection
1. Right-click **"Servers"** in the left panel
2. Select **"Create"** → **"Server..."**

### Step 2: General Tab
- **Name**: `RCA Engine` (or any name you prefer)

### Step 3: Connection Tab (THIS IS THE KEY!)
Fill in these fields **exactly**:

```
Host name/address: localhost
Port: 5432
Maintenance database: postgres
Username: niyathnair    ⚠️ NOT "postgres"!
Password: (leave empty OR set a password if you want)
Save password?: ✅ CHECK THIS BOX
```

### Step 4: Save
Click **"Save"** button

## 🔍 Why This Happens

When you install PostgreSQL via Homebrew on macOS, it creates a database user with your **macOS username** (in your case: `niyathnair`), not the traditional `postgres` user.

This is actually **more secure** because:
- It uses your system user
- No default password needed for local connections
- Better integration with macOS

## ✅ Verification

After connecting successfully, you should see:
- Your server listed under "Servers"
- You can expand it to see "Databases"
- The `rca_engine` database should be visible

## 🆘 Still Having Issues?

If you still can't connect:

1. **Check your macOS username**:
   ```bash
   whoami
   ```
   Use whatever that returns as the username.

2. **Create a postgres user** (if you really want to use "postgres"):
   ```bash
   psql -d postgres
   CREATE USER postgres WITH SUPERUSER PASSWORD 'your_password';
   \q
   ```
   Then you can use `postgres` as username.

3. **Check PostgreSQL is running**:
   ```bash
   pg_isready
   ```

## 📋 Quick Reference

**For pgAdmin:**
- Username: `niyathnair`
- Password: (empty or your chosen password)
- Host: `localhost`
- Port: `5432`

**For .env file:**
```
DATABASE_URL=postgresql://niyathnair@localhost:5432/rca_engine
```

**For command line:**
```bash
psql -d rca_engine
# (uses your macOS username automatically)
```

