# PostgreSQL Connection Troubleshooting

## Error: "Connection refused" or "could not receive data from server"

This means PostgreSQL is not running or not accepting connections.

## Step 1: Check if PostgreSQL is Running

### On macOS:
```bash
# Check if PostgreSQL is running
brew services list | grep postgresql

# Or check process
ps aux | grep postgres
```

### On Windows:
1. Press `Win + R`, type `services.msc`, press Enter
2. Look for "postgresql" service
3. Check if it's "Running"

### On Linux:
```bash
# Check service status
sudo systemctl status postgresql
# Or
sudo service postgresql status
```

## Step 2: Start PostgreSQL

### On macOS (Homebrew):
```bash
# Start PostgreSQL
brew services start postgresql@14
# Or for latest version:
brew services start postgresql

# Verify it's running
brew services list | grep postgresql
```

### On Windows:
1. Open Services (`Win + R` → `services.msc`)
2. Find "postgresql" service
3. Right-click → "Start"
4. Or use command prompt (as Administrator):
```cmd
net start postgresql-x64-14
# (version number may vary)
```

### On Linux:
```bash
# Start PostgreSQL
sudo systemctl start postgresql
# Or
sudo service postgresql start

# Enable auto-start on boot
sudo systemctl enable postgresql
```

## Step 3: Verify PostgreSQL is Listening

### Test connection from command line:
```bash
# On macOS/Linux
psql -h localhost -p 5432 -U postgres -d postgres

# Or test if port is open
pg_isready -h localhost -p 5432
```

### On Windows:
```cmd
# Navigate to PostgreSQL bin directory (usually)
cd "C:\Program Files\PostgreSQL\14\bin"
# (version number may vary)

# Test connection
pg_isready -h localhost -p 5432
```

If `pg_isready` returns "accepting connections", PostgreSQL is running!

## Step 4: Check PostgreSQL Configuration

If PostgreSQL is running but still refusing connections, check the config:

### Find PostgreSQL config file:
- **macOS (Homebrew)**: `/opt/homebrew/var/postgresql@14/postgresql.conf` or `/usr/local/var/postgresql@14/postgresql.conf`
- **Windows**: `C:\Program Files\PostgreSQL\14\data\postgresql.conf`
- **Linux**: `/etc/postgresql/14/main/postgresql.conf` (version may vary)

### Check these settings:
```conf
# Should be:
listen_addresses = 'localhost'  # or '*' for all interfaces
port = 5432

# In pg_hba.conf (usually in same directory):
# Should have:
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5
```

## Step 5: Common Issues & Solutions

### Issue: PostgreSQL installed but service not started
**Solution**: Start the service (see Step 2)

### Issue: Wrong port
**Solution**: Check what port PostgreSQL is actually using:
```bash
# macOS/Linux
sudo lsof -i -P | grep LISTEN | grep postgres

# Windows
netstat -ano | findstr :5432
```

### Issue: PostgreSQL data directory not initialized
**Solution**: Initialize the database:
```bash
# macOS (Homebrew)
initdb /opt/homebrew/var/postgresql@14
# Or
initdb /usr/local/var/postgresql@14

# Then start the service
brew services start postgresql@14
```

### Issue: Permission problems
**Solution**: Check file permissions on data directory (should be owned by postgres user)

## Step 6: Quick Verification

Once PostgreSQL is running, test with:

```bash
# Test connection
pg_isready

# Connect and create database
psql -U postgres -c "CREATE DATABASE rca_engine;"

# List databases
psql -U postgres -l
```

## Still Having Issues?

1. **Check PostgreSQL logs**:
   - macOS: `tail -f /opt/homebrew/var/log/postgresql@14.log`
   - Windows: Check Event Viewer → Applications
   - Linux: `sudo tail -f /var/log/postgresql/postgresql-14-main.log`

2. **Reinstall PostgreSQL** (last resort):
   - Make sure to backup any existing data first
   - Follow installation instructions for your OS

3. **Use Docker** (alternative):
   ```bash
   docker run --name postgres-rca -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:14
   ```

## After PostgreSQL is Running

Once `pg_isready` returns "accepting connections":
1. Go back to pgAdmin
2. Try connecting again with:
   - Host: `localhost`
   - Port: `5432`
   - Username: `postgres`
   - Password: (your password)
3. Create the `rca_engine` database
4. Run the schema SQL

