# Installing PostgreSQL on macOS

## Option 1: Install via Homebrew (Recommended)

### Step 1: Install Homebrew (if not already installed)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### Step 2: Install PostgreSQL
```bash
brew install postgresql@14
```

### Step 3: Start PostgreSQL
```bash
brew services start postgresql@14
```

### Step 4: Verify it's running
```bash
brew services list | grep postgresql
# Should show: postgresql@14 started
```

### Step 5: Add to PATH (optional, for command line access)
Add to your `~/.zshrc` or `~/.bash_profile`:
```bash
export PATH="/opt/homebrew/opt/postgresql@14/bin:$PATH"
# Or if on Intel Mac:
# export PATH="/usr/local/opt/postgresql@14/bin:$PATH"
```

Then reload:
```bash
source ~/.zshrc
```

### Step 6: Set up initial database and user
```bash
# Create a database (if needed)
createdb postgres

# Set password for postgres user
psql postgres
# Then in psql prompt:
ALTER USER postgres WITH PASSWORD 'your_password';
\q
```

## Option 2: Install via Official PostgreSQL Installer

1. **Download PostgreSQL**:
   - Go to https://www.postgresql.org/download/macosx/
   - Download the installer for your Mac (Intel or Apple Silicon)

2. **Run the installer**:
   - Follow the installation wizard
   - **Remember the password you set for the postgres user!**
   - Default port: 5432
   - Default installation location: `/Library/PostgreSQL/14`

3. **Start PostgreSQL**:
   - The installer should start PostgreSQL automatically
   - If not, check System Preferences → PostgreSQL → Start

4. **Verify installation**:
   - Open pgAdmin
   - Try connecting with:
     - Host: `localhost`
     - Port: `5432`
     - Username: `postgres`
     - Password: (the one you set during installation)

## Option 3: Install via Postgres.app (Easiest for beginners)

1. **Download Postgres.app**:
   - Go to https://postgresapp.com/
   - Download and install

2. **Start Postgres.app**:
   - Open the app from Applications
   - Click "Initialize" to create a new server
   - The server should start automatically

3. **Connect from pgAdmin**:
   - Host: `localhost`
   - Port: `5432`
   - Username: (your macOS username)
   - Password: (leave empty, or set one in Postgres.app settings)

## After Installation: Verify Connection

### Test from command line:
```bash
# If using Homebrew
/opt/homebrew/opt/postgresql@14/bin/pg_isready
# Or if added to PATH:
pg_isready

# Should return: /tmp/.s.PGSQL.5432: accepting connections
```

### Test from pgAdmin:
1. Open pgAdmin
2. Right-click "Servers" → "Create" → "Server"
3. Connection tab:
   - Host: `localhost`
   - Port: `5432`
   - Username: `postgres` (or your macOS username for Postgres.app)
   - Password: (your password)
4. Click "Save"

## Troubleshooting

### If PostgreSQL won't start:
```bash
# Check logs (Homebrew)
tail -f /opt/homebrew/var/log/postgresql@14.log

# Check if port is in use
lsof -i :5432
```

### If you forgot the password:
```bash
# Reset postgres user password (Homebrew)
psql postgres
ALTER USER postgres WITH PASSWORD 'new_password';
\q
```

### If you get "permission denied":
```bash
# Fix permissions (Homebrew)
chmod 700 /opt/homebrew/var/postgresql@14
```

## Quick Start After Installation

Once PostgreSQL is running:

1. **Create database in pgAdmin**:
   - Connect to server
   - Right-click "Databases" → "Create" → "Database"
   - Name: `rca_engine`

2. **Run schema SQL**:
   - Right-click `rca_engine` → "Query Tool"
   - Paste SQL from `POSTGRES_SETUP_GUIDE.md`
   - Execute (F5)

3. **Configure .env**:
   ```bash
   DATABASE_URL=postgresql://postgres:your_password@localhost:5432/rca_engine
   ```

4. **Test connection**:
   ```bash
   cargo run --bin test_db_connection
   ```

