# Setup Guide

Complete setup instructions for Spyne IDE.

## Prerequisites

### Required
- **Python 3.10+** - [Download](https://www.python.org/downloads/)
- **Rust 1.70+** - [Install via rustup](https://rustup.rs/)
- **Git** - [Download](https://git-scm.com/downloads)

### Optional
- **Docker** - For containerized deployment
- **PostgreSQL/MySQL** - For data storage
- **Node.js 18+** - For UI development

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/Niyath1234/spyne-ide.git
cd spyne-ide
```

### 2. Python Setup

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Rust Setup

```bash
# Rust should be installed via rustup
# Verify installation
rustc --version
cargo --version

# Build Rust components (automatic on first run)
cargo build --release
```

### 4. Environment Configuration

```bash
# Copy environment template
cp env.example .env

# Edit .env file with your settings
nano .env  # or use your preferred editor
```

**Required Environment Variables:**
```bash
# LLM Configuration (for query generation)
OPENAI_API_KEY=your-openai-api-key
OPENAI_MODEL=gpt-4
OPENAI_BASE_URL=https://api.openai.com/v1

# Database Configuration
RCA_DB_TYPE=postgresql
RCA_DB_HOST=localhost
RCA_DB_PORT=5432
RCA_DB_NAME=spyne_db
RCA_DB_USER=spyne_user
RCA_DB_PASSWORD=your-password

# Server Configuration
RCA_HOST=0.0.0.0
RCA_PORT=8080
RCA_DEBUG=false
```

### 5. Database Setup

```bash
# Create database
createdb spyne_db  # PostgreSQL
# or
mysql -u root -p -e "CREATE DATABASE spyne_db;"  # MySQL

# Run migrations (if applicable)
# python manage.py migrate
```

### 6. Metadata Setup

```bash
# Ensure metadata directory exists
mkdir -p metadata

# Load initial metadata (if provided)
# python scripts/load_metadata.py
```

## Running the Application

### Development Mode

```bash
# Start backend server
cd backend
python app_production.py

# Server will start on http://localhost:8080
```

### Production Mode

```bash
# Using gunicorn
cd backend
gunicorn -c gunicorn.conf.py app_production:app

# Or using Docker
docker-compose up -d
```

## Verification

### 1. Health Check

```bash
curl http://localhost:8080/api/v1/health
```

Expected response:
```json
{
  "status": "healthy",
  "service": "rca-engine"
}
```

### 2. Test Query

```bash
curl -X POST http://localhost:8080/api/agent/run \
  -H "Content-Type: application/json" \
  -d '{"query": "show me all tables"}'
```

### 3. Test Clarification

```bash
curl -X POST http://localhost:8080/api/clarification/analyze \
  -H "Content-Type: application/json" \
  -d '{"query": "show me customers"}'
```

## Troubleshooting

### Common Issues

**1. Python Import Errors**
```bash
# Ensure virtual environment is activated
source venv/bin/activate
pip install -r requirements.txt
```

**2. Rust Build Errors**
```bash
# Update Rust
rustup update
cargo clean
cargo build --release
```

**3. Database Connection Errors**
- Verify database is running
- Check connection credentials in .env
- Ensure database exists

**4. LLM API Errors**
- Verify OPENAI_API_KEY is set
- Check API key validity
- Verify network connectivity

**5. Port Already in Use**
```bash
# Change port in .env
RCA_PORT=8081

# Or kill process using port
lsof -ti:8080 | xargs kill -9  # macOS/Linux
```

## Next Steps

- Read [README.md](../README.md) for overview
- Check [CLARIFICATION_API_GUIDE.md](./CLARIFICATION_API_GUIDE.md) for API usage
- Review [PRODUCTION_READINESS.md](./PRODUCTION_READINESS.md) for deployment

## Getting Help

- Check documentation in `/docs` directory
- Open an issue on GitHub
- Review existing issues for solutions

