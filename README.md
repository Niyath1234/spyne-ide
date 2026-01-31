# Spyne IDE

**Intelligent Data Assistant with Proactive Clarification System**

Spyne IDE is a production-ready natural language to SQL query engine with advanced features including proactive clarification for ambiguous queries, comprehensive metadata management, and intelligent query planning.

##  Features

### Core Capabilities
- **Natural Language to SQL** - Convert natural language queries to optimized SQL
- **Proactive Clarification** - Asks intelligent questions for ambiguous queries
- **Context-Aware LLM** - Uses comprehensive metadata for accurate query generation
- **Multi-Engine Execution** - Supports DuckDB, Trino, Polars, and traditional databases
- **Intelligent Planning** - Multi-stage planning with intent extraction and schema selection

### Production Features
-  **Rate Limiting** - Token bucket algorithm for API protection
-  **Structured Logging** - JSON logs with correlation IDs
-  **Metrics & Monitoring** - Golden signals, Prometheus metrics
-  **Error Handling** - Graceful degradation and fallbacks
-  **Health Checks** - Comprehensive health endpoints
-  **Security** - CORS, request validation, SQL injection protection

### Clarification System
- **Proactive Questions** - Detects ambiguities and asks clarifying questions
- **Answer Resolution** - Merges user answers into query intent
- **LLM-Powered** - Natural, context-aware question generation
- **Metrics Tracking** - Monitors clarification usage and success rates

##  Table of Contents

- [Quick Start](#quick-start)
- [How Organizations Use This](#how-organizations-use-this)
- [Architecture](#architecture)
- [API Documentation](#api-documentation)
- [Configuration](#configuration)
- [Production Deployment](#production-deployment)
- [Development](#development)
- [Documentation](#documentation)

##  Quick Start

### Prerequisites

- Python 3.10+
- Rust 1.70+
- Node.js 18+ (for UI)
- PostgreSQL/MySQL/SQLite (for data)

### Installation

```bash
# Clone repository
git clone https://github.com/Niyath1234/spyne-ide.git
cd spyne-ide

# Install Python dependencies
pip install -r requirements.txt

# Install Rust dependencies (automatic on build)
cargo build --release

# Copy environment template
cp env.example .env

# Edit .env with your configuration
# - Set OPENAI_API_KEY for LLM features
# - Configure database connections
# - Set other environment variables
```

### Running the Application

```bash
# Start backend server
cd backend
python app_production.py

# Or use gunicorn for production
gunicorn -c gunicorn.conf.py app_production:app

# Server runs on http://localhost:8080
```

### Testing

```bash
# Run unit tests
python -m pytest tests/ -v

# Run specific test suite
python -m pytest tests/test_clarification_agent.py -v
```

## How Organizations Use This

**Spyne IDE is a query layer that sits on top of your existing data infrastructure.**

### Key Points:

1. **No Data Migration Required** - Connect to your existing databases
2. **APIs Continue Working** - Your existing APIs that write to tables work unchanged
3. **Read-Only Access** - Spyne IDE queries your tables (doesn't modify data)
4. **Metadata Registration** - Tell Spyne IDE about your tables and relationships

### Quick Integration:

```bash
# 1. Connect to your existing database
# Edit .env:
RCA_DB_TYPE=postgresql
RCA_DB_HOST=your-db-host
RCA_DB_NAME=your_database
RCA_DB_USER=your_user
RCA_DB_PASSWORD=your_password

# 2. Register your tables (describe them)
curl -X POST http://localhost:8080/api/metadata/ingest/table \
  -H "Content-Type: application/json" \
  -d '{
    "table_description": "Table: customers - Customer data with customer_id, name, email columns"
  }'

# 3. Start querying with natural language
curl -X POST http://localhost:8080/api/agent/run \
  -H "Content-Type: application/json" \
  -d '{"query": "show me top 10 customers"}'
```

**See [DATA_ENTRY_GUIDE.md](./docs/DATA_ENTRY_GUIDE.md) for complete integration guide.**

## ️ Architecture

### Four-Plane Architecture

```
┌─────────────────┐
│  Ingress Plane  │  Request validation, auth, rate limiting
└────────┬────────┘
         │
┌────────▼────────┐
│ Planning Plane  │  Intent extraction, SQL generation, clarification
└────────┬────────┘
         │
┌────────▼────────┐
│Execution Plane  │  Query execution, engine selection
└────────┬────────┘
         │
┌────────▼────────┐
│Presentation     │  Result formatting, explanation generation
│     Plane       │
└─────────────────┘
```

### Key Components

- **Planning Plane** - Intent extraction, SQL generation, clarification
- **Execution Plane** - Multi-engine query execution
- **Knowledge Base** - RAG-based knowledge retrieval
- **Metadata System** - Node-level metadata isolation
- **Clarification Agent** - Proactive question generation

##  API Documentation

### Main Endpoints

#### Query Generation
```bash
POST /api/agent/run
{
  "query": "show me top 10 customers by revenue",
  "clarification_mode": true
}
```

#### Clarification Endpoints
```bash
# Analyze query for clarification needs
POST /api/clarification/analyze
{
  "query": "show me customers",
  "use_llm": true
}

# Resolve clarified query
POST /api/clarification/resolve
{
  "query": "show me customers",
  "answers": {
    "metric": "revenue",
    "time_range": "last 30 days"
  }
}

# Get metrics
GET /api/clarification/metrics
```

#### Health & Metrics
```bash
GET /api/v1/health
GET /api/v1/health/detailed
GET /api/v1/metrics
GET /api/v1/metrics/prometheus
```

See [CLARIFICATION_API_GUIDE.md](./docs/CLARIFICATION_API_GUIDE.md) for detailed API documentation.

## ️ Configuration

### Environment Variables

```bash
# Server
RCA_HOST=0.0.0.0
RCA_PORT=8080
RCA_DEBUG=false

# Security
RCA_SECRET_KEY=your-secret-key
RCA_RATE_LIMIT_RPM=60
RCA_RATE_LIMIT_RPH=1000
RCA_CORS_ORIGINS=*

# LLM
OPENAI_API_KEY=your-api-key
OPENAI_MODEL=gpt-4
OPENAI_BASE_URL=https://api.openai.com/v1
RCA_LLM_TIMEOUT=120

# Database
RCA_DB_TYPE=postgresql
RCA_DB_HOST=localhost
RCA_DB_PORT=5432
RCA_DB_NAME=spyne_db
RCA_DB_USER=spyne_user
RCA_DB_PASSWORD=password

# Observability
RCA_LOG_LEVEL=INFO
RCA_ENABLE_METRICS=true
RCA_ENABLE_TRACING=true

# Clarification
SPYNE_CLARIFICATION_MODE=true
```

See `env.example` for all available options.

##  Production Deployment

### Docker Deployment

```bash
# Build image
docker build -t spyne-ide:latest .

# Run container
docker run -p 8080:8080 \
  -e OPENAI_API_KEY=your-key \
  -e RCA_DB_HOST=db \
  spyne-ide:latest
```

### Docker Compose

```bash
cd docker
docker-compose up -d
```

### Production Checklist

- [x] Rate limiting configured
- [x] Logging configured
- [x] Metrics enabled
- [x] Health checks configured
- [x] Error handling implemented
- [x] Security settings configured
- [x] Database connections configured
- [x] LLM API keys configured

See [PRODUCTION_READINESS.md](./docs/PRODUCTION_READINESS.md) for detailed checklist.

## ️ Development

### Project Structure

```
spyne-ide/
├── backend/                 # Python backend
│   ├── api/                # API endpoints
│   ├── planes/             # Four-plane architecture
│   ├── planning/           # Query planning components
│   ├── execution/          # Query execution engines
│   ├── invariants/         # System invariants
│   └── app_production.py   # Production Flask app
├── frontend/                # Frontend UI (React/TypeScript)
│   ├── src/                # Source code
│   └── ...
├── rust/                    # Rust core
│   ├── node_registry.rs    # Node registry
│   ├── sql_engine.rs       # SQL execution
│   └── ...
├── components/             # Shared components
│   ├── Hypergraph/         # Hypergraph implementation
│   ├── KnowledgeBase/      # Knowledge base server
│   ├── WorldState/         # World state management
│   └── hypergraph-visualizer/ # Visualization component
├── docs/                    # Documentation
│   ├── PRODUCTION_READINESS.md
│   ├── CLARIFICATION_API_GUIDE.md
│   └── ...
├── database/                # Database schemas
│   ├── schema.sql
│   └── ...
├── scripts/                 # Utility scripts
│   └── fix_vendor_checksums.py
├── tests/                   # Test suite
├── config/                  # Configuration files
├── data/                     # Data files
├── docker/                   # Docker configuration
│   ├── docker-compose.yml   # Docker Compose config
│   ├── Dockerfile           # Backend Dockerfile
│   └── Dockerfile.frontend  # Frontend Dockerfile
├── infrastructure/          # Infrastructure as code
│   └── airflow/             # Airflow DAGs and configs
├── Cargo.toml               # Rust dependencies
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

### Code Style

- Python: Follow PEP 8
- Rust: Follow rustfmt defaults
- Use type hints in Python
- Document public APIs

### Running Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=backend --cov-report=html

# Specific test
pytest tests/test_clarification_agent.py -v
```

##  Documentation

### Core Documentation
- [DATA_ENTRY_GUIDE.md](./docs/DATA_ENTRY_GUIDE.md) - **How organizations integrate with existing data**
- [END_TO_END_PIPELINE.md](./docs/END_TO_END_PIPELINE.md) - Complete pipeline flow
- [CLARIFICATION_API_GUIDE.md](./docs/CLARIFICATION_API_GUIDE.md) - Clarification API reference
- [PRODUCTION_READINESS.md](./docs/PRODUCTION_READINESS.md) - Production deployment guide
- [PRODUCTION_FEATURES_CHECKLIST.md](./docs/PRODUCTION_FEATURES_CHECKLIST.md) - Production features
- [SETUP.md](./docs/SETUP.md) - Installation and setup guide
- [CHANGELOG.md](./docs/CHANGELOG.md) - Version history

### Feature Documentation
- [PERMISSIVE_MODE.md](./docs/PERMISSIVE_MODE.md) - Permissive mode (fail-open)
- [CLARIFICATION_SUMMARY.md](./docs/CLARIFICATION_SUMMARY.md) - Clarification system overview
- [IMPLEMENTATION_COMPLETE.md](./docs/IMPLEMENTATION_COMPLETE.md) - Implementation details
- [INTEGRATION_STATUS.md](./docs/INTEGRATION_STATUS.md) - Integration status
- [SHIP_READY_CHECKLIST.md](./docs/SHIP_READY_CHECKLIST.md) - Pre-deployment checklist

##  Key Features Explained

### Proactive Clarification

When a query is ambiguous, the system proactively asks clarifying questions:

```python
# Ambiguous query
query = "show me customers"

# System response
{
  "needs_clarification": true,
  "questions": [
    {
      "question": "What would you like to see about customers?",
      "field": "metric",
      "options": ["revenue", "total_customers"],
      "required": true
    }
  ]
}
```

### Context-Aware LLM

The LLM uses comprehensive context:
- Table schemas and relationships
- Metrics and dimensions definitions
- Business rules and constraints
- Knowledge base concepts
- Historical query patterns

### Intelligent Engine Selection

Automatically selects the best execution engine:
- DuckDB for analytical queries
- Trino for federated queries
- Polars for data transformations
- Traditional DB for simple queries

##  Security

- Rate limiting per API key/IP
- SQL injection protection
- Request size limits
- CORS configuration
- Input validation
- Error message sanitization

##  Monitoring

### Metrics Available

- Request latency (P50, P95, P99)
- Error rates
- Throughput
- Clarification rates
- Engine selection distribution
- LLM token usage

### Logging

Structured JSON logs with:
- Correlation IDs
- Request tracing
- Performance metrics
- Error details

##  Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

##  License

[Add your license here]

##  Acknowledgments

Built with:
- Flask (Python web framework)
- Rust (High-performance core)
- OpenAI GPT (LLM)
- DuckDB/Trino/Polars (Query engines)

##  Support

For issues and questions:
- GitHub Issues: [Link to issues]
- Documentation: See `/docs` directory
- API Reference: [CLARIFICATION_API_GUIDE.md](./docs/CLARIFICATION_API_GUIDE.md)

---

**Status:**  Production Ready

**Version:** 2.0.0

**Last Updated:** January 2024

