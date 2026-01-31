# Project Structure

This document describes the organization of the Spyne IDE repository.

## Directory Layout

```
spyne-ide/
├── backend/                    # Python backend application
│   ├── api/                   # API endpoints and routes
│   ├── planes/                # Four-plane architecture components
│   ├── planning/              # Query planning logic
│   ├── execution/             # Query execution engines
│   ├── observability/         # Logging, metrics, tracing
│   ├── security/              # Security and authentication
│   ├── models/                # Data models
│   ├── services/              # Business logic services
│   └── app_production.py      # Production Flask application
│
├── frontend/                  # Frontend UI (React/TypeScript)
│   ├── src/                   # Source code
│   ├── public/                # Static assets
│   ├── dist/                  # Build output (gitignored)
│   └── package.json           # Node.js dependencies
│
├── rust/                      # Rust core library
│   ├── bin/                   # Binary executables
│   ├── core/                  # Core functionality
│   ├── execution/             # Execution engines
│   ├── intent/                # Intent processing
│   ├── semantic/              # Semantic analysis
│   └── lib.rs                 # Library entry point
│
├── components/                # Shared components and modules
│   ├── Hypergraph/            # Hypergraph implementation
│   ├── KnowledgeBase/         # Knowledge base server
│   ├── WorldState/            # World state management
│   └── hypergraph-visualizer/ # Visualization component
│
├── docs/                      # Documentation
│   ├── README.md              # Documentation index
│   ├── PROJECT_STRUCTURE.md    # This file
│   ├── SETUP.md               # Setup instructions
│   ├── PRODUCTION_READINESS.md # Production deployment guide
│   └── ...                    # Other documentation files
│
├── docker/                    # Docker configuration
│   ├── docker-compose.yml     # Docker Compose configuration
│   ├── Dockerfile             # Backend Dockerfile
│   └── Dockerfile.frontend    # Frontend Dockerfile
│
├── infrastructure/            # Infrastructure as code
│   └── airflow/               # Apache Airflow DAGs and configs
│       ├── dags/              # Airflow DAG definitions
│       └── plugins/           # Airflow plugins
│
├── config/                    # Configuration files
│   ├── config.yaml            # Main configuration
│   ├── prometheus.yml         # Prometheus configuration
│   └── trino/                 # Trino configuration
│
├── database/                  # Database schemas and migrations
│   └── schema.sql             # Database schema
│
├── scripts/                   # Utility scripts
│   ├── fix_vendor_checksums.py
│   └── load_metadata.sh
│
├── tests/                     # Test suite
│   ├── unit/                  # Unit tests
│   ├── integration/           # Integration tests
│   └── fixtures/              # Test fixtures
│
├── data/                      # Data files (gitignored)
│   └── .gitkeep
│
├── logs/                      # Log files (gitignored)
│   └── .gitkeep
│
├── tables/                    # Table definitions (gitignored)
│
├── vendor/                    # Vendored dependencies (Rust)
│
├── target/                    # Rust build artifacts (gitignored)
│
├── node_modules/              # Node.js dependencies (gitignored)
│
├── venv/                      # Python virtual environment (gitignored)
│
├── .env.example               # Environment variables template
├── .gitignore                 # Git ignore rules
├── Cargo.toml                 # Rust dependencies
├── Cargo.lock                 # Rust dependency lock file
├── requirements.txt           # Python dependencies
├── pyproject.toml             # Python project configuration
├── package.json               # Node.js project configuration
└── README.md                  # Project README

```

## Key Directories Explained

### Backend (`backend/`)
Python Flask application containing:
- API endpoints
- Business logic
- Query planning and execution
- Observability and security features

### Frontend (`frontend/`)
React/TypeScript application providing:
- User interface
- Query builder
- Results visualization
- Knowledge base and metadata management

### Rust Core (`rust/`)
High-performance Rust library providing:
- Core query engine
- SQL compilation
- Intent processing
- Semantic analysis

### Components (`components/`)
Shared components used across the application:
- Hypergraph data structure
- Knowledge base server
- World state management
- Visualization tools

### Docker (`docker/`)
All Docker-related files:
- Docker Compose configuration
- Dockerfiles for backend and frontend
- Development and production configurations

### Infrastructure (`infrastructure/`)
Infrastructure as code:
- Airflow DAGs for data pipelines
- Deployment configurations

### Configuration (`config/`)
Application configuration files:
- Main application config
- Prometheus monitoring config
- Trino query engine config

## File Naming Conventions

- **Python files**: `snake_case.py`
- **Rust files**: `snake_case.rs`
- **TypeScript files**: `PascalCase.tsx` (components), `camelCase.ts` (utilities)
- **Config files**: `kebab-case.yaml` or `snake_case.yaml`
- **Documentation**: `UPPER_SNAKE_CASE.md` or `PascalCase.md`

## Build Artifacts

The following directories are gitignored and contain build artifacts:
- `target/` - Rust build output
- `node_modules/` - Node.js dependencies
- `venv/` - Python virtual environment
- `dist/` - Frontend build output
- `logs/` - Application logs
- `data/` - Data files
- `tables/` - Table definitions

## Migration Notes

If you're migrating from the old structure:
- `src/` → `rust/`
- `ui/` → `frontend/`
- `Hypergraph/`, `KnowledgeBase/`, `WorldState/` → `components/`
- `airflow/` → `infrastructure/airflow/`
- Docker files → `docker/`
- Root markdown files → `docs/`

## Development Workflow

1. **Backend Development**: Work in `backend/`
2. **Frontend Development**: Work in `frontend/`
3. **Rust Core Development**: Work in `rust/`
4. **Running Tests**: Use `tests/` directory
5. **Docker Development**: Use `docker/docker-compose.yml`
6. **Documentation**: Add to `docs/`

