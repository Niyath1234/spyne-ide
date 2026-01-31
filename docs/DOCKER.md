# Docker Setup Guide

This guide explains how to run the complete RCA Engine project using Docker and Docker Compose.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 8GB RAM (16GB recommended for full stack)
- At least 20GB free disk space

## Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd RCA-Engine

# Copy environment template
cp env.example .env

# Edit .env with your configuration
# - Set OPENAI_API_KEY for LLM features
# - Configure database passwords
# - Set other environment variables
```

### 2. Start Core Services

```bash
# Start backend and frontend (core services)
docker-compose up -d backend frontend

# Access the application
# Frontend: http://localhost:5173
# Backend API: http://localhost:8080
```

### 3. Start with Database

```bash
# Start with PostgreSQL database
docker-compose --profile with-db up -d

# This starts: backend, frontend, postgres
```

### 4. Start Full Stack

```bash
# Start all services (core + optional services)
docker-compose --profile with-db --profile with-cache --profile with-monitoring up -d

# This starts:
# - Backend (Flask API)
# - Frontend (React UI)
# - PostgreSQL
# - Redis
# - Prometheus
# - Grafana
# - Trino
```

## Service Profiles

Services are organized into profiles for flexible deployment:

### Core Services (Always Running)
- `backend` - Flask API server (port 8080)
- `frontend` - React UI (port 5173)

### Optional Services (Use Profiles)

#### Database
```bash
docker-compose --profile with-db up -d
```
- `postgres` - PostgreSQL database (port 5432)

#### Caching
```bash
docker-compose --profile with-cache up -d
```
- `redis` - Redis cache (port 6379)

#### Monitoring
```bash
docker-compose --profile with-monitoring up -d
```
- `prometheus` - Metrics collection (port 9090)
- `grafana` - Dashboards (port 3000)

#### Airflow
```bash
docker-compose --profile with-airflow up -d
```
- `airflow-webserver` - Airflow UI (port 8083)
- `airflow-scheduler` - Task scheduler
- `airflow-init` - Initialization service

#### Rust Server
```bash
docker-compose --profile with-rust up -d
```
- `rust-server` - High-performance Rust server (port 8082)

## Service Ports

| Service | Port | URL |
|---------|------|-----|
| Frontend | 5173 | http://localhost:5173 |
| Backend API | 8080 | http://localhost:8080 |
| PostgreSQL | 5432 | localhost:5432 |
| Redis | 6379 | localhost:6379 |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 |
| Trino | 8081 | http://localhost:8081 |
| Rust Server | 8082 | http://localhost:8082 |
| Airflow | 8083 | http://localhost:8083 |

## Common Commands

### Start Services
```bash
# Start all core services
docker-compose up -d

# Start specific services
docker-compose up -d backend frontend

# Start with profiles
docker-compose --profile with-db --profile with-cache up -d
```

### Stop Services
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (⚠️ deletes data)
docker-compose down -v
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f backend

# Last 100 lines
docker-compose logs --tail=100 backend
```

### Rebuild Services
```bash
# Rebuild all services
docker-compose build

# Rebuild specific service
docker-compose build backend

# Rebuild and restart
docker-compose up -d --build backend
```

### Check Status
```bash
# List running containers
docker-compose ps

# Check service health
docker-compose ps --format json | jq '.[] | {name: .Name, status: .State}'
```

## Environment Variables

Key environment variables (set in `.env` file):

### Required
- `OPENAI_API_KEY` - OpenAI API key for LLM features

### Database (if using with-db profile)
- `RCA_DB_NAME` - Database name (default: rca_engine)
- `RCA_DB_USER` - Database user (default: rca_user)
- `RCA_DB_PASSWORD` - Database password (default: change_me)

### Airflow (if using with-airflow profile)
- `AIRFLOW_USERNAME` - Airflow admin username (default: admin)
- `AIRFLOW_PASSWORD` - Airflow admin password (default: admin)
- `AIRFLOW_EMAIL` - Admin email (default: admin@example.com)
- `AIRFLOW_FERNET_KEY` - Encryption key (generate with: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)
- `AIRFLOW_SECRET_KEY` - Secret key (generate random string)

## Volumes

Data persistence is handled via Docker volumes:

- `postgres-data` - PostgreSQL database files
- `redis-data` - Redis persistence
- `prometheus-data` - Prometheus metrics
- `grafana-data` - Grafana dashboards and config
- `trino-data` - Trino data directory
- `airflow-data` - Airflow metadata

## Development

### Hot Reload

For development, mount source code as volumes:

```bash
# Backend hot reload (if supported)
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
```

### Debugging

```bash
# Execute commands in container
docker-compose exec backend bash
docker-compose exec postgres psql -U rca_user -d rca_engine

# Check container resources
docker stats

# Inspect container
docker-compose exec backend env
```

## Production Deployment

### Security Checklist

1. ✅ Change all default passwords
2. ✅ Set strong `RCA_SECRET_KEY`
3. ✅ Generate `AIRFLOW_FERNET_KEY` and `AIRFLOW_SECRET_KEY`
4. ✅ Set `RCA_CORS_ORIGINS` to specific domains
5. ✅ Use secrets management (Docker secrets, Vault, etc.)
6. ✅ Enable HTTPS/TLS
7. ✅ Set up firewall rules
8. ✅ Configure resource limits appropriately

### Resource Limits

Default resource limits are set in `docker-compose.yml`. Adjust based on your infrastructure:

- **Backend**: 2GB RAM, 2 CPUs
- **Frontend**: 256MB RAM, 0.5 CPUs
- **PostgreSQL**: 512MB RAM, 1 CPU
- **Trino**: 3GB RAM, 2 CPUs
- **Airflow**: 1GB RAM per service, 1 CPU

### Scaling

```bash
# Scale backend workers
docker-compose up -d --scale backend=3

# Note: Ensure load balancer/ingress is configured
```

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker-compose logs

# Check Docker resources
docker system df
docker system prune  # Clean up unused resources

# Verify network
docker network ls
docker network inspect rca-network
```

### Database Connection Issues

```bash
# Verify PostgreSQL is running
docker-compose ps postgres

# Check connection
docker-compose exec postgres psql -U rca_user -d rca_engine -c "SELECT 1;"

# Reset database (⚠️ deletes data)
docker-compose down -v postgres
docker-compose up -d postgres
```

### Port Conflicts

If ports are already in use:

1. Change port mappings in `docker-compose.yml`
2. Or stop conflicting services

### Airflow Issues

```bash
# Initialize Airflow database
docker-compose run --rm airflow-init

# Check Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
```

## Backup and Restore

### Backup PostgreSQL

```bash
# Create backup
docker-compose exec postgres pg_dump -U rca_user rca_engine > backup.sql

# Restore
docker-compose exec -T postgres psql -U rca_user rca_engine < backup.sql
```

### Backup Volumes

```bash
# Backup all volumes
docker run --rm -v rca-postgres-data:/data -v $(pwd):/backup alpine tar czf /backup/postgres-backup.tar.gz /data
```

## Additional Resources

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Trino Documentation](https://trino.io/docs/)
- [Airflow Documentation](https://airflow.apache.org/docs/)
