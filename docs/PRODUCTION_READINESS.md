# Production Readiness Guide

**Spyne IDE - Production Deployment Checklist and Guide**

##  Production Readiness Status

**Overall Status: PRODUCTION READY** 

The system has been thoroughly prepared for production deployment with all critical features implemented, tested, and documented.

##  Pre-Deployment Checklist

### 1. Security 

- [x] **Rate Limiting** - Token bucket algorithm (60 RPM, 1000 RPH default)
- [x] **CORS Configuration** - Configurable origins via `RCA_CORS_ORIGINS`
- [x] **Request Size Limits** - 16MB MAX_CONTENT_LENGTH
- [x] **Secret Key Management** - Environment-based via `RCA_SECRET_KEY`
- [x] **SQL Injection Protection** - Query validation and sanitization
- [x] **Environment Variables** - All secrets in `.env` (not committed)
- [x] **Input Validation** - Request validation on all endpoints
- [x] **Error Message Sanitization** - No sensitive data in error responses

**Action Required:**
```bash
# Generate a strong secret key for production
python -c "import secrets; print(secrets.token_urlsafe(32))"
# Set in .env: RCA_SECRET_KEY=<generated-key>
```

### 2. Observability 

- [x] **Structured Logging** - JSON format with correlation IDs
- [x] **Golden Signals Metrics** - Latency (P50, P95, P99), errors, throughput
- [x] **Prometheus Metrics** - `/api/v1/metrics/prometheus` endpoint
- [x] **Health Checks** - `/api/v1/health` and `/api/v1/health/detailed`
- [x] **Request Tracking** - Correlation IDs for request tracing
- [x] **Error Tracking** - Failure reasons and stack traces logged
- [x] **Performance Metrics** - LLM latency, query execution time

**Monitoring Setup:**
```bash
# Health check endpoint
curl http://localhost:8080/api/v1/health

# Prometheus metrics
curl http://localhost:8080/api/v1/metrics/prometheus

# Detailed health
curl http://localhost:8080/api/v1/health/detailed
```

### 3. Error Handling 

- [x] **Global Error Handler** - Catches all unhandled exceptions
- [x] **HTTP Error Handling** - Proper status codes (400, 401, 403, 404, 500)
- [x] **Graceful Degradation** - Fallbacks for LLM failures
- [x] **Error Logging** - Structured error logs with context
- [x] **LLM Failure Handling** - Retry logic with exponential backoff
- [x] **Database Error Handling** - Connection retry logic
- [x] **Timeout Handling** - LLM timeout (120s default)

### 4. Performance 

- [x] **Request Timeout** - Configurable timeout (120s default)
- [x] **Latency Tracking** - Percentile tracking (P50, P95, P99)
- [x] **Metrics Collection** - Performance metrics exposed
- [x] **Connection Pooling** - Database connection pooling
- [x] **Resource Limits** - Memory and CPU limits in Docker

**Performance Tuning:**
```bash
# Adjust workers based on CPU cores
RCA_WORKERS=4  # Default: 4
RCA_THREADS=4  # Default: 4

# Adjust timeout based on query complexity
RCA_LLM_TIMEOUT=120  # Default: 120s
```

### 5. Configuration 

- [x] **Environment Variables** - All configuration via env vars
- [x] **Production Config Class** - Centralized configuration
- [x] **Feature Flags** - Enable/disable features
- [x] **Debug Mode** - Configurable debug mode (`RCA_DEBUG=false`)
- [x] **Log Level** - Configurable log level (`RCA_LOG_LEVEL=info`)

**Required Environment Variables:**
```bash
# LLM (Required)
OPENAI_API_KEY=your-api-key
RCA_LLM_MODEL=gpt-4

# Server (Required)
RCA_HOST=0.0.0.0
RCA_PORT=8080
RCA_SECRET_KEY=<generate-strong-key>

# Optional
RCA_DB_TYPE=postgresql
RCA_DB_HOST=localhost
RCA_DB_PORT=5432
RCA_DB_NAME=spyne_db
RCA_DB_USER=spyne_user
RCA_DB_PASSWORD=password
```

### 6. Clarification System 

- [x] **ClarificationAgent** - Proactive question generation
- [x] **ClarificationResolver** - Answer merging and query resolution
- [x] **API Endpoints** - Full CRUD for clarification flow
- [x] **Error Handling** - Graceful fallback if clarification fails
- [x] **Metrics** - Clarification usage and success rates tracked
- [x] **Testing** - Unit and integration tests

**Enable Clarification:**
```bash
# In .env
SPYNE_CLARIFICATION_MODE=true

# Or per-request
POST /api/agent/run
{
  "query": "show me customers",
  "clarification_mode": true
}
```

### 7. Testing 

- [x] **Unit Tests** - Core components tested
- [x] **Integration Tests** - End-to-end flow tests
- [x] **Test Coverage** - Major components covered
- [x] **Test Documentation** - Test instructions in README

**Run Tests:**
```bash
# All tests
pytest tests/ -v

# With coverage
pytest --cov=backend --cov-report=html

# Specific test suite
pytest tests/test_clarification_agent.py -v
```

### 8. Documentation 

- [x] **README.md** - Complete project overview
- [x] **SETUP.md** - Installation and setup guide
- [x] **API Documentation** - CLARIFICATION_API_GUIDE.md
- [x] **Production Guide** - This document
- [x] **Architecture Docs** - END_TO_END_PIPELINE.md
- [x] **Changelog** - CHANGELOG.md

##  Deployment Options

### Option 1: Docker Compose (Recommended)

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f backend

# Stop services
docker-compose down
```

**Services Included:**
- Backend (Flask/Gunicorn)
- Frontend (React UI) - Optional
- PostgreSQL - Optional (with-db profile)
- Redis - Optional (with-cache profile)
- Prometheus - Optional (with-monitoring profile)
- Grafana - Optional (with-monitoring profile)

### Option 2: Standalone Docker

```bash
# Build image
docker build -f backend/Dockerfile -t spyne-ide:latest .

# Run container
docker run -d \
  -p 8080:8080 \
  -e OPENAI_API_KEY=your-key \
  -e RCA_SECRET_KEY=your-secret \
  spyne-ide:latest
```

### Option 3: Direct Python Deployment

```bash
# Install dependencies
pip install -r requirements.txt

# Run with gunicorn
cd backend
gunicorn -c gunicorn.conf.py app_production:app
```

##  Production Configuration

### Gunicorn Configuration

The production server uses Gunicorn with the following defaults:
- **Workers**: 4 (adjust based on CPU cores)
- **Threads**: 4 per worker
- **Timeout**: 120 seconds
- **Bind**: 0.0.0.0:8080

**Customize in `backend/gunicorn.conf.py`:**

```python
workers = int(os.getenv('RCA_WORKERS', 4))
threads = int(os.getenv('RCA_THREADS', 4))
timeout = int(os.getenv('RCA_TIMEOUT', 120))
bind = f"{os.getenv('RCA_HOST', '0.0.0.0')}:{os.getenv('RCA_PORT', 8080)}"
```

### Resource Limits

**Recommended for Production:**
- **Memory**: 2GB minimum, 4GB recommended
- **CPU**: 2 cores minimum, 4 cores recommended
- **Disk**: 10GB for logs and data

**Docker Resource Limits:**
```yaml
deploy:
  resources:
    limits:
      memory: 2G
      cpus: '2'
    reservations:
      memory: 512M
      cpus: '0.5'
```

##  Monitoring & Alerting

### Health Checks

```bash
# Basic health check
curl http://localhost:8080/api/v1/health

# Detailed health check
curl http://localhost:8080/api/v1/health/detailed

# Metrics endpoint
curl http://localhost:8080/api/v1/metrics

# Prometheus metrics
curl http://localhost:8080/api/v1/metrics/prometheus
```

### Key Metrics to Monitor

1. **Request Rate** - Requests per second
2. **Error Rate** - Percentage of failed requests
3. **Latency** - P50, P95, P99 response times
4. **LLM Latency** - Time to generate SQL
5. **Clarification Rate** - Percentage of queries needing clarification
6. **Throughput** - Successful queries per minute

### Logging

**Log Format:** JSON structured logs
**Log Level:** INFO (set via `RCA_LOG_LEVEL`)

**Log Fields:**
- `timestamp` - ISO 8601 timestamp
- `level` - Log level (INFO, ERROR, WARNING)
- `correlation_id` - Request correlation ID
- `message` - Log message
- `context` - Additional context data

**Example Log:**
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "correlation_id": "abc123",
  "message": "Query generated successfully",
  "context": {
    "query": "show me customers",
    "latency_ms": 1250
  }
}
```

##  Security Best Practices

### 1. Secret Management

-  Never commit `.env` files
-  Use strong secret keys (32+ characters)
-  Rotate API keys regularly
-  Use environment-specific configs

### 2. Network Security

-  Use HTTPS in production (via reverse proxy)
-  Configure CORS appropriately
-  Use firewall rules to restrict access
-  Enable rate limiting

### 3. Application Security

-  Input validation on all endpoints
-  SQL injection protection
-  Error message sanitization
-  Request size limits

##  Troubleshooting

### Common Issues

**1. High Memory Usage**
```bash
# Reduce workers
RCA_WORKERS=2

# Check memory usage
docker stats
```

**2. Slow Response Times**
```bash
# Increase timeout
RCA_LLM_TIMEOUT=180

# Check LLM API latency
curl http://localhost:8080/api/v1/metrics
```

**3. Database Connection Errors**
```bash
# Verify database is running
docker ps | grep postgres

# Check connection string
echo $RCA_DB_HOST
```

**4. LLM API Errors**
```bash
# Verify API key
echo $OPENAI_API_KEY | cut -c1-10

# Check API status
curl https://api.openai.com/v1/models
```

##  Scaling Considerations

### Horizontal Scaling

For high-traffic deployments:

1. **Load Balancer** - Use nginx or similar
2. **Multiple Instances** - Run multiple backend instances
3. **Shared State** - Use Redis for rate limiting
4. **Database Pooling** - Configure connection pooling
5. **Caching** - Implement query result caching

### Vertical Scaling

For single-instance deployments:

1. **Increase Workers** - Match CPU cores
2. **Increase Memory** - For larger queries
3. **Optimize LLM Calls** - Batch requests if possible

##  Final Checklist Before Production

- [ ] All environment variables configured
- [ ] Strong secret key generated
- [ ] Database configured and accessible
- [ ] LLM API key valid and tested
- [ ] Health checks passing
- [ ] Logs configured and accessible
- [ ] Metrics endpoint accessible
- [ ] Rate limiting configured appropriately
- [ ] CORS configured for your domain
- [ ] Backup strategy in place
- [ ] Monitoring and alerting configured
- [ ] Documentation reviewed
- [ ] Team trained on deployment process

##  Post-Deployment

### 1. Verify Deployment

```bash
# Health check
curl http://your-domain/api/v1/health

# Test query
curl -X POST http://your-domain/api/agent/run \
  -H "Content-Type: application/json" \
  -d '{"query": "show me all tables"}'
```

### 2. Monitor Metrics

- Check error rates
- Monitor latency
- Watch resource usage
- Review clarification rates

### 3. Gather Feedback

- User feedback on clarification questions
- Query success rates
- Performance feedback
- Error reports

##  Support

For production issues:
1. Check logs: `docker-compose logs -f backend`
2. Review metrics: `/api/v1/metrics`
3. Check health: `/api/v1/health/detailed`
4. Review documentation: See [docs/](../docs/) directory

##  Success Criteria

Your deployment is successful when:
-  Health checks return 200 OK
-  Test queries execute successfully
-  Metrics are being collected
-  Logs are structured and accessible
-  No critical errors in logs
-  Response times are acceptable (< 5s for simple queries)

---

**Status:**  **PRODUCTION READY**

**Last Updated:** 2024-01-15

**Version:** 2.0.0
