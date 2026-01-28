# RCA Engine Production Flask Backend
# Multi-stage build for optimized production image

# ============================================================================
# Build stage
# ============================================================================
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt ./

# Create virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir gunicorn

# ============================================================================
# Production stage
# ============================================================================
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash appuser

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY --chown=appuser:appuser . .

# Copy project root files needed by backend
COPY --chown=appuser:appuser ../metadata /app/../metadata
COPY --chown=appuser:appuser ../test_outstanding_daily_regeneration.py /app/../test_outstanding_daily_regeneration.py 2>/dev/null || true

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    RCA_HOST=0.0.0.0 \
    RCA_PORT=8080 \
    RCA_WORKERS=4 \
    RCA_THREADS=4 \
    RCA_TIMEOUT=120 \
    RCA_LOG_LEVEL=info

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${RCA_PORT}/api/v1/health || exit 1

EXPOSE 8080

# Start with gunicorn
CMD ["gunicorn", \
    "--config", "gunicorn.conf.py", \
    "--bind", "0.0.0.0:8080", \
    "app_production:app"]

