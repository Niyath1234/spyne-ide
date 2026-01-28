#!/usr/bin/env python3
"""
Gunicorn Configuration for RCA Engine Production Server

This configuration provides production-ready settings for running
the RCA Engine Flask application with gunicorn.
"""

import os
import multiprocessing

# ============================================================================
# Server Socket
# ============================================================================

bind = os.getenv('RCA_BIND', '0.0.0.0:8080')
backlog = 2048

# ============================================================================
# Worker Processes
# ============================================================================

# Number of worker processes
# Recommended: 2-4 x $(NUM_CORES) for CPU-bound, 4-12 x $(NUM_CORES) for I/O-bound
workers = int(os.getenv('RCA_WORKERS', (multiprocessing.cpu_count() * 2) + 1))

# Worker class: sync, gthread, gevent, eventlet
worker_class = os.getenv('RCA_WORKER_CLASS', 'gthread')

# Threads per worker (only for gthread worker class)
threads = int(os.getenv('RCA_THREADS', 4))

# Maximum requests a worker will process before restarting
max_requests = int(os.getenv('RCA_MAX_REQUESTS', 10000))
max_requests_jitter = int(os.getenv('RCA_MAX_REQUESTS_JITTER', 1000))

# Worker timeout (seconds)
timeout = int(os.getenv('RCA_TIMEOUT', 120))

# Graceful timeout for workers to finish (seconds)
graceful_timeout = int(os.getenv('RCA_GRACEFUL_TIMEOUT', 30))

# Keep-alive connections timeout
keepalive = int(os.getenv('RCA_KEEPALIVE', 5))

# ============================================================================
# Server Mechanics
# ============================================================================

# Daemonize the process
daemon = False

# PID file location
pidfile = os.getenv('RCA_PIDFILE', None)

# User/group to run as (requires root)
user = os.getenv('RCA_USER', None)
group = os.getenv('RCA_GROUP', None)

# Working directory
chdir = os.path.dirname(os.path.abspath(__file__))

# Temp file directory
tmp_upload_dir = None

# ============================================================================
# SSL Configuration (if needed)
# ============================================================================

# SSL certificate file
certfile = os.getenv('RCA_SSL_CERT', None)

# SSL key file
keyfile = os.getenv('RCA_SSL_KEY', None)

# SSL CA certificates file
ca_certs = os.getenv('RCA_SSL_CA_CERTS', None)

# SSL protocol
ssl_version = 'TLSv1_2'

# Require client certificates
cert_reqs = 0

# ============================================================================
# Logging
# ============================================================================

# Access log file (- for stdout)
accesslog = os.getenv('RCA_ACCESS_LOG', '-')

# Error log file (- for stderr)
errorlog = os.getenv('RCA_ERROR_LOG', '-')

# Log level: debug, info, warning, error, critical
loglevel = os.getenv('RCA_LOG_LEVEL', 'info')

# Access log format
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Disable access log (rely on application logging)
# accesslog = None

# Enable X-Forwarded-For header parsing
forwarded_allow_ips = os.getenv('RCA_FORWARDED_IPS', '*')

# ============================================================================
# Process Naming
# ============================================================================

proc_name = 'rca-engine'

# ============================================================================
# Server Hooks
# ============================================================================

def on_starting(server):
    """Called before the master process is initialized."""
    print(f"🚀 RCA Engine starting with {workers} workers")


def on_reload(server):
    """Called when the server is reloaded."""
    print("🔄 RCA Engine reloading...")


def when_ready(server):
    """Called when the server is ready to receive requests."""
    print(f"✅ RCA Engine ready on {bind}")


def worker_int(worker):
    """Called when a worker receives the INT signal."""
    print(f"⚠️  Worker {worker.pid} interrupted")


def worker_abort(worker):
    """Called when a worker times out."""
    print(f"❌ Worker {worker.pid} aborted (timeout)")


def pre_fork(server, worker):
    """Called before a worker is forked."""
    pass


def post_fork(server, worker):
    """Called after a worker is forked."""
    print(f"👷 Worker {worker.pid} spawned")


def pre_exec(server):
    """Called before a new master process is forked."""
    print("⏳ RCA Engine pre-exec...")


def child_exit(server, worker):
    """Called when a worker exits."""
    print(f"👋 Worker {worker.pid} exited")


def worker_exit(server, worker):
    """Called after a worker exits."""
    pass


def nworkers_changed(server, new_value, old_value):
    """Called when the number of workers changes."""
    print(f"📊 Workers changed: {old_value} → {new_value}")


def on_exit(server):
    """Called when the arbiter is shutting down."""
    print("🛑 RCA Engine shutting down...")


# ============================================================================
# Health Checks (for container orchestration)
# ============================================================================

# Used by some load balancers
def health_check(environ):
    """Health check for load balancers."""
    return True

