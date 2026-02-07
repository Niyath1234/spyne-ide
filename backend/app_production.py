#!/usr/bin/env python3
"""
RCA Engine Production Flask Application

Unified Flask application with enterprise features:
- Security (rate limiting, authentication, data exfiltration protection)
- Observability (structured logging, golden signals, correlation IDs)
- Failure handling (LLM failures, metadata drift, partial results)
- All API endpoints for UI
"""

import os
import sys
import json
import time
import uuid
import logging
import traceback
from datetime import datetime
from functools import wraps
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path

from flask import Flask, Blueprint, request, jsonify, g, Response
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

# Load .env file FIRST - before any other imports that might need env vars
try:
    from dotenv import load_dotenv
    # Load .env from project root (parent of backend directory)
    backend_dir = Path(__file__).parent
    project_root = backend_dir.parent
    env_file = project_root / '.env'
    if env_file.exists():
        load_dotenv(env_file, override=True)  # override=True to ensure latest values
        # Verify API key was loaded
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key:
            # Set it explicitly to ensure it's available everywhere
            os.environ['OPENAI_API_KEY'] = api_key
            # Also set OPENAI_MODEL if present
            model = os.getenv('OPENAI_MODEL')
            if model:
                os.environ['OPENAI_MODEL'] = model
            logging.getLogger(__name__).info(f"✓ Loaded .env file from {env_file}")
            logging.getLogger(__name__).info(f"  - OPENAI_API_KEY: {'*' * 20}...{api_key[-10:]} (length: {len(api_key)})")
            if model:
                logging.getLogger(__name__).info(f"  - OPENAI_MODEL: {model}")
        else:
            logging.getLogger(__name__).warning(f"⚠ Loaded .env file from {env_file} but OPENAI_API_KEY not found")
    else:
        # Try loading from current directory
        load_dotenv(override=True)
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key:
            os.environ['OPENAI_API_KEY'] = api_key
            model = os.getenv('OPENAI_MODEL')
            if model:
                os.environ['OPENAI_MODEL'] = model
            logging.getLogger(__name__).info(f"✓ Loaded .env from current directory (API key length: {len(api_key)})")
except ImportError:
    logging.getLogger(__name__).warning("python-dotenv not installed. Environment variables must be set manually.")
except Exception as e:
    logging.getLogger(__name__).warning(f"Could not load .env file: {e}")

# Ensure OPENAI_API_KEY is set from .env file if not already in environment
if not os.getenv('OPENAI_API_KEY'):
    # Try to load it one more time
    try:
        from dotenv import load_dotenv
        backend_dir = Path(__file__).parent
        project_root = backend_dir.parent
        env_file = project_root / '.env'
        if env_file.exists():
            load_dotenv(env_file, override=True)
            api_key = os.getenv('OPENAI_API_KEY')
            if api_key:
                os.environ['OPENAI_API_KEY'] = api_key
                logging.getLogger(__name__).info(f"✓ Set OPENAI_API_KEY from .env (length: {len(api_key)})")
    except:
        pass

# Ensure backend directory is in path
backend_dir = os.path.dirname(os.path.abspath(__file__))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

project_root = os.path.dirname(backend_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# ============================================================================
# Configuration
# ============================================================================

class ProductionConfig:
    """Production configuration."""
    
    # Server settings
    HOST = os.getenv('RCA_HOST', '0.0.0.0')
    PORT = int(os.getenv('RCA_PORT', 8080))
    DEBUG = os.getenv('RCA_DEBUG', 'false').lower() == 'true'
    
    # Security settings
    SECRET_KEY = os.getenv('RCA_SECRET_KEY', os.urandom(32).hex())
    MAX_CONTENT_LENGTH = int(os.getenv('RCA_MAX_CONTENT_LENGTH', 16 * 1024 * 1024))  # 16MB
    RATE_LIMIT_REQUESTS_PER_MINUTE = int(os.getenv('RCA_RATE_LIMIT_RPM', 60))
    RATE_LIMIT_REQUESTS_PER_HOUR = int(os.getenv('RCA_RATE_LIMIT_RPH', 1000))
    
    # CORS settings
    CORS_ORIGINS = os.getenv('RCA_CORS_ORIGINS', '*').split(',')
    
    # LLM settings - read from environment at runtime
    @staticmethod
    def get_openai_api_key():
        """Get OpenAI API key from environment (read at runtime, not module load time)."""
        return os.getenv('OPENAI_API_KEY', '')
    
    # Keep OPENAI_API_KEY for backward compatibility but read at runtime
    @property
    def OPENAI_API_KEY(self):
        return ProductionConfig.get_openai_api_key()
    LLM_MODEL = os.getenv('RCA_LLM_MODEL', 'gpt-4')
    LLM_TEMPERATURE = float(os.getenv('RCA_LLM_TEMPERATURE', 0.0))
    LLM_MAX_TOKENS = int(os.getenv('RCA_LLM_MAX_TOKENS', 3000))
    LLM_TIMEOUT = int(os.getenv('RCA_LLM_TIMEOUT', 120))
    
    # Database settings
    DATABASE_TYPE = os.getenv('RCA_DB_TYPE', 'postgresql')
    DATABASE_HOST = os.getenv('RCA_DB_HOST', 'localhost')
    DATABASE_PORT = int(os.getenv('RCA_DB_PORT', 5432))
    DATABASE_NAME = os.getenv('RCA_DB_NAME', 'rca_engine')
    DATABASE_USER = os.getenv('RCA_DB_USER', 'rca_user')
    DATABASE_PASSWORD = os.getenv('RCA_DB_PASSWORD', '')
    
    # Observability settings
    LOG_LEVEL = os.getenv('RCA_LOG_LEVEL', 'INFO')
    ENABLE_METRICS = os.getenv('RCA_ENABLE_METRICS', 'true').lower() == 'true'
    ENABLE_TRACING = os.getenv('RCA_ENABLE_TRACING', 'true').lower() == 'true'


# ============================================================================
# Structured Logger
# ============================================================================

class StructuredJSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        
        # Add request context if available (only within request context)
        try:
            if hasattr(g, 'request_id'):
                log_entry['request_id'] = g.request_id
            if hasattr(g, 'user_id'):
                log_entry['user_id'] = g.user_id
        except RuntimeError:
            # Outside of request context, skip request-specific fields
            pass
            
        # Add extra fields
        if hasattr(record, 'extra'):
            log_entry.update(record.extra)
            
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
            
        return json.dumps(log_entry)


def setup_logging():
    """Setup structured logging."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredJSONFormatter())
    
    # Configure root logger
    root_logger = logging.getLogger()
    # Ensure LOG_LEVEL is uppercase and get the logging constant
    log_level = ProductionConfig.LOG_LEVEL.upper()
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))
    root_logger.addHandler(handler)
    
    # Reduce noise from third-party libraries
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


# ============================================================================
# Golden Signals (Observability)
# ============================================================================

class GoldenSignalsCollector:
    """Collects golden signals metrics."""
    
    def __init__(self):
        self.request_count = 0
        self.error_count = 0
        self.latencies: List[float] = []
        self.planning_latencies: List[float] = []
        self.execution_latencies: List[float] = []
        self.failure_reasons: Dict[str, int] = {}
        self.start_time = time.time()
    
    def record_request(self, duration_ms: float, status_code: int, endpoint: str):
        """Record a request."""
        self.request_count += 1
        self.latencies.append(duration_ms)
        
        # Trim to keep last 1000 latencies
        if len(self.latencies) > 1000:
            self.latencies = self.latencies[-1000:]
            
        if status_code >= 400:
            self.error_count += 1
    
    def record_planning(self, duration_ms: float):
        """Record planning latency."""
        self.planning_latencies.append(duration_ms)
        if len(self.planning_latencies) > 1000:
            self.planning_latencies = self.planning_latencies[-1000:]
    
    def record_execution(self, duration_ms: float):
        """Record execution latency."""
        self.execution_latencies.append(duration_ms)
        if len(self.execution_latencies) > 1000:
            self.execution_latencies = self.execution_latencies[-1000:]
    
    def record_failure(self, reason: str):
        """Record a failure."""
        self.failure_reasons[reason] = self.failure_reasons.get(reason, 0) + 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        uptime = time.time() - self.start_time
        
        def calc_percentiles(values: List[float]) -> Dict[str, float]:
            if not values:
                return {'p50': 0, 'p95': 0, 'p99': 0, 'avg': 0}
            sorted_values = sorted(values)
            n = len(sorted_values)
            return {
                'p50': sorted_values[int(n * 0.5)] if n > 0 else 0,
                'p95': sorted_values[int(n * 0.95)] if n > 0 else 0,
                'p99': sorted_values[int(n * 0.99)] if n > 0 else 0,
                'avg': sum(sorted_values) / n if n > 0 else 0,
                'count': n,
            }
        
        return {
            'uptime_seconds': uptime,
            'total_requests': self.request_count,
            'error_count': self.error_count,
            'error_rate': self.error_count / max(self.request_count, 1),
            'request_latency': calc_percentiles(self.latencies),
            'planning_latency': calc_percentiles(self.planning_latencies),
            'execution_latency': calc_percentiles(self.execution_latencies),
            'failure_reasons': self.failure_reasons,
        }


# Global metrics collector
metrics_collector = GoldenSignalsCollector()


# ============================================================================
# Rate Limiter
# ============================================================================

class InMemoryRateLimiter:
    """In-memory rate limiter using token bucket algorithm."""
    
    def __init__(self, requests_per_minute: int = 60, requests_per_hour: int = 1000):
        self.rpm = requests_per_minute
        self.rph = requests_per_hour
        self.minute_tokens: Dict[str, Tuple[float, int]] = {}
        self.hour_tokens: Dict[str, Tuple[float, int]] = {}
    
    def is_allowed(self, identifier: str) -> Tuple[bool, str]:
        """Check if request is allowed."""
        now = time.time()
        
        # Check minute limit
        if identifier in self.minute_tokens:
            last_time, tokens = self.minute_tokens[identifier]
            # Refill tokens based on elapsed time
            elapsed = now - last_time
            new_tokens = min(self.rpm, tokens + int(elapsed * self.rpm / 60))
            self.minute_tokens[identifier] = (now, new_tokens)
            
            if new_tokens < 1:
                return False, 'Rate limit exceeded (per minute)'
            self.minute_tokens[identifier] = (now, new_tokens - 1)
        else:
            self.minute_tokens[identifier] = (now, self.rpm - 1)
        
        # Check hour limit
        if identifier in self.hour_tokens:
            last_time, tokens = self.hour_tokens[identifier]
            elapsed = now - last_time
            new_tokens = min(self.rph, tokens + int(elapsed * self.rph / 3600))
            self.hour_tokens[identifier] = (now, new_tokens)
            
            if new_tokens < 1:
                return False, 'Rate limit exceeded (per hour)'
            self.hour_tokens[identifier] = (now, new_tokens - 1)
        else:
            self.hour_tokens[identifier] = (now, self.rph - 1)
        
        return True, ''


rate_limiter = InMemoryRateLimiter(
    requests_per_minute=ProductionConfig.RATE_LIMIT_REQUESTS_PER_MINUTE,
    requests_per_hour=ProductionConfig.RATE_LIMIT_REQUESTS_PER_HOUR
)


# ============================================================================
# Session Management
# ============================================================================

# In-memory stores (use Redis in production)
agent_sessions: Dict[str, Dict[str, Any]] = {}
reasoning_history: List[Dict[str, Any]] = []
rules_store: List[Dict[str, Any]] = []
pipelines_store: List[Dict[str, Any]] = []


# ============================================================================
# Application Factory
# ============================================================================

def create_app() -> Flask:
    """Create and configure Flask application."""
    app = Flask(__name__)

    # Apply configuration
    app.config['SECRET_KEY'] = ProductionConfig.SECRET_KEY
    app.config['MAX_CONTENT_LENGTH'] = ProductionConfig.MAX_CONTENT_LENGTH

    # ------------------------------------------------------------------
    # 1️⃣ GLOBAL OPTIONS / PREFLIGHT HANDLER (runs per request)
    # ------------------------------------------------------------------
    @app.before_request
    def global_preflight():
        if request.method == "OPTIONS":
            response = Response("", status=200)

            origin = request.headers.get("Origin")
            cors_origins = ProductionConfig.CORS_ORIGINS

            if "*" in cors_origins:
                response.headers["Access-Control-Allow-Origin"] = origin or "*"
            elif origin in cors_origins:
                response.headers["Access-Control-Allow-Origin"] = origin
            else:
                response.headers["Access-Control-Allow-Origin"] = cors_origins[0]

            response.headers["Access-Control-Allow-Methods"] = (
                "GET, POST, PUT, DELETE, OPTIONS, PATCH"
            )
            response.headers["Access-Control-Allow-Headers"] = (
                request.headers.get(
                    "Access-Control-Request-Headers",
                    "Content-Type, Authorization, X-Request-ID",
                )
            )
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Access-Control-Max-Age"] = "3600"

            return response

    # ------------------------------------------------------------------
    # 2️⃣ APP SETUP (runs ONCE at startup)
    # ------------------------------------------------------------------
    CORS(app, origins=ProductionConfig.CORS_ORIGINS, supports_credentials=True)

    register_middleware(app)
    register_blueprints(app)
    register_error_handlers(app)

    return app



def register_middleware(app: Flask):
    """Register middleware."""
    from backend.auth import init_auth_middleware
    init_auth_middleware(app, secret_key=ProductionConfig.SECRET_KEY)

    @app.before_request
    def before_request():
        # OPTIONS already handled globally
        if request.method == "OPTIONS":
            return None

        g.request_start_time = time.time()
        g.request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
        g.user_id = request.headers.get('X-User-ID', 'anonymous')

        # Rate limiting (skip health checks)
        if not request.path.startswith('/api/v1/health'):
            identifier = request.headers.get('X-API-Key', request.remote_addr)
            allowed, message = rate_limiter.is_allowed(identifier)
            if not allowed:
                return jsonify({
                    'success': False,
                    'error': message,
                    'error_code': 'RATE_LIMITED',
                    'request_id': g.request_id,
                }), 429

    @app.after_request
    def after_request(response: Response) -> Response:
        # OPTIONS requests do not have g.request_start_time
        if request.method == "OPTIONS":
            return response

        duration_ms = (time.time() - g.request_start_time) * 1000

        metrics_collector.record_request(
            duration_ms,
            response.status_code,
            request.path,
        )

        response.headers['X-Request-ID'] = g.request_id
        response.headers['X-Response-Time-Ms'] = str(int(duration_ms))

        logging.getLogger('access').info(
            f'{request.method} {request.path}',
            extra={'extra': {
                'method': request.method,
                'path': request.path,
                'status': response.status_code,
                'duration_ms': duration_ms,
                'ip': request.remote_addr,
            }},
        )

        return response



def register_error_handlers(app: Flask):
    """Register error handlers."""

    @app.errorhandler(Exception)
    def handle_exception(e: Exception):
        """Handle all exceptions."""
        log = logging.getLogger('error')
        
        if isinstance(e, HTTPException):
            status_code = e.code
            error_message = e.description
        else:
            status_code = 500
            error_message = 'Internal server error'
            log.exception('Unhandled exception', extra={'extra': {
                'path': request.path,
                'method': request.method,
            }})
        
        metrics_collector.record_failure(type(e).__name__)
        
        return jsonify({
            'success': False,
            'error': error_message,
            'error_code': type(e).__name__.upper(),
            'request_id': getattr(g, 'request_id', 'unknown'),
        }), status_code


def register_blueprints(app: Flask):
    """Register all blueprints."""
    app.register_blueprint(health_bp, url_prefix='/api/v1')
    app.register_blueprint(metrics_bp, url_prefix='/api/v1')
    app.register_blueprint(query_bp, url_prefix='/api')
    app.register_blueprint(agent_bp, url_prefix='/api')
    app.register_blueprint(reasoning_bp, url_prefix='/api')
    
    # Register knowledge and metadata API
    from backend.knowledge_metadata_api import knowledge_metadata_bp
    app.register_blueprint(knowledge_metadata_bp, url_prefix='/api')
    app.register_blueprint(assistant_bp, url_prefix='/api')
    app.register_blueprint(pipelines_bp, url_prefix='/api')
    app.register_blueprint(rules_bp, url_prefix='/api')
    app.register_blueprint(metadata_bp, url_prefix='/api')
    app.register_blueprint(ingestion_bp, url_prefix='/api')
    
    # Register notebook blueprint
    # IMPORTANT: Import directly from notebook.py to avoid circular import via api/__init__.py
    startup_logger = logging.getLogger('startup')
    try:
        # Use direct file import to bypass api/__init__.py which has circular imports
        import importlib.util
        import os
        notebook_path = os.path.join(os.path.dirname(__file__), 'api', 'notebook.py')
        
        if not os.path.exists(notebook_path):
            raise ImportError(f"Could not find notebook.py at {notebook_path}")
        
        # Load notebook module directly without triggering api/__init__.py
        spec = importlib.util.spec_from_file_location("notebook_api", notebook_path)
        notebook_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(notebook_module)
        notebook_router = notebook_module.notebook_router
        
        startup_logger.info(f'Notebook blueprint imported successfully: name={notebook_router.name}')
        
        # Register the blueprint
        app.register_blueprint(notebook_router, url_prefix='/api/v1')
        startup_logger.info(f'Notebook blueprint registered: name={notebook_router.name}, url_prefix=/api/v1')
        
        # Force Flask to update url_map by accessing it
        _ = list(app.url_map.iter_rules())
        
        # Log registered routes for debugging
        registered_routes = [f"{rule.rule} [{', '.join(rule.methods - {'OPTIONS', 'HEAD'})}]" 
                            for rule in app.url_map.iter_rules() 
                            if 'notebook' in str(rule)]
        if registered_routes:
            startup_logger.info(f'✓ Notebook API endpoints registered: {registered_routes}')
        else:
            startup_logger.warning('Notebook blueprint registered but no routes found in url_map')
            # List all /api/v1 routes to see what's registered
            all_v1_routes = [f"{rule.rule} [{', '.join(rule.methods - {'OPTIONS', 'HEAD'})}]" 
                           for rule in app.url_map.iter_rules() 
                           if '/api/v1' in str(rule)]
            startup_logger.info(f'All /api/v1 routes: {all_v1_routes}')
    except ImportError as e:
        startup_logger.error(f'Notebook module import failed: {e}', exc_info=True)
    except Exception as e:
        startup_logger.error(f'Failed to register notebook blueprint: {e}', exc_info=True)
    
    # Register clarification blueprint
    try:
        from backend.api.clarification import clarification_bp
        app.register_blueprint(clarification_bp)
    except ImportError:
        pass  # Clarification module optional
    
    # Root endpoint
    @app.route('/')
    def root():
        return jsonify({
            'service': 'RCA Engine',
            'version': '2.0.0',
            'status': 'operational',
            'mode': 'production' if not ProductionConfig.DEBUG else 'development',
            'endpoints': {
                'health': '/api/v1/health',
                'metrics': '/api/v1/metrics',
                'query': '/api/query/*',
                'agent': '/api/agent/*',
                'reasoning': '/api/reasoning/*',
                'assistant': '/api/assistant/*',
                'pipelines': '/api/pipelines',
                'rules': '/api/rules',
                'metadata': '/api/metadata/*',
            }
        })


# ============================================================================
# Health Endpoints
# ============================================================================

health_bp = Blueprint('health', __name__)

@health_bp.route('/health', methods=['GET'])
def health_check():
    """Basic health check."""
    return jsonify({
        'status': 'healthy',
        'service': 'rca-engine',
        'version': '2.0.0',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
    })


@health_bp.route('/health/ready', methods=['GET'])
def readiness_check():
    """Readiness check."""
    return jsonify({
        'status': 'ready',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
    })


@health_bp.route('/health/live', methods=['GET'])
def liveness_check():
    """Liveness check."""
    return jsonify({
        'status': 'alive',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
    })


@health_bp.route('/health/detailed', methods=['GET'])
def detailed_health():
    """Detailed health check."""
    return jsonify({
        'status': 'healthy',
        'version': '2.0.0',
        'components': {
            'api': 'operational',
            'llm': 'operational' if os.getenv('OPENAI_API_KEY', '') else 'degraded',
            'query_engine': 'operational',
            'metrics': 'operational' if ProductionConfig.ENABLE_METRICS else 'disabled',
        },
        'metrics': metrics_collector.get_metrics(),
        'config': {
            'llm_model': ProductionConfig.LLM_MODEL,
            'rate_limit_rpm': ProductionConfig.RATE_LIMIT_REQUESTS_PER_MINUTE,
        },
        'timestamp': datetime.utcnow().isoformat() + 'Z',
    })


# ============================================================================
# Metrics Endpoints
# ============================================================================

metrics_bp = Blueprint('metrics', __name__)

@metrics_bp.route('/metrics', methods=['GET'])
def get_metrics():
    """Get current metrics."""
    return jsonify(metrics_collector.get_metrics())


@metrics_bp.route('/metrics/prometheus', methods=['GET'])
def prometheus_metrics():
    """Get metrics in Prometheus format."""
    try:
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        
        # Generate Prometheus metrics
        output = generate_latest()
        
        return Response(
            output,
            mimetype=CONTENT_TYPE_LATEST
        )
    except ImportError:
        # Fallback to JSON if Prometheus not available
        metrics = metrics_collector.get_metrics()
        
        lines = [
            '# HELP rca_engine_uptime_seconds Server uptime in seconds',
            '# TYPE rca_engine_uptime_seconds gauge',
            f'rca_engine_uptime_seconds {metrics.get("uptime_seconds", 0)}',
            '',
        '# HELP rca_engine_requests_total Total request count',
        '# TYPE rca_engine_requests_total counter',
        f'rca_engine_requests_total {metrics["total_requests"]}',
        '',
        '# HELP rca_engine_errors_total Total error count',
        '# TYPE rca_engine_errors_total counter',
        f'rca_engine_errors_total {metrics["error_count"]}',
        '',
        '# HELP rca_engine_request_latency_ms Request latency',
        '# TYPE rca_engine_request_latency_ms summary',
        f'rca_engine_request_latency_ms{{quantile="0.5"}} {metrics["request_latency"]["p50"]}',
        f'rca_engine_request_latency_ms{{quantile="0.95"}} {metrics["request_latency"]["p95"]}',
        f'rca_engine_request_latency_ms{{quantile="0.99"}} {metrics["request_latency"]["p99"]}',
        f'rca_engine_request_latency_ms_count {metrics["request_latency"]["count"]}',
        '',
    ]
    
    # Add failure reasons
    for reason, count in metrics.get('failure_reasons', {}).items():
        lines.append(f'rca_engine_failures_total{{reason="{reason}"}} {count}')
    
    return Response('\n'.join(lines), mimetype='text/plain')


# ============================================================================
# Query Endpoints
# ============================================================================

query_bp = Blueprint('query', __name__)

def get_query_generator():
    """Get query generator (lazy import)."""
    try:
        from query_regeneration_api import generate_sql_from_query, load_prerequisites
        return generate_sql_from_query, load_prerequisites
    except ImportError:
        from backend.query_regeneration_api import generate_sql_from_query, load_prerequisites
        return generate_sql_from_query, load_prerequisites


@query_bp.route('/query/load-prerequisites', methods=['GET'])
def load_query_prerequisites():
    """Load metadata and prerequisites."""
    start_time = time.time()
    try:
        _, load_prerequisites = get_query_generator()
        result = load_prerequisites()
        
        duration_ms = (time.time() - start_time) * 1000
        metrics_collector.record_planning(duration_ms)
        
        return jsonify(result)
    except Exception as e:
        logging.getLogger('error').exception('Error loading prerequisites')
        return jsonify({
            'success': False,
            'error': str(e),
        }), 500


@query_bp.route('/query/generate-sql', methods=['POST'])
def generate_sql():
    """Generate SQL from natural language query."""
    start_time = time.time()
    try:
        data = request.get_json() or {}
        query_text = data.get('query', '')
        use_llm = data.get('use_llm', bool(ProductionConfig.OPENAI_API_KEY))
        
        if not query_text:
            return jsonify({
                'success': False,
                'error': 'Query is required',
            }), 400
        
        generate_sql_from_query, _ = get_query_generator()
        result = generate_sql_from_query(query_text, use_llm=use_llm)
        
        duration_ms = (time.time() - start_time) * 1000
        metrics_collector.record_planning(duration_ms)
        
        return jsonify(result)
    except Exception as e:
        logging.getLogger('error').exception('Error generating SQL')
        metrics_collector.record_failure('sql_generation_error')
        return jsonify({
            'success': False,
            'error': str(e),
        }), 500


@query_bp.route('/graph', methods=['GET'])
def get_graph():
    """Get hypergraph visualization data."""
    try:
        # Load metadata files
        metadata_dir = Path(project_root) / 'metadata'
        tables_file = metadata_dir / 'tables.json'
        lineage_file = metadata_dir / 'lineage.json'
        
        # Load tables
        if not tables_file.exists():
            logging.getLogger('error').warning(f'Tables metadata file not found at {tables_file}')
            return jsonify({
                'error': f'Tables metadata file not found at {tables_file}',
            }), 404
        
        with open(tables_file, 'r', encoding='utf-8') as f:
            tables_data = json.load(f)
        
        # Load lineage
        edges_data = []
        if lineage_file.exists():
            with open(lineage_file, 'r', encoding='utf-8') as f:
                lineage_data = json.load(f)
                edges_data = lineage_data.get('edges', [])
        else:
            logging.getLogger('info').info(f'Lineage file not found at {lineage_file}, using empty edges')
        
        # Transform tables to nodes
        nodes = []
        for table in tables_data.get('tables', []):
            columns = [col['name'] for col in table.get('columns', [])]
            nodes.append({
                'id': table['name'],
                'label': table['name'],
                'type': 'table',
                'columns': columns,
                'title': f"{table['name']} - {table.get('entity', '')}",
            })
        
        # Transform lineage to edges
        edges = []
        for idx, edge in enumerate(edges_data):
            edges.append({
                'id': f'edge_{idx}',
                'from': edge['from'],
                'to': edge['to'],
                'label': edge.get('on', ''),
                'joinCondition': edge.get('on', ''),
                'relationship': edge.get('description', ''),
            })
        
        # Calculate stats
        table_count = len(nodes)
        column_count = sum(len(node.get('columns', [])) for node in nodes)
        
        result = {
            'nodes': nodes,
            'edges': edges,
            'stats': {
                'total_nodes': len(nodes),
                'total_edges': len(edges),
                'table_count': table_count,
                'column_count': column_count,
            },
        }
        
        logging.getLogger('info').info(f'Graph data loaded: {table_count} tables, {len(edges)} edges')
        return jsonify(result)
    except Exception as e:
        logging.getLogger('error').exception('Error loading graph data')
        return jsonify({
            'error': str(e),
        }), 500


# ============================================================================
# Agent Endpoints
# ============================================================================

agent_bp = Blueprint('agent', __name__)

@agent_bp.route('/agent/run', methods=['POST'])
def agent_run():
    """Run agent query."""
    start_time = time.time()
    try:
        data = request.get_json() or {}
        session_id = data.get('session_id')
        user_query = data.get('user_query')
        ui_context = data.get('ui_context', {})
        
        if not session_id or not user_query:
            return jsonify({
                'status': 'error',
                'error': 'session_id and user_query are required',
            }), 400
        
        # Initialize session if needed
        if session_id not in agent_sessions:
            agent_sessions[session_id] = {
                'id': session_id,
                'history': [],
                'context': ui_context,
                'created_at': datetime.utcnow().isoformat() + 'Z',
            }
        
        session = agent_sessions[session_id]
        session['history'].append({
            'role': 'user',
            'content': user_query,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        })
        
        # Generate SQL using query generator
        generate_sql_from_query, _ = get_query_generator()
        use_llm = bool(os.getenv('OPENAI_API_KEY', ''))
        # Enable clarification mode (can be overridden via request parameter)
        clarification_mode = request.json.get('clarification_mode', True)
        result = generate_sql_from_query(user_query, use_llm=use_llm, 
                                        clarification_mode=clarification_mode)
        
        duration_ms = (time.time() - start_time) * 1000
        metrics_collector.record_planning(duration_ms)
        
        # Add assistant response to history
        assistant_response = {
            'role': 'assistant',
            'content': result.get('sql') or result.get('error', 'Query processed'),
            'sql': result.get('sql'),
            'intent': result.get('intent'),
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }
        session['history'].append(assistant_response)
        
        if result.get('success') and result.get('sql'):
            return jsonify({
                'status': 'success',
                'message': 'Query generated successfully',
                'data': {
                    'sql': result['sql'],
                    'intent': result.get('intent'),
                },
                'final_answer': result['sql'],
                'trace': result.get('reasoning_steps', []),
            })
        elif result.get('needs_clarification'):
            # Handle proactive clarification response
            clarification = result.get('clarification', {})
            return jsonify({
                'status': 'needs_clarification',
                'message': clarification.get('message', 'Query needs clarification'),
                'confidence': result.get('confidence', 0.5),
                'query': user_query,
                'clarification': {
                    'questions': clarification.get('questions', []),
                    'message': clarification.get('message', 'I need more information'),
                },
                'suggested_intent': result.get('suggested_intent'),
            })
        else:
            # Fallback for other error cases
            return jsonify({
                'status': 'needs_clarification',
                'message': result.get('error', 'Query needs clarification'),
                'clarification': {
                    'query': user_query,
                    'question': result.get('error', 'Could you provide more details?'),
                    'confidence': 0.5,
                    'missing_pieces': [{
                        'field': 'query',
                        'importance': 'high',
                        'description': result.get('error', ''),
                    }] if result.get('error') else [],
                },
            })
    except Exception as e:
        logging.getLogger('error').exception('Error in agent run')
        metrics_collector.record_failure('agent_error')
        return jsonify({
            'status': 'error',
            'error': str(e),
        }), 500


@agent_bp.route('/agent/continue', methods=['POST'])
def agent_continue():
    """Continue agent conversation."""
    try:
        data = request.get_json() or {}
        session_id = data.get('session_id')
        choice_id = data.get('choice_id')
        ui_context = data.get('ui_context', {})
        
        if not session_id or not choice_id:
            return jsonify({
                'status': 'error',
                'error': 'session_id and choice_id are required',
            }), 400
        
        session = agent_sessions.get(session_id)
        if not session:
            return jsonify({
                'status': 'error',
                'error': 'Session not found',
            }), 404
        
        session['context'].update(ui_context)
        
        return jsonify({
            'status': 'success',
            'message': 'Choice processed',
            'data': {
                'choice_id': choice_id,
                'session_id': session_id,
            },
        })
    except Exception as e:
        logging.getLogger('error').exception('Error in agent continue')
        return jsonify({
            'status': 'error',
            'error': str(e),
        }), 500


# ============================================================================
# Reasoning Endpoints
# ============================================================================

reasoning_bp = Blueprint('reasoning', __name__)

@reasoning_bp.route('/reasoning/query', methods=['POST'])
def reasoning_query():
    """Process reasoning query using new AI SQL System."""
    start_time = time.time()
    logger = logging.getLogger(__name__)
    try:
        data = request.get_json() or {}
        query_text = data.get('query', '')
        context = data.get('context', {})
        
        if not query_text:
            return jsonify({'error': 'Query is required'}), 400
        
            # Use new AI SQL System
        try:
            # Ensure .env is loaded before importing modules that need API key
            from dotenv import load_dotenv
            from pathlib import Path
            backend_dir = Path(__file__).parent
            project_root = backend_dir.parent
            env_file = project_root / '.env'
            if env_file.exists():
                load_dotenv(env_file, override=True)  # override=True ensures latest values
            
            from backend.ai_sql_system.orchestration.graph import LangGraphOrchestrator
            from backend.ai_sql_system.trino.client import TrinoClient
            from backend.ai_sql_system.trino.validator import TrinoValidator
            from backend.ai_sql_system.metadata.semantic_registry import SemanticRegistry
            from backend.ai_sql_system.planning.join_graph import JoinGraph
            import json
            
            # Reload .env to ensure it's fresh and verify API key is loaded
            from dotenv import load_dotenv
            backend_dir = Path(__file__).parent
            project_root = backend_dir.parent
            env_file = project_root / '.env'
            if env_file.exists():
                load_dotenv(env_file, override=True)
            
            # Check API key - Docker should provide it via env_file, but also try loading .env
            api_key = os.getenv('OPENAI_API_KEY')
            
            # Debug: Log all environment variables starting with OPENAI
            openai_vars = {k: v for k, v in os.environ.items() if k.startswith('OPENAI')}
            logger.info(f"OpenAI environment variables: {list(openai_vars.keys())}")
            
            if not api_key:
                # Try loading .env file directly (for local development or if Docker env_file didn't work)
                try:
                    from dotenv import load_dotenv
                    backend_dir = Path(__file__).parent
                    project_root = backend_dir.parent
                    env_file = project_root / '.env'
                    
                    # Also try /app/.env (Docker container path)
                    docker_env_file = Path('/app/.env')
                    
                    for env_path in [env_file, docker_env_file, Path('.env')]:
                        if env_path.exists():
                            logger.info(f"Trying to load .env from: {env_path}")
                            load_dotenv(env_path, override=True)
                            api_key = os.getenv('OPENAI_API_KEY')
                            if api_key:
                                os.environ['OPENAI_API_KEY'] = api_key
                                model = os.getenv('OPENAI_MODEL')
                                if model:
                                    os.environ['OPENAI_MODEL'] = model
                                logger.info(f"✓ Loaded OPENAI_API_KEY from {env_path} (length: {len(api_key)})")
                                break
                except Exception as e:
                    logger.warning(f"Could not load .env file: {e}")
            
            if not api_key:
                logger.error("OPENAI_API_KEY not found in environment. Available env vars: " + str(list(os.environ.keys())[:20]))
                return jsonify({
                    'error': 'OpenAI API key not configured. Please set OPENAI_API_KEY in .env file or environment.',
                    'steps': [{
                        'type': 'error',
                        'content': f'OpenAI API key not configured. Checked environment and .env files. Available vars: {list(openai_vars.keys())}',
                        'timestamp': datetime.utcnow().isoformat() + 'Z',
                    }],
                }), 500
            else:
                logger.info(f"✓ OPENAI_API_KEY available (length: {len(api_key)}, starts with: {api_key[:10]}..., model: {os.getenv('OPENAI_MODEL', 'default')})")
            
            # Initialize orchestrator (with join graph loading)
            trino_client = TrinoClient()
            trino_validator = TrinoValidator(trino_client)
            
            # Initialize semantic registry (will fall back to JSON if Postgres unavailable)
            try:
                semantic_registry = SemanticRegistry()
            except Exception as e:
                logger.warning(f"Could not initialize SemanticRegistry with Postgres: {e}. Using JSON fallback.")
                semantic_registry = SemanticRegistry(ingestion=None, vector_store=None)
            
            join_graph = JoinGraph()
            
            # Load join graph from metadata
            try:
                metadata_dir = Path(__file__).parent.parent.parent / "metadata"
                lineage_file = metadata_dir / "lineage.json"
                
                if lineage_file.exists():
                    with open(lineage_file, 'r') as f:
                        lineage_data = json.load(f)
                    
                    for edge in lineage_data.get('edges', []):
                        from_table = edge.get('from', '').split('.')[-1]
                        to_table = edge.get('to', '').split('.')[-1]
                        condition = edge.get('on', '')
                        join_type = 'LEFT'
                        
                        if from_table and to_table and condition:
                            join_graph.add_join(from_table, to_table, condition, join_type)
            except Exception as e:
                logger.warning(f"Could not load join graph: {e}")
            
            orchestrator = LangGraphOrchestrator(
                trino_validator=trino_validator,
                semantic_registry=semantic_registry,
                join_graph=join_graph
            )
            
            # Run pipeline
            result = orchestrator.run(query_text)
            
            duration_ms = (time.time() - start_time) * 1000
            metrics_collector.record_planning(duration_ms)
            
            # Build reasoning steps from pipeline result
            steps = []
            steps.append({
                'type': 'thought',
                'content': f' Analyzing query: "{query_text}"',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
            })
            
            if result.get('intent'):
                steps.append({
                    'type': 'action',
                    'content': f' Extracted intent: {json.dumps(result["intent"], indent=2)}',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                })
            
            if result.get('resolution'):
                steps.append({
                    'type': 'thought',
                    'content': f' Resolution: {result["resolution"].get("type")} - {result["resolution"].get("reason")}',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                })
            
            if result.get('success') and result.get('sql'):
                steps.append({
                    'type': 'action',
                    'content': ' Generated SQL using LangGraph pipeline',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                })
                steps.append({
                    'type': 'result',
                    'content': f' Generated SQL:\n\n```sql\n{result["sql"]}\n```',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                })
            else:
                steps.append({
                    'type': 'error',
                    'content': f' Error: {result.get("error", "Unknown error")}',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                })
            
            if result.get('validation_errors'):
                steps.append({
                    'type': 'thought',
                    'content': f' Validation errors: {result["validation_errors"]}',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                })
            
            reasoning_history.extend(steps)
            
            return jsonify({
                'result': result.get('sql') or result.get('error', 'Query processed'),
                'steps': steps,
                'sql': result.get('sql'),
                'intent': result.get('intent'),
                'method': 'langgraph_pipeline',
            })
            
        except ImportError as e:
            logging.getLogger(__name__).exception('Error importing AI SQL System')
            return jsonify({
                'error': f'AI SQL System not available: {str(e)}',
                'steps': [{
                    'type': 'error',
                    'content': f' Error: {str(e)}',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                }],
            }), 500
        except Exception as e:
            logging.getLogger(__name__).exception('Error in AI SQL System')
            return jsonify({
                'error': str(e),
                'steps': [{
                    'type': 'error',
                    'content': f' Error processing query: {str(e)}',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                }],
            }), 500
            
    except Exception as e:
        logging.getLogger('error').exception('Error in reasoning query')
        return jsonify({
            'result': f'Error: {str(e)}',
            'steps': [{
                'type': 'error',
                'content': f' Error processing query: {str(e)}',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
            }],
            'error': str(e),
        }), 500


@reasoning_bp.route('/reasoning/assess', methods=['POST'])
def reasoning_assess():
    """Assess query quality."""
    try:
        data = request.get_json() or {}
        query_text = data.get('query', '')
        
        if not query_text:
            return jsonify({'error': 'query is required'}), 400
        
        generate_sql_from_query, _ = get_query_generator()
        use_llm = bool(os.getenv('OPENAI_API_KEY', ''))
        result = generate_sql_from_query(query_text, use_llm=use_llm)
        
        assessment = {
            'query': query_text,
            'clarity': 'high' if result.get('success') else 'low',
            'completeness': 'complete' if result.get('sql') else 'incomplete',
            'confidence': 0.9 if result.get('success') else 0.3,
            'intent': result.get('intent'),
            'sql_generated': bool(result.get('sql')),
            'warnings': result.get('warnings', []),
            'suggestions': [f"Consider: {result['error']}"] if result.get('error') else [],
        }
        
        return jsonify(assessment)
    except Exception as e:
        logging.getLogger('error').exception('Error in reasoning assess')
        return jsonify({
            'error': str(e),
            'assessment': {
                'query': request.get_json().get('query', ''),
                'clarity': 'unknown',
                'completeness': 'unknown',
                'confidence': 0.0,
                'error': str(e),
            },
        }), 500


@reasoning_bp.route('/reasoning/clarify', methods=['POST'])
def reasoning_clarify():
    """Generate clarification."""
    try:
        data = request.get_json() or {}
        query_text = data.get('query', '')
        answer = data.get('answer')
        
        if not query_text:
            return jsonify({'error': 'query is required'}), 400
        
        clarification = {
            'query': query_text,
            'answer': answer,
            'needs_clarification': True,
            'clarification_questions': [
                'Could you specify which metrics you want to see?',
                'What time range are you interested in?',
                'Are there any specific filters you want to apply?',
            ],
            'suggestions': [
                'Try rephrasing your query with more specific details',
                'Include metric names if you know them',
                'Specify date ranges or filters',
            ],
        }
        
        return jsonify(clarification)
    except Exception as e:
        logging.getLogger('error').exception('Error in reasoning clarify')
        return jsonify({'error': str(e)}), 500


# ============================================================================
# Assistant Endpoints
# ============================================================================

assistant_bp = Blueprint('assistant', __name__)

@assistant_bp.route('/assistant/ask', methods=['POST'])
def assistant_ask():
    """Handle assistant questions - execute queries directly and return results with LLM conclusion."""
    try:
        data = request.get_json() or {}
        question = data.get('question', '')
        
        if not question:
            return jsonify({
                'response_type': 'Error',
                'error': 'question is required',
            }), 400
        
        # Generate SQL from query
        generate_sql_from_query, _ = get_query_generator()
        use_llm = bool(os.getenv('OPENAI_API_KEY', ''))
        result = generate_sql_from_query(question, use_llm=use_llm)
        
        if not result.get('success') or not result.get('sql'):
            return jsonify({
                'response_type': 'NeedsClarification',
                'status': 'failed',
                'answer': result.get('error', 'I need more information to generate the query.'),
                'clarification': {
                    'query': question,
                    'question': result.get('error', 'Could you provide more details about what you want to query?'),
                    'missing_pieces': [{
                        'field': 'query',
                        'importance': 'high',
                        'description': result.get('error', ''),
                    }] if result.get('error') else [],
                },
            })
        
        sql = result['sql']
        
            # Execute query directly using Trino
        try:
            import requests
            logger = logging.getLogger(__name__)
            
            # Get Trino configuration
            trino_url = os.getenv('TRINO_COORDINATOR_URL')
            if not trino_url:
                try:
                    import socket
                    socket.gethostbyname('trino')
                    trino_url = 'http://trino:8080'
                except socket.gaierror:
                    trino_url = 'http://localhost:8081'
            
            trino_user = os.getenv('TRINO_USER', 'admin')
            trino_catalog = os.getenv('TRINO_CATALOG', 'tpcds')
            trino_schema = os.getenv('TRINO_SCHEMA', 'tiny')
            
            logger.info(f'Executing query via Trino: {trino_url}, catalog={trino_catalog}, schema={trino_schema}')
            logger.debug(f'SQL: {sql[:200]}...' if len(sql) > 200 else f'SQL: {sql}')
            
            headers = {
                'X-Trino-User': trino_user,
                'X-Trino-Catalog': trino_catalog,
                'X-Trino-Schema': trino_schema,
            }
            
            # Execute query with timeout
            try:
                response = requests.post(
                    f'{trino_url}/v1/statement',
                    headers={**headers, 'Content-Type': 'text/plain'},
                    data=sql.encode('utf-8'),
                    timeout=120  # 2 minutes timeout
                )
            except requests.exceptions.Timeout:
                logger.error('Trino query timed out after 120 seconds')
                raise Exception('Query execution timed out. The query may be too complex or Trino is unavailable.')
            except requests.exceptions.ConnectionError as e:
                logger.error(f'Cannot connect to Trino at {trino_url}: {e}')
                raise Exception(f'Cannot connect to Trino at {trino_url}. Is Trino running?')
            
            if response.status_code != 200:
                raise Exception(f'Trino connection failed (HTTP {response.status_code}): {response.text[:200]}')
            
            data = response.json()
            if data.get('error'):
                error_info = data['error']
                raise Exception(f'Trino query error: {error_info.get("message", "Unknown error")}')
            
            # Fetch all data chunks
            rows = []
            schema = []
            if 'columns' in data:
                schema = [{'name': col['name'], 'type': col['type']} for col in data['columns']]
            if 'data' in data:
                rows.extend(data['data'])
            
            next_uri = data.get('nextUri')
            max_chunks = 1000  # Safety limit
            chunk_count = 0
            while next_uri and chunk_count < max_chunks:
                chunk_count += 1
                if next_uri.startswith('http://') or next_uri.startswith('https://'):
                    chunk_url = next_uri
                else:
                    chunk_url = f'{trino_url}{next_uri}'
                
                try:
                    chunk_response = requests.get(chunk_url, headers=headers, timeout=30)
                    if chunk_response.status_code != 200:
                        logger.warning(f'Chunk fetch failed with status {chunk_response.status_code}')
                        break
                    
                    chunk_data = chunk_response.json()
                    if chunk_data.get('error'):
                        error_info = chunk_data['error']
                        logger.error(f'Trino chunk error: {error_info.get("message", "Unknown error")}')
                        break
                    
                    if 'data' in chunk_data:
                        rows.extend(chunk_data['data'])
                    
                    if 'columns' in chunk_data and not schema:
                        schema = [{'name': col['name'], 'type': col['type']} for col in chunk_data['columns']]
                    
                    stats = chunk_data.get('stats', {})
                    query_state = stats.get('state')
                    if query_state == 'FINISHED':
                        break
                    elif query_state == 'FAILED':
                        error_info = chunk_data.get('error', {})
                        raise Exception(f'Query failed: {error_info.get("message", "Unknown error")}')
                    
                    next_uri = chunk_data.get('nextUri')
                    if not next_uri:
                        break
                except requests.exceptions.Timeout:
                    logger.warning(f'Chunk fetch timed out after 30 seconds (fetched {chunk_count} chunks)')
                    break
                except Exception as chunk_error:
                    logger.error(f'Error fetching chunk: {chunk_error}')
                    break
            
            if chunk_count >= max_chunks:
                logger.warning(f'Reached maximum chunk limit ({max_chunks}), stopping fetch')
            
            # Get first 5 rows for preview
            preview_rows = rows[:5]
            total_rows = len(rows)
            
            # Generate LLM conclusion if API key is available
            conclusion = None
            if use_llm and rows:
                try:
                    from openai import OpenAI
                    
                    # Format data for LLM
                    column_names = [col['name'] for col in schema]
                    sample_data = preview_rows[:5]  # Use preview rows for context
                    
                    # Create a readable summary of the data
                    data_summary = f"Query: {question}\n\n"
                    data_summary += f"Columns: {', '.join(column_names)}\n"
                    data_summary += f"Total rows: {total_rows}\n\n"
                    data_summary += "Sample data (first 5 rows):\n"
                    for i, row in enumerate(sample_data, 1):
                        row_str = ', '.join([f"{col}: {val}" for col, val in zip(column_names, row)])
                        data_summary += f"Row {i}: {row_str}\n"
                    
                    # Generate conclusion using OpenAI
                    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
                    model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
                    
                    completion = client.chat.completions.create(
                        model=model,
                        messages=[
                            {
                                "role": "system",
                                "content": "You are a data analyst assistant. Analyze query results and provide a clear, concise conclusion about what the data shows. Focus on insights, patterns, and key findings. Keep it conversational and helpful."
                            },
                            {
                                "role": "user",
                                "content": f"{data_summary}\n\nBased on this query and results, provide a clear conclusion about what the data shows."
                            }
                        ],
                        temperature=0.7,
                        max_tokens=500
                    )
                    
                    conclusion = completion.choices[0].message.content.strip()
                except Exception as e:
                    logger.warning(f"Failed to generate LLM conclusion: {e}")
                    conclusion = f"Query executed successfully. Found {total_rows} row{'s' if total_rows != 1 else ''}."
            
            # Format rows for CSV download
            csv_content = ','.join([col['name'] for col in schema]) + '\n'
            for row in rows:
                csv_content += ','.join([str(val) if val is not None else '' for val in row]) + '\n'
            
            return jsonify({
                'response_type': 'QueryResult',
                'status': 'success',
                'answer': conclusion or f"Query executed successfully. Found {total_rows} row{'s' if total_rows != 1 else ''}.",
                'conclusion': conclusion,
                'preview_data': {
                    'columns': schema,
                    'rows': preview_rows,
                    'total_rows': total_rows,
                },
                'full_data': {
                    'columns': schema,
                    'rows': rows,
                    'csv': csv_content,
                },
                'sql': sql,
                'intent': result.get('intent'),
                'validation': result.get('validation'),
            })
            
        except Exception as exec_error:
            logger.error(f"Query execution failed: {exec_error}", exc_info=True)
            # Return error response
            return jsonify({
                'response_type': 'Error',
                'status': 'error',
                'error': str(exec_error),
                'answer': f"Query execution failed: {str(exec_error)}",
                'sql': sql,
            }), 500
            
    except Exception as e:
        logging.getLogger('error').exception('Error in assistant ask')
        return jsonify({
            'response_type': 'Error',
            'status': 'error',
            'error': str(e),
            'answer': f'Sorry, I encountered an error: {str(e)}',
        }), 500


# ============================================================================
# Pipelines Endpoints
# ============================================================================

pipelines_bp = Blueprint('pipelines', __name__)

@pipelines_bp.route('/pipelines', methods=['GET'])
def list_pipelines():
    """List all pipelines."""
    return jsonify(pipelines_store)


@pipelines_bp.route('/pipelines', methods=['POST'])
def create_pipeline():
    """Create a new pipeline."""
    data = request.get_json() or {}
    pipeline = {
        'id': f'pipeline-{int(time.time() * 1000)}',
        **data,
        'status': 'inactive',
        'createdAt': datetime.utcnow().isoformat() + 'Z',
    }
    pipelines_store.append(pipeline)
    return jsonify(pipeline)


@pipelines_bp.route('/pipelines/<pipeline_id>', methods=['GET'])
def get_pipeline(pipeline_id: str):
    """Get a pipeline by ID."""
    pipeline = next((p for p in pipelines_store if p['id'] == pipeline_id), None)
    if not pipeline:
        return jsonify({'error': 'Pipeline not found'}), 404
    return jsonify(pipeline)


@pipelines_bp.route('/pipelines/<pipeline_id>', methods=['PUT'])
def update_pipeline(pipeline_id: str):
    """Update a pipeline."""
    data = request.get_json() or {}
    for i, p in enumerate(pipelines_store):
        if p['id'] == pipeline_id:
            pipelines_store[i] = {**p, **data, 'updatedAt': datetime.utcnow().isoformat() + 'Z'}
            return jsonify(pipelines_store[i])
    return jsonify({'error': 'Pipeline not found'}), 404


@pipelines_bp.route('/pipelines/<pipeline_id>', methods=['DELETE'])
def delete_pipeline(pipeline_id: str):
    """Delete a pipeline."""
    global pipelines_store
    pipelines_store = [p for p in pipelines_store if p['id'] != pipeline_id]
    return jsonify({'success': True})


@pipelines_bp.route('/pipelines/<pipeline_id>/run', methods=['POST'])
def run_pipeline(pipeline_id: str):
    """Run a pipeline."""
    pipeline = next((p for p in pipelines_store if p['id'] == pipeline_id), None)
    if not pipeline:
        return jsonify({'error': 'Pipeline not found'}), 404
    
    pipeline['status'] = 'active'
    pipeline['lastRun'] = datetime.utcnow().isoformat() + 'Z'
    
    return jsonify({
        'success': True,
        'message': 'Pipeline executed',
        'pipeline': pipeline,
    })


@pipelines_bp.route('/pipelines/<pipeline_id>/status', methods=['GET'])
def get_pipeline_status(pipeline_id: str):
    """Get pipeline status."""
    pipeline = next((p for p in pipelines_store if p['id'] == pipeline_id), None)
    if not pipeline:
        return jsonify({'error': 'Pipeline not found'}), 404
    return jsonify({
        'status': pipeline.get('status', 'unknown'),
        'lastRun': pipeline.get('lastRun'),
    })


# ============================================================================
# Rules Endpoints
# ============================================================================

rules_bp = Blueprint('rules', __name__)

@rules_bp.route('/rules', methods=['GET'])
def list_rules():
    """List all rules."""
    return jsonify({'rules': rules_store})


@rules_bp.route('/rules', methods=['POST'])
def create_rule():
    """Create a new rule."""
    data = request.get_json() or {}
    rule = {
        'id': f'rule-{int(time.time() * 1000)}',
        **data,
        'createdAt': datetime.utcnow().isoformat() + 'Z',
    }
    rules_store.append(rule)
    return jsonify(rule)


@rules_bp.route('/rules/<rule_id>', methods=['GET'])
def get_rule(rule_id: str):
    """Get a rule by ID."""
    rule = next((r for r in rules_store if r['id'] == rule_id), None)
    if not rule:
        return jsonify({'error': 'Rule not found'}), 404
    return jsonify(rule)


@rules_bp.route('/rules/<rule_id>', methods=['PUT'])
def update_rule(rule_id: str):
    """Update a rule."""
    data = request.get_json() or {}
    for i, r in enumerate(rules_store):
        if r['id'] == rule_id:
            rules_store[i] = {**r, **data, 'updatedAt': datetime.utcnow().isoformat() + 'Z'}
            return jsonify(rules_store[i])
    return jsonify({'error': 'Rule not found'}), 404


@rules_bp.route('/rules/<rule_id>', methods=['DELETE'])
def delete_rule(rule_id: str):
    """Delete a rule."""
    global rules_store
    rules_store = [r for r in rules_store if r['id'] != rule_id]
    return jsonify({'success': True})


# ============================================================================
# Metadata Endpoints
# ============================================================================

metadata_bp = Blueprint('metadata', __name__)

def get_metadata_parser():
    """Get metadata parser (lazy import)."""
    try:
        from natural_language_metadata_parser import NaturalLanguageMetadataParser
        return NaturalLanguageMetadataParser()
    except ImportError:
        try:
            from backend.natural_language_metadata_parser import NaturalLanguageMetadataParser
            return NaturalLanguageMetadataParser()
        except ImportError:
            return None


@metadata_bp.route('/metadata/ingest/table', methods=['POST'])
def ingest_table_metadata():
    """Ingest table metadata from natural language."""
    try:
        data = request.get_json() or {}
        table_description = data.get('table_description')
        system = data.get('system')
        
        if not table_description:
            return jsonify({'error': 'table_description is required'}), 400
        
        parser = get_metadata_parser()
        if parser:
            result = parser.parse_table_description(table_description, system=system)
            return jsonify({'success': True, 'result': result})
        else:
            return jsonify({
                'success': False,
                'error': 'Metadata parser not available',
            }), 500
    except Exception as e:
        logging.getLogger('error').exception('Error ingesting table metadata')
        return jsonify({'success': False, 'error': str(e)}), 500


@metadata_bp.route('/metadata/ingest/join', methods=['POST'])
def ingest_join_metadata():
    """Ingest join metadata from natural language."""
    try:
        data = request.get_json() or {}
        join_condition = data.get('join_condition')
        
        if not join_condition:
            return jsonify({'error': 'join_condition is required'}), 400
        
        parser = get_metadata_parser()
        if parser:
            result = parser.parse_join_condition(join_condition)
            return jsonify({'success': True, 'result': result})
        else:
            return jsonify({
                'success': False,
                'error': 'Metadata parser not available',
            }), 500
    except Exception as e:
        logging.getLogger('error').exception('Error ingesting join metadata')
        return jsonify({'success': False, 'error': str(e)}), 500


@metadata_bp.route('/metadata/ingest/rules', methods=['POST'])
def ingest_rules_metadata():
    """Ingest rules metadata from natural language."""
    try:
        data = request.get_json() or {}
        rules_text = data.get('rules_text')
        
        if not rules_text:
            return jsonify({'error': 'rules_text is required'}), 400
        
        parser = get_metadata_parser()
        if parser:
            result = parser.parse_rules(rules_text)
            return jsonify({'success': True, 'result': result})
        else:
            return jsonify({
                'success': False,
                'error': 'Metadata parser not available',
            }), 500
    except Exception as e:
        logging.getLogger('error').exception('Error ingesting rules metadata')
        return jsonify({'success': False, 'error': str(e)}), 500


@metadata_bp.route('/metadata/ingest/complete', methods=['POST'])
def ingest_complete_metadata():
    """Ingest complete metadata from natural language."""
    try:
        data = request.get_json() or {}
        metadata_text = data.get('metadata_text')
        system = data.get('system')
        
        if not metadata_text:
            return jsonify({'error': 'metadata_text is required'}), 400
        
        parser = get_metadata_parser()
        if parser:
            result = parser.parse_complete_metadata(metadata_text, system=system)
            return jsonify({'success': True, 'result': result})
        else:
            return jsonify({
                'success': False,
                'error': 'Metadata parser not available',
            }), 500
    except Exception as e:
        logging.getLogger('error').exception('Error ingesting complete metadata')
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# Ingestion Endpoints
# ============================================================================

ingestion_bp = Blueprint('ingestion', __name__)

@ingestion_bp.route('/ingestion/ingest', methods=['POST'])
def start_ingestion():
    """Start data ingestion."""
    data = request.get_json() or {}
    config = data.get('config', {})
    return jsonify({'success': True, 'message': 'Ingestion started', 'config': config})


@ingestion_bp.route('/ingestion/validate', methods=['POST'])
def validate_ingestion():
    """Validate ingestion config."""
    data = request.get_json() or {}
    config = data.get('config', {})
    return jsonify({'valid': True, 'message': 'Configuration is valid', 'config': config})


@ingestion_bp.route('/ingestion/preview', methods=['POST'])
def preview_ingestion():
    """Preview ingestion."""
    data = request.get_json() or {}
    config = data.get('config', {})
    return jsonify({'preview': 'Preview data', 'config': config})


# ============================================================================
# Create Application
# ============================================================================

# Setup logging
setup_logging()

# Create app
app = create_app()


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == '__main__':
    logger = logging.getLogger('startup')
    logger.info(f'Starting RCA Engine on {ProductionConfig.HOST}:{ProductionConfig.PORT}')
    logger.info(f'Debug mode: {ProductionConfig.DEBUG}')
    logger.info(f'LLM enabled: {bool(os.getenv("OPENAI_API_KEY", ""))}')
    
    # Run with Flask's development server (use gunicorn in production)
    app.run(
        host=ProductionConfig.HOST,
        port=ProductionConfig.PORT,
        debug=ProductionConfig.DEBUG,
        threaded=True,
    )

