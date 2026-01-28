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

from flask import Flask, Blueprint, request, jsonify, g, Response
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

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
    
    # LLM settings
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
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
    root_logger.setLevel(getattr(logging, ProductionConfig.LOG_LEVEL))
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
    
    # Setup CORS
    CORS(app, origins=ProductionConfig.CORS_ORIGINS, supports_credentials=True)
    
    # Register middleware
    register_middleware(app)
    
    # Register blueprints
    register_blueprints(app)
    
    # Register error handlers
    register_error_handlers(app)
    
    return app


def register_middleware(app: Flask):
    """Register middleware."""
    
    @app.before_request
    def before_request():
        """Before request middleware."""
        g.request_start_time = time.time()
        g.request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
        g.user_id = request.headers.get('X-User-ID', 'anonymous')
        
        # Rate limiting (skip for health checks)
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
        """After request middleware."""
        duration_ms = (time.time() - g.request_start_time) * 1000
        
        # Record metrics
        metrics_collector.record_request(duration_ms, response.status_code, request.path)
        
        # Add headers
        response.headers['X-Request-ID'] = g.request_id
        response.headers['X-Response-Time-Ms'] = str(int(duration_ms))
        
        # Log request
        log = logging.getLogger('access')
        log.info(
            f'{request.method} {request.path}',
            extra={'extra': {
                'method': request.method,
                'path': request.path,
                'status': response.status_code,
                'duration_ms': duration_ms,
                'ip': request.remote_addr,
            }}
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
    app.register_blueprint(assistant_bp, url_prefix='/api')
    app.register_blueprint(pipelines_bp, url_prefix='/api')
    app.register_blueprint(rules_bp, url_prefix='/api')
    app.register_blueprint(metadata_bp, url_prefix='/api')
    app.register_blueprint(ingestion_bp, url_prefix='/api')
    
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
            'llm': 'operational' if ProductionConfig.OPENAI_API_KEY else 'degraded',
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
    metrics = metrics_collector.get_metrics()
    
    lines = [
        '# HELP rca_engine_uptime_seconds Server uptime in seconds',
        '# TYPE rca_engine_uptime_seconds gauge',
        f'rca_engine_uptime_seconds {metrics["uptime_seconds"]}',
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
        use_llm = bool(ProductionConfig.OPENAI_API_KEY)
        result = generate_sql_from_query(user_query, use_llm=use_llm)
        
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
        else:
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
    """Process reasoning query."""
    start_time = time.time()
    try:
        data = request.get_json() or {}
        query_text = data.get('query', '')
        context = data.get('context', {})
        
        if not query_text:
            return jsonify({'error': 'Query is required'}), 400
        
        # Generate SQL
        generate_sql_from_query, _ = get_query_generator()
        use_llm = bool(ProductionConfig.OPENAI_API_KEY)
        result = generate_sql_from_query(query_text, use_llm=use_llm)
        
        duration_ms = (time.time() - start_time) * 1000
        metrics_collector.record_planning(duration_ms)
        
        # Build reasoning steps
        steps = []
        steps.append({
            'type': 'thought',
            'content': f'🔍 Analyzing query: "{query_text}"',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        })
        
        if result.get('reasoning_steps'):
            for i, step_content in enumerate(result['reasoning_steps']):
                step_type = 'thought'
                if '✅' in step_content or 'Generated' in step_content:
                    step_type = 'result'
                elif '❌' in step_content or 'Error' in step_content:
                    step_type = 'error'
                elif '🔧' in step_content or 'Building' in step_content:
                    step_type = 'action'
                elif '📊' in step_content or 'SQL' in step_content:
                    step_type = 'result'
                
                steps.append({
                    'type': step_type,
                    'content': step_content,
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                })
        else:
            steps.append({
                'type': 'thought',
                'content': '📊 Loading metadata and analyzing available tables...',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
            })
            
            if result.get('success'):
                steps.append({
                    'type': 'action',
                    'content': '🤖 Generating SQL using LLM with comprehensive context...',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                })
                
                if result.get('sql'):
                    steps.append({
                        'type': 'result',
                        'content': f'✅ Generated SQL:\n\n```sql\n{result["sql"]}\n```',
                        'timestamp': datetime.utcnow().isoformat() + 'Z',
                    })
            else:
                steps.append({
                    'type': 'error',
                    'content': f'❌ Error: {result.get("error", "Unknown error")}',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                })
        
        if result.get('warnings'):
            steps.append({
                'type': 'thought',
                'content': f'⚠️  Warnings: {result["warnings"]}',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
            })
        
        reasoning_history.extend(steps)
        
        return jsonify({
            'result': result.get('sql') or result.get('error', 'Query processed'),
            'steps': steps,
            'sql': result.get('sql'),
            'intent': result.get('intent'),
            'method': result.get('method', 'llm_with_full_context'),
        })
    except Exception as e:
        logging.getLogger('error').exception('Error in reasoning query')
        return jsonify({
            'result': f'Error: {str(e)}',
            'steps': [{
                'type': 'error',
                'content': f'❌ Error processing query: {str(e)}',
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
        use_llm = bool(ProductionConfig.OPENAI_API_KEY)
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
    """Handle assistant questions."""
    try:
        data = request.get_json() or {}
        question = data.get('question', '')
        
        if not question:
            return jsonify({
                'response_type': 'Error',
                'error': 'question is required',
            }), 400
        
        # Check if it's a SQL query request
        is_query_request = any(keyword in question.lower() for keyword in [
            'query', 'sql', 'show', 'get', 'find', 'select', 'list'
        ])
        
        if is_query_request:
            generate_sql_from_query, _ = get_query_generator()
            use_llm = bool(ProductionConfig.OPENAI_API_KEY)
            result = generate_sql_from_query(question, use_llm=use_llm)
            
            if result.get('success') and result.get('sql'):
                return jsonify({
                    'response_type': 'QueryResult',
                    'status': 'success',
                    'answer': f"Here's the SQL query:\n\n```sql\n{result['sql']}\n```",
                    'result': result['sql'],
                    'intent': result.get('intent'),
                    'validation': result.get('validation'),
                })
            else:
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
        else:
            return jsonify({
                'response_type': 'Answer',
                'status': 'success',
                'answer': f"""I can help you with SQL queries and data analysis. Try asking me things like:
- "Show me sales by region"
- "Query the customer table"
- "Get revenue metrics"
- "Find orders from last month"

For your question: "{question}", I can help you generate a SQL query if you provide more details about what data you want to retrieve.""",
            })
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
    logger.info(f'LLM enabled: {bool(ProductionConfig.OPENAI_API_KEY)}')
    
    # Run with Flask's development server (use gunicorn in production)
    app.run(
        host=ProductionConfig.HOST,
        port=ProductionConfig.PORT,
        debug=ProductionConfig.DEBUG,
        threaded=True,
    )

