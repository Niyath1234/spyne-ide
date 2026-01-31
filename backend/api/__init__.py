"""
API Package

All API blueprints for the Spyne IDE application.
"""

from flask import Blueprint

# Import all API routers
from .query import query_router
from .health import health_router
from .metrics import metrics_router
from .clarification import clarification_router
from .table_state import table_state_router
from .ingestion import ingestion_router
from .joins import joins_router
from .query_preview import query_preview_router
from .drift import drift_router

# Create main API blueprint
api_router = Blueprint('api', __name__, url_prefix='/api')

# Register all routers
api_router.register_blueprint(query_router, url_prefix='')
api_router.register_blueprint(health_router, url_prefix='/v1')
api_router.register_blueprint(metrics_router, url_prefix='/v1')
api_router.register_blueprint(clarification_router, url_prefix='')
api_router.register_blueprint(table_state_router, url_prefix='')
api_router.register_blueprint(ingestion_router, url_prefix='')
api_router.register_blueprint(joins_router, url_prefix='')
api_router.register_blueprint(query_preview_router, url_prefix='')
api_router.register_blueprint(drift_router, url_prefix='')

__all__ = [
    'api_router',
    'query_router',
    'health_router',
    'metrics_router',
    'clarification_router',
    'table_state_router',
    'ingestion_router',
    'joins_router',
    'query_preview_router',
    'drift_router',
]
