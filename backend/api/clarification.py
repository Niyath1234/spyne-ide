"""
Clarification API Endpoints

Handles clarification questions and user responses.
"""

from flask import Blueprint, request, jsonify
import logging
from typing import Dict, Any

from backend.planning.clarification_agent import ClarificationAgent
from backend.planning.clarification_resolver import ClarificationResolver
from backend.metadata_provider import MetadataProvider
from backend.query_regeneration_api import generate_sql_from_query

logger = logging.getLogger(__name__)

# Import metrics if available
try:
    from backend.planning.clarification_metrics import (
        get_clarification_metrics,
        log_clarification_event
    )
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False

clarification_bp = Blueprint('clarification', __name__, url_prefix='/api/clarification')


@clarification_bp.route('/analyze', methods=['POST'])
def analyze_query():
    """
    Analyze a query for clarification needs.
    
    Request:
        {
            "query": "show me customers",
            "use_llm": true
        }
    
    Response:
        {
            "needs_clarification": true,
            "questions": [...],
            "confidence": 0.6
        }
    """
    try:
        data = request.json or {}
        query = data.get('query', '').strip()
        use_llm = data.get('use_llm', True)
        
        if not query:
            return jsonify({
                'success': False,
                'error': 'Query is required'
            }), 400
        
        # Load metadata
        metadata = MetadataProvider.load()
        
        # Get LLM provider if available
        llm_provider = None
        if use_llm:
            try:
                from backend.implementations.llm_provider import OpenAIProvider
                llm_provider = OpenAIProvider()
            except Exception as e:
                logger.warning(f"LLM provider not available: {e}")
        
        # Create clarification agent
        clarification_agent = ClarificationAgent(
            llm_provider=llm_provider,
            metadata=metadata
        )
        
        # Analyze query
        result = clarification_agent.analyze_query(query, metadata=metadata)
        
        # Log metrics
        if METRICS_AVAILABLE:
            log_clarification_event(
                'clarification_analyze_api',
                query=query[:100],  # Truncate for logging
                needs_clarification=result.needs_clarification,
                questions_count=len(result.questions) if result.needs_clarification else 0
            )
        
        response = {
            'success': True,
            'needs_clarification': result.needs_clarification,
            'confidence': result.confidence,
            'query': query
        }
        
        if result.needs_clarification:
            response['questions'] = [q.to_dict() for q in result.questions]
            response['suggested_intent'] = result.suggested_intent
        else:
            response['message'] = 'Query is clear, no clarification needed'
            response['suggested_intent'] = result.suggested_intent
        
        return jsonify(response)
        
    except Exception as e:
        logger.exception('Error in clarification analyze')
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@clarification_bp.route('/resolve', methods=['POST'])
def resolve_clarification():
    """
    Resolve a clarified query by merging user answers.
    
    Request:
        {
            "query": "show me customers",
            "original_intent": {...},  // Optional, from previous clarification
            "answers": {
                "metric": "revenue",
                "time_range": "last 30 days",
                "dimensions": ["region"]
            },
            "use_llm": true
        }
    
    Response:
        {
            "success": true,
            "resolved_intent": {...},
            "sql": "...",
            "clarified_query": "..."
        }
    """
    try:
        data = request.json or {}
        query = data.get('query', '').strip()
        original_intent = data.get('original_intent')
        answers = data.get('answers', {})
        use_llm = data.get('use_llm', True)
        
        if not query:
            return jsonify({
                'success': False,
                'error': 'Query is required'
            }), 400
        
        if not answers:
            return jsonify({
                'success': False,
                'error': 'Answers are required'
            }), 400
        
        # Load metadata
        metadata = MetadataProvider.load()
        
        # Create resolver
        resolver = ClarificationResolver(metadata=metadata)
        
        # Resolve clarified query
        resolved = resolver.resolve_clarified_query(
            original_query=query,
            original_intent=original_intent,
            answers=answers,
            metadata=metadata
        )
        
        # Generate SQL from resolved intent
        # We'll use the resolved intent to generate SQL
        # For now, regenerate from clarified query text
        # NOTE: Future enhancement - could directly use resolved_intent to build SQL
        # for better performance and accuracy
        
        # Build a clarified query string that includes the answers
        clarified_query = resolver._build_clarified_query_text(query, answers)
        
        # Generate SQL
        sql_result = generate_sql_from_query(clarified_query, use_llm=use_llm, 
                                            clarification_mode=False)  # Don't ask again
        
        response = {
            'success': sql_result.get('success', False),
            'resolved_intent': resolved['resolved_intent'],
            'clarified_query': resolved['clarified_query'],
            'answers': answers
        }
        
        if sql_result.get('success'):
            response['sql'] = sql_result.get('sql')
            response['intent'] = sql_result.get('intent')
            response['reasoning_steps'] = sql_result.get('reasoning_steps', [])
            if sql_result.get('explain_plan'):
                response['explain_plan'] = sql_result.get('explain_plan')
            
            # Log successful resolution
            if METRICS_AVAILABLE:
                log_clarification_event('clarification_resolved', success=True)
        else:
            response['error'] = sql_result.get('error')
            response['warnings'] = sql_result.get('warnings')
            
            # Log failed resolution
            if METRICS_AVAILABLE:
                log_clarification_event('clarification_resolved', success=False, 
                                      error=sql_result.get('error'))
        
        return jsonify(response)
        
    except Exception as e:
        logger.exception('Error in clarification resolve')
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@clarification_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        metadata = MetadataProvider.load()
        response = {
            'status': 'healthy',
            'metadata_loaded': bool(metadata),
            'tables_count': len(metadata.get('tables', {}).get('tables', [])) if metadata else 0
        }
        
        # Add metrics if available
        if METRICS_AVAILABLE:
            metrics = get_clarification_metrics()
            response['metrics'] = metrics.get_stats()
        
        return jsonify(response)
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@clarification_bp.route('/metrics', methods=['GET'])
def get_metrics():
    """Get clarification metrics."""
    if not METRICS_AVAILABLE:
        return jsonify({
            'error': 'Metrics not available'
        }), 503
    
    metrics = get_clarification_metrics()
    return jsonify(metrics.get_stats())

