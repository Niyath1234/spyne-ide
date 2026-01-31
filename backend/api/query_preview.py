"""
Query Preview API

Endpoints for query preview and explainability.
Implements Section 6 from EXECUTION_PLAN.md
"""

from flask import Blueprint, request, jsonify
from typing import Dict, Any, Optional
import logging

from backend.models.table_state import UserRole

logger = logging.getLogger(__name__)

query_preview_router = Blueprint('query_preview', __name__)

# Global stores (would be injected in production)
_query_preview_store = None
_query_executor = None


def init_query_preview_api(query_preview_store: Any, query_executor: Any):
    """Initialize query preview API."""
    global _query_preview_store, _query_executor
    _query_preview_store = query_preview_store
    _query_executor = query_executor


def get_current_user_email() -> str:
    """Get current user email from request context."""
    return request.headers.get('X-User-Email', 'unknown@example.com')


@query_preview_router.route('/query/preview', methods=['POST'])
def preview_query():
    """
    Generate query preview before execution.
    Mandatory preview (skip_preview defaults to false).
    Implements Section 6.1 from EXECUTION_PLAN.md
    """
    if not _query_preview_store or not _query_executor:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    data = request.get_json()
    if not data or 'query' not in data:
        return jsonify({
            'success': False,
            'error': 'Missing required field: query'
        }), 400
    
    query = data['query']
    skip_preview = data.get('skip_preview', False)  # Default: false (preview required)
    skip_reason = data.get('skip_reason', '')
    
    try:
        # Generate SQL and preview
        preview_result = _query_executor.generate_preview(
            query=query,
            user_email=get_current_user_email()
        )
        
        # Store preview
        preview_id = _query_preview_store.store_preview(
            query=query,
            preview_data=preview_result,
            user_email=get_current_user_email()
        )
        
        response = {
            'success': True,
            'preview_id': preview_id,
            'preview': preview_result,
            'requires_confirmation': True  # Always requires confirmation
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error generating preview: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@query_preview_router.route('/query/execute', methods=['POST'])
def execute_query():
    """
    Execute query after preview confirmation.
    Requires preview_id from preview endpoint.
    """
    if not _query_preview_store or not _query_executor:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    data = request.get_json()
    if not data:
        return jsonify({
            'success': False,
            'error': 'Request body required'
        }), 400
    
    preview_id = data.get('preview_id')
    query = data.get('query')
    
    if not preview_id and not query:
        return jsonify({
            'success': False,
            'error': 'Either preview_id or query required'
        }), 400
    
    try:
        # Get preview if preview_id provided
        preview_data = None
        if preview_id:
            preview_data = _query_preview_store.get_preview(preview_id)
            if not preview_data:
                return jsonify({
                    'success': False,
                    'error': f'Preview {preview_id} not found'
                }), 404
        
        # Execute query with explanation
        execution_result = _query_executor.execute_with_explanation(
            query=query or preview_data['query_text'],
            preview_data=preview_data,
            user_email=get_current_user_email()
        )
        
        # Store execution
        execution_id = _query_preview_store.store_execution(
            preview_id=preview_id,
            execution_data=execution_result,
            user_email=get_current_user_email()
        )
        
        return jsonify({
            'success': True,
            'execution_id': execution_id,
            'result': execution_result.get('result'),
            'explanation': execution_result.get('explanation'),
            'execution_time_ms': execution_result.get('execution_time_ms')
        })
        
    except Exception as e:
        logger.error(f"Error executing query: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@query_preview_router.route('/query/correct', methods=['POST'])
def correct_query():
    """
    Provide corrections to a query execution.
    Implements Section 6.3 from EXECUTION_PLAN.md
    """
    if not _query_preview_store or not _query_executor:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    data = request.get_json()
    if not data:
        return jsonify({
            'success': False,
            'error': 'Request body required'
        }), 400
    
    query_id = data.get('query_id')  # execution_id
    corrections = data.get('corrections', [])
    
    if not query_id or not corrections:
        return jsonify({
            'success': False,
            'error': 'Missing required fields: query_id, corrections'
        }), 400
    
    try:
        # Get original execution
        execution = _query_preview_store.get_execution(query_id)
        if not execution:
            return jsonify({
                'success': False,
                'error': f'Query execution {query_id} not found'
            }), 404
        
        # Store corrections
        _query_preview_store.store_corrections(
            query_execution_id=query_id,
            corrections=corrections,
            user_email=get_current_user_email()
        )
        
        # Regenerate query with corrections
        corrected_result = _query_executor.regenerate_with_corrections(
            original_query=execution['query_text'],
            corrections=corrections,
            user_email=get_current_user_email()
        )
        
        return jsonify({
            'success': True,
            'message': 'Query corrected and regenerated',
            'new_preview': corrected_result
        })
        
    except Exception as e:
        logger.error(f"Error correcting query: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@query_preview_router.route('/query/validate', methods=['POST'])
def validate_query():
    """
    Validate query for guardrails.
    Implements Section 6.4 from EXECUTION_PLAN.md
    """
    if not _query_executor:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    data = request.get_json()
    if not data:
        return jsonify({
            'success': False,
            'error': 'Request body required'
        }), 400
    
    sql = data.get('sql')
    tables = data.get('tables', [])
    
    if not sql:
        return jsonify({
            'success': False,
            'error': 'Missing required field: sql'
        }), 400
    
    try:
        # Validate query against guardrails
        validation_result = _query_executor.validate_guardrails(
            sql=sql,
            tables=tables
        )
        
        return jsonify({
            'success': True,
            'valid': validation_result.get('valid', False),
            'warnings': validation_result.get('warnings', []),
            'errors': validation_result.get('errors', [])
        })
        
    except Exception as e:
        logger.error(f"Error validating query: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

