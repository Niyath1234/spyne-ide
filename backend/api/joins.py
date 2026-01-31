"""
Join Candidates API

Endpoints for join candidate suggestions and acceptance.
Implements Section 4 and 11.3 from EXECUTION_PLAN.md
"""

from flask import Blueprint, request, jsonify
from typing import Dict, Any, Optional, List
import logging

from backend.models.table_state import UserRole, RolePermissions

logger = logging.getLogger(__name__)

joins_router = Blueprint('joins', __name__)

# Global stores (would be injected in production)
_join_store = None


def init_joins_api(join_store: Any):
    """Initialize joins API with join store."""
    global _join_store
    _join_store = join_store


def get_current_user_role() -> UserRole:
    """Get current user role from request context."""
    role_str = request.headers.get('X-User-Role', 'VIEWER')
    try:
        return UserRole(role_str.upper())
    except ValueError:
        return UserRole.VIEWER


def get_current_user_email() -> str:
    """Get current user email from request context."""
    return request.headers.get('X-User-Email', 'unknown@example.com')


@joins_router.route('/joins/candidates', methods=['GET'])
def get_join_candidates():
    """
    Get join candidates for tables.
    Returns suggestions, not active joins.
    """
    if not _join_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    table1 = request.args.get('table1')
    table2 = request.args.get('table2')
    
    try:
        if table1 and table2:
            # Get candidates for specific pair
            candidates = _join_store.get_candidates_for_pair(table1, table2)
        else:
            # Get all candidates
            candidates = _join_store.get_all_candidates()
        
        return jsonify({
            'success': True,
            'candidates': candidates,
            'count': len(candidates)
        })
        
    except Exception as e:
        logger.error(f"Error getting join candidates: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@joins_router.route('/joins/accept', methods=['POST'])
def accept_join():
    """
    Accept a join candidate.
    Requires ADMIN role.
    Creates an active join after validation.
    """
    if not _join_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    role = get_current_user_role()
    user_email = get_current_user_email()
    
    # Check permission
    if role != UserRole.ADMIN:
        return jsonify({
            'success': False,
            'error': 'Join acceptance requires ADMIN role'
        }), 403
    
    data = request.get_json()
    if not data:
        return jsonify({
            'success': False,
            'error': 'Request body required'
        }), 400
    
    candidate_id = data.get('candidate_id')
    owner = data.get('owner', user_email)
    rationale = data.get('rationale', '')
    version = data.get('version', 'v1')
    
    if not candidate_id:
        return jsonify({
            'success': False,
            'error': 'Missing required field: candidate_id'
        }), 400
    
    try:
        # Validate join before acceptance
        validation_result = _join_store.validate_join_candidate(candidate_id)
        
        if not validation_result.get('valid', False):
            return jsonify({
                'success': False,
                'error': 'Join validation failed',
                'validation_errors': validation_result.get('errors', [])
            }), 400
        
        # Accept join
        accepted_join = _join_store.accept_join(
            candidate_id=candidate_id,
            owner=owner,
            rationale=rationale,
            version=version
        )
        
        return jsonify({
            'success': True,
            'message': 'Join accepted successfully',
            'join': accepted_join
        }), 201
        
    except Exception as e:
        logger.error(f"Error accepting join: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@joins_router.route('/joins/<join_id>', methods=['GET'])
def get_join(join_id: str):
    """Get accepted join details."""
    if not _join_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    try:
        join = _join_store.get_join(join_id)
        if not join:
            return jsonify({
                'success': False,
                'error': f'Join {join_id} not found'
            }), 404
        
        return jsonify({
            'success': True,
            'join': join
        })
        
    except Exception as e:
        logger.error(f"Error getting join: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@joins_router.route('/joins/<join_id>/deprecate', methods=['POST'])
def deprecate_join(join_id: str):
    """
    Deprecate an accepted join.
    Requires ADMIN role.
    """
    if not _join_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    role = get_current_user_role()
    user_email = get_current_user_email()
    
    # Check permission
    if role != UserRole.ADMIN:
        return jsonify({
            'success': False,
            'error': 'Join deprecation requires ADMIN role'
        }), 403
    
    try:
        result = _join_store.deprecate_join(
            join_id=join_id,
            changed_by=user_email
        )
        
        return jsonify({
            'success': True,
            'message': f'Join {join_id} deprecated',
            'join': result
        })
        
    except Exception as e:
        logger.error(f"Error deprecating join: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

