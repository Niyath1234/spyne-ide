"""
Table State Management API

Endpoints for managing table states, promotion, and deprecation.
Implements Section 11.1 from EXECUTION_PLAN.md
"""

from flask import Blueprint, request, jsonify, g
from typing import Dict, Any, Optional
import logging

from backend.models.table_state import (
    TableState,
    UserRole,
    TableStateManager,
    RolePermissions,
)

logger = logging.getLogger(__name__)

table_state_router = Blueprint('table_state', __name__)

# Global table store (would be injected in production)
_table_store = None


def init_table_state_api(table_store: Any):
    """Initialize table state API with table store."""
    global _table_store
    _table_store = table_store


def get_current_user_role() -> UserRole:
    """Get current user role from request context."""
    # In production, this would come from authentication
    # For now, check header or default to VIEWER
    role_str = request.headers.get('X-User-Role', 'VIEWER')
    try:
        return UserRole(role_str.upper())
    except ValueError:
        return UserRole.VIEWER


def get_current_user_email() -> str:
    """Get current user email from request context."""
    # In production, this would come from authentication
    return request.headers.get('X-User-Email', 'unknown@example.com')


@table_state_router.route('/tables/<table_id>/state', methods=['GET'])
def get_table_state(table_id: str):
    """Get table state."""
    if not _table_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    try:
        table = _table_store.get_table_by_id(table_id)
        if not table:
            return jsonify({
                'success': False,
                'error': f'Table {table_id} not found'
            }), 404
        
        return jsonify({
            'success': True,
            'table': {
                'id': table_id,
                'name': table.get('name'),
                'state': table.get('state'),
                'version': table.get('version'),
                'owner': table.get('owner'),
                'created_at': table.get('created_at'),
                'updated_at': table.get('updated_at'),
            }
        })
    except Exception as e:
        logger.error(f"Error getting table state: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@table_state_router.route('/tables/<table_id>/promote', methods=['POST'])
def promote_table(table_id: str):
    """
    Promote shadow table to active.
    Requires ADMIN role.
    """
    if not _table_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    role = get_current_user_role()
    user_email = get_current_user_email()
    
    # Check permission
    try:
        RolePermissions.require(role, 'can_promote')
    except PermissionError as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 403
    
    data = request.get_json() or {}
    from_state = data.get('from_state', 'SHADOW')
    to_state = data.get('to_state', 'ACTIVE')
    dry_run = data.get('dry_run', False)
    
    try:
        table = _table_store.get_table_by_id(table_id)
        if not table:
            return jsonify({
                'success': False,
                'error': f'Table {table_id} not found'
            }), 404
        
        current_state = TableState(table.get('state', 'SHADOW'))
        target_state = TableState(to_state)
        
        # Validate transition
        is_valid, error_msg = TableStateManager.validate_transition(
            current_state, target_state, role
        )
        if not is_valid:
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        if dry_run:
            return jsonify({
                'success': True,
                'dry_run': True,
                'message': f'Would promote {table_id} from {current_state} to {target_state}',
                'table': {
                    'id': table_id,
                    'current_state': current_state.value,
                    'target_state': target_state.value,
                }
            })
        
        # Perform promotion
        result = _table_store.promote_table(
            table_id=table_id,
            from_state=current_state,
            to_state=target_state,
            changed_by=user_email
        )
        
        return jsonify({
            'success': True,
            'message': f'Table {table_id} promoted from {current_state} to {target_state}',
            'table': result
        })
        
    except Exception as e:
        logger.error(f"Error promoting table: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@table_state_router.route('/tables/<table_id>/deprecate', methods=['POST'])
def deprecate_table(table_id: str):
    """
    Deprecate active table.
    Requires ADMIN role.
    """
    if not _table_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    role = get_current_user_role()
    user_email = get_current_user_email()
    
    # Check permission
    try:
        RolePermissions.require(role, 'can_deprecate')
    except PermissionError as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 403
    
    data = request.get_json() or {}
    reason = data.get('reason', 'No reason provided')
    
    try:
        table = _table_store.get_table_by_id(table_id)
        if not table:
            return jsonify({
                'success': False,
                'error': f'Table {table_id} not found'
            }), 404
        
        current_state = TableState(table.get('state', 'ACTIVE'))
        target_state = TableState.DEPRECATED
        
        # Validate transition
        is_valid, error_msg = TableStateManager.validate_transition(
            current_state, target_state, role
        )
        if not is_valid:
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        # Perform deprecation
        result = _table_store.deprecate_table(
            table_id=table_id,
            reason=reason,
            changed_by=user_email
        )
        
        return jsonify({
            'success': True,
            'message': f'Table {table_id} deprecated',
            'table': result
        })
        
    except Exception as e:
        logger.error(f"Error deprecating table: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@table_state_router.route('/tables/<table_id>/restore', methods=['POST'])
def restore_table(table_id: str):
    """
    Restore deprecated table to active.
    Requires ADMIN role.
    """
    if not _table_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    role = get_current_user_role()
    user_email = get_current_user_email()
    
    # Check permission
    try:
        RolePermissions.require(role, 'can_deprecate')  # Same permission for restore
    except PermissionError as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 403
    
    try:
        table = _table_store.get_table_by_id(table_id)
        if not table:
            return jsonify({
                'success': False,
                'error': f'Table {table_id} not found'
            }), 404
        
        current_state = TableState(table.get('state', 'DEPRECATED'))
        target_state = TableState.ACTIVE
        
        # Validate transition
        is_valid, error_msg = TableStateManager.validate_transition(
            current_state, target_state, role
        )
        if not is_valid:
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        # Perform restore
        result = _table_store.restore_table(
            table_id=table_id,
            changed_by=user_email
        )
        
        return jsonify({
            'success': True,
            'message': f'Table {table_id} restored to ACTIVE',
            'table': result
        })
        
    except Exception as e:
        logger.error(f"Error restoring table: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

