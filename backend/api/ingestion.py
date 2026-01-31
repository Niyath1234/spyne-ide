"""
Ingestion API

Endpoints for contract registration with ingestion semantics.
Implements Section 3.1 and 11.2 from EXECUTION_PLAN.md
"""

from flask import Blueprint, request, jsonify
from typing import Dict, Any, Optional
import logging

from backend.models.table_state import UserRole, RolePermissions, SystemMode

logger = logging.getLogger(__name__)

ingestion_router = Blueprint('ingestion', __name__)

# Global stores (would be injected in production)
_contract_store = None
_system_config = None


def init_ingestion_api(contract_store: Any, system_config: Any):
    """Initialize ingestion API with stores."""
    global _contract_store, _system_config
    _contract_store = contract_store
    _system_config = system_config


def get_current_user_role() -> UserRole:
    """Get current user role from request context."""
    role_str = request.headers.get('X-User-Role', 'VIEWER')
    try:
        return UserRole(role_str.upper())
    except ValueError:
        return UserRole.VIEWER


def get_system_mode() -> SystemMode:
    """Get current system mode."""
    if _system_config:
        mode_str = _system_config.get('system_mode', 'READ_ONLY')
        try:
            return SystemMode(mode_str)
        except ValueError:
            return SystemMode.READ_ONLY
    return SystemMode.READ_ONLY


def validate_ingestion_semantics(semantics: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate ingestion semantics.
    
    Required fields:
    - mode: 'append' | 'upsert'
    - idempotency_key: array of column names
    - event_time_column: column name
    - processing_time_column: column name
    - dedupe_window: interval string (e.g., '24h')
    - conflict_resolution: 'latest_wins' | 'error'
    
    Returns:
        (is_valid, error_message)
    """
    required_fields = [
        'mode',
        'idempotency_key',
        'event_time_column',
        'processing_time_column',
        'dedupe_window',
        'conflict_resolution'
    ]
    
    for field in required_fields:
        if field not in semantics:
            return False, f"Missing required field: {field}"
    
    # Validate mode
    if semantics['mode'] not in ['append', 'upsert']:
        return False, f"Invalid mode: {semantics['mode']}. Must be 'append' or 'upsert'"
    
    # Validate idempotency_key is array
    if not isinstance(semantics['idempotency_key'], list):
        return False, "idempotency_key must be an array"
    
    if len(semantics['idempotency_key']) == 0:
        return False, "idempotency_key cannot be empty"
    
    # Validate conflict_resolution
    if semantics['conflict_resolution'] not in ['latest_wins', 'error']:
        return False, f"Invalid conflict_resolution: {semantics['conflict_resolution']}"
    
    return True, None


@ingestion_router.route('/contracts/register', methods=['POST'])
def register_contract():
    """
    Register contract with ingestion semantics.
    Requires ENGINEER or ADMIN role.
    System must be in INGESTION_READY mode.
    """
    if not _contract_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    role = get_current_user_role()
    system_mode = get_system_mode()
    
    # Check permission
    try:
        RolePermissions.require(role, 'can_create_contracts')
    except PermissionError as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 403
    
    # Check system mode
    if system_mode != SystemMode.INGESTION_READY:
        return jsonify({
            'success': False,
            'error': f'System is in {system_mode} mode. Ingestion requires INGESTION_READY mode.'
        }), 403
    
    data = request.get_json()
    if not data:
        return jsonify({
            'success': False,
            'error': 'Request body required'
        }), 400
    
    endpoint = data.get('endpoint')
    table_name = data.get('table_name')
    ingestion_semantics = data.get('ingestion_semantics')
    
    if not endpoint or not table_name or not ingestion_semantics:
        return jsonify({
            'success': False,
            'error': 'Missing required fields: endpoint, table_name, ingestion_semantics'
        }), 400
    
    # Validate ingestion semantics
    is_valid, error_msg = validate_ingestion_semantics(ingestion_semantics)
    if not is_valid:
        return jsonify({
            'success': False,
            'error': f'Invalid ingestion_semantics: {error_msg}'
        }), 400
    
    try:
        # Register contract
        contract = _contract_store.register_contract(
            endpoint=endpoint,
            table_name=table_name,
            ingestion_semantics=ingestion_semantics,
            owner=request.headers.get('X-User-Email', 'unknown@example.com')
        )
        
        return jsonify({
            'success': True,
            'message': 'Contract registered successfully',
            'contract': contract
        }), 201
        
    except Exception as e:
        logger.error(f"Error registering contract: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@ingestion_router.route('/ingestion/replay', methods=['POST'])
def replay_ingestion():
    """
    Replay ingestion for a time range.
    Requires ADMIN role.
    Implements Section 3.3 from EXECUTION_PLAN.md
    """
    if not _contract_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    role = get_current_user_role()
    
    # Check permission (replay requires admin)
    if role != UserRole.ADMIN:
        return jsonify({
            'success': False,
            'error': 'Replay requires ADMIN role'
        }), 403
    
    data = request.get_json() or {}
    contract_id = data.get('contract_id')
    time_range = data.get('time_range', {})
    dedupe_strategy = data.get('dedupe_strategy', 'idempotency_key')
    dry_run = data.get('dry_run', True)  # Default to dry-run for safety
    preview_rows = data.get('preview_rows', 100)
    
    if not contract_id:
        return jsonify({
            'success': False,
            'error': 'Missing required field: contract_id'
        }), 400
    
    if not time_range.get('start') or not time_range.get('end'):
        return jsonify({
            'success': False,
            'error': 'Missing required fields: time_range.start, time_range.end'
        }), 400
    
    try:
        # Execute replay (or dry-run)
        result = _contract_store.replay_ingestion(
            contract_id=contract_id,
            time_range=time_range,
            dedupe_strategy=dedupe_strategy,
            dry_run=dry_run,
            preview_rows=preview_rows
        )
        
        return jsonify({
            'success': True,
            'dry_run': dry_run,
            'result': result
        })
        
    except Exception as e:
        logger.error(f"Error replaying ingestion: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@ingestion_router.route('/ingestion/backfill', methods=['POST'])
def backfill_ingestion():
    """
    Backfill ingestion from archive.
    Requires ADMIN role.
    Implements Section 3.3 from EXECUTION_PLAN.md
    """
    if not _contract_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    role = get_current_user_role()
    
    # Check permission (backfill requires admin)
    if role != UserRole.ADMIN:
        return jsonify({
            'success': False,
            'error': 'Backfill requires ADMIN role'
        }), 403
    
    data = request.get_json() or {}
    contract_id = data.get('contract_id')
    source = data.get('source')
    time_range = data.get('time_range', {})
    batch_size = data.get('batch_size', 1000)
    dedupe_strategy = data.get('dedupe_strategy', 'idempotency_key')
    dry_run = data.get('dry_run', True)  # Default to dry-run for safety
    
    if not contract_id:
        return jsonify({
            'success': False,
            'error': 'Missing required field: contract_id'
        }), 400
    
    if not time_range.get('start') or not time_range.get('end'):
        return jsonify({
            'success': False,
            'error': 'Missing required fields: time_range.start, time_range.end'
        }), 400
    
    try:
        # Execute backfill (or dry-run)
        result = _contract_store.backfill_ingestion(
            contract_id=contract_id,
            source=source,
            time_range=time_range,
            batch_size=batch_size,
            dedupe_strategy=dedupe_strategy,
            dry_run=dry_run
        )
        
        return jsonify({
            'success': True,
            'dry_run': dry_run,
            'result': result
        })
        
    except Exception as e:
        logger.error(f"Error backfilling ingestion: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

