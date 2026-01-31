"""
Drift Detection API

Endpoints for metadata drift detection and management.
Implements Section 5 from EXECUTION_PLAN.md
"""

from flask import Blueprint, request, jsonify
from typing import Dict, Any
import logging

from backend.models.table_state import UserRole
from backend.services.drift_detection import DriftDetectionEngine

logger = logging.getLogger(__name__)

drift_router = Blueprint('drift', __name__)

# Global stores (would be injected in production)
_contract_store = None
_drift_store = None


def init_drift_api(contract_store: Any, drift_store: Any):
    """Initialize drift API."""
    global _contract_store, _drift_store
    _contract_store = contract_store
    _drift_store = drift_store


def get_current_user_role() -> UserRole:
    """Get current user role from request context."""
    role_str = request.headers.get('X-User-Role', 'VIEWER')
    try:
        return UserRole(role_str.upper())
    except ValueError:
        return UserRole.VIEWER


@drift_router.route('/contracts/<contract_id>/diff', methods=['GET'])
def get_contract_diff(contract_id: str):
    """
    Get diff between contract versions.
    Implements Section 5.4 from EXECUTION_PLAN.md
    """
    if not _contract_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    from_version = request.args.get('from_version')
    to_version = request.args.get('to_version')
    
    if not from_version or not to_version:
        return jsonify({
            'success': False,
            'error': 'Missing required parameters: from_version, to_version'
        }), 400
    
    try:
        # Get schemas for both versions
        from_schema = _contract_store.get_schema_version(contract_id, from_version)
        to_schema = _contract_store.get_schema_version(contract_id, to_version)
        
        if not from_schema:
            return jsonify({
                'success': False,
                'error': f'Version {from_version} not found'
            }), 404
        
        if not to_schema:
            return jsonify({
                'success': False,
                'error': f'Version {to_version} not found'
            }), 404
        
        # Detect drift
        drift_report = DriftDetectionEngine.detect_drift(from_schema, to_schema)
        
        # Format changes for API response
        changes = []
        for change in drift_report.changes:
            change_dict = {
                'type': change.change_type.value,
                'severity': change.severity.value
            }
            
            if change.change_type.value == 'ADD_COLUMN':
                change_dict['column'] = change.column
                change_dict['data_type'] = getattr(change, 'new_type', None)
            elif change.change_type.value == 'RENAME_COLUMN':
                change_dict['old_column'] = change.old_column
                change_dict['new_column'] = change.new_column
            elif change.change_type.value == 'TYPE_CHANGE':
                change_dict['column'] = change.column
                change_dict['old_type'] = change.old_type
                change_dict['new_type'] = change.new_type
            
            changes.append(change_dict)
        
        return jsonify({
            'success': True,
            'from_version': from_version,
            'to_version': to_version,
            'changes': changes,
            'severity': drift_report.severity.value,
            'migration_guide': _generate_migration_guide(drift_report)
        })
        
    except Exception as e:
        logger.error(f"Error getting contract diff: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@drift_router.route('/contracts/<contract_id>/drift', methods=['POST'])
def detect_contract_drift(contract_id: str):
    """
    Detect drift for a contract.
    Called on schema refresh.
    """
    if not _contract_store or not _drift_store:
        return jsonify({
            'success': False,
            'error': 'Service not initialized'
        }), 503
    
    data = request.get_json() or {}
    new_schema = data.get('new_schema')
    
    if not new_schema:
        return jsonify({
            'success': False,
            'error': 'Missing required field: new_schema'
        }), 400
    
    try:
        # Get current schema
        current_schema = _contract_store.get_current_schema(contract_id)
        if not current_schema:
            return jsonify({
                'success': False,
                'error': f'Contract {contract_id} not found'
            }), 404
        
        # Detect drift
        drift_report = DriftDetectionEngine.detect_drift(current_schema, new_schema)
        
        # Store drift report
        report_id = _drift_store.store_drift_report(
            contract_id=contract_id,
            drift_report=drift_report
        )
        
        return jsonify({
            'success': True,
            'report_id': report_id,
            'drift_report': {
                'severity': drift_report.severity.value,
                'changes_count': len(drift_report.changes),
                'changes': [
                    {
                        'type': c.change_type.value,
                        'column': c.column,
                        'severity': c.severity.value
                    }
                    for c in drift_report.changes
                ]
            },
            'action_required': _get_action_required(drift_report.severity)
        })
        
    except Exception as e:
        logger.error(f"Error detecting drift: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def _generate_migration_guide(drift_report) -> str:
    """Generate migration guide from drift report."""
    if not drift_report.changes:
        return "No changes detected."
    
    guide_parts = [f"Migration from {drift_report.from_version} to {drift_report.to_version}:"]
    
    for change in drift_report.changes:
        if change.change_type.value == 'ADD':
            guide_parts.append(f"- Add column: {change.column}")
        elif change.change_type.value == 'REMOVE':
            guide_parts.append(f"- Remove column: {change.column} (BREAKING)")
        elif change.change_type.value == 'RENAME':
            guide_parts.append(
                f"- Rename column: {change.old_column} → {change.new_column} (BREAKING)"
            )
        elif change.change_type.value == 'TYPE_CHANGE':
            guide_parts.append(
                f"- Change type: {change.column} {change.old_type} → {change.new_type}"
            )
    
    return "\n".join(guide_parts)


def _get_action_required(severity) -> str:
    """Get action required based on severity."""
    if severity.value == 'COMPATIBLE':
        return "Auto-apply compatible changes"
    elif severity.value == 'WARNING':
        return "Review changes, allow override if needed"
    elif severity.value == 'BREAKING':
        return "Require new version, cannot auto-apply"
    return "Unknown"

