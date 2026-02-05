#!/usr/bin/env python3
"""
API endpoints for Knowledge Register and Metadata Register.
"""

from flask import Blueprint, jsonify
from backend.metadata_provider import MetadataProvider
import logging

logger = logging.getLogger(__name__)

knowledge_metadata_bp = Blueprint('knowledge_metadata', __name__)


@knowledge_metadata_bp.route('/knowledge/entries', methods=['GET'])
def get_knowledge_entries():
    """Get knowledge register entries from knowledge base."""
    try:
        metadata = MetadataProvider.load()
        knowledge_base = metadata.get('knowledge_base', {})
        
        # Convert knowledge base tables to entries format
        entries = []
        tables = knowledge_base.get('tables', {})
        relationships = knowledge_base.get('relationships', {})
        
        # Add table entries
        for table_name, table_info in tables.items():
            entries.append({
                'id': f'table_{table_name}',
                'title': table_name.split('.')[-1] if '.' in table_name else table_name,
                'content': table_info.get('description', ''),
                'type': 'table',
                'tags': [table_info.get('system', ''), table_info.get('entity', '')],
                'created_at': None,
                'updated_at': None,
            })
        
        # Add relationship entries
        for rel_name, rel_info in relationships.items():
            entries.append({
                'id': f'rel_{rel_name}',
                'title': rel_info.get('description', rel_name),
                'content': f"{rel_info.get('from', '')} -> {rel_info.get('to', '')} ({rel_info.get('type', '')})",
                'type': 'relationship',
                'tags': ['relationship', rel_info.get('type', '')],
                'created_at': None,
                'updated_at': None,
            })
        
        return jsonify(entries)
    except Exception as e:
        logger.error(f'Error loading knowledge entries: {e}', exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@knowledge_metadata_bp.route('/metadata', methods=['GET'])
def get_metadata():
    """Get metadata register (tables and metrics)."""
    try:
        metadata = MetadataProvider.load()
        tables_data = metadata.get('tables', {}).get('tables', [])
        registry = metadata.get('semantic_registry', {})
        metrics = registry.get('metrics', [])
        
        # Format tables
        tables = []
        for table in tables_data:
            columns = table.get('columns', [])
            # Find primary key (usually first column or explicitly marked)
            primary_key = []
            for col in columns:
                if 'key' in col.get('name', '').lower() and 'primary' in col.get('description', '').lower():
                    primary_key.append(col['name'])
                elif col.get('name', '').endswith('_key') or col.get('name', '').endswith('key'):
                    if not primary_key:
                        primary_key.append(col['name'])
            
            tables.append({
                'name': table.get('name', ''),
                'system': table.get('system', ''),
                'entity': table.get('entity', ''),
                'columns': [{'name': c['name'], 'data_type': c.get('type', '')} for c in columns],
                'primary_key': primary_key if primary_key else ([columns[0]['name']] if columns else [])
            })
        
        # Format metrics
        formatted_metrics = []
        for metric in metrics:
            formatted_metrics.append({
                'id': metric.get('id', metric.get('name', '')),
                'name': metric.get('name', ''),
                'description': metric.get('description', ''),
                'dimensions': metric.get('dimensions', [])
            })
        
        return jsonify({
            'success': True,
            'tables': tables,
            'metrics': formatted_metrics
        })
    except Exception as e:
        logger.error(f'Error loading metadata: {e}', exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
