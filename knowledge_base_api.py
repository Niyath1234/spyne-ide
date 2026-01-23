#!/usr/bin/env python3
"""
Knowledge Base Enrichment API
REST API endpoint for enriching knowledge base from documents
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
from pathlib import Path
from dotenv import load_dotenv
from knowledge_base_enricher import KnowledgeBaseEnricher
import json

load_dotenv()

app = Flask(__name__)
CORS(app)

# Initialize enricher
enricher = KnowledgeBaseEnricher()


@app.route('/api/knowledge-base/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'service': 'knowledge-base-enricher'
    })


@app.route('/api/knowledge-base/enrich', methods=['POST'])
def enrich_knowledge_base():
    """
    Enrich knowledge base from documents
    
    Expected JSON body:
    {
        "prd_path": "path/to/prd.json",  // Optional
        "ard_path": "path/to/ard.json",  // Optional
        "trd_path": "path/to/trd.json",  // Optional
        "er_diagram_path": "path/to/er_diagram.json"  // Optional
    }
    """
    try:
        payload = request.get_json()
        
        if not payload:
            return jsonify({
                'success': False,
                'error': 'JSON body required'
            }), 400
        
        # Get document paths
        prd_path = Path(payload.get('prd_path')) if payload.get('prd_path') else None
        ard_path = Path(payload.get('ard_path')) if payload.get('ard_path') else None
        trd_path = Path(payload.get('trd_path')) if payload.get('trd_path') else None
        er_path = Path(payload.get('er_diagram_path')) if payload.get('er_diagram_path') else None
        
        # Enrich knowledge base
        results = enricher.enrich_from_documents(
            prd_path=prd_path,
            ard_path=ard_path,
            trd_path=trd_path,
            er_diagram_path=er_path
        )
        
        return jsonify({
            'success': True,
            'message': 'Knowledge base enriched successfully',
            **results
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/knowledge-base/joins', methods=['GET'])
def get_joins():
    """Get all join information from knowledge base"""
    try:
        joins = enricher.list_all_joins()
        
        return jsonify({
            'success': True,
            'joins': joins,
            'count': len(joins)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/knowledge-base/joins/<table1>/<table2>', methods=['GET'])
def get_join_between_tables(table1: str, table2: str):
    """Get join information between two specific tables"""
    try:
        join_info = enricher.get_join_information(table1, table2)
        
        if join_info:
            return jsonify({
                'success': True,
                'join': join_info
            })
        else:
            return jsonify({
                'success': False,
                'error': f'No join information found between {table1} and {table2}'
            }), 404
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/knowledge-base/upload', methods=['POST'])
def upload_document():
    """
    Upload and process a document file
    
    Expected form data:
    - file: JSON file (PRD, ARD, TRD, or ER diagram)
    - document_type: "PRD", "ARD", "TRD", or "ER_DIAGRAM"
    """
    try:
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'error': 'No file provided'
            }), 400
        
        file = request.files['file']
        document_type = request.form.get('document_type', '').upper()
        
        if not document_type:
            return jsonify({
                'success': False,
                'error': 'document_type is required (PRD, ARD, TRD, or ER_DIAGRAM)'
            }), 400
        
        # Save uploaded file temporarily
        upload_dir = Path('data/document_uploads')
        upload_dir.mkdir(parents=True, exist_ok=True)
        
        filename = file.filename
        file_path = upload_dir / filename
        file.save(str(file_path))
        
        # Process based on document type
        if document_type == 'PRD':
            results = enricher.enrich_from_documents(prd_path=file_path)
        elif document_type == 'ARD':
            results = enricher.enrich_from_documents(ard_path=file_path)
        elif document_type == 'TRD':
            results = enricher.enrich_from_documents(trd_path=file_path)
        elif document_type == 'ER_DIAGRAM':
            results = enricher.enrich_from_documents(er_diagram_path=file_path)
        else:
            return jsonify({
                'success': False,
                'error': f'Invalid document_type: {document_type}'
            }), 400
        
        return jsonify({
            'success': True,
            'message': f'{document_type} document processed successfully',
            **results
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/knowledge-base/stats', methods=['GET'])
def get_stats():
    """Get statistics about the knowledge base"""
    try:
        kb = enricher.knowledge_base
        
        stats = {
            'terms_count': len(kb.get('terms', {})),
            'tables_count': len(kb.get('tables', {})),
            'relationships_count': len(kb.get('relationships', {})),
            'joins_count': len(kb.get('joins', {})),
            'business_rules_count': len(kb.get('business_rules', {})),
            'metrics_count': len(kb.get('metrics', {}))
        }
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    port = int(os.getenv('KNOWLEDGE_BASE_API_PORT', 8083))
    print(f"🚀 Knowledge Base Enrichment API starting on http://localhost:{port}")
    print(f"📚 Knowledge base location: {enricher.kb_path}")
    app.run(host='0.0.0.0', port=port, debug=True)

