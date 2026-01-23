#!/usr/bin/env python3
"""
Pipeline API Server
Receives data from external APIs, processes through pipeline, and stores results
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
from pathlib import Path
from dotenv import load_dotenv
from pipeline_processor import PipelineProcessor, PipelineStorage
import json

load_dotenv()

app = Flask(__name__)
CORS(app)

# Initialize pipeline processor
processor = PipelineProcessor(output_dir="data/pipeline_output")

# Configuration
USE_S3 = os.getenv('USE_S3', 'false').lower() == 'true'
S3_CONFIG = {
    'bucket_name': os.getenv('S3_BUCKET_NAME', ''),
    'region': os.getenv('S3_REGION', 'us-east-1')
} if USE_S3 else None

storage = PipelineStorage(use_s3=USE_S3, s3_config=S3_CONFIG)


@app.route('/api/pipeline/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'service': 'pipeline-api',
        'storage_type': 's3' if USE_S3 else 'local'
    })


@app.route('/api/pipeline/ingest', methods=['POST'])
def ingest_data():
    """
    Main ingestion endpoint
    Receives data from external APIs and processes through pipeline
    
    Expected JSON body:
    {
        "data": [...],  // Array of records
        "source_name": "api_source_name",
        "table_name": "optional_table_name",
        "group_by": ["column1", "column2"],  // Optional
        "metrics": {  // Optional
            "total": "sum",
            "average": "mean"
        },
        "metadata": {  // Optional
            "entity": "entity_name",
            "description": "table description"
        }
    }
    """
    try:
        payload = request.get_json()
        
        if not payload:
            return jsonify({
                'success': False,
                'error': 'JSON body required'
            }), 400
        
        # Validate required fields
        if 'data' not in payload:
            return jsonify({
                'success': False,
                'error': 'data field is required'
            }), 400
        
        if 'source_name' not in payload:
            return jsonify({
                'success': False,
                'error': 'source_name field is required'
            }), 400
        
        data = payload['data']
        if not isinstance(data, list) or len(data) == 0:
            return jsonify({
                'success': False,
                'error': 'data must be a non-empty array'
            }), 400
        
        # Extract parameters
        source_name = payload['source_name']
        table_name = payload.get('table_name')
        group_by = payload.get('group_by')
        metrics = payload.get('metrics')
        metadata = payload.get('metadata', {})
        
        # Process through pipeline
        result = processor.process_api_response(
            data=data,
            source_name=source_name,
            table_name=table_name,
            group_by=group_by,
            metrics=metrics,
            metadata=metadata
        )
        
        return jsonify({
            'success': True,
            'message': 'Data processed successfully through pipeline',
            **result
        })
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/pipeline/ingest/batch', methods=['POST'])
def ingest_batch():
    """
    Batch ingestion endpoint
    Processes multiple API responses in one call
    
    Expected JSON body:
    {
        "sources": [
            {
                "data": [...],
                "source_name": "source1",
                "table_name": "optional",
                "group_by": [...],
                "metrics": {...},
                "metadata": {...}
            },
            ...
        ]
    }
    """
    try:
        payload = request.get_json()
        
        if not payload or 'sources' not in payload:
            return jsonify({
                'success': False,
                'error': 'sources array is required'
            }), 400
        
        sources = payload['sources']
        if not isinstance(sources, list):
            return jsonify({
                'success': False,
                'error': 'sources must be an array'
            }), 400
        
        results = []
        errors = []
        
        for idx, source_config in enumerate(sources):
            try:
                if 'data' not in source_config or 'source_name' not in source_config:
                    errors.append({
                        'index': idx,
                        'error': 'data and source_name are required'
                    })
                    continue
                
                result = processor.process_api_response(
                    data=source_config['data'],
                    source_name=source_config['source_name'],
                    table_name=source_config.get('table_name'),
                    group_by=source_config.get('group_by'),
                    metrics=source_config.get('metrics'),
                    metadata=source_config.get('metadata', {})
                )
                
                results.append(result)
                
            except Exception as e:
                errors.append({
                    'index': idx,
                    'source_name': source_config.get('source_name', 'unknown'),
                    'error': str(e)
                })
        
        return jsonify({
            'success': len(errors) == 0,
            'processed': len(results),
            'failed': len(errors),
            'results': results,
            'errors': errors
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/pipeline/tables', methods=['GET'])
def list_tables():
    """List all tables created by the pipeline"""
    try:
        metadata_dir = Path("metadata/pipeline_metadata")
        
        if not metadata_dir.exists():
            return jsonify({
                'success': True,
                'tables': [],
                'count': 0
            })
        
        tables = []
        for metadata_file in metadata_dir.glob("*_metadata.json"):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    tables.append({
                        'table_name': metadata.get('table_name'),
                        'source_name': metadata.get('source_metadata', {}).get('source_name'),
                        'created_at': metadata.get('created_at'),
                        'row_count': metadata.get('schema', {}).get('row_count', 0),
                        'columns': [col['name'] for col in metadata.get('schema', {}).get('columns', [])],
                        'csv_path': metadata.get('storage', {}).get('path')
                    })
            except Exception as e:
                continue
        
        # Sort by created_at descending
        tables.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return jsonify({
            'success': True,
            'tables': tables,
            'count': len(tables)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/pipeline/tables/<table_name>', methods=['GET'])
def get_table_metadata(table_name):
    """Get metadata for a specific table"""
    try:
        metadata_file = Path("metadata/pipeline_metadata") / f"{table_name}_metadata.json"
        
        if not metadata_file.exists():
            return jsonify({
                'success': False,
                'error': f'Table {table_name} not found'
            }), 404
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        return jsonify({
            'success': True,
            'metadata': metadata
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/pipeline/aggregate', methods=['POST'])
def aggregate_existing_table():
    """
    Aggregate an existing table with new grouping/metrics
    
    Expected JSON body:
    {
        "table_name": "existing_table",
        "group_by": ["column1", "column2"],
        "metrics": {
            "total": "sum",
            "average": "mean"
        },
        "output_table_name": "aggregated_table"  // Optional
    }
    """
    try:
        payload = request.get_json()
        
        if not payload:
            return jsonify({
                'success': False,
                'error': 'JSON body required'
            }), 400
        
        table_name = payload.get('table_name')
        if not table_name:
            return jsonify({
                'success': False,
                'error': 'table_name is required'
            }), 400
        
        # Load existing table
        metadata_file = Path("metadata/pipeline_metadata") / f"{table_name}_metadata.json"
        if not metadata_file.exists():
            return jsonify({
                'success': False,
                'error': f'Table {table_name} not found'
            }), 404
        
        with open(metadata_file, 'r') as f:
            existing_metadata = json.load(f)
        
        csv_path = Path(existing_metadata['storage']['path'])
        if not csv_path.exists():
            return jsonify({
                'success': False,
                'error': f'CSV file not found: {csv_path}'
            }), 404
        
        # Load DataFrame
        import pandas as pd
        df = pd.read_csv(csv_path)
        
        # Get aggregation parameters
        group_by = payload.get('group_by')
        metrics = payload.get('metrics')
        
        if not group_by or not metrics:
            return jsonify({
                'success': False,
                'error': 'group_by and metrics are required'
            }), 400
        
        # Process aggregation
        result = processor.process_api_response(
            data=df.to_dict('records'),
            source_name=existing_metadata['source_metadata']['source_name'],
            table_name=payload.get('output_table_name'),
            group_by=group_by,
            metrics=metrics,
            metadata={
                'entity': existing_metadata['source_metadata'].get('entity'),
                'parent_table': table_name,
                'aggregation_type': 'post_processing'
            }
        )
        
        return jsonify({
            'success': True,
            'message': 'Table aggregated successfully',
            **result
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    port = int(os.getenv('PIPELINE_API_PORT', 8082))
    print(f"🚀 Pipeline API Server starting on http://localhost:{port}")
    print(f"📁 Output directory: {Path('data/pipeline_output').absolute()}")
    print(f"💾 Storage type: {'S3' if USE_S3 else 'Local CSV'}")
    app.run(host='0.0.0.0', port=port, debug=True)

