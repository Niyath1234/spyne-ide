#!/usr/bin/env python3
"""
Flask Upload API Server
Handles CSV and Excel file uploads with schema inference
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv
from upload_handler import UploadHandler
from werkzeug.utils import secure_filename

load_dotenv()

app = Flask(__name__)
CORS(app)

# Configuration
UPLOAD_FOLDER = Path('data/uploads')
UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

def get_db_connection():
    """Get PostgreSQL connection"""
    database_url = os.getenv('DATABASE_URL', 'postgresql://niyathnair@localhost:5432/rca_engine')
    if database_url.startswith('postgresql://'):
        conn_string = database_url.replace('postgresql://', '')
        if '@' in conn_string:
            user_part, rest = conn_string.split('@', 1)
            if '/' in rest:
                host_port, dbname = rest.split('/', 1)
                if ':' in host_port:
                    host, port = host_port.split(':')
                else:
                    host, port = host_port, '5432'
            else:
                host, port, dbname = 'localhost', '5432', 'rca_engine'
        else:
            host, port, dbname = 'localhost', '5432', 'rca_engine'
            user_part = 'niyathnair'
        
        conn_string = f"host={host} port={port} dbname={dbname} user={user_part}"
    else:
        conn_string = database_url
    
    return psycopg2.connect(conn_string)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'upload-api'})

@app.route('/api/upload/csv', methods=['POST'])
def upload_csv():
    """Handle CSV file upload"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'Only CSV files allowed'}), 400
    
    try:
        # Save file
        filename = secure_filename(file.filename)
        filepath = UPLOAD_FOLDER / filename
        file.save(str(filepath))
        
        # Process upload
        conn = get_db_connection()
        handler = UploadHandler(conn, str(UPLOAD_FOLDER))
        
        uploaded_by = request.form.get('uploaded_by', 'api_user')
        result = handler.handle_csv_upload(filepath, filename, uploaded_by)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'CSV uploaded and processed successfully',
            **result
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/upload/excel', methods=['POST'])
def upload_excel():
    """Handle Excel file upload"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'Only Excel files (.xlsx, .xls) allowed'}), 400
    
    try:
        # Save file
        filename = secure_filename(file.filename)
        filepath = UPLOAD_FOLDER / filename
        file.save(str(filepath))
        
        # Get sheet name if provided
        sheet_name = request.form.get('sheet_name')
        
        # Process upload
        conn = get_db_connection()
        handler = UploadHandler(conn, str(UPLOAD_FOLDER))
        
        uploaded_by = request.form.get('uploaded_by', 'api_user')
        result = handler.handle_excel_upload(filepath, filename, sheet_name, uploaded_by)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Excel uploaded and processed successfully',
            **result
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/uploads', methods=['GET'])
def get_uploads():
    """Get upload history"""
    try:
        conn = get_db_connection()
        handler = UploadHandler(conn)
        
        limit = request.args.get('limit', 50, type=int)
        history = handler.get_upload_history(limit)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'uploads': history,
            'count': len(history)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/datasets/<table_name>/versions', methods=['GET'])
def get_dataset_versions(table_name):
    """Get version history for a dataset"""
    try:
        conn = get_db_connection()
        handler = UploadHandler(conn)
        
        versions = handler.get_dataset_versions(table_name)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'table_name': table_name,
            'versions': versions,
            'count': len(versions)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/datasets', methods=['GET'])
def get_datasets():
    """Get all uploaded datasets"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT t.name, t.system, t.path, t.last_uploaded_at,
                   dv.version_number, dv.row_count, dv.created_at
            FROM tables t
            LEFT JOIN dataset_versions dv ON t.current_version_id = dv.id
            WHERE t.upload_enabled = TRUE
            ORDER BY t.last_uploaded_at DESC
        """)
        
        results = []
        for row in cur.fetchall():
            results.append({
                'name': row[0],
                'system': row[1],
                'path': row[2],
                'last_uploaded': row[3].isoformat() if row[3] else None,
                'current_version': row[4],
                'row_count': row[5],
                'version_created': row[6].isoformat() if row[6] else None
            })
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'datasets': results,
            'count': len(results)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.getenv('UPLOAD_API_PORT', 8081))
    print(f"🚀 Upload API Server starting on http://localhost:{port}")
    print(f"📁 Upload directory: {UPLOAD_FOLDER.absolute()}")
    print(f"📝 Supported formats: CSV, Excel (.xlsx, .xls)")
    app.run(host='0.0.0.0', port=port, debug=True)

