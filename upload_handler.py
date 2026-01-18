#!/usr/bin/env python3
"""
File Upload Handler for RCA Engine
Handles CSV and Excel uploads with schema inference and metadata registration
"""

import os
import uuid
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from datetime import datetime

class UploadHandler:
    def __init__(self, db_conn, upload_dir: str = "data/uploads"):
        self.conn = db_conn
        self.upload_dir = Path(upload_dir)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
    
    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def infer_column_type(self, series: pd.Series) -> str:
        """Infer data type from pandas series"""
        dtype = str(series.dtype)
        
        if dtype.startswith('int'):
            return 'integer'
        elif dtype.startswith('float'):
            return 'float'
        elif dtype == 'bool':
            return 'boolean'
        elif dtype == 'datetime64':
            return 'datetime'
        elif dtype == 'object':
            # Try to detect dates
            try:
                pd.to_datetime(series.dropna().head(100))
                return 'date'
            except:
                return 'string'
        else:
            return 'string'
    
    def analyze_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze dataframe and extract schema + statistics"""
        schema = []
        
        for idx, col in enumerate(df.columns):
            col_data = df[col]
            col_type = self.infer_column_type(col_data)
            
            # Get statistics
            null_count = int(col_data.isnull().sum())
            unique_count = int(col_data.nunique())
            
            # Sample values (non-null, unique, up to 5)
            sample_values = col_data.dropna().unique()[:5].tolist()
            # Convert to strings to ensure JSON serialization
            sample_values = [str(v) for v in sample_values]
            
            # Min/max for numeric/date columns
            min_val = None
            max_val = None
            if col_type in ['integer', 'float', 'date', 'datetime']:
                try:
                    min_val = str(col_data.min())
                    max_val = str(col_data.max())
                except:
                    pass
            
            schema.append({
                'column_name': col,
                'column_position': idx,
                'data_type': col_type,
                'nullable': null_count > 0,
                'null_count': null_count,
                'unique_values_count': unique_count,
                'sample_values': sample_values,
                'min_value': min_val,
                'max_value': max_val
            })
        
        return {
            'columns': schema,
            'row_count': len(df),
            'column_count': len(df.columns)
        }
    
    def run_quality_checks(self, df: pd.DataFrame, upload_job_id: str) -> List[Dict]:
        """Run data quality checks"""
        checks = []
        cur = self.conn.cursor()
        
        # Check 1: Duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            checks.append({
                'check_type': 'duplicate_rows',
                'status': 'warning',
                'message': f'Found {duplicate_count} duplicate rows',
                'details': {'count': int(duplicate_count)}
            })
            cur.execute("""
                INSERT INTO data_quality_checks 
                (upload_job_id, check_type, status, message, details)
                VALUES (%s, %s, %s, %s, %s)
            """, (upload_job_id, 'duplicate_rows', 'warning', 
                  f'Found {duplicate_count} duplicate rows',
                  Json({'count': int(duplicate_count)})))
        else:
            checks.append({
                'check_type': 'duplicate_rows',
                'status': 'passed',
                'message': 'No duplicate rows found'
            })
            cur.execute("""
                INSERT INTO data_quality_checks 
                (upload_job_id, check_type, status, message)
                VALUES (%s, %s, %s, %s)
            """, (upload_job_id, 'duplicate_rows', 'passed', 'No duplicate rows found'))
        
        # Check 2: Null checks for each column
        for col in df.columns:
            null_pct = (df[col].isnull().sum() / len(df)) * 100
            if null_pct > 50:
                status = 'warning'
                message = f'Column {col} has {null_pct:.1f}% null values'
                checks.append({
                    'check_type': 'null_check',
                    'column_name': col,
                    'status': status,
                    'message': message,
                    'details': {'null_percentage': null_pct}
                })
                cur.execute("""
                    INSERT INTO data_quality_checks 
                    (upload_job_id, check_type, column_name, status, message, details)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (upload_job_id, 'null_check', col, status, message,
                      Json({'null_percentage': float(null_pct)})))
        
        self.conn.commit()
        cur.close()
        return checks
    
    def handle_csv_upload(self, file_path: Path, original_filename: str, 
                         uploaded_by: Optional[str] = None) -> Dict:
        """Process CSV file upload"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        job_id = str(uuid.uuid4())
        
        try:
            # Rollback any previous failed transactions
            self.conn.rollback()
            
            # Create upload job
            file_size = file_path.stat().st_size
            
            cur.execute("""
                INSERT INTO upload_jobs 
                (id, file_name, file_type, file_size_bytes, upload_path, status, uploaded_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (job_id, original_filename, 'csv', file_size, str(file_path), 
                  'processing', uploaded_by))
            self.conn.commit()
            
            # Read and analyze CSV
            df = pd.read_csv(file_path)
            analysis = self.analyze_dataframe(df)
            
            # Calculate checksum
            checksum = self.calculate_checksum(file_path)
            
            # Update upload job with results
            cur.execute("""
                UPDATE upload_jobs 
                SET rows_count = %s, 
                    columns_count = %s,
                    inferred_schema = %s,
                    status = %s,
                    processed_at = NOW()
                WHERE id = %s
            """, (analysis['row_count'], analysis['column_count'],
                  Json(analysis['columns']), 'completed', job_id))
            self.conn.commit()
            
            # Run quality checks
            quality_checks = self.run_quality_checks(df, job_id)
            
            # Create dataset version
            table_name = original_filename.replace('.csv', '').lower()
            table_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)
            
            version_id = self.create_dataset_version(
                table_name, job_id, str(file_path), 
                analysis, checksum, uploaded_by
            )
            
            result = {
                'job_id': job_id,
                'version_id': version_id,
                'table_name': table_name,
                'status': 'completed',
                'rows': analysis['row_count'],
                'columns': analysis['column_count'],
                'schema': analysis['columns'],
                'quality_checks': quality_checks,
                'file_path': str(file_path)
            }
            
            cur.close()
            return result
            
        except Exception as e:
            # Rollback and mark job as failed
            self.conn.rollback()
            try:
                cur.execute("""
                    UPDATE upload_jobs 
                    SET status = %s, error_message = %s, processed_at = NOW()
                    WHERE id = %s
                """, ('failed', str(e), job_id))
                self.conn.commit()
            except:
                self.conn.rollback()
            cur.close()
            raise
    
    def handle_excel_upload(self, file_path: Path, original_filename: str,
                           sheet_name: Optional[str] = None,
                           uploaded_by: Optional[str] = None) -> Dict:
        """Process Excel file upload"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        job_id = str(uuid.uuid4())
        
        try:
            # Rollback any previous failed transactions
            self.conn.rollback()
            
            # Create upload job
            file_size = file_path.stat().st_size
            
            cur.execute("""
                INSERT INTO upload_jobs 
                (id, file_name, file_type, file_size_bytes, upload_path, 
                 sheet_name, status, uploaded_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (job_id, original_filename, 'excel', file_size, 
                  str(file_path), sheet_name, 'processing', uploaded_by))
            self.conn.commit()
            
            # Read Excel file
            if sheet_name:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            else:
                # Read first sheet if no sheet specified
                df = pd.read_excel(file_path, sheet_name=0)
                # Get actual sheet name
                xl_file = pd.ExcelFile(file_path)
                sheet_name = xl_file.sheet_names[0]
                cur.execute("""
                    UPDATE upload_jobs SET sheet_name = %s WHERE id = %s
                """, (sheet_name, job_id))
                self.conn.commit()
            
            # Analyze dataframe
            analysis = self.analyze_dataframe(df)
            
            # Convert to CSV for storage (easier for RCA engine to read)
            csv_path = file_path.parent / f"{file_path.stem}_{sheet_name}.csv"
            df.to_csv(csv_path, index=False)
            
            # Calculate checksum
            checksum = self.calculate_checksum(csv_path)
            
            # Update upload job
            cur.execute("""
                UPDATE upload_jobs 
                SET rows_count = %s, 
                    columns_count = %s,
                    inferred_schema = %s,
                    status = %s,
                    processed_at = NOW()
                WHERE id = %s
            """, (analysis['row_count'], analysis['column_count'],
                  Json(analysis['columns']), 'completed', job_id))
            self.conn.commit()
            
            # Run quality checks
            quality_checks = self.run_quality_checks(df, job_id)
            
            # Create dataset version
            table_name = f"{original_filename.replace('.xlsx', '').lower()}_{sheet_name}".lower()
            table_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)
            
            version_id = self.create_dataset_version(
                table_name, job_id, str(csv_path), 
                analysis, checksum, uploaded_by
            )
            
            result = {
                'job_id': job_id,
                'version_id': version_id,
                'table_name': table_name,
                'sheet_name': sheet_name,
                'status': 'completed',
                'rows': analysis['row_count'],
                'columns': analysis['column_count'],
                'schema': analysis['columns'],
                'quality_checks': quality_checks,
                'file_path': str(csv_path)
            }
            
            cur.close()
            return result
            
        except Exception as e:
            # Rollback and mark job as failed
            self.conn.rollback()
            try:
                cur.execute("""
                    UPDATE upload_jobs 
                    SET status = %s, error_message = %s, processed_at = NOW()
                    WHERE id = %s
                """, ('failed', str(e), job_id))
                self.conn.commit()
            except:
                self.conn.rollback()
            cur.close()
            raise
    
    def create_dataset_version(self, table_name: str, upload_job_id: str,
                              file_path: str, analysis: Dict, checksum: str,
                              created_by: Optional[str] = None) -> str:
        """Create a new dataset version"""
        cur = self.conn.cursor()
        
        # Get current max version for this table
        cur.execute("""
            SELECT COALESCE(MAX(version_number), 0) as max_version
            FROM dataset_versions
            WHERE table_name = %s
        """, (table_name,))
        max_version = cur.fetchone()[0]
        new_version = max_version + 1
        
        # Deactivate previous versions
        cur.execute("""
            UPDATE dataset_versions 
            SET is_active = FALSE 
            WHERE table_name = %s AND is_active = TRUE
        """, (table_name,))
        
        # Create new version
        version_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO dataset_versions
            (id, table_name, version_number, upload_job_id, file_path,
             schema_snapshot, row_count, checksum, is_active, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (version_id, table_name, new_version, upload_job_id, file_path,
              Json(analysis['columns']), analysis['row_count'], 
              checksum, True, created_by))
        
        # Insert column metadata
        for col_info in analysis['columns']:
            cur.execute("""
                INSERT INTO dataset_columns
                (dataset_version_id, table_name, column_name, column_position,
                 data_type, nullable, unique_values_count, null_count,
                 sample_values, min_value, max_value)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (version_id, table_name, col_info['column_name'],
                  col_info['column_position'], col_info['data_type'],
                  col_info['nullable'], col_info['unique_values_count'],
                  col_info['null_count'], Json(col_info['sample_values']),
                  col_info.get('min_value'), col_info.get('max_value')))
        
        # Update or create entry in tables metadata
        cur.execute("""
            INSERT INTO tables (name, system, entity_id, path, primary_key, 
                               current_version_id, last_uploaded_at, upload_enabled)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), TRUE)
            ON CONFLICT (name) DO UPDATE SET
                path = EXCLUDED.path,
                current_version_id = EXCLUDED.current_version_id,
                last_uploaded_at = NOW(),
                columns = %s
        """, (table_name, 'uploaded', 'uploaded', file_path, 
              Json([analysis['columns'][0]['column_name']]),  # First column as PK
              version_id,
              Json([{'name': col['column_name'], 'type': col['data_type']} 
                    for col in analysis['columns']])))
        
        self.conn.commit()
        cur.close()
        return version_id
    
    def get_upload_history(self, limit: int = 50) -> List[Dict]:
        """Get recent upload history"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, file_name, file_type, sheet_name, status,
                   rows_count, columns_count, uploaded_at, processed_at,
                   error_message
            FROM upload_jobs
            ORDER BY uploaded_at DESC
            LIMIT %s
        """, (limit,))
        results = cur.fetchall()
        cur.close()
        return [dict(row) for row in results]
    
    def get_dataset_versions(self, table_name: str) -> List[Dict]:
        """Get version history for a dataset"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT dv.*, uj.file_name, uj.uploaded_at
            FROM dataset_versions dv
            LEFT JOIN upload_jobs uj ON dv.upload_job_id = uj.id
            WHERE dv.table_name = %s
            ORDER BY dv.version_number DESC
        """, (table_name,))
        results = cur.fetchall()
        cur.close()
        return [dict(row) for row in results]

