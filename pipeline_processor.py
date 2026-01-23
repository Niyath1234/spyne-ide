#!/usr/bin/env python3
"""
Pipeline Processor for RCA Engine
Processes API responses, aggregates data, extracts metadata, and stores results
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import hashlib
from collections import defaultdict
import uuid


class PipelineProcessor:
    """Main pipeline processor that handles data flow from API to storage"""
    
    def __init__(self, output_dir: str = "data/pipeline_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir = Path("metadata/pipeline_metadata")
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
    def process_api_response(
        self,
        data: List[Dict[str, Any]],
        source_name: str,
        table_name: Optional[str] = None,
        group_by: Optional[List[str]] = None,
        metrics: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process API response data through the pipeline
        
        Args:
            data: List of records from API response
            source_name: Name of the API source
            table_name: Optional table name (auto-generated if not provided)
            group_by: List of columns to group by for aggregation
            metrics: Dict of metric_name: aggregation_function (e.g., {'total': 'sum', 'avg': 'mean'})
            metadata: Additional metadata to attach
            
        Returns:
            Dict with processing results including table info, metadata, and file path
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Extract metadata
        extracted_metadata = self._extract_metadata(df, source_name, metadata)
        
        # Generate table name if not provided
        if not table_name:
            table_name = self._generate_table_name(source_name, df)
        
        # Process and aggregate if group_by and metrics are provided
        if group_by and metrics:
            df_processed = self._aggregate_data(df, group_by, metrics)
        else:
            df_processed = df
        
        # Calculate additional metrics
        calculated_metrics = self._calculate_metrics(df_processed)
        
        # Store as CSV
        csv_path = self._store_as_csv(df_processed, table_name, source_name)
        
        # Update metadata with table information
        table_metadata = self._create_table_metadata(
            table_name, df_processed, extracted_metadata, 
            calculated_metrics, csv_path, group_by, metrics
        )
        
        # Save metadata
        self._save_metadata(table_metadata, table_name)
        
        return {
            'success': True,
            'table_name': table_name,
            'source_name': source_name,
            'rows_processed': len(df),
            'rows_output': len(df_processed),
            'columns': list(df_processed.columns),
            'csv_path': str(csv_path),
            'metadata': table_metadata,
            'calculated_metrics': calculated_metrics
        }
    
    def _extract_metadata(
        self, 
        df: pd.DataFrame, 
        source_name: str,
        additional_metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Extract metadata from DataFrame"""
        metadata = {
            'source_name': source_name,
            'extracted_at': datetime.now().isoformat(),
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': []
        }
        
        # Column-level metadata
        for col in df.columns:
            col_metadata = {
                'name': col,
                'data_type': str(df[col].dtype),
                'nullable': bool(df[col].isnull().any()),  # Convert numpy bool to Python bool
                'null_count': int(df[col].isnull().sum()),
                'unique_count': int(df[col].nunique()),
                'sample_values': df[col].dropna().head(5).tolist()
            }
            
            # Add min/max for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_metadata['min'] = float(df[col].min())
                col_metadata['max'] = float(df[col].max())
                col_metadata['mean'] = float(df[col].mean())
                col_metadata['std'] = float(df[col].std())
            
            metadata['columns'].append(col_metadata)
        
        # Add additional metadata if provided
        if additional_metadata:
            metadata.update(additional_metadata)
        
        return metadata
    
    def _generate_table_name(self, source_name: str, df: pd.DataFrame) -> str:
        """Generate a table name from source name and timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Sanitize source name
        safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in source_name.lower())
        return f"{safe_name}_{timestamp}"
    
    def _aggregate_data(
        self, 
        df: pd.DataFrame, 
        group_by: List[str],
        metrics: Dict[str, str]
    ) -> pd.DataFrame:
        """Aggregate data by grouping columns and calculating metrics"""
        # Validate group_by columns exist
        missing_cols = [col for col in group_by if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Group by columns not found: {missing_cols}")
        
        # Build aggregation dictionary
        agg_dict = {}
        for metric_name, agg_func in metrics.items():
            # Find numeric columns to aggregate (or use all numeric if not specified)
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            if numeric_cols:
                # Map aggregation function
                if agg_func.lower() == 'sum':
                    for col in numeric_cols:
                        agg_dict[col] = 'sum'
                elif agg_func.lower() in ['mean', 'avg', 'average']:
                    for col in numeric_cols:
                        agg_dict[col] = 'mean'
                elif agg_func.lower() == 'count':
                    for col in numeric_cols:
                        agg_dict[col] = 'count'
                elif agg_func.lower() == 'min':
                    for col in numeric_cols:
                        agg_dict[col] = 'min'
                elif agg_func.lower() == 'max':
                    for col in numeric_cols:
                        agg_dict[col] = 'max'
        
        if not agg_dict:
            # If no aggregation specified, just group and count
            agg_dict = {df.columns[0]: 'count'}
        
        # Perform aggregation
        grouped = df.groupby(group_by).agg(agg_dict).reset_index()
        
        # Flatten column names if multi-index
        if isinstance(grouped.columns, pd.MultiIndex):
            grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
        
        return grouped
    
    def _calculate_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate additional metrics on the processed data"""
        metrics = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'numeric_columns': [],
            'categorical_columns': []
        }
        
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                metrics['numeric_columns'].append({
                    'column': col,
                    'sum': float(df[col].sum()) if df[col].notna().any() else None,
                    'mean': float(df[col].mean()) if df[col].notna().any() else None,
                    'min': float(df[col].min()) if df[col].notna().any() else None,
                    'max': float(df[col].max()) if df[col].notna().any() else None
                })
            else:
                metrics['categorical_columns'].append({
                    'column': col,
                    'unique_count': int(df[col].nunique()),
                    'most_common': df[col].mode().tolist()[:5] if len(df[col].mode()) > 0 else []
                })
        
        return metrics
    
    def _store_as_csv(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        source_name: str
    ) -> Path:
        """Store DataFrame as CSV file"""
        # Create subdirectory for source
        source_dir = self.output_dir / source_name
        source_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        csv_filename = f"{table_name}.csv"
        csv_path = source_dir / csv_filename
        
        # Write CSV
        df.to_csv(csv_path, index=False)
        
        return csv_path
    
    def _create_table_metadata(
        self,
        table_name: str,
        df: pd.DataFrame,
        extracted_metadata: Dict[str, Any],
        calculated_metrics: Dict[str, Any],
        csv_path: Path,
        group_by: Optional[List[str]] = None,
        metrics: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Create comprehensive table metadata"""
        # Calculate checksum
        checksum = self._calculate_checksum(csv_path)
        
        metadata = {
            'table_name': table_name,
            'created_at': datetime.now().isoformat(),
            'source_metadata': extracted_metadata,
            'schema': {
                'columns': [
                    {
                        'name': col,
                        'type': str(df[col].dtype),
                        'nullable': bool(df[col].isnull().any())  # Convert numpy bool to Python bool
                    }
                    for col in df.columns
                ],
                'primary_key': list(df.columns[:1]) if len(df.columns) > 0 else [],
                'row_count': len(df)
            },
            'storage': {
                'type': 'csv',
                'path': str(csv_path),
                'checksum': checksum,
                'size_bytes': csv_path.stat().st_size
            },
            'processing': {
                'grouped_by': group_by or [],
                'metrics_calculated': metrics or {},
                'aggregation_applied': group_by is not None and metrics is not None
            },
            'calculated_metrics': calculated_metrics
        }
        
        return metadata
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def _save_metadata(self, metadata: Dict[str, Any], table_name: str):
        """Save metadata to JSON file"""
        metadata_file = self.metadata_dir / f"{table_name}_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # Also update tables.json if it exists
        self._update_tables_registry(metadata)
    
    def _update_tables_registry(self, metadata: Dict[str, Any]):
        """Update the main tables.json registry"""
        tables_file = Path("metadata/tables.json")
        
        # Load existing tables or create new
        if tables_file.exists():
            try:
                with open(tables_file, 'r') as f:
                    data = json.load(f)
                    # Handle both formats: {"tables": [...]} and [...]
                    if isinstance(data, dict) and 'tables' in data:
                        tables = data['tables']
                    elif isinstance(data, list):
                        tables = data
                    else:
                        tables = []
            except Exception as e:
                print(f"Warning: Failed to load tables.json: {e}")
                tables = []
        else:
            tables = []
        
        # Create table entry
        # Remove 'data/' prefix from path if present (data_dir will add it)
        storage_path = metadata['storage']['path']
        if storage_path.startswith('data/'):
            storage_path = storage_path[5:]  # Remove 'data/' prefix
        
        table_entry = {
            'name': metadata['table_name'],
            'system': metadata['source_metadata'].get('source_name', 'pipeline'),
            'entity': metadata['source_metadata'].get('entity', 'unknown'),
            'path': storage_path,
            'primary_key': metadata['schema']['primary_key'],
            'columns': [
                {
                    'name': col['name'],
                    'type': col['type']
                }
                for col in metadata['schema']['columns']
            ],
            'created_at': metadata['created_at'],
            'row_count': metadata['schema']['row_count'],
            'source': 'pipeline',
            'metadata_file': str(self.metadata_dir / f"{metadata['table_name']}_metadata.json")
        }
        
        # Remove existing entry if present
        tables = [t for t in tables if t.get('name') != table_entry['name']]
        
        # Add new entry
        tables.append(table_entry)
        
        # Save in the correct format (preserve original structure)
        with open(tables_file, 'w') as f:
            json.dump({'tables': tables}, f, indent=2)


class S3StorageAdapter:
    """Adapter for S3 storage (future implementation)"""
    
    def __init__(self, bucket_name: str, region: str = "us-east-1"):
        self.bucket_name = bucket_name
        self.region = region
        # For now, this is a placeholder
        # In production, would use boto3
    
    def upload_file(self, local_path: Path, s3_key: str) -> str:
        """Upload file to S3"""
        # TODO: Implement S3 upload using boto3
        raise NotImplementedError("S3 upload not yet implemented. Using local storage.")
    
    def download_file(self, s3_key: str, local_path: Path):
        """Download file from S3"""
        # TODO: Implement S3 download using boto3
        raise NotImplementedError("S3 download not yet implemented.")


class PipelineStorage:
    """Unified storage interface for local CSV and future S3"""
    
    def __init__(self, use_s3: bool = False, s3_config: Optional[Dict[str, str]] = None):
        self.use_s3 = use_s3
        self.local_dir = Path("data/pipeline_output")
        self.local_dir.mkdir(parents=True, exist_ok=True)
        
        if use_s3 and s3_config:
            self.s3_adapter = S3StorageAdapter(
                bucket_name=s3_config.get('bucket_name', ''),
                region=s3_config.get('region', 'us-east-1')
            )
        else:
            self.s3_adapter = None
    
    def store(self, df: pd.DataFrame, table_name: str, source_name: str) -> Dict[str, str]:
        """Store DataFrame and return storage info"""
        if self.use_s3 and self.s3_adapter:
            # Store locally first, then upload to S3
            local_path = self.local_dir / source_name / f"{table_name}.csv"
            local_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(local_path, index=False)
            
            # Upload to S3
            s3_key = f"{source_name}/{table_name}.csv"
            try:
                s3_path = self.s3_adapter.upload_file(local_path, s3_key)
                return {
                    'type': 's3',
                    'local_path': str(local_path),
                    's3_path': s3_path,
                    's3_key': s3_key
                }
            except NotImplementedError:
                # Fallback to local if S3 not implemented
                return {
                    'type': 'local',
                    'path': str(local_path)
                }
        else:
            # Store locally
            local_path = self.local_dir / source_name / f"{table_name}.csv"
            local_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(local_path, index=False)
            return {
                'type': 'local',
                'path': str(local_path)
            }

