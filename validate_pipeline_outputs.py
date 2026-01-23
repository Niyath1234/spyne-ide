#!/usr/bin/env python3
"""
Pipeline Output Validator
Validates that pipeline outputs are correct and queryable
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import requests


class PipelineOutputValidator:
    """Validates pipeline outputs"""
    
    def __init__(self, pipeline_api_url: str = "http://localhost:8082/api/pipeline"):
        self.pipeline_api_url = pipeline_api_url
    
    def validate_table(self, table_name: str, csv_path: str, 
                      expected_schema: Optional[Dict] = None,
                      expected_row_count: Optional[int] = None) -> Dict[str, Any]:
        """Validate a single table"""
        print(f"\n🔍 Validating table: {table_name}")
        print(f"   CSV Path: {csv_path}")
        
        validation = {
            'table_name': table_name,
            'csv_path': csv_path,
            'file_exists': False,
            'readable': False,
            'row_count': 0,
            'column_count': 0,
            'columns': [],
            'schema_match': None,
            'row_count_match': None,
            'data_types': {},
            'null_counts': {},
            'sample_data': [],
            'valid': False,
            'errors': []
        }
        
        try:
            # Check file exists
            path = Path(csv_path)
            if not path.exists():
                validation['errors'].append(f"File does not exist: {csv_path}")
                return validation
            
            validation['file_exists'] = True
            
            # Read CSV
            df = pd.read_csv(path)
            validation['readable'] = True
            validation['row_count'] = len(df)
            validation['column_count'] = len(df.columns)
            validation['columns'] = list(df.columns)
            validation['data_types'] = df.dtypes.astype(str).to_dict()
            validation['null_counts'] = df.isnull().sum().to_dict()
            validation['sample_data'] = df.head(5).to_dict('records')
            
            # Validate schema
            if expected_schema:
                expected_cols = set(expected_schema.get('columns', []))
                actual_cols = set(validation['columns'])
                
                missing_cols = expected_cols - actual_cols
                extra_cols = actual_cols - expected_cols
                
                if missing_cols:
                    validation['errors'].append(f"Missing columns: {missing_cols}")
                if extra_cols:
                    validation['errors'].append(f"Extra columns: {extra_cols}")
                
                validation['schema_match'] = len(missing_cols) == 0 and len(extra_cols) == 0
            
            # Validate row count
            if expected_row_count is not None:
                validation['row_count_match'] = validation['row_count'] == expected_row_count
                if not validation['row_count_match']:
                    validation['errors'].append(
                        f"Row count mismatch: expected {expected_row_count}, got {validation['row_count']}"
                    )
            
            # Overall validation
            validation['valid'] = len(validation['errors']) == 0
            
            if validation['valid']:
                print(f"   ✅ Validation passed")
                print(f"      Rows: {validation['row_count']}")
                print(f"      Columns: {validation['column_count']}")
            else:
                print(f"   ❌ Validation failed:")
                for error in validation['errors']:
                    print(f"      - {error}")
            
        except Exception as e:
            validation['errors'].append(f"Exception during validation: {str(e)}")
            print(f"   ❌ Validation error: {e}")
        
        return validation
    
    def validate_aggregation(self, original_data: List[Dict], aggregated_csv_path: str,
                            group_by: List[str], metrics: Dict[str, str]) -> Dict[str, Any]:
        """Validate that aggregation was performed correctly"""
        print(f"\n🔍 Validating aggregation")
        print(f"   Group by: {group_by}")
        print(f"   Metrics: {metrics}")
        
        validation = {
            'valid': False,
            'errors': [],
            'original_count': len(original_data),
            'aggregated_count': 0,
            'group_by_columns_present': False,
            'metric_columns_present': False
        }
        
        try:
            # Read original data
            df_original = pd.DataFrame(original_data)
            
            # Read aggregated CSV
            df_agg = pd.read_csv(aggregated_csv_path)
            validation['aggregated_count'] = len(df_agg)
            
            # Check group_by columns exist
            missing_group_cols = [col for col in group_by if col not in df_agg.columns]
            if missing_group_cols:
                validation['errors'].append(f"Missing group_by columns: {missing_group_cols}")
            else:
                validation['group_by_columns_present'] = True
            
            # Check metric columns exist (they might have suffixes)
            metric_cols_found = []
            for metric_name in metrics.keys():
                # Look for columns that contain the metric name
                matching_cols = [col for col in df_agg.columns if metric_name.lower() in col.lower()]
                if matching_cols:
                    metric_cols_found.extend(matching_cols)
                else:
                    validation['errors'].append(f"Metric column not found: {metric_name}")
            
            if metric_cols_found:
                validation['metric_columns_present'] = True
            
            # Verify aggregation logic (basic check)
            if validation['group_by_columns_present']:
                # Group original data manually
                grouped_original = df_original.groupby(group_by).size()
                if len(df_agg) <= len(df_original):
                    print(f"   ✅ Aggregation reduced rows: {len(df_original)} → {len(df_agg)}")
                else:
                    validation['errors'].append(
                        f"Aggregation increased rows: {len(df_original)} → {len(df_agg)}"
                    )
            
            validation['valid'] = len(validation['errors']) == 0
            
            if validation['valid']:
                print(f"   ✅ Aggregation validation passed")
            else:
                print(f"   ❌ Aggregation validation failed:")
                for error in validation['errors']:
                    print(f"      - {error}")
            
        except Exception as e:
            validation['errors'].append(f"Exception: {str(e)}")
            print(f"   ❌ Validation error: {e}")
        
        return validation
    
    def query_and_validate(self, csv_path: str, query_type: str = "all") -> Dict[str, Any]:
        """Query table and validate results"""
        print(f"\n🔎 Querying and validating: {csv_path}")
        
        results = {
            'query_successful': False,
            'row_count': 0,
            'columns': [],
            'numeric_summary': {},
            'categorical_summary': {},
            'sample_rows': []
        }
        
        try:
            df = pd.read_csv(csv_path)
            results['query_successful'] = True
            results['row_count'] = len(df)
            results['columns'] = list(df.columns)
            
            # Numeric summary
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                results['numeric_summary'] = df[numeric_cols].describe().to_dict()
            
            # Categorical summary
            categorical_cols = df.select_dtypes(include=['object']).columns
            for col in categorical_cols[:5]:  # Limit to first 5
                results['categorical_summary'][col] = {
                    'unique_count': int(df[col].nunique()),
                    'most_common': df[col].mode().tolist()[:5] if len(df[col].mode()) > 0 else []
                }
            
            # Sample rows
            results['sample_rows'] = df.head(10).to_dict('records')
            
            print(f"   ✅ Query successful")
            print(f"      Rows: {results['row_count']}")
            print(f"      Columns: {len(results['columns'])}")
            
        except Exception as e:
            print(f"   ❌ Query failed: {e}")
            results['error'] = str(e)
        
        return results
    
    def validate_all_tables(self) -> Dict[str, Any]:
        """Validate all tables from pipeline API"""
        print("\n" + "=" * 70)
        print("VALIDATING ALL TABLES")
        print("=" * 70)
        
        try:
            response = requests.get(f"{self.pipeline_api_url}/tables", timeout=10)
            response.raise_for_status()
            result = response.json()
            tables = result.get('tables', [])
            
            print(f"\nFound {len(tables)} tables to validate\n")
            
            validations = []
            for table in tables:
                table_name = table.get('table_name')
                csv_path = table.get('csv_path')
                
                if csv_path:
                    validation = self.validate_table(
                        table_name=table_name,
                        csv_path=csv_path,
                        expected_row_count=table.get('row_count')
                    )
                    validations.append(validation)
            
            # Summary
            valid_count = sum(1 for v in validations if v.get('valid'))
            total_count = len(validations)
            
            print("\n" + "=" * 70)
            print("VALIDATION SUMMARY")
            print("=" * 70)
            print(f"Total tables: {total_count}")
            print(f"Valid: {valid_count}")
            print(f"Invalid: {total_count - valid_count}")
            
            return {
                'total_tables': total_count,
                'valid_tables': valid_count,
                'invalid_tables': total_count - valid_count,
                'validations': validations
            }
            
        except Exception as e:
            print(f"❌ Failed to validate tables: {e}")
            return {'error': str(e)}


if __name__ == '__main__':
    import sys
    
    validator = PipelineOutputValidator()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--all":
            validator.validate_all_tables()
        elif sys.argv[1] == "--table" and len(sys.argv) > 2:
            table_name = sys.argv[2]
            # Get table metadata first
            try:
                response = requests.get(f"{validator.pipeline_api_url}/tables/{table_name}")
                if response.status_code == 200:
                    metadata = response.json()
                    csv_path = metadata['metadata']['storage']['path']
                    validator.validate_table(table_name, csv_path)
                    validator.query_and_validate(csv_path)
                else:
                    print(f"Table not found: {table_name}")
            except Exception as e:
                print(f"Error: {e}")
        else:
            print("Usage:")
            print("  python validate_pipeline_outputs.py --all")
            print("  python validate_pipeline_outputs.py --table <table_name>")
    else:
        # Default: validate all
        validator.validate_all_tables()

