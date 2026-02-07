#!/usr/bin/env python3
"""
Extract TPC-DS table schemas from Trino and generate metadata JSON
"""
import sys
import os
import json
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from ai_sql_system.trino.client import TrinoClient
from ai_sql_system.trino.schema_loader import SchemaLoader

def extract_tpcds_schema(schema_name: str = "tiny"):
    """Extract TPC-DS schema from Trino"""
    print(f"Connecting to Trino to extract TPC-DS schema '{schema_name}'...")
    
    # Connect to Trino with TPC-DS catalog
    client = TrinoClient(
        host=os.getenv('TRINO_HOST', 'localhost'),
        port=int(os.getenv('TRINO_PORT', '8081')),
        catalog='tpcds',
        schema=schema_name
    )
    
    loader = SchemaLoader(client)
    
    try:
        # Get all tables
        print(f"Fetching tables from tpcds.{schema_name}...")
        tables = loader.get_tables()
        
        if not tables:
            print(f"No tables found in tpcds.{schema_name}")
            print("Make sure Trino is running and TPC-DS connector is configured.")
            return []
        
        print(f"Found {len(tables)} tables")
        
        # Extract schema for each table
        table_schemas = []
        for table in tables:
            table_name = table['table_name']
            print(f"  Extracting schema for {table_name}...")
            
            columns = loader.get_columns(table_name)
            
            table_schema = {
                "name": f"tpcds.{schema_name}.{table_name}",
                "entity": table_name,
                "system": "tpcds",
                "columns": [
                    {
                        "name": col['column_name'],
                        "type": col['data_type'],
                        "description": f"{col['column_name']} column"
                    }
                    for col in columns
                ]
            }
            table_schemas.append(table_schema)
        
        return table_schemas
    
    except Exception as e:
        print(f"Error extracting schema: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        client.close()

if __name__ == "__main__":
    schema_name = sys.argv[1] if len(sys.argv) > 1 else "tiny"
    
    print("=" * 60)
    print("TPC-DS Schema Extractor")
    print("=" * 60)
    
    schemas = extract_tpcds_schema(schema_name)
    
    if schemas:
        # Output JSON
        output = {
            "tables": schemas
        }
        
        output_file = Path(__file__).parent.parent / "metadata" / f"tpcds_{schema_name}_tables.json"
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"\n✓ Extracted {len(schemas)} tables")
        print(f"✓ Saved to {output_file}")
        print("\nTables extracted:")
        for schema in schemas:
            print(f"  - {schema['name']} ({len(schema['columns'])} columns)")
    else:
        print("\n✗ Failed to extract schemas")
        sys.exit(1)
