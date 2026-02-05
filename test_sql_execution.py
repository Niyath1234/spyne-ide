#!/usr/bin/env python3
"""
Test script to execute the generated SQL query against Trino
"""
import os
import sys
from pathlib import Path

# Add backend to path
backend_path = Path(__file__).parent / "backend"
sys.path.insert(0, str(backend_path))

from dotenv import load_dotenv
from ai_sql_system.trino.client import TrinoClient

# Load environment variables
load_dotenv()

# SQL query from the API response
sql = """SELECT
    c.c_custkey,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_price
FROM
    customer c
LEFT JOIN
    orders o ON o.o_custkey = c.c_custkey
LEFT JOIN
    lineitem l ON l.l_orderkey = o.o_orderkey
GROUP BY
    c.c_custkey"""

def main():
    print("=" * 80)
    print("Testing SQL Execution Against Trino")
    print("=" * 80)
    print(f"\nSQL Query:\n{sql}\n")
    print("-" * 80)
    
    try:
        # Initialize Trino client
        trino_host = os.getenv('TRINO_HOST', 'localhost')
        trino_port = int(os.getenv('TRINO_PORT', '8080'))
        trino_catalog = os.getenv('TRINO_CATALOG', 'tpch')
        trino_schema = os.getenv('TRINO_SCHEMA', 'tiny')
        
        print(f"Connecting to Trino: {trino_host}:{trino_port}")
        print(f"Catalog: {trino_catalog}, Schema: {trino_schema}\n")
        
        client = TrinoClient(
            host=trino_host,
            port=trino_port,
            catalog=trino_catalog,
            schema=trino_schema
        )
        
        # Execute query with limit for testing
        print("Executing query...")
        results = client.execute_query(sql, limit=10)
        
        print(f"\n✅ Query executed successfully!")
        print(f"Results: {len(results)} rows returned\n")
        
        if results:
            print("Sample Results (first 10 rows):")
            print("-" * 80)
            # Print column headers
            if results:
                columns = list(results[0].keys())
                print(" | ".join(columns))
                print("-" * 80)
                # Print rows
                for row in results:
                    values = [str(row[col]) for col in columns]
                    print(" | ".join(values))
        else:
            print("No rows returned.")
        
        print("\n" + "=" * 80)
        print("✅ SQL execution test completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error executing SQL:")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()
