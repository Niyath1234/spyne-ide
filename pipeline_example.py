#!/usr/bin/env python3
"""
Example script demonstrating the Pipeline API usage
Shows how to send data from API responses to the pipeline
"""

import requests
import json
from datetime import datetime

# Pipeline API endpoint
PIPELINE_API_URL = "http://localhost:8082/api/pipeline"


def example_simple_ingestion():
    """Example: Simple data ingestion without aggregation"""
    print("=" * 60)
    print("Example 1: Simple Data Ingestion")
    print("=" * 60)
    
    # Simulate API response data
    api_data = [
        {"user_id": 1, "name": "Alice", "age": 30, "amount": 100.50, "date": "2024-01-15"},
        {"user_id": 2, "name": "Bob", "age": 25, "amount": 200.75, "date": "2024-01-15"},
        {"user_id": 3, "name": "Charlie", "age": 35, "amount": 150.25, "date": "2024-01-16"},
        {"user_id": 4, "name": "Alice", "age": 30, "amount": 75.00, "date": "2024-01-16"},
        {"user_id": 5, "name": "Bob", "age": 25, "amount": 300.00, "date": "2024-01-17"},
    ]
    
    payload = {
        "data": api_data,
        "source_name": "user_api",
        "table_name": "user_transactions",
        "metadata": {
            "entity": "user",
            "description": "User transaction data from external API"
        }
    }
    
    response = requests.post(f"{PIPELINE_API_URL}/ingest", json=payload)
    print(f"Status: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
    print()


def example_aggregated_ingestion():
    """Example: Data ingestion with grouping and metrics"""
    print("=" * 60)
    print("Example 2: Aggregated Data Ingestion")
    print("=" * 60)
    
    # Simulate API response data
    api_data = [
        {"date": "2024-01-15", "category": "A", "amount": 100.50, "count": 5},
        {"date": "2024-01-15", "category": "B", "amount": 200.75, "count": 3},
        {"date": "2024-01-15", "category": "A", "amount": 150.25, "count": 2},
        {"date": "2024-01-16", "category": "A", "amount": 75.00, "count": 4},
        {"date": "2024-01-16", "category": "B", "amount": 300.00, "count": 6},
        {"date": "2024-01-16", "category": "C", "amount": 50.00, "count": 1},
    ]
    
    payload = {
        "data": api_data,
        "source_name": "sales_api",
        "table_name": "daily_sales_summary",
        "group_by": ["date", "category"],
        "metrics": {
            "total_amount": "sum",
            "total_count": "sum",
            "avg_amount": "mean"
        },
        "metadata": {
            "entity": "sales",
            "description": "Daily sales aggregated by category"
        }
    }
    
    response = requests.post(f"{PIPELINE_API_URL}/ingest", json=payload)
    print(f"Status: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
    print()


def example_batch_ingestion():
    """Example: Batch ingestion from multiple API sources"""
    print("=" * 60)
    print("Example 3: Batch Ingestion from Multiple Sources")
    print("=" * 60)
    
    # Simulate multiple API responses
    sources = [
        {
            "data": [
                {"product_id": 1, "name": "Widget A", "price": 10.00, "stock": 100},
                {"product_id": 2, "name": "Widget B", "price": 20.00, "stock": 50},
            ],
            "source_name": "products_api",
            "table_name": "products",
            "metadata": {"entity": "product"}
        },
        {
            "data": [
                {"order_id": 1, "product_id": 1, "quantity": 2, "total": 20.00},
                {"order_id": 2, "product_id": 2, "quantity": 1, "total": 20.00},
                {"order_id": 3, "product_id": 1, "quantity": 5, "total": 50.00},
            ],
            "source_name": "orders_api",
            "table_name": "orders",
            "group_by": ["product_id"],
            "metrics": {
                "total_quantity": "sum",
                "total_revenue": "sum"
            },
            "metadata": {"entity": "order"}
        }
    ]
    
    payload = {"sources": sources}
    
    response = requests.post(f"{PIPELINE_API_URL}/ingest/batch", json=payload)
    print(f"Status: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
    print()


def example_list_tables():
    """Example: List all tables created by pipeline"""
    print("=" * 60)
    print("Example 4: List All Pipeline Tables")
    print("=" * 60)
    
    response = requests.get(f"{PIPELINE_API_URL}/tables")
    print(f"Status: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
    print()


def example_get_table_metadata():
    """Example: Get metadata for a specific table"""
    print("=" * 60)
    print("Example 5: Get Table Metadata")
    print("=" * 60)
    
    # First, list tables to get a table name
    tables_response = requests.get(f"{PIPELINE_API_URL}/tables")
    if tables_response.status_code == 200:
        tables = tables_response.json().get('tables', [])
        if tables:
            table_name = tables[0]['table_name']
            print(f"Fetching metadata for: {table_name}")
            
            response = requests.get(f"{PIPELINE_API_URL}/tables/{table_name}")
            print(f"Status: {response.status_code}")
            print(json.dumps(response.json(), indent=2))
        else:
            print("No tables found. Run ingestion examples first.")
    print()


def example_aggregate_existing():
    """Example: Aggregate an existing table"""
    print("=" * 60)
    print("Example 6: Aggregate Existing Table")
    print("=" * 60)
    
    # First, list tables to get a table name
    tables_response = requests.get(f"{PIPELINE_API_URL}/tables")
    if tables_response.status_code == 200:
        tables = tables_response.json().get('tables', [])
        if tables:
            # Find a table with numeric columns
            for table in tables:
                if 'amount' in str(table.get('columns', [])) or 'total' in str(table.get('columns', [])):
                    table_name = table['table_name']
                    print(f"Aggregating table: {table_name}")
                    
                    payload = {
                        "table_name": table_name,
                        "group_by": ["date"] if 'date' in str(table.get('columns', [])) else [table['columns'][0] if table.get('columns') else 'id'],
                        "metrics": {
                            "total": "sum",
                            "average": "mean"
                        },
                        "output_table_name": f"{table_name}_aggregated"
                    }
                    
                    response = requests.post(f"{PIPELINE_API_URL}/aggregate", json=payload)
                    print(f"Status: {response.status_code}")
                    print(json.dumps(response.json(), indent=2))
                    break
            else:
                print("No suitable table found for aggregation.")
        else:
            print("No tables found. Run ingestion examples first.")
    print()


if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("Pipeline API Examples")
    print("=" * 60)
    print("\nMake sure the pipeline API server is running on port 8082")
    print("Start it with: python pipeline_api_server.py\n")
    
    try:
        # Run examples
        example_simple_ingestion()
        example_aggregated_ingestion()
        example_batch_ingestion()
        example_list_tables()
        example_get_table_metadata()
        example_aggregate_existing()
        
        print("=" * 60)
        print("All examples completed!")
        print("=" * 60)
        
    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect to pipeline API server.")
        print("Please start the server first: python pipeline_api_server.py")
    except Exception as e:
        print(f"ERROR: {e}")

