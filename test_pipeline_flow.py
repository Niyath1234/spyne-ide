#!/usr/bin/env python3
"""
Complete Pipeline Flow Test
Tests the entire user flow: API data → Pipeline → Table Building → Querying → Validation
"""

import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import random
import time
from typing import Dict, List, Any


class PipelineFlowTester:
    """Complete pipeline flow tester"""
    
    def __init__(self, pipeline_api_url: str = "http://localhost:8082/api/pipeline"):
        self.pipeline_api_url = pipeline_api_url
        self.created_tables = []
        self.test_results = []
    
    def generate_dummy_api_data(self, data_type: str = "transactions") -> List[Dict[str, Any]]:
        """Generate dummy API response data"""
        if data_type == "transactions":
            return self._generate_transaction_data()
        elif data_type == "customers":
            return self._generate_customer_data()
        elif data_type == "products":
            return self._generate_product_data()
        elif data_type == "orders":
            return self._generate_order_data()
        else:
            return self._generate_generic_data()
    
    def _generate_transaction_data(self, count: int = 100) -> List[Dict[str, Any]]:
        """Generate transaction data"""
        data = []
        base_date = datetime.now() - timedelta(days=30)
        
        for i in range(count):
            data.append({
                "transaction_id": f"TXN{1000 + i}",
                "customer_id": random.randint(1, 50),
                "amount": round(random.uniform(10.0, 1000.0), 2),
                "transaction_date": (base_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
                "transaction_type": random.choice(["PAYMENT", "REFUND", "CHARGE", "ADJUSTMENT"]),
                "status": random.choice(["SUCCESS", "PENDING", "FAILED"]),
                "payment_method": random.choice(["CREDIT_CARD", "DEBIT_CARD", "BANK_TRANSFER", "WALLET"])
            })
        
        return data
    
    def _generate_customer_data(self, count: int = 50) -> List[Dict[str, Any]]:
        """Generate customer data"""
        data = []
        first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
        
        for i in range(count):
            data.append({
                "customer_id": i + 1,
                "first_name": random.choice(first_names),
                "last_name": random.choice(last_names),
                "email": f"customer{i+1}@example.com",
                "phone": f"+1-555-{random.randint(1000, 9999)}",
                "registration_date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
                "status": random.choice(["ACTIVE", "INACTIVE", "SUSPENDED"]),
                "total_spent": round(random.uniform(0, 5000), 2)
            })
        
        return data
    
    def _generate_product_data(self, count: int = 30) -> List[Dict[str, Any]]:
        """Generate product data"""
        data = []
        categories = ["Electronics", "Clothing", "Food", "Books", "Toys"]
        
        for i in range(count):
            data.append({
                "product_id": i + 1,
                "product_name": f"Product {i+1}",
                "category": random.choice(categories),
                "price": round(random.uniform(5.0, 500.0), 2),
                "stock_quantity": random.randint(0, 1000),
                "supplier_id": random.randint(1, 10),
                "created_date": (datetime.now() - timedelta(days=random.randint(0, 180))).strftime("%Y-%m-%d")
            })
        
        return data
    
    def _generate_order_data(self, count: int = 200) -> List[Dict[str, Any]]:
        """Generate order data"""
        data = []
        base_date = datetime.now() - timedelta(days=60)
        
        for i in range(count):
            data.append({
                "order_id": f"ORD{1000 + i}",
                "customer_id": random.randint(1, 50),
                "product_id": random.randint(1, 30),
                "quantity": random.randint(1, 10),
                "unit_price": round(random.uniform(10.0, 200.0), 2),
                "total_amount": 0,  # Will be calculated
                "order_date": (base_date + timedelta(days=random.randint(0, 60))).strftime("%Y-%m-%d"),
                "status": random.choice(["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"])
            })
        
        # Calculate total_amount
        for order in data:
            order["total_amount"] = round(order["quantity"] * order["unit_price"], 2)
        
        return data
    
    def _generate_generic_data(self, count: int = 50) -> List[Dict[str, Any]]:
        """Generate generic test data"""
        data = []
        for i in range(count):
            data.append({
                "id": i + 1,
                "name": f"Item {i+1}",
                "value": random.randint(1, 100),
                "category": random.choice(["A", "B", "C"]),
                "created_at": datetime.now().isoformat()
            })
        return data
    
    def test_pipeline_ingestion(self, data: List[Dict], source_name: str, 
                                table_name: str = None, group_by: List[str] = None,
                                metrics: Dict[str, str] = None) -> Dict[str, Any]:
        """Test pipeline ingestion"""
        print(f"\n📥 Testing pipeline ingestion for: {source_name}")
        
        payload = {
            "data": data,
            "source_name": source_name,
            "table_name": table_name
        }
        
        if group_by:
            payload["group_by"] = group_by
        if metrics:
            payload["metrics"] = metrics
        
        try:
            response = requests.post(f"{self.pipeline_api_url}/ingest", json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            
            if result.get('success'):
                self.created_tables.append({
                    'table_name': result['table_name'],
                    'source_name': source_name,
                    'csv_path': result.get('csv_path'),
                    'rows': result.get('rows_output', 0)
                })
                print(f"  ✅ Successfully created table: {result['table_name']}")
                print(f"     Rows: {result.get('rows_output', 0)}")
                print(f"     CSV: {result.get('csv_path', 'N/A')}")
                return result
            else:
                print(f"  ❌ Failed: {result.get('error', 'Unknown error')}")
                return result
                
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def test_aggregated_ingestion(self, data: List[Dict], source_name: str,
                                  group_by: List[str], metrics: Dict[str, str]) -> Dict[str, Any]:
        """Test aggregated ingestion"""
        print(f"\n📊 Testing aggregated ingestion for: {source_name}")
        
        return self.test_pipeline_ingestion(
            data=data,
            source_name=source_name,
            table_name=f"{source_name}_aggregated",
            group_by=group_by,
            metrics=metrics
        )
    
    def list_tables(self) -> List[Dict]:
        """List all created tables"""
        try:
            response = requests.get(f"{self.pipeline_api_url}/tables", timeout=10)
            response.raise_for_status()
            result = response.json()
            return result.get('tables', [])
        except Exception as e:
            print(f"  ❌ Failed to list tables: {e}")
            return []
    
    def get_table_metadata(self, table_name: str) -> Dict:
        """Get table metadata"""
        try:
            response = requests.get(f"{self.pipeline_api_url}/tables/{table_name}", timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"  ❌ Failed to get metadata: {e}")
            return {}
    
    def validate_csv_file(self, csv_path: str, expected_rows: int = None) -> Dict[str, Any]:
        """Validate CSV file exists and has correct structure"""
        print(f"\n🔍 Validating CSV file: {csv_path}")
        
        validation = {
            'file_exists': False,
            'row_count': 0,
            'column_count': 0,
            'columns': [],
            'sample_data': [],
            'valid': False
        }
        
        try:
            path = Path(csv_path)
            if not path.exists():
                print(f"  ❌ File does not exist: {csv_path}")
                return validation
            
            validation['file_exists'] = True
            
            # Read CSV
            df = pd.read_csv(path)
            validation['row_count'] = len(df)
            validation['column_count'] = len(df.columns)
            validation['columns'] = list(df.columns)
            validation['sample_data'] = df.head(3).to_dict('records')
            
            # Check expected rows
            if expected_rows is not None:
                if validation['row_count'] == expected_rows:
                    print(f"  ✅ Row count matches: {validation['row_count']}")
                else:
                    print(f"  ⚠️  Row count mismatch: expected {expected_rows}, got {validation['row_count']}")
            
            validation['valid'] = True
            print(f"  ✅ CSV is valid")
            print(f"     Rows: {validation['row_count']}")
            print(f"     Columns: {validation['column_count']}")
            print(f"     Columns: {', '.join(validation['columns'][:5])}...")
            
        except Exception as e:
            print(f"  ❌ Validation failed: {e}")
            validation['error'] = str(e)
        
        return validation
    
    def query_table(self, csv_path: str, query_type: str = "summary") -> Dict[str, Any]:
        """Query table data"""
        print(f"\n🔎 Querying table: {csv_path}")
        
        try:
            df = pd.read_csv(csv_path)
            
            if query_type == "summary":
                result = {
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'columns': list(df.columns),
                    'dtypes': df.dtypes.astype(str).to_dict(),
                    'null_counts': df.isnull().sum().to_dict(),
                    'numeric_summary': {}
                }
                
                # Add numeric summaries
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    result['numeric_summary'] = df[numeric_cols].describe().to_dict()
                
                print(f"  ✅ Query successful")
                print(f"     Rows: {result['row_count']}")
                print(f"     Columns: {result['column_count']}")
                
            elif query_type == "sample":
                result = {
                    'sample': df.head(10).to_dict('records')
                }
                print(f"  ✅ Retrieved {len(result['sample'])} sample rows")
                
            elif query_type == "aggregate":
                # Simple aggregation
                numeric_cols = df.select_dtypes(include=['number']).columns
                result = {
                    'aggregations': {}
                }
                for col in numeric_cols[:3]:  # Limit to first 3 numeric columns
                    result['aggregations'][col] = {
                        'sum': float(df[col].sum()),
                        'mean': float(df[col].mean()),
                        'min': float(df[col].min()),
                        'max': float(df[col].max())
                    }
                print(f"  ✅ Aggregation successful")
            
            else:
                result = {'error': f'Unknown query type: {query_type}'}
            
            return result
            
        except Exception as e:
            print(f"  ❌ Query failed: {e}")
            return {'error': str(e)}
    
    def run_complete_flow(self):
        """Run complete user flow test"""
        print("=" * 70)
        print("🚀 COMPLETE PIPELINE FLOW TEST")
        print("=" * 70)
        
        # Step 1: Generate dummy API data
        print("\n" + "=" * 70)
        print("STEP 1: Generating Dummy API Data")
        print("=" * 70)
        
        transaction_data = self.generate_dummy_api_data("transactions")
        customer_data = self.generate_dummy_api_data("customers")
        order_data = self.generate_dummy_api_data("orders")
        
        print(f"✅ Generated {len(transaction_data)} transactions")
        print(f"✅ Generated {len(customer_data)} customers")
        print(f"✅ Generated {len(order_data)} orders")
        
        # Step 2: Ingest data through pipeline
        print("\n" + "=" * 70)
        print("STEP 2: Ingesting Data Through Pipeline")
        print("=" * 70)
        
        # Ingest transactions (simple)
        tx_result = self.test_pipeline_ingestion(
            data=transaction_data,
            source_name="transactions_api",
            table_name="transactions"
        )
        
        # Ingest customers (simple)
        cust_result = self.test_pipeline_ingestion(
            data=customer_data,
            source_name="customers_api",
            table_name="customers"
        )
        
        # Ingest orders with aggregation
        order_result = self.test_aggregated_ingestion(
            data=order_data,
            source_name="orders_api",
            group_by=["customer_id", "status"],
            metrics={"total_amount": "sum", "quantity": "sum", "avg_price": "mean"}
        )
        
        # Step 3: Validate outputs
        print("\n" + "=" * 70)
        print("STEP 3: Validating Outputs")
        print("=" * 70)
        
        validations = []
        if tx_result.get('success'):
            tx_validation = self.validate_csv_file(
                tx_result.get('csv_path', ''),
                expected_rows=len(transaction_data)
            )
            validations.append(('transactions', tx_validation))
        
        if cust_result.get('success'):
            cust_validation = self.validate_csv_file(
                cust_result.get('csv_path', ''),
                expected_rows=len(customer_data)
            )
            validations.append(('customers', cust_validation))
        
        if order_result.get('success'):
            order_validation = self.validate_csv_file(
                order_result.get('csv_path', ''),
                expected_rows=None  # Aggregated, so count will be different
            )
            validations.append(('orders_aggregated', order_validation))
        
        # Step 4: Query tables
        print("\n" + "=" * 70)
        print("STEP 4: Querying Tables")
        print("=" * 70)
        
        queries = []
        for table_name, validation in validations:
            if validation.get('valid'):
                csv_path = validation.get('csv_path', '')
                if not csv_path:
                    # Find CSV path from created_tables
                    for table in self.created_tables:
                        if table['table_name'] == table_name or table['source_name'] == table_name:
                            csv_path = table['csv_path']
                            break
                
                if csv_path:
                    summary = self.query_table(csv_path, "summary")
                    sample = self.query_table(csv_path, "sample")
                    queries.append((table_name, summary, sample))
        
        # Step 5: List all tables
        print("\n" + "=" * 70)
        print("STEP 5: Listing All Tables")
        print("=" * 70)
        
        all_tables = self.list_tables()
        print(f"✅ Found {len(all_tables)} tables")
        for table in all_tables[:10]:  # Show first 10
            print(f"   - {table.get('table_name', 'N/A')} ({table.get('row_count', 0)} rows)")
        
        # Step 6: Summary
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        
        print(f"\n✅ Created Tables: {len(self.created_tables)}")
        for table in self.created_tables:
            print(f"   - {table['table_name']}: {table['rows']} rows")
        
        print(f"\n✅ Validated Files: {sum(1 for _, v in validations if v.get('valid'))}")
        print(f"\n✅ Successful Queries: {len(queries)}")
        
        # Return results for further analysis
        return {
            'created_tables': self.created_tables,
            'validations': validations,
            'queries': queries,
            'all_tables': all_tables
        }


def interactive_menu():
    """Interactive menu for user to choose actions"""
    tester = PipelineFlowTester()
    
    while True:
        print("\n" + "=" * 70)
        print("PIPELINE FLOW TESTER - INTERACTIVE MENU")
        print("=" * 70)
        print("1. Generate and ingest transaction data")
        print("2. Generate and ingest customer data")
        print("3. Generate and ingest order data (with aggregation)")
        print("4. Run complete flow test")
        print("5. List all tables")
        print("6. Query a specific table")
        print("7. Validate all outputs")
        print("8. Exit")
        
        choice = input("\nEnter your choice (1-8): ").strip()
        
        if choice == "1":
            data = tester.generate_dummy_api_data("transactions")
            result = tester.test_pipeline_ingestion(data, "transactions_api", "transactions")
            if result.get('success'):
                tester.validate_csv_file(result.get('csv_path', ''))
        
        elif choice == "2":
            data = tester.generate_dummy_api_data("customers")
            result = tester.test_pipeline_ingestion(data, "customers_api", "customers")
            if result.get('success'):
                tester.validate_csv_file(result.get('csv_path', ''))
        
        elif choice == "3":
            data = tester.generate_dummy_api_data("orders")
            result = tester.test_aggregated_ingestion(
                data, "orders_api",
                group_by=["customer_id", "status"],
                metrics={"total_amount": "sum", "quantity": "sum"}
            )
            if result.get('success'):
                tester.validate_csv_file(result.get('csv_path', ''))
        
        elif choice == "4":
            tester.run_complete_flow()
        
        elif choice == "5":
            tables = tester.list_tables()
            print(f"\nFound {len(tables)} tables:")
            for table in tables:
                print(f"  - {table.get('table_name')}: {table.get('row_count', 0)} rows")
        
        elif choice == "6":
            table_name = input("Enter table name: ").strip()
            metadata = tester.get_table_metadata(table_name)
            if metadata.get('success'):
                csv_path = metadata['metadata']['storage']['path']
                query_type = input("Query type (summary/sample/aggregate): ").strip() or "summary"
                tester.query_table(csv_path, query_type)
            else:
                print(f"Table not found: {table_name}")
        
        elif choice == "7":
            tables = tester.list_tables()
            for table in tables:
                table_name = table.get('table_name')
                csv_path = table.get('csv_path')
                if csv_path:
                    tester.validate_csv_file(csv_path)
        
        elif choice == "8":
            print("Exiting...")
            break
        
        else:
            print("Invalid choice. Please try again.")


if __name__ == '__main__':
    import sys
    
    # Check if pipeline API is running
    try:
        response = requests.get("http://localhost:8082/api/pipeline/health", timeout=5)
        if response.status_code != 200:
            print("⚠️  Pipeline API server may not be running.")
            print("   Start it with: python pipeline_api_server.py")
            sys.exit(1)
    except requests.exceptions.RequestException:
        print("❌ Cannot connect to Pipeline API server.")
        print("   Make sure it's running on http://localhost:8082")
        print("   Start it with: python pipeline_api_server.py")
        sys.exit(1)
    
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        interactive_menu()
    else:
        # Run complete flow test
        tester = PipelineFlowTester()
        results = tester.run_complete_flow()
        
        # Save results
        results_file = Path("test_results.json")
        with open(results_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'created_tables': results['created_tables'],
                'validation_summary': {
                    name: {
                        'valid': v.get('valid'),
                        'row_count': v.get('row_count'),
                        'column_count': v.get('column_count')
                    }
                    for name, v in results['validations']
                }
            }, f, indent=2)
        
        print(f"\n💾 Test results saved to: {results_file}")

