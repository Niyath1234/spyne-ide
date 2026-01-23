#!/usr/bin/env python3
"""
ER Diagram Flow Test
Tests dummy APIs with 10 records each, mapping to columns based on ER diagram logic,
and verifies data flows to tables and complex queries work.
"""

import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import random
from typing import Dict, List, Any, Optional


class ERDiagramFlowTester:
    """Test ER diagram-based data flow and querying"""
    
    def __init__(
        self, 
        pipeline_api_url: str = "http://localhost:8082/api/pipeline",
        rca_api_url: str = "http://localhost:8080/api",
        knowledge_base_api_url: str = "http://localhost:8083/api/knowledge-base"
    ):
        self.pipeline_api_url = pipeline_api_url
        self.rca_api_url = rca_api_url
        self.knowledge_base_api_url = knowledge_base_api_url
        self.created_tables = []
        self.er_entities = {}  # Hardcoded ER diagram entities
        self.er_relationships = []
        self._initialize_er_diagram()
    
    def _initialize_er_diagram(self):
        """Initialize hardcoded ER diagram structure"""
        # Entity definitions based on ER diagram logic
        self.er_entities = {
            "customer": {
                "id": "customer",
                "name": "Customer",
                "description": "Customer entity",
                "grain": ["customer_id"],
                "attributes": ["customer_id", "customer_name", "customer_type", "registration_date"]
            },
            "account": {
                "id": "account",
                "name": "Account",
                "description": "Customer account entity",
                "grain": ["customer_id", "account_id"],
                "attributes": ["customer_id", "account_id", "account_balance", "account_status", "account_type"]
            },
            "transaction": {
                "id": "transaction",
                "name": "Transaction",
                "description": "Transaction entity",
                "grain": ["customer_id", "transaction_id"],
                "attributes": ["customer_id", "transaction_id", "transaction_date", "transaction_amount", "transaction_type"]
            }
        }
        
        # Relationships
        self.er_relationships = [
            {
                "from_entity": "customer",
                "to_entity": "account",
                "type": "one-to-many",
                "description": "Customer has many accounts",
                "join_keys": {"customer_id": "customer_id"}
            },
            {
                "from_entity": "customer",
                "to_entity": "transaction",
                "type": "one-to-many",
                "description": "Customer has many transactions",
                "join_keys": {"customer_id": "customer_id"}
            }
        ]
        
        # Joins for knowledge base
        self.er_joins = [
            {
                "from_table": "customer_accounts_er",
                "to_table": "transactions_er",
                "join_type": "left_join",
                "keys": {"customer_id": "customer_id"},
                "description": "Join customer accounts with transactions",
                "condition": "customer_accounts_er.customer_id = transactions_er.customer_id"
            }
        ]
    
    def generate_api_1_data(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        Generate first dummy API data (10 records)
        Maps to Customer entity with Account relationships
        """
        data = []
        base_date = datetime.now() - timedelta(days=365)
        
        for i in range(count):
            customer_id = f"CUST{1000 + i}"
            # Generate customer master data
            customer_record = {
                "customer_id": customer_id,
                "customer_name": f"Customer {i+1}",
                "customer_type": random.choice(["INDIVIDUAL", "CORPORATE", "SME"]),
                "registration_date": (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
                # Account data (one-to-many relationship)
                "account_id": f"ACC{1000 + i}",
                "account_balance": round(random.uniform(1000.0, 100000.0), 2),
                "account_status": random.choice(["ACTIVE", "CLOSED", "SUSPENDED"]),
                "account_type": random.choice(["SAVINGS", "CHECKING", "BUSINESS"])
            }
            data.append(customer_record)
        
        return data
    
    def generate_api_2_data(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        Generate second dummy API data (10 records)
        Maps to Transaction entity with Customer relationships
        """
        data = []
        base_date = datetime.now() - timedelta(days=30)
        
        # Use customer IDs from API 1
        customer_ids = [f"CUST{1000 + i}" for i in range(10)]
        
        for i in range(count):
            transaction_record = {
                "customer_id": random.choice(customer_ids),  # Link to customer
                "transaction_id": f"TXN{2000 + i}",
                "transaction_date": (base_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
                "transaction_amount": round(random.uniform(10.0, 5000.0), 2),
                "transaction_type": random.choice(["DEBIT", "CREDIT", "TRANSFER", "PAYMENT"]),
                "transaction_status": random.choice(["SUCCESS", "PENDING", "FAILED"]),
                "payment_method": random.choice(["CARD", "BANK_TRANSFER", "WALLET"])
            }
            data.append(transaction_record)
        
        return data
    
    def map_to_er_columns(self, data: List[Dict], entity_name: str) -> List[Dict]:
        """
        Map API data to columns based on ER diagram entity definition
        """
        if entity_name not in self.er_entities:
            return data  # Return as-is if entity not found
        
        entity = self.er_entities[entity_name]
        mapped_data = []
        
        for record in data:
            mapped_record = {}
            # Map only attributes defined in ER diagram
            for attr in entity["attributes"]:
                if attr in record:
                    mapped_record[attr] = record[attr]
                else:
                    # Set default if attribute missing
                    mapped_record[attr] = None
            
            mapped_data.append(mapped_record)
        
        return mapped_data
    
    def ingest_api_1(self) -> Dict[str, Any]:
        """Ingest first API data through pipeline"""
        print("\n" + "=" * 70)
        print("API 1: Customer & Account Data (10 records)")
        print("=" * 70)
        
        # Generate data
        raw_data = self.generate_api_1_data(10)
        print(f"✅ Generated {len(raw_data)} records")
        
        # Map to ER diagram columns
        mapped_data = self.map_to_er_columns(raw_data, "customer")
        print(f"✅ Mapped to ER entity 'customer' with {len(mapped_data[0])} columns")
        print(f"   Columns: {', '.join(mapped_data[0].keys())}")
        
        # Ingest through pipeline
        payload = {
            "data": mapped_data,
            "source_name": "customer_accounts_api",
            "table_name": "customer_accounts_er",
            "metadata": {
                "entity": "customer",
                "description": "Customer and account data mapped from ER diagram",
                "er_entity": "customer",
                "grain": self.er_entities["customer"]["grain"]
            }
        }
        
        try:
            response = requests.post(
                f"{self.pipeline_api_url}/ingest",
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get('success'):
                self.created_tables.append({
                    'table_name': result['table_name'],
                    'source_name': 'customer_accounts_api',
                    'entity': 'customer',
                    'csv_path': result.get('csv_path'),
                    'rows': result.get('rows_output', 0),
                    'columns': result.get('columns', [])
                })
                print(f"✅ Successfully ingested to table: {result['table_name']}")
                print(f"   Rows: {result.get('rows_output', 0)}")
                print(f"   CSV: {result.get('csv_path', 'N/A')}")
                return result
            else:
                print(f"❌ Failed: {result.get('error', 'Unknown error')}")
                return result
                
        except Exception as e:
            print(f"❌ Request failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def ingest_api_2(self) -> Dict[str, Any]:
        """Ingest second API data through pipeline"""
        print("\n" + "=" * 70)
        print("API 2: Transaction Data (10 records)")
        print("=" * 70)
        
        # Generate data
        raw_data = self.generate_api_2_data(10)
        print(f"✅ Generated {len(raw_data)} records")
        
        # Map to ER diagram columns
        mapped_data = self.map_to_er_columns(raw_data, "transaction")
        print(f"✅ Mapped to ER entity 'transaction' with {len(mapped_data[0])} columns")
        print(f"   Columns: {', '.join(mapped_data[0].keys())}")
        
        # Ingest through pipeline
        payload = {
            "data": mapped_data,
            "source_name": "transactions_api",
            "table_name": "transactions_er",
            "metadata": {
                "entity": "transaction",
                "description": "Transaction data mapped from ER diagram",
                "er_entity": "transaction",
                "grain": self.er_entities["transaction"]["grain"]
            }
        }
        
        try:
            response = requests.post(
                f"{self.pipeline_api_url}/ingest",
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get('success'):
                self.created_tables.append({
                    'table_name': result['table_name'],
                    'source_name': 'transactions_api',
                    'entity': 'transaction',
                    'csv_path': result.get('csv_path'),
                    'rows': result.get('rows_output', 0),
                    'columns': result.get('columns', [])
                })
                print(f"✅ Successfully ingested to table: {result['table_name']}")
                print(f"   Rows: {result.get('rows_output', 0)}")
                print(f"   CSV: {result.get('csv_path', 'N/A')}")
                return result
            else:
                print(f"❌ Failed: {result.get('error', 'Unknown error')}")
                return result
                
        except Exception as e:
            print(f"❌ Request failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def create_er_diagram_json(self) -> Dict[str, Any]:
        """Create ER diagram JSON from hardcoded entities for knowledge base enrichment"""
        er_diagram = {
            "entities": [
                {
                    "name": "customer_accounts_er",
                    "description": "Customer and account data mapped from ER diagram",
                    "primary_key": ["customer_id"],
                    "attributes": ["customer_id", "customer_name", "customer_type", "registration_date", 
                                  "account_id", "account_balance", "account_status", "account_type"]
                },
                {
                    "name": "transactions_er",
                    "description": "Transaction data mapped from ER diagram",
                    "primary_key": ["customer_id", "transaction_id"],
                    "attributes": ["customer_id", "transaction_id", "transaction_date", 
                                  "transaction_amount", "transaction_type"]
                }
            ],
            "relationships": [
                {
                    "from_entity": "customer_accounts_er",
                    "to_entity": "transactions_er",
                    "type": "one-to-many",
                    "description": "Customer accounts have many transactions",
                    "join_keys": {"customer_id": "customer_id"}
                }
            ],
            "joins": [
                {
                    "from_table": "customer_accounts_er",
                    "to_table": "transactions_er",
                    "join_type": "left_join",
                    "keys": {"customer_id": "customer_id"},
                    "description": "Join customer accounts with transactions on customer_id",
                    "condition": "customer_accounts_er.customer_id = transactions_er.customer_id"
                }
            ]
        }
        return er_diagram
    
    def enrich_knowledge_base(self) -> Dict[str, Any]:
        """Enrich knowledge base with ER diagram and table information"""
        print("\n" + "=" * 70)
        print("ENRICHING KNOWLEDGE BASE")
        print("=" * 70)
        
        # Create ER diagram JSON
        er_diagram = self.create_er_diagram_json()
        
        # Save ER diagram to temp file
        er_file = Path("data/temp_er_diagram.json")
        er_file.parent.mkdir(parents=True, exist_ok=True)
        with open(er_file, 'w') as f:
            json.dump(er_diagram, f, indent=2)
        
        print(f"✅ Created ER diagram JSON: {er_file}")
        
        # Enrich knowledge base via API
        try:
            payload = {
                "er_diagram_path": str(er_file.absolute())
            }
            
            response = requests.post(
                f"{self.knowledge_base_api_url}/enrich",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Knowledge base enriched successfully")
                print(f"   Added tables: {result.get('added_tables', 0)}")
                print(f"   Added relationships: {result.get('added_relationships', 0)}")
                print(f"   Added joins: {result.get('added_joins', 0)}")
                return result
            else:
                print(f"⚠️  Knowledge base enrichment returned {response.status_code}")
                print(f"   Response: {response.text[:200]}")
                return {'success': False, 'error': f"HTTP {response.status_code}"}
                
        except Exception as e:
            print(f"⚠️  Knowledge base enrichment failed: {e}")
            print(f"   (This is optional - continuing without it)")
            return {'success': False, 'error': str(e)}
    
    def verify_knowledge_base(self) -> Dict[str, Any]:
        """Verify knowledge base has nodes, relationships, and rules"""
        print("\n" + "=" * 70)
        print("VERIFYING KNOWLEDGE BASE")
        print("=" * 70)
        
        verification = {
            'nodes_found': [],
            'relationships_found': [],
            'joins_found': [],
            'rules_found': [],
            'all_valid': True
        }
        
        try:
            # Get knowledge base stats
            response = requests.get(f"{self.knowledge_base_api_url}/stats", timeout=10)
            if response.status_code == 200:
                stats = response.json().get('stats', {})
                print(f"✅ Knowledge Base Stats:")
                print(f"   Tables (Nodes): {stats.get('tables_count', 0)}")
                print(f"   Relationships: {stats.get('relationships_count', 0)}")
                print(f"   Joins: {stats.get('joins_count', 0)}")
                print(f"   Business Rules: {stats.get('business_rules_count', 0)}")
                print(f"   Terms: {stats.get('terms_count', 0)}")
                
                verification['nodes_found'] = stats.get('tables_count', 0)
                verification['relationships_found'] = stats.get('relationships_count', 0)
                verification['joins_found'] = stats.get('joins_count', 0)
                verification['rules_found'] = stats.get('business_rules_count', 0)
                
                # Check if our tables are in knowledge base
                if stats.get('tables_count', 0) >= 2:
                    print(f"   ✅ Found at least 2 tables (nodes)")
                else:
                    print(f"   ⚠️  Expected at least 2 tables")
                    verification['all_valid'] = False
                
                # Check joins
                if stats.get('joins_count', 0) > 0:
                    print(f"   ✅ Found {stats.get('joins_count', 0)} joins")
                else:
                    print(f"   ⚠️  No joins found")
                
            else:
                print(f"⚠️  Could not get knowledge base stats: {response.status_code}")
                verification['all_valid'] = False
                
            # Get joins
            try:
                response = requests.get(f"{self.knowledge_base_api_url}/joins", timeout=10)
                if response.status_code == 200:
                    joins = response.json().get('joins', [])
                    verification['joins_found'] = joins
                    print(f"✅ Found {len(joins)} join specifications")
                    for join in joins[:3]:  # Show first 3
                        print(f"   - {join.get('from_table', 'N/A')} → {join.get('to_table', 'N/A')}")
            except Exception as e:
                print(f"⚠️  Could not get joins: {e}")
                
        except Exception as e:
            print(f"⚠️  Knowledge base verification failed: {e}")
            print(f"   (Knowledge base API may not be running)")
            verification['all_valid'] = False
        
        return verification
    
    def verify_tables_created(self) -> Dict[str, Any]:
        """Verify tables were created and registered"""
        print("\n" + "=" * 70)
        print("VERIFYING TABLES CREATED")
        print("=" * 70)
        
        verification = {
            'tables_found': [],
            'csv_files_exist': [],
            'metadata_registered': [],
            'all_valid': True
        }
        
        # Check pipeline API tables
        try:
            response = requests.get(f"{self.pipeline_api_url}/tables", timeout=10)
            response.raise_for_status()
            result = response.json()
            tables = result.get('tables', [])
            
            print(f"✅ Found {len(tables)} tables in pipeline API")
            
            for table in tables:
                table_name = table.get('table_name', '')
                if 'er' in table_name.lower():
                    verification['tables_found'].append(table_name)
                    print(f"   - {table_name}: {table.get('row_count', 0)} rows")
                    
                    # Check CSV file exists
                    csv_path = table.get('csv_path', '')
                    if csv_path and Path(csv_path).exists():
                        verification['csv_files_exist'].append(table_name)
                        print(f"     ✅ CSV exists: {csv_path}")
                    else:
                        print(f"     ❌ CSV missing: {csv_path}")
                        verification['all_valid'] = False
        except Exception as e:
            print(f"❌ Failed to list tables: {e}")
            verification['all_valid'] = False
        
        # Check metadata/tables.json
        tables_json_path = Path("metadata/tables.json")
        if tables_json_path.exists():
            try:
                with open(tables_json_path, 'r') as f:
                    tables_data = json.load(f)
                    tables_list = tables_data.get('tables', []) if isinstance(tables_data, dict) else tables_data
                    
                    er_tables = [t for t in tables_list if 'er' in t.get('name', '').lower()]
                    verification['metadata_registered'] = [t.get('name') for t in er_tables]
                    print(f"✅ Found {len(er_tables)} ER tables in metadata")
                    for table in er_tables:
                        print(f"   - {table.get('name')}: {table.get('row_count', 0)} rows")
            except Exception as e:
                print(f"⚠️  Failed to read metadata: {e}")
        
        return verification
    
    def test_complex_query(self, query: str) -> Dict[str, Any]:
        """Test a complex query using natural language query system"""
        print(f"\n🔍 Testing Complex Query:")
        print(f"   Query: {query}")
        
        try:
            # Try assistant endpoint first
            response = requests.post(
                f"{self.rca_api_url}/assistant/ask",
                json={"question": query},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                answer = result.get('answer', 'N/A')
                print(f"   ✅ Query successful")
                print(f"   Answer: {answer[:300]}...")
                return {
                    'success': True,
                    'answer': answer,
                    'method': 'assistant'
                }
            else:
                print(f"   ⚠️  Assistant endpoint returned {response.status_code}")
                
                # Try query/execute endpoint
                response2 = requests.post(
                    f"{self.rca_api_url}/query/execute",
                    json={"query": query, "mode": "nlq"},
                    timeout=30
                )
                
                if response2.status_code == 200:
                    result2 = response2.json()
                    print(f"   ✅ Query executed")
                    return {
                        'success': True,
                        'result': result2,
                        'method': 'query_execute'
                    }
                else:
                    print(f"   ❌ Query failed: {response2.status_code}")
                    return {'success': False, 'error': f"HTTP {response2.status_code}"}
                    
        except Exception as e:
            print(f"   ❌ Query error: {e}")
            return {'success': False, 'error': str(e)}
    
    def run_complete_test(self):
        """Run complete ER diagram flow test"""
        print("=" * 70)
        print("🚀 ER DIAGRAM FLOW TEST")
        print("=" * 70)
        print("\nThis test will:")
        print("1. Generate 2 dummy APIs with 10 records each")
        print("2. Map them to columns based on ER diagram logic")
        print("3. Ingest through pipeline API")
        print("4. Verify tables are created and data flows correctly")
        print("5. Enrich knowledge base with ER diagram (nodes, relationships, rules)")
        print("6. Verify knowledge base is populated correctly")
        print("7. Test complex queries")
        
        # Step 1: Ingest API 1
        api1_result = self.ingest_api_1()
        
        # Step 2: Ingest API 2
        api2_result = self.ingest_api_2()
        
        # Step 3: Verify tables
        verification = self.verify_tables_created()
        
        # Step 4: Enrich knowledge base with ER diagram
        kb_enrichment = self.enrich_knowledge_base()
        
        # Step 5: Verify knowledge base (nodes, relationships, rules)
        kb_verification = self.verify_knowledge_base()
        
        # Step 6: Test complex queries
        print("\n" + "=" * 70)
        print("TESTING COMPLEX QUERIES")
        print("=" * 70)
        
        complex_queries = [
            "How many customer records are in the customer_accounts_er table?",
            "What is the total transaction amount in transactions_er?",
            "Show me the average account balance by customer type",
            "What are the different transaction types and their counts?",
            "Can you join customer_accounts_er and transactions_er on customer_id and show total transactions per customer?",
            "What is the distribution of account statuses in customer_accounts_er?",
            "Show me customers with account balance greater than 50000",
            "What is the total transaction amount grouped by transaction type?",
            "How many transactions does each customer have?",
            "What is the relationship between customer accounts and transactions?"
        ]
        
        query_results = []
        for query in complex_queries:
            result = self.test_complex_query(query)
            query_results.append({
                'query': query,
                'success': result.get('success', False),
                'answer': result.get('answer', result.get('result', 'N/A'))
            })
            # Small delay between queries
            import time
            time.sleep(1)
        
        # Summary
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        
        print(f"\n✅ API 1 Ingestion: {'Success' if api1_result.get('success') else 'Failed'}")
        print(f"✅ API 2 Ingestion: {'Success' if api2_result.get('success') else 'Failed'}")
        print(f"✅ Tables Created: {len(verification['tables_found'])}")
        print(f"✅ CSV Files Exist: {len(verification['csv_files_exist'])}")
        print(f"✅ Metadata Registered: {len(verification['metadata_registered'])}")
        
        print(f"\n✅ Knowledge Base Enrichment: {'Success' if kb_enrichment.get('success') else 'Partial/Failed'}")
        print(f"✅ Knowledge Base Nodes (Tables): {kb_verification.get('nodes_found', 0)}")
        print(f"✅ Knowledge Base Relationships: {kb_verification.get('relationships_found', 0)}")
        print(f"✅ Knowledge Base Joins: {kb_verification.get('joins_found', 0) if isinstance(kb_verification.get('joins_found'), int) else len(kb_verification.get('joins_found', []))}")
        print(f"✅ Knowledge Base Rules: {kb_verification.get('rules_found', 0)}")
        
        successful_queries = sum(1 for q in query_results if q.get('success'))
        print(f"\n✅ Complex Queries: {successful_queries}/{len(query_results)} successful")
        
        print(f"\n📊 Created Tables:")
        for table in self.created_tables:
            print(f"   - {table['table_name']}: {table['rows']} rows, {len(table['columns'])} columns")
            print(f"     Entity: {table['entity']}")
            print(f"     CSV: {table['csv_path']}")
        
        print(f"\n📝 Query Results:")
        for i, qr in enumerate(query_results, 1):
            status = "✅" if qr.get('success') else "❌"
            print(f"   {status} Query {i}: {qr['query'][:60]}...")
        
        return {
            'api1_result': api1_result,
            'api2_result': api2_result,
            'verification': verification,
            'kb_enrichment': kb_enrichment,
            'kb_verification': kb_verification,
            'query_results': query_results,
            'created_tables': self.created_tables
        }


def main():
    """Main test execution"""
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
    
    # Check if RCA API is running (optional)
    try:
        response = requests.get("http://localhost:8080/api/health", timeout=5)
        print("✅ RCA API server is running")
    except requests.exceptions.RequestException:
        print("⚠️  RCA API server may not be running.")
        print("   Complex queries may not work without it.")
        print("   Start it with: cargo run --bin server")
    
    # Check if Knowledge Base API is running (optional)
    try:
        response = requests.get("http://localhost:8083/api/knowledge-base/health", timeout=5)
        print("✅ Knowledge Base API server is running")
    except requests.exceptions.RequestException:
        print("⚠️  Knowledge Base API server may not be running.")
        print("   Knowledge base enrichment will be skipped.")
        print("   Start it with: python knowledge_base_api.py")
    
    # Run test
    tester = ERDiagramFlowTester()
    results = tester.run_complete_test()
    
    # Save results
    results_file = Path("er_diagram_test_results.json")
    with open(results_file, 'w') as f:
            json.dump({
            'timestamp': datetime.now().isoformat(),
            'test_type': 'er_diagram_flow',
            'results': {
                'api1_success': results['api1_result'].get('success', False),
                'api2_success': results['api2_result'].get('success', False),
                'tables_created': len(results['created_tables']),
                'kb_enrichment_success': results['kb_enrichment'].get('success', False),
                'kb_nodes': results['kb_verification'].get('nodes_found', 0),
                'kb_relationships': results['kb_verification'].get('relationships_found', 0),
                'kb_joins': results['kb_verification'].get('joins_found', 0) if isinstance(results['kb_verification'].get('joins_found'), int) else len(results['kb_verification'].get('joins_found', [])),
                'kb_rules': results['kb_verification'].get('rules_found', 0),
                'queries_successful': sum(1 for q in results['query_results'] if q.get('success')),
                'queries_total': len(results['query_results'])
            },
            'created_tables': results['created_tables'],
            'kb_enrichment': results['kb_enrichment'],
            'kb_verification': results['kb_verification'],
            'query_results': results['query_results']
        }, f, indent=2)
    
    print(f"\n💾 Test results saved to: {results_file}")


if __name__ == '__main__':
    main()

