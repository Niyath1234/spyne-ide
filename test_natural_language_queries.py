#!/usr/bin/env python3
"""
Natural Language Query Test
Tests natural language querying capabilities with pipeline data
"""

import requests
import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from test_pipeline_flow import PipelineFlowTester


class NaturalLanguageQueryTester:
    """Test natural language query capabilities"""
    
    def __init__(self, 
                 pipeline_api_url: str = "http://localhost:8082/api/pipeline",
                 rca_api_url: str = "http://localhost:8080"):
        self.pipeline_api_url = pipeline_api_url
        self.rca_api_url = rca_api_url
        self.created_tables = []
    
    def setup_pipeline_data(self):
        """Setup pipeline data first"""
        print("=" * 70)
        print("SETUP: Creating Pipeline Data")
        print("=" * 70)
        
        tester = PipelineFlowTester(self.pipeline_api_url)
        
        # Generate and ingest test data
        transaction_data = tester.generate_dummy_api_data("transactions")
        customer_data = tester.generate_dummy_api_data("customers")
        order_data = tester.generate_dummy_api_data("orders")
        
        # Ingest data
        tx_result = tester.test_pipeline_ingestion(
            data=transaction_data,
            source_name="transactions_api",
            table_name="transactions"
        )
        
        cust_result = tester.test_pipeline_ingestion(
            data=customer_data,
            source_name="customers_api",
            table_name="customers"
        )
        
        order_result = tester.test_aggregated_ingestion(
            data=order_data,
            source_name="orders_api",
            group_by=["customer_id", "status"],
            metrics={"total_amount": "sum", "quantity": "sum"}
        )
        
        # Store created tables
        if tx_result.get('success'):
            self.created_tables.append({
                'name': 'transactions',
                'source': 'transactions_api',
                'rows': len(transaction_data)
            })
        
        if cust_result.get('success'):
            self.created_tables.append({
                'name': 'customers',
                'source': 'customers_api',
                'rows': len(customer_data)
            })
        
        if order_result.get('success'):
            self.created_tables.append({
                'name': 'orders_aggregated',
                'source': 'orders_api',
                'rows': len(order_data)
            })
        
        print(f"\n✅ Setup complete: {len(self.created_tables)} tables created")
        time.sleep(2)  # Wait for metadata to be written
    
    def test_assistant_query(self, question: str) -> Dict[str, Any]:
        """Test natural language query via assistant endpoint"""
        print(f"\n💬 Testing Assistant Query:")
        print(f"   Question: {question}")
        
        try:
            payload = {"question": question}
            response = requests.post(
                f"{self.rca_api_url}/api/assistant/ask",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Query successful")
                print(f"   Answer: {result.get('answer', 'N/A')[:200]}...")
                return result
            else:
                print(f"   ❌ Query failed: {response.status_code}")
                print(f"   Response: {response.text[:200]}")
                return {'success': False, 'error': f"HTTP {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def test_reasoning_assess(self, query: str) -> Dict[str, Any]:
        """Test query assessment"""
        print(f"\n🔍 Testing Query Assessment:")
        print(f"   Query: {query}")
        
        try:
            payload = {"query": query}
            response = requests.post(
                f"{self.rca_api_url}/api/reasoning/assess",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                status = result.get('status', 'unknown')
                print(f"   ✅ Assessment successful")
                print(f"   Status: {status}")
                
                if status == "success":
                    print(f"   ✅ Query understood")
                elif status == "needs_clarification":
                    print(f"   ⚠️  Needs clarification: {result.get('question', 'N/A')}")
                else:
                    print(f"   ❌ Query failed: {result.get('error', 'N/A')}")
                
                return result
            else:
                print(f"   ❌ Assessment failed: {response.status_code}")
                return {'success': False, 'error': f"HTTP {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def test_query_execute(self, query: str) -> Dict[str, Any]:
        """Test SQL query execution"""
        print(f"\n🔎 Testing Query Execution:")
        print(f"   Query: {query}")
        
        try:
            payload = {"query": query, "mode": "sql"}
            response = requests.post(
                f"{self.rca_api_url}/api/query/execute",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success':
                    print(f"   ✅ Query executed successfully")
                    print(f"   Rows returned: {result.get('row_count', 0)}")
                    return result
                else:
                    print(f"   ❌ Query execution failed: {result.get('error', 'N/A')}")
                    return result
            else:
                print(f"   ❌ Execution failed: {response.status_code}")
                return {'success': False, 'error': f"HTTP {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def test_natural_language_queries(self):
        """Test various natural language queries"""
        print("\n" + "=" * 70)
        print("TESTING NATURAL LANGUAGE QUERIES")
        print("=" * 70)
        
        # Test queries about the data
        test_queries = [
            # Basic questions about tables
            {
                "type": "knowledge",
                "question": "What tables are available in the system?",
                "expected": "Should list available tables"
            },
            {
                "type": "knowledge",
                "question": "What is the transactions table?",
                "expected": "Should describe transactions table"
            },
            {
                "type": "knowledge",
                "question": "How many columns does the customers table have?",
                "expected": "Should return column count"
            },
            
            # Data queries
            {
                "type": "data",
                "question": "How many transactions are there?",
                "expected": "Should return count of transactions"
            },
            {
                "type": "data",
                "question": "What is the total amount of all transactions?",
                "expected": "Should return sum of transaction amounts"
            },
            {
                "type": "data",
                "question": "Show me the first 5 customers",
                "expected": "Should return sample customer data"
            },
            {
                "type": "data",
                "question": "What are the different transaction types?",
                "expected": "Should return unique transaction types"
            },
            {
                "type": "data",
                "question": "How many customers have status ACTIVE?",
                "expected": "Should return count of active customers"
            },
            
            # Aggregation queries
            {
                "type": "data",
                "question": "What is the average transaction amount?",
                "expected": "Should return average amount"
            },
            {
                "type": "data",
                "question": "Group transactions by status and show the count",
                "expected": "Should return grouped counts"
            },
            {
                "type": "data",
                "question": "What is the total amount by transaction type?",
                "expected": "Should return aggregated amounts by type"
            },
            
            # Complex queries
            {
                "type": "data",
                "question": "Show me transactions with amount greater than 500",
                "expected": "Should return filtered transactions"
            },
            {
                "type": "data",
                "question": "What is the maximum transaction amount?",
                "expected": "Should return max amount"
            },
            {
                "type": "data",
                "question": "How many orders are in PENDING status?",
                "expected": "Should return count of pending orders"
            }
        ]
        
        results = {
            'total_queries': len(test_queries),
            'successful': 0,
            'failed': 0,
            'needs_clarification': 0,
            'details': []
        }
        
        for i, test_query in enumerate(test_queries, 1):
            print(f"\n{'='*70}")
            print(f"Test {i}/{len(test_queries)}: {test_query['type'].upper()} Query")
            print(f"{'='*70}")
            
            question = test_query['question']
            query_type = test_query['type']
            
            # First assess the query
            assess_result = self.test_reasoning_assess(question)
            
            # Then try to get answer via assistant
            assistant_result = self.test_assistant_query(question)
            
            # Record results
            test_result = {
                'question': question,
                'type': query_type,
                'expected': test_query['expected'],
                'assess_status': assess_result.get('status', 'unknown'),
                'assistant_success': assistant_result.get('answer') is not None,
                'has_answer': 'answer' in assistant_result or 'result' in assistant_result
            }
            
            if test_result['assistant_success'] or test_result['has_answer']:
                results['successful'] += 1
                test_result['status'] = 'success'
            elif test_result['assess_status'] == 'needs_clarification':
                results['needs_clarification'] += 1
                test_result['status'] = 'needs_clarification'
            else:
                results['failed'] += 1
                test_result['status'] = 'failed'
            
            results['details'].append(test_result)
            
            # Show summary
            print(f"\n   📊 Result: {test_result['status'].upper()}")
            if test_result['has_answer']:
                answer = assistant_result.get('answer', assistant_result.get('result', ''))
                print(f"   💡 Answer preview: {str(answer)[:150]}...")
        
        return results
    
    def test_detailed_queries(self):
        """Test detailed/complex natural language queries"""
        print("\n" + "=" * 70)
        print("TESTING DETAILED NATURAL LANGUAGE QUERIES")
        print("=" * 70)
        
        detailed_queries = [
            {
                "question": "Can you show me a summary of all the transaction data including the total count, average amount, and breakdown by transaction type?",
                "description": "Complex aggregation query"
            },
            {
                "question": "I want to see customer information along with their total transaction amounts. Can you join the customers and transactions tables?",
                "description": "Join query request"
            },
            {
                "question": "What are the top 10 customers by total spending? Show me their names and total amounts.",
                "description": "Top N query with sorting"
            },
            {
                "question": "Can you analyze the transaction patterns? Show me transactions grouped by date and status, with counts and total amounts for each group.",
                "description": "Multi-level grouping"
            },
            {
                "question": "I need to understand the data structure. Can you tell me what columns are in the transactions table and what each column represents?",
                "description": "Schema exploration"
            },
            {
                "question": "Show me all failed transactions from the last week, including customer details if available.",
                "description": "Filtered query with date range"
            },
            {
                "question": "What is the distribution of transaction amounts? Can you show me min, max, average, and median?",
                "description": "Statistical analysis"
            },
            {
                "question": "How many unique customers have made transactions, and what is the average number of transactions per customer?",
                "description": "Uniqueness and aggregation"
            }
        ]
        
        results = []
        
        for i, query_info in enumerate(detailed_queries, 1):
            print(f"\n{'='*70}")
            print(f"Detailed Query {i}/{len(detailed_queries)}")
            print(f"{'='*70}")
            print(f"Description: {query_info['description']}")
            
            question = query_info['question']
            
            # Test assessment
            assess_result = self.test_reasoning_assess(question)
            
            # Test assistant
            assistant_result = self.test_assistant_query(question)
            
            # Evaluate result
            has_answer = (
                assistant_result.get('answer') is not None or
                assistant_result.get('result') is not None or
                assistant_result.get('rows') is not None
            )
            
            result = {
                'question': question,
                'description': query_info['description'],
                'assess_status': assess_result.get('status', 'unknown'),
                'has_answer': has_answer,
                'success': has_answer and assess_result.get('status') != 'failed'
            }
            
            results.append(result)
            
            if result['success']:
                print(f"   ✅ Query handled successfully")
            else:
                print(f"   ⚠️  Query may need clarification or failed")
        
        return results
    
    def run_complete_test(self):
        """Run complete natural language query test"""
        print("=" * 70)
        print("🚀 NATURAL LANGUAGE QUERY TEST - COMPLETE FLOW")
        print("=" * 70)
        
        # Step 1: Setup pipeline data
        try:
            self.setup_pipeline_data()
        except Exception as e:
            print(f"❌ Setup failed: {e}")
            print("Make sure pipeline API is running: python pipeline_api_server.py")
            return
        
        # Step 2: Check RCA API is running
        print("\n" + "=" * 70)
        print("CHECKING RCA API CONNECTION")
        print("=" * 70)
        
        try:
            response = requests.get(f"{self.rca_api_url}/api/health", timeout=5)
            if response.status_code == 200:
                print("✅ RCA API is running")
            else:
                print(f"⚠️  RCA API returned status {response.status_code}")
        except requests.exceptions.RequestException:
            print("❌ RCA API is not running")
            print("   Start it with: cargo run --bin server")
            print("   Or: cd src && cargo run --bin server")
            return
        
        # Step 3: Test basic queries
        basic_results = self.test_natural_language_queries()
        
        # Step 4: Test detailed queries
        detailed_results = self.test_detailed_queries()
        
        # Step 5: Summary
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        
        print(f"\n📊 Basic Queries:")
        print(f"   Total: {basic_results['total_queries']}")
        print(f"   ✅ Successful: {basic_results['successful']}")
        print(f"   ⚠️  Needs Clarification: {basic_results['needs_clarification']}")
        print(f"   ❌ Failed: {basic_results['failed']}")
        
        print(f"\n📊 Detailed Queries:")
        successful_detailed = sum(1 for r in detailed_results if r.get('success'))
        print(f"   Total: {len(detailed_results)}")
        print(f"   ✅ Successful: {successful_detailed}")
        print(f"   ❌ Failed: {len(detailed_results) - successful_detailed}")
        
        # Save results
        results_file = Path("natural_language_test_results.json")
        with open(results_file, 'w') as f:
            json.dump({
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'basic_queries': basic_results,
                'detailed_queries': detailed_results,
                'created_tables': self.created_tables
            }, f, indent=2)
        
        print(f"\n💾 Results saved to: {results_file}")
        
        # Overall success rate
        total_tests = basic_results['total_queries'] + len(detailed_results)
        total_successful = basic_results['successful'] + successful_detailed
        success_rate = (total_successful / total_tests * 100) if total_tests > 0 else 0
        
        print(f"\n🎯 Overall Success Rate: {success_rate:.1f}%")
        
        if success_rate >= 70:
            print("✅ Natural language querying is working well!")
        elif success_rate >= 50:
            print("⚠️  Natural language querying is partially working")
        else:
            print("❌ Natural language querying needs improvement")


if __name__ == '__main__':
    import sys
    
    tester = NaturalLanguageQueryTester()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        # Quick test with just a few queries
        tester.setup_pipeline_data()
        tester.test_assistant_query("What tables are available?")
        tester.test_assistant_query("How many transactions are there?")
    else:
        # Full test
        tester.run_complete_test()

