#!/usr/bin/env python3
"""
Test Complex Queries
Tests queries requiring multiple joins, filters, and aggregations
"""

import requests
import json
from typing import Dict, Any

RCA_API_URL = "http://localhost:8080/api"

def test_query(question: str) -> Dict[str, Any]:
    """Test a complex query"""
    print(f"\n{'='*70}")
    print(f"Query: {question}")
    print(f"{'='*70}")
    
    try:
        response = requests.post(
            f"{RCA_API_URL}/assistant/ask",
            json={"question": question},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            answer = result.get('answer', 'N/A')
            sql = result.get('query_result', {}).get('sql', 'N/A')
            
            print(f"✅ Status: Success")
            print(f"📝 SQL Generated:")
            print(f"   {sql}")
            print(f"📊 Answer:")
            
            # Try to parse JSON answer if it's a string
            if isinstance(answer, str):
                try:
                    answer_json = json.loads(answer)
                    print(f"   {json.dumps(answer_json, indent=2)}")
                except:
                    print(f"   {answer[:500]}")
            else:
                print(f"   {answer}")
            
            return {
                'success': True,
                'answer': answer,
                'sql': sql,
                'full_response': result
            }
        else:
            print(f"❌ Status: Failed ({response.status_code})")
            print(f"   {response.text[:200]}")
            return {
                'success': False,
                'error': f"HTTP {response.status_code}",
                'response': response.text
            }
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return {'success': False, 'error': str(e)}


def main():
    """Run complex query tests"""
    print("="*70)
    print("🧪 COMPLEX QUERY TESTS")
    print("="*70)
    print("\nTesting queries requiring:")
    print("  - Multiple joins")
    print("  - Filter conditions")
    print("  - Aggregations")
    print("  - Complex SQL generation")
    
    complex_queries = [
        # Simple count (baseline)
        "How many records are in customer_accounts_er?",
        
        # Filter condition
        "How many customers in customer_accounts_er have customer_type = 'CORPORATE'?",
        
        # Join with filter
        "Show me all transactions from customer_accounts_er joined with transactions_er where customer_type is 'CORPORATE'",
        
        # Aggregation with join
        "What is the total transaction amount per customer? Join customer_accounts_er with transactions_er on customer_id",
        
        # Complex filter and aggregation
        "What is the average transaction amount for CORPORATE customers? Join the tables and filter by customer_type",
        
        # Multiple conditions
        "Show me customers with more than 1 transaction, joining customer_accounts_er and transactions_er",
        
        # Group by with filter
        "What is the total transaction amount grouped by customer_type? Join customer_accounts_er with transactions_er",
        
        # Complex multi-condition query
        "Find all CORPORATE customers who have transactions greater than 1000, showing customer name and transaction amount",
        
        # Aggregation with multiple filters
        "What is the count of transactions per customer type, only for customers registered after 2025-01-01?",
        
        # Most complex: Multiple joins, filters, and aggregations
        "Show me the top 3 customers by total transaction amount, including their customer name and customer type, joining customer_accounts_er with transactions_er"
    ]
    
    results = []
    for i, query in enumerate(complex_queries, 1):
        print(f"\n\n🔍 Test {i}/{len(complex_queries)}")
        result = test_query(query)
        results.append({
            'query_number': i,
            'query': query,
            **result
        })
        
        # Small delay
        import time
        time.sleep(1)
    
    # Summary
    print("\n\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)
    
    successful = sum(1 for r in results if r.get('success'))
    print(f"\n✅ Successful: {successful}/{len(results)}")
    print(f"❌ Failed: {len(results) - successful}/{len(results)}")
    
    print(f"\n📝 Query Breakdown:")
    for r in results:
        status = "✅" if r.get('success') else "❌"
        sql = r.get('sql', 'N/A')
        print(f"\n{status} Query {r['query_number']}: {r['query'][:60]}...")
        if sql != 'N/A':
            print(f"   SQL: {sql[:100]}...")
    
    # Save results
    with open('complex_query_test_results.json', 'w') as f:
        json.dump({
            'total_queries': len(results),
            'successful': successful,
            'failed': len(results) - successful,
            'results': results
        }, f, indent=2)
    
    print(f"\n💾 Results saved to: complex_query_test_results.json")


if __name__ == '__main__':
    main()

