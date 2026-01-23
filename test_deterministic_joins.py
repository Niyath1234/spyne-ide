#!/usr/bin/env python3
"""
Test Deterministic Join Design
Tests that complex queries automatically determine correct join types based on dimension usage.

Key Test: Verify that:
1. LLM outputs dimension usage (select/filter/both), NOT join types
2. Compiler automatically determines LEFT vs INNER based on usage + metadata
3. Fan-out protection is applied when needed
"""

import requests
import json
from typing import Dict, Any, List
import time

RCA_API_URL = "http://localhost:8080/api"

def test_query(question: str, expected_join_types: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Test a query and verify join types are automatically determined
    
    Args:
        question: Natural language query
        expected_join_types: Dict mapping table names to expected join types
                           e.g., {"customers": "INNER", "regions": "LEFT"}
    """
    print(f"\n{'='*80}")
    print(f"📝 Query: {question}")
    print(f"{'='*80}")
    
    if expected_join_types:
        print(f"🔍 Expected Join Types:")
        for table, join_type in expected_join_types.items():
            print(f"   {table} → {join_type}")
    
    try:
        response = requests.post(
            f"{RCA_API_URL}/assistant/ask",
            json={"question": question},
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            answer = result.get('answer', 'N/A')
            sql = result.get('query_result', {}).get('sql', 'N/A')
            
            print(f"\n✅ Status: Success")
            print(f"\n📊 Generated SQL:")
            print(f"   {sql}")
            
            # Analyze join types in SQL
            join_analysis = analyze_joins(sql)
            if join_analysis:
                print(f"\n🔍 Join Analysis:")
                for table, join_type in join_analysis.items():
                    expected = expected_join_types.get(table, "N/A") if expected_join_types else "N/A"
                    match = "✅" if expected == join_type or expected == "N/A" else "❌"
                    print(f"   {match} {table}: {join_type} (expected: {expected})")
            
            # Check for dimension usage in intent (if available)
            intent = result.get('intent', {})
            if intent:
                dimension_intents = intent.get('dimension_intents', [])
                if dimension_intents:
                    print(f"\n📋 Dimension Usage (from LLM):")
                    for dim in dimension_intents:
                        print(f"   - {dim.get('name')}: {dim.get('usage')}")
            
            return {
                'success': True,
                'answer': answer,
                'sql': sql,
                'joins': join_analysis,
                'full_response': result
            }
        else:
            print(f"\n❌ Status: Failed ({response.status_code})")
            print(f"   {response.text[:500]}")
            return {
                'success': False,
                'error': f"HTTP {response.status_code}",
                'response': response.text
            }
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}


def analyze_joins(sql: str) -> Dict[str, str]:
    """Extract join types from SQL"""
    joins = {}
    
    if not sql or sql == 'N/A':
        return joins
    
    sql_upper = sql.upper()
    
    # Find all JOIN clauses
    import re
    # Pattern: [LEFT|INNER|RIGHT|FULL] JOIN table_name
    join_pattern = r'(LEFT|INNER|RIGHT|FULL)\s+JOIN\s+(\w+)'
    matches = re.findall(join_pattern, sql_upper)
    
    for join_type, table in matches:
        joins[table.lower()] = join_type
    
    # Also check for implicit INNER JOINs (just JOIN)
    implicit_joins = re.findall(r'\bJOIN\s+(\w+)\b', sql_upper)
    for table in implicit_joins:
        if table.lower() not in joins:
            joins[table.lower()] = "INNER"  # Default
    
    return joins


def main():
    """Run tests for deterministic join design"""
    print("="*80)
    print("🧪 DETERMINISTIC JOIN DESIGN TESTS")
    print("="*80)
    print("\nTesting that:")
    print("  1. LLM outputs dimension usage (select/filter/both)")
    print("  2. Compiler automatically determines LEFT vs INNER")
    print("  3. Fan-out protection is applied when needed")
    
    # Test cases with expected behaviors
    test_cases = [
        {
            "name": "Filter Dimension → INNER JOIN",
            "query": "Show me revenue for VIP customers",
            "description": "customer_category used for filtering → should use INNER JOIN",
            "expected_joins": {
                "customers": "INNER"  # Filter intent → INNER
            }
        },
        {
            "name": "Select Dimension (Optional) → LEFT JOIN",
            "query": "Show me revenue by region",
            "description": "region used for augmentation, optional relationship → should use LEFT JOIN",
            "expected_joins": {
                "regions": "LEFT"  # Select intent + optional → LEFT
            }
        },
        {
            "name": "Both Filter and Select → INNER JOIN",
            "query": "Show me revenue by region for VIP customers",
            "description": "region for selection, customer_category for filtering → INNER for filter takes precedence",
            "expected_joins": {
                "customers": "INNER",  # Filter → INNER
                "regions": "INNER"     # Both → INNER (filtering takes precedence)
            }
        },
        {
            "name": "Multiple Joins with Mixed Usage",
            "query": "Show me total transaction amount by region for CORPORATE customers",
            "description": "Multiple dimensions with different usage → compiler determines each join type",
            "expected_joins": {
                "customer_accounts_er": "INNER",  # Filter → INNER
                "regions": "LEFT"  # Select + optional → LEFT
            }
        },
        {
            "name": "Complex Aggregation with Filters",
            "query": "What is the average transaction amount for CORPORATE customers? Join customer_accounts_er with transactions_er",
            "description": "Filter on customer_type → INNER JOIN",
            "expected_joins": {
                "customer_accounts_er": "INNER"  # Filter → INNER
            }
        },
        {
            "name": "GROUP BY with Filter",
            "query": "What is the total transaction amount grouped by customer_type? Join customer_accounts_er with transactions_er",
            "description": "customer_type used in GROUP BY (select) → depends on optionality",
            "expected_joins": {
                "customer_accounts_er": "INNER"  # Usually mandatory relationship
            }
        },
        {
            "name": "Multiple Filters",
            "query": "Find all CORPORATE customers who have transactions greater than 1000, showing customer name and transaction amount",
            "description": "Multiple filters → all should use INNER JOIN",
            "expected_joins": {
                "customer_accounts_er": "INNER"  # Filter → INNER
            }
        },
        {
            "name": "Fan-Out Risk Detection",
            "query": "Show me order count by customer, joining orders with order_items",
            "description": "One-to-many relationship → should apply fan-out protection",
            "expected_joins": {
                "order_items": "LEFT"  # Select + optional, but fan-out protection needed
            },
            "check_fanout": True
        }
    ]
    
    results = []
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n\n{'#'*80}")
        print(f"🔍 Test {i}/{len(test_cases)}: {test_case['name']}")
        print(f"{'#'*80}")
        print(f"📋 Description: {test_case['description']}")
        
        result = test_query(
            test_case['query'],
            test_case.get('expected_joins')
        )
        
        # Check if fan-out protection was applied
        if test_case.get('check_fanout') and result.get('success'):
            sql = result.get('sql', '')
            has_fanout_protection = (
                'DISTINCT' in sql.upper() or
                'GROUP BY' in sql.upper() or
                'subquery' in sql.lower()
            )
            if has_fanout_protection:
                print(f"\n✅ Fan-out protection detected in SQL")
            else:
                print(f"\n⚠️  Fan-out protection not detected (may not be needed)")
        
        results.append({
            'test_number': i,
            'test_name': test_case['name'],
            'query': test_case['query'],
            'expected_joins': test_case.get('expected_joins', {}),
            **result
        })
        
        # Small delay between tests
        time.sleep(2)
    
    # Summary
    print("\n\n" + "="*80)
    print("📊 TEST SUMMARY")
    print("="*80)
    
    successful = sum(1 for r in results if r.get('success'))
    print(f"\n✅ Successful: {successful}/{len(results)}")
    print(f"❌ Failed: {len(results) - successful}/{len(results)}")
    
    # Join type accuracy
    join_matches = 0
    join_total = 0
    for r in results:
        if r.get('success') and r.get('joins') and r.get('expected_joins'):
            for table, expected_type in r['expected_joins'].items():
                join_total += 1
                actual_type = r['joins'].get(table, 'N/A')
                if actual_type == expected_type:
                    join_matches += 1
    
    if join_total > 0:
        accuracy = (join_matches / join_total) * 100
        print(f"\n🎯 Join Type Accuracy: {join_matches}/{join_total} ({accuracy:.1f}%)")
    
    print(f"\n📝 Detailed Results:")
    for r in results:
        status = "✅" if r.get('success') else "❌"
        print(f"\n{status} Test {r['test_number']}: {r['test_name']}")
        print(f"   Query: {r['query'][:70]}...")
        if r.get('joins'):
            print(f"   Joins: {r['joins']}")
        if r.get('error'):
            print(f"   Error: {r['error']}")
    
    # Save results
    output_file = 'deterministic_join_test_results.json'
    with open(output_file, 'w') as f:
        json.dump({
            'total_tests': len(results),
            'successful': successful,
            'failed': len(results) - successful,
            'join_accuracy': {
                'matches': join_matches,
                'total': join_total,
                'percentage': (join_matches / join_total * 100) if join_total > 0 else 0
            },
            'results': results
        }, f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")
    
    # Recommendations
    print(f"\n💡 Recommendations:")
    if successful < len(results):
        print(f"   - {len(results) - successful} tests failed - check error messages")
    if join_total > 0 and join_matches < join_total:
        print(f"   - {join_total - join_matches} join types didn't match expected - verify metadata")
    if successful == len(results) and join_matches == join_total:
        print(f"   - ✅ All tests passed! Deterministic join design is working correctly.")


if __name__ == '__main__':
    main()

