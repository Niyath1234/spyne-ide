#!/usr/bin/env python3
"""
Timezone-Aware Complex Query Test
Tests if the system can automatically handle timezone conversions based on rules/metadata.
Scenario: Dates in tables are stored in UTC, but system operates in IST (UTC+5:30)
"""

import requests
import json
from pathlib import Path
from typing import Dict, Any

RCA_API_URL = "http://localhost:8080/api"
KB_API_URL = "http://localhost:8083/api/knowledge-base"

def add_timezone_rules_to_kb():
    """Add timezone rules to knowledge base"""
    print("="*70)
    print("ADDING TIMEZONE RULES TO KNOWLEDGE BASE")
    print("="*70)
    
    # Create timezone rules document
    timezone_rules = {
        "business_rules": [
            {
                "id": "timezone_rule_001",
                "description": "All date/time columns in customer_accounts_er and transactions_er tables are stored in UTC timezone",
                "condition": "table IN ('customer_accounts_er', 'transactions_er') AND column_type = 'date' OR column_type = 'timestamp'",
                "applies_to": ["customer_accounts_er", "transactions_er"],
                "priority": "high",
                "rule_type": "timezone",
                "source_timezone": "UTC",
                "target_timezone": "IST",
                "offset_hours": 5.5
            },
            {
                "id": "timezone_rule_002", 
                "description": "System operates in IST (Indian Standard Time, UTC+5:30)",
                "condition": "system_default = true",
                "applies_to": ["all_tables"],
                "priority": "high",
                "rule_type": "system_timezone",
                "timezone": "IST",
                "offset_from_utc": 5.5
            }
        ],
        "table_timezone_mappings": {
            "customer_accounts_er": {
                "timezone": "UTC",
                "date_columns": ["registration_date"],
                "conversion_required": True,
                "target_timezone": "IST"
            },
            "transactions_er": {
                "timezone": "UTC",
                "date_columns": ["transaction_date"],
                "conversion_required": True,
                "target_timezone": "IST"
            }
        }
    }
    
    # Save to knowledge base directory
    kb_dir = Path("KnowledgeBase")
    kb_dir.mkdir(exist_ok=True)
    
    rules_file = kb_dir / "timezone_rules.json"
    with open(rules_file, 'w') as f:
        json.dump(timezone_rules, f, indent=2)
    
    print(f"✅ Created timezone rules file: {rules_file}")
    
    # Try to enrich knowledge base with these rules
    try:
        # Check if knowledge base API supports adding rules directly
        # For now, we'll document the rules and test if LLM can use them
        print("✅ Timezone rules documented")
        print("\nRules:")
        print("  - customer_accounts_er.registration_date: UTC → IST conversion required")
        print("  - transactions_er.transaction_date: UTC → IST conversion required")
        print("  - System default timezone: IST (UTC+5:30)")
        
        return rules_file
    except Exception as e:
        print(f"⚠️  Could not add rules via API: {e}")
        return rules_file


def test_timezone_aware_query(question: str, expected_utc_conversion: bool = True) -> Dict[str, Any]:
    """Test a query that should trigger timezone conversion"""
    print(f"\n{'='*70}")
    print(f"Query: {question}")
    print(f"{'='*70}")
    
    if expected_utc_conversion:
        print("🔍 Expected: SQL should convert UTC dates to IST (add 5 hours 30 minutes)")
    
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
            
            # Check if SQL includes timezone conversion
            sql_lower = sql.lower()
            has_timezone_conversion = (
                'utc' in sql_lower or 
                'ist' in sql_lower or
                'timezone' in sql_lower or
                'at time zone' in sql_lower or
                '+05:30' in sql or
                '+5:30' in sql or
                "interval '5 hours 30 minutes'" in sql_lower or
                "interval '5.5 hours'" in sql_lower
            )
            
            if expected_utc_conversion:
                if has_timezone_conversion:
                    print(f"✅ Timezone conversion detected in SQL!")
                else:
                    print(f"⚠️  No timezone conversion detected in SQL")
                    print(f"   Expected UTC→IST conversion but SQL doesn't show it")
            
            print(f"📊 Answer:")
            if isinstance(answer, str):
                try:
                    answer_json = json.loads(answer)
                    print(f"   {json.dumps(answer_json, indent=2)[:500]}")
                except:
                    print(f"   {answer[:300]}")
            else:
                print(f"   {answer}")
            
            return {
                'success': True,
                'answer': answer,
                'sql': sql,
                'has_timezone_conversion': has_timezone_conversion,
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
    """Run timezone-aware query tests"""
    print("="*70)
    print("🧪 TIMEZONE-AWARE COMPLEX QUERY TESTS")
    print("="*70)
    print("\nTesting if system can automatically handle:")
    print("  - UTC → IST timezone conversion based on rules")
    print("  - Date comparisons with timezone awareness")
    print("  - Business rules about timezone storage")
    
    # Add timezone rules
    rules_file = add_timezone_rules_to_kb()
    
    # Test queries that should trigger timezone conversion
    test_queries = [
        {
            "query": "Show me customers registered after 2025-01-15 10:00 AM IST in customer_accounts_er",
            "expected_conversion": True,
            "note": "Should convert IST time to UTC for comparison"
        },
        {
            "query": "Find transactions on 2025-01-20 IST in transactions_er table. Note that dates are stored in UTC",
            "expected_conversion": True,
            "note": "Explicitly mentions UTC storage, should convert IST query to UTC"
        },
        {
            "query": "What transactions occurred between 2025-01-15 09:00 IST and 2025-01-15 18:00 IST? The transaction_date column is in UTC",
            "expected_conversion": True,
            "note": "Date range query with timezone conversion"
        },
        {
            "query": "Show me customers registered in January 2025 IST. The registration_date is stored in UTC timezone",
            "expected_conversion": True,
            "note": "Month-based query with timezone awareness"
        },
        {
            "query": "Count transactions per day in IST timezone. transaction_date is stored as UTC",
            "expected_conversion": True,
            "note": "Group by day with timezone conversion"
        },
        {
            "query": "Find all CORPORATE customers registered after 2025-01-01 00:00 IST. Note: dates in customer_accounts_er are UTC",
            "expected_conversion": True,
            "note": "Filter with timezone conversion"
        }
    ]
    
    results = []
    for i, test_case in enumerate(test_queries, 1):
        print(f"\n\n🔍 Test {i}/{len(test_queries)}")
        print(f"Note: {test_case['note']}")
        
        result = test_timezone_aware_query(
            test_case['query'],
            expected_utc_conversion=test_case['expected_conversion']
        )
        results.append({
            'test_number': i,
            'query': test_case['query'],
            'note': test_case['note'],
            **result
        })
        
        import time
        time.sleep(2)  # Delay between queries
    
    # Summary
    print("\n\n" + "="*70)
    print("📊 TIMEZONE-AWARE QUERY TEST SUMMARY")
    print("="*70)
    
    successful = sum(1 for r in results if r.get('success'))
    with_conversion = sum(1 for r in results if r.get('has_timezone_conversion'))
    
    print(f"\n✅ Successful Queries: {successful}/{len(results)}")
    print(f"✅ Queries with Timezone Conversion: {with_conversion}/{len(results)}")
    
    print(f"\n📝 Detailed Results:")
    for r in results:
        status = "✅" if r.get('success') else "❌"
        tz_status = "🕐" if r.get('has_timezone_conversion') else "⚠️"
        print(f"\n{status} {tz_status} Test {r['test_number']}: {r['query'][:60]}...")
        if r.get('sql'):
            print(f"   SQL: {r['sql'][:100]}...")
    
    # Save results
    with open('timezone_query_test_results.json', 'w') as f:
        json.dump({
            'total_queries': len(results),
            'successful': successful,
            'with_timezone_conversion': with_conversion,
            'results': results
        }, f, indent=2)
    
    print(f"\n💾 Results saved to: timezone_query_test_results.json")
    
    # Assessment
    print(f"\n{'='*70}")
    print("ASSESSMENT")
    print("="*70)
    
    if with_conversion == len(results):
        print("✅ EXCELLENT: All queries include timezone conversion!")
    elif with_conversion > len(results) * 0.5:
        print("⚠️  PARTIAL: Some queries include timezone conversion")
        print("   The system understands timezone rules but doesn't always apply them")
    else:
        print("❌ NEEDS IMPROVEMENT: Most queries don't include timezone conversion")
        print("   The system may not be reading/applying timezone rules from knowledge base")
    
    print(f"\nRecommendation:")
    print("  - Ensure timezone rules are in knowledge base")
    print("  - LLM prompt should include timezone context")
    print("  - SQL compiler should apply timezone conversions automatically")


if __name__ == '__main__':
    main()

