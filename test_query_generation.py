#!/usr/bin/env python3
"""
Test query generation for current POS query
Compares generated SQL with expected manual SQL
"""

import json
import requests
import sys
from datetime import datetime, timedelta

# Calculate 2 days ago
two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')

# Expected manual SQL (with dynamic date)
EXPECTED_SQL = f"""select
        case 
            when lower(a.order_type) = 'credin' then 'credin'
            else 'Digital'
        end as order_type,
        'OS' as region,
        'Digital' as product_group,
        sum(
            case 
                when date_trunc('month', date(da.da_date)) = date '2024-02-01' then a.principal_outstanding * 0.01
                when date_trunc('month', date(da.da_date)) is null then a.principal_outstanding
                else a.principal_outstanding * 0.05
            end
        ) as current_pos
    from s3_tool_propagator.outstanding_daily a
    left join s3_tool_propagator.da_orders da on da.order_id = a.order_id
    left join s3_tool_propagator.writeoff_users wu on wu.order_id = a.order_id
    left join s3_tool_propagator.provisional_writeoff pwo on pwo.order_id = a.order_id
    where wu.order_id is null
      and pwo.order_id is null
      and a.settlement_flag = 'unsettled'
      and a.last_day = date(dateadd(day, -2, current_date))
      and coalesce(a.nbfc_name_colending, a.parent_nbfc_name) in ('quadrillion', 'slicenesfb', 'nesfb')
      and lower(a.order_type) NOT IN ('credit_card','no_cost_emi')
    group by 1, 2, 3"""

# Natural language query
NATURAL_LANGUAGE_QUERY = """Show me current POS by order type (credin/Digital), region OS, product group Digital, 
excluding written off orders, for unsettled settlements from 2 days ago, 
for quadrillion/slicenesfb/nesfb NBFCs, excluding credit_card and no_cost_emi order types"""

def test_query_generation():
    """Test if the system generates the correct SQL"""
    
    print("=" * 80)
    print("TESTING QUERY GENERATION")
    print("=" * 80)
    print()
    
    print("📝 Natural Language Query:")
    print(NATURAL_LANGUAGE_QUERY)
    print()
    
    # Try to call the API
    try:
        print("🔍 Calling RCA Engine API...")
        response = requests.post(
            "http://localhost:8080/api/assistant/ask",
            json={"question": NATURAL_LANGUAGE_QUERY},
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ API Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
        
        result = response.json()
        
        print("✅ API Response Received")
        print()
        
        # Try to extract SQL from response
        # The response structure may vary, so we'll check multiple places
        sql_generated = None
        
        if "query_result" in result:
            query_result = result["query_result"]
            if isinstance(query_result, dict):
                if "sql" in query_result:
                    sql_generated = query_result["sql"]
                elif "query" in query_result:
                    sql_generated = query_result["query"]
        
        if "answer" in result:
            answer = result["answer"]
            # Try to extract SQL from answer text
            if "SELECT" in answer.upper():
                # Look for SQL block
                import re
                sql_match = re.search(r'```sql\s*(.*?)\s*```', answer, re.DOTALL)
                if sql_match:
                    sql_generated = sql_match.group(1).strip()
                else:
                    # Try to find SQL in the answer
                    sql_match = re.search(r'(SELECT.*?GROUP BY.*?)(?:\n\n|\Z)', answer, re.DOTALL | re.IGNORECASE)
                    if sql_match:
                        sql_generated = sql_match.group(1).strip()
        
        if not sql_generated:
            print("⚠️  Could not extract SQL from response")
            print("Full response:")
            print(json.dumps(result, indent=2))
            return False
        
        print("📊 Generated SQL:")
        print("-" * 80)
        print(sql_generated)
        print("-" * 80)
        print()
        
        print("📋 Expected SQL:")
        print("-" * 80)
        print(EXPECTED_SQL)
        print("-" * 80)
        print()
        
        # Compare key components
        print("🔍 Comparison:")
        print()
        
        checks = {
            "CASE statement in SELECT": "CASE" in sql_generated.upper() and "WHEN" in sql_generated.upper(),
            "Literal 'OS'": "'OS'" in sql_generated or '"OS"' in sql_generated,
            "Literal 'Digital'": "'Digital'" in sql_generated or '"Digital"' in sql_generated,
            "CASE in SUM aggregation": "SUM(CASE" in sql_generated.upper() or "SUM (CASE" in sql_generated.upper(),
            "LEFT JOIN da_orders": "LEFT JOIN" in sql_generated.upper() and "da_orders" in sql_generated,
            "LEFT JOIN writeoff_users": "LEFT JOIN" in sql_generated.upper() and "writeoff_users" in sql_generated,
            "LEFT JOIN provisional_writeoff": "LEFT JOIN" in sql_generated.upper() and "provisional_writeoff" in sql_generated,
            "IS NULL filter": "IS NULL" in sql_generated.upper(),
            "settlement_flag = 'unsettled'": "settlement_flag" in sql_generated.lower() and "unsettled" in sql_generated.lower(),
            "COALESCE in WHERE": "COALESCE" in sql_generated.upper(),
            "lower() function": "lower(" in sql_generated.lower(),
            "NOT IN filter": "NOT IN" in sql_generated.upper(),
            "GROUP BY": "GROUP BY" in sql_generated.upper(),
        }
        
        all_passed = True
        for check, passed in checks.items():
            status = "✅" if passed else "❌"
            print(f"{status} {check}")
            if not passed:
                all_passed = False
        
        print()
        
        if all_passed:
            print("✅ All checks passed! SQL structure matches expected.")
        else:
            print("⚠️  Some checks failed. SQL may not match exactly.")
        
        return all_passed
        
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to API server")
        print("Make sure the server is running: cargo run --bin server")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_query_generation()
    sys.exit(0 if success else 1)

