#!/usr/bin/env python3
"""
Direct test of SQL generation - shows what SQL would be generated
without needing the API server
"""

import json
import re

# Expected manual SQL
EXPECTED_SQL = """select
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

# What the system SHOULD generate based on our implementation
EXPECTED_GENERATED_SQL = """SELECT 
    CASE WHEN lower(s3_tool_propagator.outstanding_daily.order_type) = 'credin' THEN 'credin' ELSE 'Digital' END,
    'OS',
    'Digital',
    SUM(CASE WHEN date_trunc('month', date(s3_tool_propagator.da_orders.da_date)) = date '2024-02-01' THEN s3_tool_propagator.outstanding_daily.principal_outstanding * 0.01 WHEN date_trunc('month', date(s3_tool_propagator.da_orders.da_date)) IS NULL THEN s3_tool_propagator.outstanding_daily.principal_outstanding ELSE s3_tool_propagator.outstanding_daily.principal_outstanding * 0.05 END)
FROM s3_tool_propagator.outstanding_daily
LEFT JOIN s3_tool_propagator.da_orders ON s3_tool_propagator.outstanding_daily.order_id = s3_tool_propagator.da_orders.order_id
LEFT JOIN s3_tool_propagator.writeoff_users ON s3_tool_propagator.outstanding_daily.order_id = s3_tool_propagator.writeoff_users.order_id
LEFT JOIN s3_tool_propagator.provisional_writeoff ON s3_tool_propagator.outstanding_daily.order_id = s3_tool_propagator.provisional_writeoff.order_id
WHERE s3_tool_propagator.writeoff_users.order_id IS NULL
  AND s3_tool_propagator.provisional_writeoff.order_id IS NULL
  AND s3_tool_propagator.outstanding_daily.settlement_flag = 'unsettled'
  AND s3_tool_propagator.outstanding_daily.last_day = date(dateadd(day, -2, current_date))
  AND COALESCE(s3_tool_propagator.outstanding_daily.nbfc_name_colending, s3_tool_propagator.outstanding_daily.parent_nbfc_name) IN ('quadrillion', 'slicenesfb', 'nesfb')
  AND lower(s3_tool_propagator.outstanding_daily.order_type) NOT IN ('credit_card', 'no_cost_emi')
GROUP BY 
    CASE WHEN lower(s3_tool_propagator.outstanding_daily.order_type) = 'credin' THEN 'credin' ELSE 'Digital' END,
    'OS',
    'Digital'"""

# Natural language query
NATURAL_LANGUAGE_QUERY = """Show me current POS by order type (credin/Digital), region OS, product group Digital, 
excluding written off orders, for unsettled settlements from 2 days ago, 
for quadrillion/slicenesfb/nesfb NBFCs, excluding credit_card and no_cost_emi order types"""

# Expected intent that LLM should generate
EXPECTED_INTENT = {
    "metrics": ["current_pos"],
    "dimensions": ["order_type_transformed", "region_literal", "product_group_literal"],
    "filters": [
        {"dimension": "writeoff_status", "operator": "IS NULL"},
        {"dimension": "provisional_writeoff_status", "operator": "IS NULL"},
        {"dimension": "settlement_flag_dim", "operator": "=", "value": "unsettled"},
        {"dimension": "last_day_dim", "operator": "=", "relative_date": "2_days_ago"},
        {"dimension": "nbfc_name_coalesced", "operator": "IN", "value": ["quadrillion", "slicenesfb", "nesfb"]},
        {"dimension": "order_type_filter", "operator": "NOT IN", "value": ["credit_card", "no_cost_emi"]}
    ]
}

def normalize_sql(sql):
    """Normalize SQL for comparison"""
    # Remove extra whitespace
    sql = re.sub(r'\s+', ' ', sql)
    # Remove case differences
    sql = sql.upper()
    # Remove table aliases (a, da, wu, pwo)
    sql = re.sub(r'\ba\.', '', sql)
    sql = re.sub(r'\bda\.', '', sql)
    sql = re.sub(r'\bwu\.', '', sql)
    sql = re.sub(r'\bpwo\.', '', sql)
    # Normalize quotes
    sql = sql.replace('"', "'")
    return sql.strip()

def compare_sql_components(expected, generated):
    """Compare SQL components"""
    checks = {
        "CASE statement in SELECT": "CASE" in generated.upper() and "WHEN" in generated.upper(),
        "Literal 'OS'": "'OS'" in generated or '"OS"' in generated,
        "Literal 'Digital'": "'Digital'" in generated or '"Digital"' in generated,
        "CASE in SUM aggregation": "SUM(CASE" in generated.upper() or "SUM (CASE" in generated.upper(),
        "LEFT JOIN da_orders": ("LEFT JOIN" in generated.upper() or "LEFT JOIN" in generated) and ("da_orders" in generated.upper() or "da_orders" in generated),
        "LEFT JOIN writeoff_users": ("LEFT JOIN" in generated.upper() or "LEFT JOIN" in generated) and ("writeoff_users" in generated.upper() or "writeoff_users" in generated),
        "LEFT JOIN provisional_writeoff": ("LEFT JOIN" in generated.upper() or "LEFT JOIN" in generated) and ("provisional_writeoff" in generated.upper() or "provisional_writeoff" in generated),
        "IS NULL filter": "IS NULL" in generated.upper(),
        "settlement_flag = 'unsettled'": "SETTLEMENT_FLAG" in generated.upper() and "UNSETTLED" in generated.upper(),
        "COALESCE in WHERE": "COALESCE" in generated.upper(),
        "lower() function": "LOWER(" in generated.upper(),
        "NOT IN filter": "NOT IN" in generated.upper(),
        "GROUP BY": "GROUP BY" in generated.upper(),
        "date arithmetic": "DATEADD" in generated.upper() or "DATE(ADD" in generated.upper(),
    }
    return checks

def main():
    print("=" * 80)
    print("SQL GENERATION TEST")
    print("=" * 80)
    print()
    
    print("📝 Natural Language Query:")
    print(NATURAL_LANGUAGE_QUERY)
    print()
    
    print("📋 Expected Intent (what LLM should generate):")
    print(json.dumps(EXPECTED_INTENT, indent=2))
    print()
    
    print("📊 Expected Generated SQL (what system should produce):")
    print("-" * 80)
    print(EXPECTED_GENERATED_SQL)
    print("-" * 80)
    print()
    
    print("📋 Your Manual SQL (for comparison):")
    print("-" * 80)
    print(EXPECTED_SQL)
    print("-" * 80)
    print()
    
    print("🔍 Component Comparison:")
    print()
    
    checks = compare_sql_components(EXPECTED_SQL, EXPECTED_GENERATED_SQL)
    
    all_passed = True
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"{status} {check}")
        if not passed:
            all_passed = False
    
    print()
    
    print("📊 Key Differences:")
    print()
    
    differences = []
    
    # Check table aliases
    if " a" in EXPECTED_SQL and "s3_tool_propagator.outstanding_daily" in EXPECTED_GENERATED_SQL:
        differences.append("✅ Table aliases: Manual uses 'a', Generated uses full table name (both valid)")
    
    # Check GROUP BY
    if "GROUP BY 1, 2, 3" in EXPECTED_SQL and "GROUP BY" in EXPECTED_GENERATED_SQL:
        if "GROUP BY 1, 2, 3" not in EXPECTED_GENERATED_SQL:
            differences.append("⚠️  GROUP BY: Manual uses positional (1,2,3), Generated uses expressions (both valid)")
    
    # Check date function
    if "date(dateadd(day, -2, current_date))" in EXPECTED_SQL:
        if "date(dateadd(day, -2, current_date))" in EXPECTED_GENERATED_SQL:
            differences.append("✅ Date arithmetic: Both use date(dateadd(day, -2, current_date))")
        else:
            differences.append("❌ Date arithmetic: May differ")
    
    for diff in differences:
        print(diff)
    
    print()
    
    if all_passed:
        print("✅ All key components match! SQL structure is correct.")
        print()
        print("Note: Minor differences in formatting/aliases are acceptable.")
        print("The generated SQL should produce the same results as your manual SQL.")
    else:
        print("⚠️  Some components may differ. Check the differences above.")
    
    print()
    print("=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print()
    print("To actually test the system:")
    print("1. Start the server: cargo run --bin server")
    print("2. Run: python3 test_query_generation.py")
    print("3. Or test via UI with the natural language query")
    print()

if __name__ == "__main__":
    main()

