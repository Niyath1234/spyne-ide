#!/usr/bin/env python3
"""
Test script to verify JOIN condition fix works correctly
"""
import sys
sys.path.insert(0, '/Users/niyathnair/Desktop/RCA-ENGINE/RCA-Engine/backend')

from ai_sql_system.sql.deterministic_builder import DeterministicSQLBuilder

def test_join_condition_normalization():
    builder = DeterministicSQLBuilder()
    
    test_cases = [
        # (input_condition, table1, table2, expected_output)
        ("orders.o_custkey = customer.c_custkey", "customer", "orders", "c.c_custkey = o.o_custkey"),
        ("customer.c_custkey = orders.o_custkey", "customer", "orders", "c.c_custkey = o.o_custkey"),
        ("o.o_custkey = c.c_custkey", "customer", "orders", "c.c_custkey = o.o_custkey"),
        ("c.c_custkey = o.o_custkey", "customer", "orders", "c.c_custkey = o.o_custkey"),
        ("orders.o_orderkey = lineitem.l_orderkey", "orders", "lineitem", "o.o_orderkey = l.l_orderkey"),
        ("lineitem.l_orderkey = orders.o_orderkey", "orders", "lineitem", "o.o_orderkey = l.l_orderkey"),
    ]
    
    print("Testing JOIN condition normalization...")
    print("=" * 80)
    
    all_passed = True
    for input_cond, t1, t2, expected in test_cases:
        result = builder._normalize_join_condition(input_cond, t1, t2)
        passed = result == expected
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {input_cond}")
        print(f"   Table1: {t1}, Table2: {t2}")
        print(f"   Expected: {expected}")
        print(f"   Got:      {result}")
        if not passed:
            all_passed = False
        print()
    
    print("=" * 80)
    if all_passed:
        print("✅ All tests passed! JOIN condition fix is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(test_join_condition_normalization())
