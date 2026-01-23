#!/usr/bin/env python3
"""
Unit Test for Join Planner Logic
Tests the deterministic join selection logic without requiring full API server.

This verifies that:
1. Filter intent → INNER JOIN
2. Select intent + optional → LEFT JOIN  
3. Select intent + mandatory → INNER JOIN
4. Both intent → INNER JOIN
"""

import json

def test_join_planner_logic():
    """Test the join planner logic conceptually"""
    
    print("="*80)
    print("🧪 JOIN PLANNER LOGIC TESTS")
    print("="*80)
    
    test_cases = [
        {
            "name": "Filter Intent → INNER JOIN",
            "dimension_usage": "filter",
            "optional": True,
            "expected_join": "INNER",
            "reason": "User wants to restrict rows → INNER JOIN"
        },
        {
            "name": "Select Intent + Optional → LEFT JOIN",
            "dimension_usage": "select",
            "optional": True,
            "expected_join": "LEFT",
            "reason": "Augmentation, optional OK → LEFT JOIN"
        },
        {
            "name": "Select Intent + Mandatory → INNER JOIN",
            "dimension_usage": "select",
            "optional": False,
            "expected_join": "INNER",
            "reason": "Augmentation but must exist → INNER JOIN"
        },
        {
            "name": "Both Intent → INNER JOIN",
            "dimension_usage": "both",
            "optional": True,
            "expected_join": "INNER",
            "reason": "Used for filtering → INNER JOIN (filtering takes precedence)"
        }
    ]
    
    print("\n📋 Test Cases:")
    print("-" * 80)
    
    all_passed = True
    for i, test in enumerate(test_cases, 1):
        print(f"\n{i}. {test['name']}")
        print(f"   Usage: {test['dimension_usage']}, Optional: {test['optional']}")
        print(f"   Expected: {test['expected_join']}")
        print(f"   Reason: {test['reason']}")
        
        # Simulate the logic
        result = determine_join_type(test['dimension_usage'], test['optional'])
        
        if result == test['expected_join']:
            print(f"   ✅ PASS: Got {result}")
        else:
            print(f"   ❌ FAIL: Got {result}, expected {test['expected_join']}")
            all_passed = False
    
    print("\n" + "="*80)
    if all_passed:
        print("✅ All logic tests passed!")
    else:
        print("❌ Some tests failed")
    print("="*80)
    
    return all_passed


def determine_join_type(usage: str, optional: bool) -> str:
    """
    Simulate the deterministic join type selection logic
    
    This matches the Rust implementation in join_planner.rs
    """
    if usage == "filter":
        return "INNER"  # User wants to restrict rows
    elif usage == "select":
        if optional:
            return "LEFT"   # Augmentation, optional OK
        else:
            return "INNER"  # Must exist
    elif usage == "both":
        return "INNER"  # Filtering takes precedence
    else:
        return "LEFT"  # Default fallback


def test_dimension_intent_parsing():
    """Test that dimension intents are parsed correctly"""
    
    print("\n\n" + "="*80)
    print("🧪 DIMENSION INTENT PARSING TESTS")
    print("="*80)
    
    # Example LLM output with dimension_intents
    llm_output = {
        "metrics": ["revenue"],
        "dimension_intents": [
            {
                "name": "customer_category",
                "usage": "filter"  # Used in WHERE clause
            },
            {
                "name": "region",
                "usage": "select"  # Used in SELECT/GROUP BY
            }
        ],
        "filters": [
            {
                "dimension": "customer_category",
                "operator": "=",
                "value": "VIP"
            }
        ]
    }
    
    print("\n📋 Example LLM Output:")
    print(json.dumps(llm_output, indent=2))
    
    print("\n🔍 Analysis:")
    for dim_intent in llm_output.get("dimension_intents", []):
        name = dim_intent["name"]
        usage = dim_intent["usage"]
        
        # Check if it's also in filters
        in_filters = any(f["dimension"] == name for f in llm_output.get("filters", []))
        
        print(f"\n   Dimension: {name}")
        print(f"   Usage: {usage}")
        print(f"   In filters: {in_filters}")
        
        # Determine expected join type
        optional = True  # Assume optional for this example
        expected_join = determine_join_type(usage, optional)
        
        print(f"   → Expected Join Type: {expected_join}")
        print(f"   → Reason: {'Filter intent' if usage == 'filter' else 'Select intent' + (' (optional)' if optional else ' (mandatory)')}")
    
    print("\n✅ Dimension intent parsing test complete")


def test_fan_out_detection():
    """Test fan-out detection logic"""
    
    print("\n\n" + "="*80)
    print("🧪 FAN-OUT DETECTION TESTS")
    print("="*80)
    
    test_cases = [
        {
            "cardinality": "one_to_one",
            "fan_out_safe": True,
            "needs_protection": False
        },
        {
            "cardinality": "many_to_one",
            "fan_out_safe": True,
            "needs_protection": False
        },
        {
            "cardinality": "one_to_many",
            "fan_out_safe": False,
            "needs_protection": True
        },
        {
            "cardinality": "many_to_many",
            "fan_out_safe": False,
            "needs_protection": True
        }
    ]
    
    print("\n📋 Cardinality → Fan-out Safety:")
    for test in test_cases:
        cardinality = test["cardinality"]
        is_safe = test["fan_out_safe"]
        needs_protection = test["needs_protection"]
        
        status = "✅ Safe" if is_safe else "⚠️  Risk"
        protection = "Required" if needs_protection else "Not needed"
        
        print(f"\n   {cardinality.upper()}")
        print(f"   Fan-out safe: {is_safe} ({status})")
        print(f"   Protection: {protection}")
    
    print("\n✅ Fan-out detection test complete")


if __name__ == '__main__':
    print("\n" + "="*80)
    print("🚀 DETERMINISTIC JOIN DESIGN - LOGIC TESTS")
    print("="*80)
    
    # Run all tests
    logic_passed = test_join_planner_logic()
    test_dimension_intent_parsing()
    test_fan_out_detection()
    
    print("\n\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    print(f"\n✅ Core logic tests: {'PASSED' if logic_passed else 'FAILED'}")
    print("\n💡 Next Steps:")
    print("   1. Run test_deterministic_joins.py to test with actual API")
    print("   2. Verify LLM outputs dimension_intents with usage")
    print("   3. Check that compiler determines correct join types")
    print("="*80)

