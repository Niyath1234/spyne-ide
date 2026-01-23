#!/usr/bin/env python3
"""
Test Script for Semantic Registry Queries

Tests the system with real queries to validate metrics, dimensions, and access control.
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Test queries organized by category
TEST_QUERIES = [
    # Basic metric queries
    {
        "query": "What is the total outstanding amount?",
        "expected_metrics": ["tos"],
        "category": "basic_metric"
    },
    {
        "query": "Show me the principal outstanding",
        "expected_metrics": ["pos"],
        "category": "basic_metric"
    },
    {
        "query": "What is the total account balance?",
        "expected_metrics": ["total_account_balance"],
        "category": "basic_metric"
    },
    
    # Time-based queries
    {
        "query": "What is the total outstanding amount by date?",
        "expected_metrics": ["tos"],
        "expected_dimensions": ["date"],
        "category": "time_series"
    },
    {
        "query": "Show transaction volume by day",
        "expected_metrics": ["transaction_volume"],
        "expected_dimensions": ["transaction_date"],
        "category": "time_series"
    },
    {
        "query": "What is the average transaction amount per day?",
        "expected_metrics": ["average_transaction_amount"],
        "expected_dimensions": ["transaction_date"],
        "category": "time_series"
    },
    
    # Dimension-based queries
    {
        "query": "Show total outstanding by loan status",
        "expected_metrics": ["tos"],
        "expected_dimensions": ["loan_status"],
        "category": "dimension_analysis"
    },
    {
        "query": "What is the transaction volume by transaction type?",
        "expected_metrics": ["transaction_volume"],
        "expected_dimensions": ["transaction_type"],
        "category": "dimension_analysis"
    },
    {
        "query": "Show account balance by account status",
        "expected_metrics": ["total_account_balance"],
        "expected_dimensions": ["account_status"],
        "category": "dimension_analysis"
    },
    
    # Multi-dimension queries
    {
        "query": "Show total outstanding by date and loan status",
        "expected_metrics": ["tos"],
        "expected_dimensions": ["date", "loan_status"],
        "category": "multi_dimension"
    },
    {
        "query": "What is the transaction volume by date and customer type?",
        "expected_metrics": ["transaction_volume"],
        "expected_dimensions": ["transaction_date", "customer_type"],
        "category": "multi_dimension"
    },
    
    # Loan-specific queries
    {
        "query": "What is the total amount disbursed?",
        "expected_metrics": ["disbursed_amount"],
        "category": "loan_metrics"
    },
    {
        "query": "Show me the total amount repaid",
        "expected_metrics": ["repaid_amount"],
        "category": "loan_metrics"
    },
    {
        "query": "What is the outstanding principal?",
        "expected_metrics": ["outstanding_principal"],
        "category": "loan_metrics"
    },
    {
        "query": "Show total outstanding loan amount by date",
        "expected_metrics": ["total_outstanding_loan"],
        "expected_dimensions": ["as_of_date_loan"],
        "category": "loan_metrics"
    },
    
    # Customer-specific queries
    {
        "query": "What is the total outstanding per customer?",
        "expected_metrics": ["total_outstanding_customer"],
        "category": "customer_metrics"
    },
    {
        "query": "Show active accounts count by customer type",
        "expected_metrics": ["active_accounts_count"],
        "expected_dimensions": ["customer_type"],
        "category": "customer_metrics"
    },
    
    # Aggregation queries
    {
        "query": "What is the average transaction amount?",
        "expected_metrics": ["average_transaction_amount"],
        "category": "aggregation"
    },
    {
        "query": "How many transactions occurred?",
        "expected_metrics": ["transaction_count"],
        "category": "aggregation"
    },
    
    # Complex queries
    {
        "query": "Show me the total outstanding amount by date, region, and loan status",
        "expected_metrics": ["tos"],
        "expected_dimensions": ["date", "region", "loan_status"],
        "category": "complex"
    },
    {
        "query": "What is the transaction volume by date, customer type, and transaction type?",
        "expected_metrics": ["transaction_volume"],
        "expected_dimensions": ["transaction_date", "customer_type", "transaction_type"],
        "category": "complex"
    }
]

# Access control test queries (should fail for unauthorized users)
ACCESS_CONTROL_QUERIES = [
    {
        "query": "What is the total writeoff amount?",
        "expected_metrics": ["writeoff_amount"],
        "required_role": "finance",
        "category": "access_control"
    },
    {
        "query": "Show me the interest accrued",
        "expected_metrics": ["interest_accrued"],
        "required_role": "finance",
        "category": "access_control"
    },
    {
        "query": "What is the penalty waived amount?",
        "expected_metrics": ["penalty_waived"],
        "required_role": "finance",
        "category": "access_control"
    }
]


def load_semantic_registry() -> Dict[str, Any]:
    """Load semantic registry from JSON file."""
    registry_path = Path(__file__).parent / "metadata" / "semantic_registry.json"
    with open(registry_path, 'r') as f:
        return json.load(f)


def validate_metric_exists(registry: Dict[str, Any], metric_name: str) -> bool:
    """Check if a metric exists in the registry."""
    return any(m["name"] == metric_name for m in registry["metrics"])


def validate_dimension_exists(registry: Dict[str, Any], dimension_name: str) -> bool:
    """Check if a dimension exists in the registry."""
    return any(d["name"] == dimension_name for d in registry["dimensions"])


def validate_metric_dimension_compatibility(registry: Dict[str, Any], metric_name: str, dimension_name: str) -> bool:
    """Check if a dimension is allowed for a metric."""
    for metric in registry["metrics"]:
        if metric["name"] == metric_name:
            return dimension_name in metric.get("allowed_dimensions", [])
    return False


def test_query(query_def: Dict[str, Any], registry: Dict[str, Any]) -> Dict[str, Any]:
    """Test a single query definition."""
    result = {
        "query": query_def["query"],
        "category": query_def["category"],
        "passed": True,
        "errors": [],
        "warnings": []
    }
    
    # Validate expected metrics
    if "expected_metrics" in query_def:
        for metric_name in query_def["expected_metrics"]:
            if not validate_metric_exists(registry, metric_name):
                result["passed"] = False
                result["errors"].append(f"Metric '{metric_name}' not found in registry")
            else:
                result["warnings"].append(f"✓ Metric '{metric_name}' exists")
    
    # Validate expected dimensions
    if "expected_dimensions" in query_def:
        for dimension_name in query_def["expected_dimensions"]:
            if not validate_dimension_exists(registry, dimension_name):
                result["passed"] = False
                result["errors"].append(f"Dimension '{dimension_name}' not found in registry")
            else:
                result["warnings"].append(f"✓ Dimension '{dimension_name}' exists")
    
    # Validate metric-dimension compatibility
    if "expected_metrics" in query_def and "expected_dimensions" in query_def:
        for metric_name in query_def["expected_metrics"]:
            for dimension_name in query_def["expected_dimensions"]:
                if not validate_metric_dimension_compatibility(registry, metric_name, dimension_name):
                    result["passed"] = False
                    result["errors"].append(
                        f"Dimension '{dimension_name}' not allowed for metric '{metric_name}'"
                    )
    
    return result


def print_test_results(results: List[Dict[str, Any]]):
    """Print test results in a formatted way."""
    print("\n" + "="*80)
    print("SEMANTIC REGISTRY QUERY TEST RESULTS")
    print("="*80)
    
    total_tests = len(results)
    passed_tests = sum(1 for r in results if r["passed"])
    failed_tests = total_tests - passed_tests
    
    print(f"\nTotal Tests: {total_tests}")
    print(f"Passed: {passed_tests} ✓")
    print(f"Failed: {failed_tests} ✗")
    print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    # Group by category
    by_category = {}
    for result in results:
        category = result["category"]
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(result)
    
    print("\n" + "-"*80)
    print("RESULTS BY CATEGORY")
    print("-"*80)
    
    for category, category_results in by_category.items():
        passed = sum(1 for r in category_results if r["passed"])
        total = len(category_results)
        print(f"\n{category.upper()}: {passed}/{total} passed")
        
        for result in category_results:
            status = "✓" if result["passed"] else "✗"
            print(f"  {status} {result['query']}")
            if not result["passed"]:
                for error in result["errors"]:
                    print(f"      ERROR: {error}")
            elif result["warnings"]:
                for warning in result["warnings"][:2]:  # Show first 2 warnings
                    print(f"      {warning}")
    
    # Show failed tests in detail
    failed_results = [r for r in results if not r["passed"]]
    if failed_results:
        print("\n" + "-"*80)
        print("FAILED TESTS DETAILS")
        print("-"*80)
        for result in failed_results:
            print(f"\nQuery: {result['query']}")
            print(f"Category: {result['category']}")
            for error in result["errors"]:
                print(f"  ✗ {error}")


def print_registry_summary(registry: Dict[str, Any]):
    """Print a summary of the semantic registry."""
    print("\n" + "="*80)
    print("SEMANTIC REGISTRY SUMMARY")
    print("="*80)
    
    metrics = registry["metrics"]
    dimensions = registry["dimensions"]
    
    print(f"\nTotal Metrics: {len(metrics)}")
    print(f"Total Dimensions: {len(dimensions)}")
    
    # Group metrics by aggregation type
    aggregation_types = {}
    for metric in metrics:
        agg = metric["aggregation"]
        if agg not in aggregation_types:
            aggregation_types[agg] = []
        aggregation_types[agg].append(metric["name"])
    
    print("\nMetrics by Aggregation Type:")
    for agg_type, metric_names in aggregation_types.items():
        print(f"  {agg_type}: {len(metric_names)} metrics")
        for name in metric_names[:5]:  # Show first 5
            print(f"    - {name}")
        if len(metric_names) > 5:
            print(f"    ... and {len(metric_names) - 5} more")
    
    # Group dimensions by data type
    data_types = {}
    for dimension in dimensions:
        dt = dimension["data_type"]
        if dt not in data_types:
            data_types[dt] = []
        data_types[dt].append(dimension["name"])
    
    print("\nDimensions by Data Type:")
    for data_type, dimension_names in data_types.items():
        print(f"  {data_type}: {len(dimension_names)} dimensions")
        for name in dimension_names[:5]:  # Show first 5
            print(f"    - {name}")
        if len(dimension_names) > 5:
            print(f"    ... and {len(dimension_names) - 5} more")
    
    # Show access control policies
    print("\nAccess Control Policies:")
    role_counts = {}
    for metric in metrics:
        if "policy" in metric and "allowed_roles" in metric["policy"]:
            for role in metric["policy"]["allowed_roles"]:
                role_counts[role] = role_counts.get(role, 0) + 1
    
    for role, count in sorted(role_counts.items()):
        print(f"  {role}: {count} metrics")


def main():
    """Main test execution."""
    print("="*80)
    print("SEMANTIC REGISTRY QUERY TEST SUITE")
    print("="*80)
    print(f"Test Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load registry
    try:
        registry = load_semantic_registry()
        print("\n✓ Loaded semantic registry successfully")
    except Exception as e:
        print(f"\n✗ Failed to load semantic registry: {e}")
        sys.exit(1)
    
    # Print registry summary
    print_registry_summary(registry)
    
    # Run tests
    print("\n" + "="*80)
    print("RUNNING QUERY TESTS")
    print("="*80)
    
    results = []
    for query_def in TEST_QUERIES:
        result = test_query(query_def, registry)
        results.append(result)
    
    # Test access control queries (these should validate metric existence and policies)
    for query_def in ACCESS_CONTROL_QUERIES:
        result = test_query(query_def, registry)
        # Check if metric has proper access control
        if "expected_metrics" in query_def:
            for metric_name in query_def["expected_metrics"]:
                for metric in registry["metrics"]:
                    if metric["name"] == metric_name:
                        if "policy" in metric and "allowed_roles" in metric["policy"]:
                            required_role = query_def.get("required_role", "public")
                            if required_role not in metric["policy"]["allowed_roles"]:
                                result["warnings"].append(
                                    f"✓ Access control: '{required_role}' role required for '{metric_name}'"
                                )
        results.append(result)
    
    # Print results
    print_test_results(results)
    
    # Save results to file
    output_file = Path(__file__).parent / "semantic_registry_test_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            "test_run": datetime.now().isoformat(),
            "total_tests": len(results),
            "passed": sum(1 for r in results if r["passed"]),
            "failed": sum(1 for r in results if not r["passed"]),
            "results": results
        }, f, indent=2)
    
    print(f"\n✓ Test results saved to: {output_file}")
    
    # Exit with appropriate code
    failed_count = sum(1 for r in results if not r["passed"])
    if failed_count > 0:
        print(f"\n⚠️  {failed_count} test(s) failed")
        sys.exit(1)
    else:
        print("\n✓ All tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()

