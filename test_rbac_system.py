#!/usr/bin/env python3
"""
Test Script for Role-Based Access Control System

Tests user management, role assignment, and access control policies.
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Load semantic registry to test access control
def load_semantic_registry() -> Dict[str, Any]:
    """Load semantic registry from JSON file."""
    registry_path = Path(__file__).parent / "metadata" / "semantic_registry.json"
    with open(registry_path, 'r') as f:
        return json.load(f)


def test_access_control_policies(registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Test access control policies for all metrics."""
    results = []
    
    for metric in registry["metrics"]:
        metric_name = metric["name"]
        policy = metric.get("policy", {})
        allowed_roles = policy.get("allowed_roles", [])
        
        result = {
            "metric": metric_name,
            "allowed_roles": allowed_roles,
            "has_policy": "policy" in metric,
            "max_time_range_days": policy.get("max_time_range_days"),
            "row_limit": policy.get("row_limit"),
            "max_dimensions": policy.get("max_dimensions"),
        }
        
        # Validate policy structure
        if not result["has_policy"]:
            result["warning"] = "Metric has no access control policy"
        elif not allowed_roles:
            result["warning"] = "Metric has empty allowed_roles list"
        else:
            result["status"] = "ok"
        
        results.append(result)
    
    return results


def test_role_access_matrix(registry: Dict[str, Any]) -> Dict[str, Dict[str, List[str]]]:
    """Create a matrix showing which roles can access which metrics."""
    roles = ["public", "analyst", "finance", "admin"]
    matrix = {role: {"allowed": [], "denied": []} for role in roles}
    
    for metric in registry["metrics"]:
        metric_name = metric["name"]
        policy = metric.get("policy", {})
        allowed_roles = policy.get("allowed_roles", [])
        
        for role in roles:
            if role in allowed_roles:
                matrix[role]["allowed"].append(metric_name)
            else:
                matrix[role]["denied"].append(metric_name)
    
    return matrix


def print_access_control_report(registry: Dict[str, Any]):
    """Print a comprehensive access control report."""
    print("\n" + "="*80)
    print("ROLE-BASED ACCESS CONTROL REPORT")
    print("="*80)
    
    # Test policies
    policy_results = test_access_control_policies(registry)
    
    print("\nMETRIC ACCESS POLICIES:")
    print("-"*80)
    
    metrics_with_policies = sum(1 for r in policy_results if r["has_policy"])
    metrics_without_policies = len(policy_results) - metrics_with_policies
    
    print(f"Total Metrics: {len(policy_results)}")
    print(f"Metrics with Policies: {metrics_with_policies}")
    print(f"Metrics without Policies: {metrics_without_policies}")
    
    if metrics_without_policies > 0:
        print("\n⚠️  Metrics without access control policies:")
        for result in policy_results:
            if not result["has_policy"]:
                print(f"  - {result['metric']}")
    
    # Role access matrix
    matrix = test_role_access_matrix(registry)
    
    print("\n" + "-"*80)
    print("ROLE ACCESS MATRIX")
    print("-"*80)
    
    for role, access in matrix.items():
        allowed_count = len(access["allowed"])
        denied_count = len(access["denied"])
        total = allowed_count + denied_count
        
        print(f"\n{role.upper()} Role:")
        print(f"  Allowed Metrics: {allowed_count}/{total}")
        print(f"  Denied Metrics: {denied_count}/{total}")
        
        if allowed_count > 0:
            print(f"  Sample Allowed: {', '.join(access['allowed'][:5])}")
            if allowed_count > 5:
                print(f"    ... and {allowed_count - 5} more")
    
    # Sensitive metrics (finance/admin only)
    print("\n" + "-"*80)
    print("SENSITIVE METRICS (Finance/Admin Only)")
    print("-"*80)
    
    sensitive_metrics = []
    for metric in registry["metrics"]:
        policy = metric.get("policy", {})
        allowed_roles = policy.get("allowed_roles", [])
        
        if "public" not in allowed_roles and "analyst" not in allowed_roles:
            sensitive_metrics.append({
                "metric": metric["name"],
                "allowed_roles": allowed_roles,
                "description": metric.get("description", "")
            })
    
    if sensitive_metrics:
        for sm in sensitive_metrics:
            print(f"\n  {sm['metric']}:")
            print(f"    Roles: {', '.join(sm['allowed_roles'])}")
            print(f"    Description: {sm['description']}")
    else:
        print("  No sensitive metrics found")
    
    # Policy constraints
    print("\n" + "-"*80)
    print("POLICY CONSTRAINTS")
    print("-"*80)
    
    metrics_with_time_limit = sum(1 for r in policy_results if r["max_time_range_days"])
    metrics_with_row_limit = sum(1 for r in policy_results if r["row_limit"])
    metrics_with_dimension_limit = sum(1 for r in policy_results if r["max_dimensions"])
    
    print(f"Metrics with Time Range Limits: {metrics_with_time_limit}")
    print(f"Metrics with Row Limits: {metrics_with_row_limit}")
    print(f"Metrics with Dimension Limits: {metrics_with_dimension_limit}")


def test_user_scenarios(registry: Dict[str, Any]):
    """Test various user access scenarios."""
    print("\n" + "="*80)
    print("USER ACCESS SCENARIOS")
    print("="*80)
    
    scenarios = [
        {
            "user_role": "public",
            "queries": [
                "What is the total outstanding amount?",
                "Show me the transaction volume",
                "What is the writeoff amount?",  # Should be denied
            ]
        },
        {
            "user_role": "analyst",
            "queries": [
                "What is the total outstanding amount?",
                "Show transaction volume by date",
                "What is the writeoff amount?",  # Should be denied
            ]
        },
        {
            "user_role": "finance",
            "queries": [
                "What is the total outstanding amount?",
                "Show me the writeoff amount",  # Should be allowed
                "What is the interest accrued?",  # Should be allowed
            ]
        },
        {
            "user_role": "admin",
            "queries": [
                "What is the total outstanding amount?",
                "Show me the writeoff amount",  # Should be allowed
                "What is the penalty waived?",  # Should be allowed
            ]
        }
    ]
    
    for scenario in scenarios:
        role = scenario["user_role"]
        print(f"\n{role.upper()} User Scenarios:")
        
        for query_desc in scenario["queries"]:
            # Simple keyword matching to find relevant metrics
            relevant_metrics = []
            for metric in registry["metrics"]:
                metric_name = metric["name"]
                description = metric.get("description", "").lower()
                
                if any(keyword in description for keyword in query_desc.lower().split()):
                    relevant_metrics.append(metric_name)
            
            # Check access
            for metric_name in relevant_metrics[:1]:  # Check first relevant metric
                for metric in registry["metrics"]:
                    if metric["name"] == metric_name:
                        policy = metric.get("policy", {})
                        allowed_roles = policy.get("allowed_roles", [])
                        
                        if role in allowed_roles:
                            print(f"  ✓ '{query_desc}' -> Access GRANTED for {metric_name}")
                        else:
                            print(f"  ✗ '{query_desc}' -> Access DENIED for {metric_name} (requires: {', '.join(allowed_roles)})")
                        break


def main():
    """Main test execution."""
    print("="*80)
    print("ROLE-BASED ACCESS CONTROL TEST SUITE")
    print("="*80)
    print(f"Test Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load registry
    try:
        registry = load_semantic_registry()
        print("\n✓ Loaded semantic registry successfully")
    except Exception as e:
        print(f"\n✗ Failed to load semantic registry: {e}")
        sys.exit(1)
    
    # Print access control report
    print_access_control_report(registry)
    
    # Test user scenarios
    test_user_scenarios(registry)
    
    # Save results
    policy_results = test_access_control_policies(registry)
    matrix = test_role_access_matrix(registry)
    
    output_file = Path(__file__).parent / "rbac_test_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            "test_run": datetime.now().isoformat(),
            "policy_results": policy_results,
            "role_access_matrix": matrix,
        }, f, indent=2)
    
    print(f"\n✓ Test results saved to: {output_file}")
    print("\n✓ RBAC test completed!")


if __name__ == "__main__":
    main()

