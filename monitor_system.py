#!/usr/bin/env python3
"""
System Monitoring Script

Monitors execution logs, metrics usage, and system health.
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any
import os

# Color codes for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_header(text: str):
    """Print a formatted header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text.center(80)}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.RESET}\n")

def print_section(text: str):
    """Print a section header."""
    print(f"\n{Colors.BOLD}{text}{Colors.RESET}")
    print("-" * 80)

def load_json_logs(log_file: Path) -> List[Dict[str, Any]]:
    """Load JSON logs from a file (one JSON object per line)."""
    logs = []
    if log_file.exists():
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            logs.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            print(f"{Colors.YELLOW}Warning: Could not read {log_file}: {e}{Colors.RESET}")
    return logs

def analyze_query_logs(logs: List[Dict[str, Any]], hours: int = 24) -> Dict[str, Any]:
    """Analyze query execution logs."""
    cutoff_time = datetime.utcnow() - timedelta(hours=hours)
    
    stats = {
        "total_queries": 0,
        "successful_queries": 0,
        "failed_queries": 0,
        "avg_execution_time": 0.0,
        "total_execution_time": 0.0,
        "metrics_used": defaultdict(int),
        "dimensions_used": defaultdict(int),
        "users": defaultdict(int),
        "roles": defaultdict(int),
        "errors": defaultdict(int),
    }
    
    execution_times = []
    
    for log in logs:
        try:
            # Parse timestamp
            timestamp_str = log.get("timestamp", "")
            if isinstance(timestamp_str, str):
                try:
                    log_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except:
                    continue
            else:
                continue
            
            if log_time < cutoff_time:
                continue
            
            stats["total_queries"] += 1
            
            if log.get("success", False):
                stats["successful_queries"] += 1
            else:
                stats["failed_queries"] += 1
                error_msg = log.get("error_message", "Unknown error")
                stats["errors"][error_msg] += 1
            
            exec_time = log.get("execution_time_ms", 0)
            if exec_time > 0:
                execution_times.append(exec_time)
                stats["total_execution_time"] += exec_time
            
            # Track metrics and dimensions
            for metric in log.get("metrics_used", []):
                stats["metrics_used"][metric] += 1
            
            for dimension in log.get("dimensions_used", []):
                stats["dimensions_used"][dimension] += 1
            
            # Track users and roles
            user_id = log.get("user_id", "unknown")
            stats["users"][user_id] += 1
            
            role = log.get("user_role", "unknown")
            stats["roles"][role] += 1
            
        except Exception as e:
            continue
    
    if execution_times:
        stats["avg_execution_time"] = sum(execution_times) / len(execution_times)
    
    return stats

def analyze_metric_usage(logs: List[Dict[str, Any]], hours: int = 24) -> Dict[str, Any]:
    """Analyze metric usage logs."""
    cutoff_time = datetime.utcnow() - timedelta(hours=hours)
    
    stats = {
        "total_usage": 0,
        "metrics": defaultdict(lambda: {"count": 0, "users": set(), "avg_time": []}),
        "top_metrics": [],
    }
    
    for log in logs:
        try:
            timestamp_str = log.get("timestamp", "")
            if isinstance(timestamp_str, str):
                try:
                    log_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except:
                    continue
            else:
                continue
            
            if log_time < cutoff_time:
                continue
            
            metric_name = log.get("metric_name", "")
            if metric_name:
                stats["total_usage"] += 1
                stats["metrics"][metric_name]["count"] += 1
                
                user_id = log.get("user_id", "unknown")
                stats["metrics"][metric_name]["users"].add(user_id)
                
                exec_time = log.get("execution_time_ms", 0)
                if exec_time > 0:
                    stats["metrics"][metric_name]["avg_time"].append(exec_time)
        except Exception:
            continue
    
    # Calculate averages and create top metrics list
    for metric_name, data in stats["metrics"].items():
        if data["avg_time"]:
            data["avg_time"] = sum(data["avg_time"]) / len(data["avg_time"])
        else:
            data["avg_time"] = 0.0
        data["user_count"] = len(data["users"])
        data["users"] = list(data["users"])[:5]  # Keep first 5 users
    
    # Sort by usage count
    stats["top_metrics"] = sorted(
        [(name, data) for name, data in stats["metrics"].items()],
        key=lambda x: x[1]["count"],
        reverse=True
    )[:10]
    
    return stats

def analyze_access_control(logs: List[Dict[str, Any]], hours: int = 24) -> Dict[str, Any]:
    """Analyze access control logs."""
    cutoff_time = datetime.utcnow() - timedelta(hours=hours)
    
    stats = {
        "total_attempts": 0,
        "granted": 0,
        "denied": 0,
        "denial_reasons": defaultdict(int),
        "by_role": defaultdict(lambda: {"granted": 0, "denied": 0}),
        "by_metric": defaultdict(lambda: {"granted": 0, "denied": 0}),
    }
    
    for log in logs:
        try:
            timestamp_str = log.get("timestamp", "")
            if isinstance(timestamp_str, str):
                try:
                    log_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except:
                    continue
            else:
                continue
            
            if log_time < cutoff_time:
                continue
            
            stats["total_attempts"] += 1
            
            if log.get("access_granted", False):
                stats["granted"] += 1
            else:
                stats["denied"] += 1
                reason = log.get("reason", "Unknown reason")
                stats["denial_reasons"][reason] += 1
            
            role = log.get("user_role", "unknown")
            if log.get("access_granted", False):
                stats["by_role"][role]["granted"] += 1
            else:
                stats["by_role"][role]["denied"] += 1
            
            metric = log.get("metric_name", "unknown")
            if log.get("access_granted", False):
                stats["by_metric"][metric]["granted"] += 1
            else:
                stats["by_metric"][metric]["denied"] += 1
                
        except Exception:
            continue
    
    return stats

def print_query_stats(stats: Dict[str, Any]):
    """Print query execution statistics."""
    print_section("Query Execution Statistics")
    
    total = stats["total_queries"]
    if total == 0:
        print(f"{Colors.YELLOW}No queries found in the specified time period.{Colors.RESET}")
        return
    
    success_rate = (stats["successful_queries"] / total * 100) if total > 0 else 0
    
    print(f"Total Queries: {Colors.BOLD}{total}{Colors.RESET}")
    print(f"Successful: {Colors.GREEN}{stats['successful_queries']} ({success_rate:.1f}%){Colors.RESET}")
    print(f"Failed: {Colors.RED}{stats['failed_queries']} ({100-success_rate:.1f}%){Colors.RESET}")
    print(f"Average Execution Time: {Colors.BOLD}{stats['avg_execution_time']:.2f} ms{Colors.RESET}")
    
    if stats["metrics_used"]:
        print(f"\n{Colors.BOLD}Top 5 Metrics Used:{Colors.RESET}")
        top_metrics = sorted(stats["metrics_used"].items(), key=lambda x: x[1], reverse=True)[:5]
        for metric, count in top_metrics:
            print(f"  {metric}: {count} times")
    
    if stats["users"]:
        print(f"\n{Colors.BOLD}Active Users:{Colors.RESET}")
        top_users = sorted(stats["users"].items(), key=lambda x: x[1], reverse=True)[:5]
        for user, count in top_users:
            print(f"  {user}: {count} queries")
    
    if stats["errors"]:
        print(f"\n{Colors.RED}Top Errors:{Colors.RESET}")
        top_errors = sorted(stats["errors"].items(), key=lambda x: x[1], reverse=True)[:5]
        for error, count in top_errors:
            print(f"  {error}: {count} times")

def print_metric_usage_stats(stats: Dict[str, Any]):
    """Print metric usage statistics."""
    print_section("Metric Usage Statistics")
    
    if stats["total_usage"] == 0:
        print(f"{Colors.YELLOW}No metric usage found in the specified time period.{Colors.RESET}")
        return
    
    print(f"Total Metric Usage: {Colors.BOLD}{stats['total_usage']}{Colors.RESET}")
    
    if stats["top_metrics"]:
        print(f"\n{Colors.BOLD}Top 10 Most Used Metrics:{Colors.RESET}")
        for i, (metric_name, data) in enumerate(stats["top_metrics"], 1):
            print(f"\n  {i}. {Colors.BOLD}{metric_name}{Colors.RESET}")
            print(f"     Usage Count: {data['count']}")
            print(f"     Unique Users: {data['user_count']}")
            if data['avg_time'] > 0:
                print(f"     Avg Execution Time: {data['avg_time']:.2f} ms")

def print_access_control_stats(stats: Dict[str, Any]):
    """Print access control statistics."""
    print_section("Access Control Statistics")
    
    if stats["total_attempts"] == 0:
        print(f"{Colors.YELLOW}No access control events found in the specified time period.{Colors.RESET}")
        return
    
    grant_rate = (stats["granted"] / stats["total_attempts"] * 100) if stats["total_attempts"] > 0 else 0
    
    print(f"Total Access Attempts: {Colors.BOLD}{stats['total_attempts']}{Colors.RESET}")
    print(f"Granted: {Colors.GREEN}{stats['granted']} ({grant_rate:.1f}%){Colors.RESET}")
    print(f"Denied: {Colors.RED}{stats['denied']} ({100-grant_rate:.1f}%){Colors.RESET}")
    
    if stats["denial_reasons"]:
        print(f"\n{Colors.RED}Denial Reasons:{Colors.RESET}")
        for reason, count in sorted(stats["denial_reasons"].items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {reason}: {count} times")
    
    if stats["by_role"]:
        print(f"\n{Colors.BOLD}Access by Role:{Colors.RESET}")
        for role, data in sorted(stats["by_role"].items()):
            total = data["granted"] + data["denied"]
            if total > 0:
                grant_pct = (data["granted"] / total * 100)
                print(f"  {role}: {data['granted']}/{total} granted ({grant_pct:.1f}%)")

def main():
    """Main monitoring function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor RCA Engine system")
    parser.add_argument("--hours", type=int, default=24, help="Hours to look back (default: 24)")
    parser.add_argument("--log-dir", type=str, default="logs", help="Log directory (default: logs)")
    
    args = parser.parse_args()
    
    log_dir = Path(args.log_dir)
    
    print_header("RCA ENGINE SYSTEM MONITOR")
    print(f"Monitoring period: Last {args.hours} hours")
    print(f"Log directory: {log_dir.absolute()}")
    
    # Load logs
    query_log_file = log_dir / "query_execution.log"
    metric_log_file = log_dir / "metric_usage.log"
    access_log_file = log_dir / "access_control.log"
    
    query_logs = load_json_logs(query_log_file)
    metric_logs = load_json_logs(metric_log_file)
    access_logs = load_json_logs(access_log_file)
    
    print(f"\nLoaded logs:")
    print(f"  Query logs: {len(query_logs)} entries")
    print(f"  Metric logs: {len(metric_logs)} entries")
    print(f"  Access logs: {len(access_logs)} entries")
    
    # Analyze and print statistics
    if query_logs:
        query_stats = analyze_query_logs(query_logs, args.hours)
        print_query_stats(query_stats)
    
    if metric_logs:
        metric_stats = analyze_metric_usage(metric_logs, args.hours)
        print_metric_usage_stats(metric_stats)
    
    if access_logs:
        access_stats = analyze_access_control(access_logs, args.hours)
        print_access_control_stats(access_stats)
    
    # System health summary
    print_section("System Health Summary")
    
    if not query_logs and not metric_logs and not access_logs:
        print(f"{Colors.YELLOW}⚠️  No logs found. System may not be running or logging may not be configured.{Colors.RESET}")
    else:
        print(f"{Colors.GREEN}✓ System is generating logs{Colors.RESET}")
    
    print(f"\n{Colors.BOLD}Next Steps:{Colors.RESET}")
    print("  1. Review query execution statistics")
    print("  2. Monitor metric usage patterns")
    print("  3. Check access control decisions")
    print("  4. Fine-tune RAG parameters based on usage")
    print("  5. Adjust access policies if needed")
    
    print(f"\n{Colors.BLUE}For detailed analysis, check:{Colors.RESET}")
    print(f"  - Query logs: {query_log_file}")
    print(f"  - Metric logs: {metric_log_file}")
    print(f"  - Access logs: {access_log_file}")

if __name__ == "__main__":
    main()

