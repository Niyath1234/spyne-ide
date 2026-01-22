"""
Test Jira API Connection

Verifies that Jira API credentials are functional and accessible.
"""

import sys
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin

def test_jira_connection(url, username, api_token):
    """
    Test Jira API connection.
    
    Args:
        url: Jira URL (e.g., https://slicepay.atlassian.net)
        username: Jira username/email
        api_token: Jira API token
        
    Returns:
        Dictionary with test results
    """
    results = {
        "success": False,
        "tests": {},
        "error": None
    }
    
    # Ensure URL doesn't end with /
    url = url.rstrip('/')
    
    # Test 1: Basic API access
    print("="*60)
    print("Test 1: Basic API Access")
    print("="*60)
    try:
        api_url = urljoin(url, "/rest/api/3/myself")
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, api_token),
            headers={"Accept": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            user_info = response.json()
            results["tests"]["basic_access"] = {
                "success": True,
                "user": user_info.get("displayName", "Unknown"),
                "email": user_info.get("emailAddress", "Unknown"),
                "account_id": user_info.get("accountId", "Unknown")
            }
            print(f"✓ Successfully connected to Jira")
            print(f"  User: {user_info.get('displayName')}")
            print(f"  Email: {user_info.get('emailAddress')}")
            print(f"  Account ID: {user_info.get('accountId')}")
        else:
            results["tests"]["basic_access"] = {
                "success": False,
                "status_code": response.status_code,
                "error": response.text[:200]
            }
            print(f"✗ Failed: Status {response.status_code}")
            print(f"  Error: {response.text[:200]}")
    
    except Exception as e:
        results["tests"]["basic_access"] = {
            "success": False,
            "error": str(e)
        }
        print(f"✗ Error: {e}")
    
    # Test 2: List projects
    print("\n" + "="*60)
    print("Test 2: List Projects")
    print("="*60)
    try:
        api_url = urljoin(url, "/rest/api/3/project")
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, api_token),
            headers={"Accept": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            projects = response.json()
            project_keys = [p.get("key") for p in projects[:10]]  # First 10
            results["tests"]["list_projects"] = {
                "success": True,
                "total_projects": len(projects),
                "project_keys": project_keys
            }
            print(f"✓ Successfully retrieved {len(projects)} projects")
            if project_keys:
                print(f"  Sample project keys: {', '.join(project_keys[:5])}")
        else:
            results["tests"]["list_projects"] = {
                "success": False,
                "status_code": response.status_code,
                "error": response.text[:200]
            }
            print(f"✗ Failed: Status {response.status_code}")
    
    except Exception as e:
        results["tests"]["list_projects"] = {
            "success": False,
            "error": str(e)
        }
        print(f"✗ Error: {e}")
    
    # Test 3: Search issues (if projects available)
    print("\n" + "="*60)
    print("Test 3: Search Issues")
    print("="*60)
    try:
        # Try API v2 first (more stable)
        api_url = urljoin(url, "/rest/api/2/search")
        params = {
            "jql": "order by created DESC",
            "maxResults": 5
        }
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, api_token),
            headers={"Accept": "application/json"},
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            search_results = response.json()
            total = search_results.get("total", 0)
            issues = search_results.get("issues", [])
            issue_keys = [issue.get("key") for issue in issues]
            
            results["tests"]["search_issues"] = {
                "success": True,
                "total_issues": total,
                "sample_issues": issue_keys
            }
            print(f"✓ Successfully searched issues")
            print(f"  Total issues: {total}")
            if issue_keys:
                print(f"  Sample issues: {', '.join(issue_keys)}")
        else:
            results["tests"]["search_issues"] = {
                "success": False,
                "status_code": response.status_code,
                "note": "Search endpoint may require different permissions or API version"
            }
            print(f"⚠ Skipped: Status {response.status_code} (may require different permissions)")
    
    except Exception as e:
        results["tests"]["search_issues"] = {
            "success": False,
            "error": str(e)
        }
        print(f"✗ Error: {e}")
    
    # Test 4: Test Confluence access (same Atlassian instance)
    print("\n" + "="*60)
    print("Test 4: Confluence Access (Same Instance)")
    print("="*60)
    try:
        # Confluence uses same credentials
        confluence_url = url.replace(".atlassian.net", ".atlassian.net/wiki")
        api_url = urljoin(confluence_url, "/rest/api/content")
        params = {"limit": 5}
        
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, api_token),
            headers={"Accept": "application/json"},
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            content = response.json()
            pages = content.get("results", [])
            results["tests"]["confluence_access"] = {
                "success": True,
                "pages_found": len(pages)
            }
            print(f"✓ Successfully accessed Confluence")
            print(f"  Pages found: {len(pages)}")
            if pages:
                print(f"  Sample pages:")
                for page in pages[:3]:
                    print(f"    - {page.get('title', 'Unknown')}")
        else:
            results["tests"]["confluence_access"] = {
                "success": False,
                "status_code": response.status_code,
                "note": "May require Confluence-specific permissions"
            }
            print(f"⚠ Status {response.status_code} (may require Confluence permissions)")
    
    except Exception as e:
        results["tests"]["confluence_access"] = {
            "success": False,
            "error": str(e)
        }
        print(f"✗ Error: {e}")
    
    # Overall result
    all_tests_passed = all(
        test.get("success", False) 
        for test in results["tests"].values()
    )
    
    results["success"] = all_tests_passed
    
    return results


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Jira API connection")
    parser.add_argument("--url", type=str, help="Jira URL")
    parser.add_argument("--username", type=str, help="Jira username/email")
    parser.add_argument("--api-token", type=str, help="Jira API token")
    
    args = parser.parse_args()
    
    # Import Config
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent / "src"))
    from src.config import Config
    
    # Use provided credentials or fall back to environment variables
    url = args.url or Config.get_jira_url()
    username = args.username or Config.get_jira_username()
    api_token = args.api_token or Config.get_jira_api_token()
    
    if not username or not api_token:
        print("Error: Jira credentials must be provided via --username and --api-token arguments")
        print("       or set JIRA_USERNAME and JIRA_API_TOKEN in .env file")
        return 1
    
    print("Jira API Connection Test")
    print("="*60)
    print(f"URL: {url}")
    print(f"Username: {username}")
    print(f"API Token: {'*' * 20}...{api_token[-10:]}")
    print()
    
    results = test_jira_connection(url, username, api_token)
    
    print("\n" + "="*60)
    print("Summary")
    print("="*60)
    
    for test_name, test_result in results["tests"].items():
        status = "✓ PASS" if test_result.get("success") else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    if results["success"]:
        print("\n✓ All tests passed! Jira API is accessible.")
        return 0
    else:
        print("\n✗ Some tests failed. Check errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

