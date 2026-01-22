"""
Test Confluence API Connection

Verifies that Confluence API credentials are functional and accessible.
Uses the same credentials as Jira (same Atlassian instance).
"""

import sys
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin

def test_confluence_connection(url, username, api_token):
    """
    Test Confluence API connection.
    
    Args:
        url: Confluence URL (e.g., https://slicepay.atlassian.net)
        username: Confluence username/email
        api_token: Confluence API token (same as Jira)
        
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
    
    # Use Confluence URL directly (should be https://slicepay.atlassian.net/wiki)
    # Remove any duplicate /wiki
    if url.endswith("/wiki"):
        confluence_base = url
    elif "/wiki/" in url:
        confluence_base = url.split("/wiki")[0] + "/wiki"
    elif ".atlassian.net" in url and "/wiki" not in url:
        # Convert Jira URL to Confluence URL
        confluence_base = url.replace(".atlassian.net", ".atlassian.net/wiki")
    else:
        confluence_base = url
    
    # Test 1: Basic API access
    print("="*60)
    print("Test 1: Basic Confluence API Access")
    print("="*60)
    try:
        api_url = urljoin(confluence_base, "/rest/api/content")
        params = {"limit": 1}
        
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, api_token),
            headers={"Accept": "application/json"},
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            content = response.json()
            results["tests"]["basic_access"] = {
                "success": True,
                "total_pages": content.get("size", 0)
            }
            print(f"✓ Successfully connected to Confluence")
            print(f"  Base URL: {confluence_base}")
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
        return results
    
    # Test 2: List spaces
    print("\n" + "="*60)
    print("Test 2: List Spaces")
    print("="*60)
    try:
        api_url = urljoin(confluence_base, "/rest/api/space")
        params = {"limit": 20}
        
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, api_token),
            headers={"Accept": "application/json"},
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            spaces_data = response.json()
            spaces = spaces_data.get("results", [])
            space_keys = [s.get("key") for s in spaces[:10]]
            space_names = [s.get("name") for s in spaces[:10]]
            
            results["tests"]["list_spaces"] = {
                "success": True,
                "total_spaces": len(spaces),
                "space_keys": space_keys,
                "space_names": space_names
            }
            print(f"✓ Successfully retrieved {len(spaces)} spaces")
            if space_names:
                print(f"  Sample spaces:")
                for name, key in zip(space_names[:5], space_keys[:5]):
                    print(f"    - {name} ({key})")
        else:
            results["tests"]["list_spaces"] = {
                "success": False,
                "status_code": response.status_code,
                "error": response.text[:200]
            }
            print(f"✗ Failed: Status {response.status_code}")
    
    except Exception as e:
        results["tests"]["list_spaces"] = {
            "success": False,
            "error": str(e)
        }
        print(f"✗ Error: {e}")
    
    # Test 3: Search for ARD/PRD/TRD pages
    print("\n" + "="*60)
    print("Test 3: Search for ARD/PRD/TRD Pages")
    print("="*60)
    try:
        api_url = urljoin(confluence_base, "/rest/api/content/search")
        
        # Search for ARD
        params = {
            "cql": 'title ~ "ARD" OR title ~ "PRD" OR title ~ "TRD"',
            "limit": 10
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
            pages = search_results.get("results", [])
            
            results["tests"]["search_documents"] = {
                "success": True,
                "pages_found": len(pages),
                "sample_titles": [p.get("title") for p in pages[:5]]
            }
            print(f"✓ Successfully searched for documents")
            print(f"  Pages found: {len(pages)}")
            if pages:
                print(f"  Sample pages:")
                for page in pages[:5]:
                    print(f"    - {page.get('title')}")
        else:
            results["tests"]["search_documents"] = {
                "success": False,
                "status_code": response.status_code,
                "note": "CQL search may require different syntax or permissions"
            }
            print(f"⚠ Status {response.status_code} (trying alternative search)")
            
            # Try alternative: regular content search
            api_url = urljoin(confluence_base, "/rest/api/content")
            params = {"limit": 20}
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
                ard_prd_trd = [
                    p for p in pages 
                    if any(term in p.get("title", "").upper() 
                          for term in ["ARD", "PRD", "TRD"])
                ]
                
                if ard_prd_trd:
                    results["tests"]["search_documents"] = {
                        "success": True,
                        "pages_found": len(ard_prd_trd),
                        "method": "content_search",
                        "sample_titles": [p.get("title") for p in ard_prd_trd[:5]]
                    }
                    print(f"✓ Found {len(ard_prd_trd)} ARD/PRD/TRD pages via content search")
    
    except Exception as e:
        results["tests"]["search_documents"] = {
            "success": False,
            "error": str(e)
        }
        print(f"✗ Error: {e}")
    
    # Overall result
    critical_tests = ["basic_access"]
    critical_passed = all(
        results["tests"].get(test, {}).get("success", False)
        for test in critical_tests
    )
    
    results["success"] = critical_passed
    
    return results


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Confluence API connection")
    parser.add_argument("--url", type=str, help="Atlassian URL (Jira/Confluence)")
    parser.add_argument("--username", type=str, help="Username/email")
    parser.add_argument("--api-token", type=str, help="API token")
    
    args = parser.parse_args()
    
    # Import Config
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent / "src"))
    from src.config import Config
    
    # Use provided credentials or fall back to environment variables
    url = args.url or Config.get_confluence_url().replace("/wiki", "")
    username = args.username or Config.get_confluence_username()
    api_token = args.api_token or Config.get_confluence_api_token()
    
    if not username or not api_token:
        print("Error: Confluence credentials must be provided via --username and --api-token arguments")
        print("       or set CONFLUENCE_USERNAME and CONFLUENCE_API_TOKEN in .env file")
        return 1
    
    print("Confluence API Connection Test")
    print("="*60)
    print(f"Base URL: {url}")
    print(f"Confluence URL: {url.replace('.atlassian.net', '.atlassian.net/wiki')}")
    print(f"Username: {username}")
    print(f"API Token: {'*' * 20}...{api_token[-10:]}")
    print()
    
    results = test_confluence_connection(url, username, api_token)
    
    print("\n" + "="*60)
    print("Summary")
    print("="*60)
    
    for test_name, test_result in results["tests"].items():
        status = "✓ PASS" if test_result.get("success") else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    if results["success"]:
        print("\n✓ Confluence API is accessible!")
        print("\nYou can now use:")
        print("  python src/confluence_ingest.py")
        return 0
    else:
        print("\n✗ Some tests failed. Check errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

