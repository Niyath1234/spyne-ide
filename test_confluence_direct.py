"""
Test Confluence API Connection - Direct Requests

Uses the correct endpoints provided by the user.
"""

import sys
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin

def test_confluence_direct(url, username, api_token):
    """Test Confluence using direct API calls."""
    
    # Base URL should be https://slicepay.atlassian.net/wiki
    # But API endpoints are relative, so we need to construct correctly
    base_url = url.rstrip('/')
    
    # If URL ends with /wiki, use it as-is for API calls
    # The API endpoints are: /rest/api/... relative to the base
    if base_url.endswith('/wiki'):
        api_base = base_url  # https://slicepay.atlassian.net/wiki
    else:
        api_base = f"{base_url}/wiki"
    
    print("="*60)
    print("Confluence API Test - Direct Requests")
    print("="*60)
    print(f"Base URL: {base_url}")
    print(f"Username: {username}")
    print()
    
    results = {}
    
    # Test 1: Current user
    print("Test 1: GET /rest/api/user/current")
    print("-" * 60)
    print(f"  Trying: {api_base}/rest/api/user/current")
    try:
        api_url = f"{api_base}/rest/api/user/current"
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, api_token),
            headers={"Accept": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            user_info = response.json()
            results["user"] = {
                "success": True,
                "user": user_info.get("displayName", "Unknown"),
                "email": user_info.get("email", "Unknown")
            }
            print(f"✓ Success: {user_info.get('displayName')} ({user_info.get('email')})")
        else:
            results["user"] = {"success": False, "status": response.status_code}
            print(f"✗ Failed: {response.status_code}")
            print(f"  {response.text[:200]}")
    except Exception as e:
        results["user"] = {"success": False, "error": str(e)}
        print(f"✗ Error: {e}")
    
    # Test 2: List content (pages)
    print("\nTest 2: GET /rest/api/content")
    print("-" * 60)
    print(f"  Trying: {api_base}/rest/api/content")
    try:
        api_url = f"{api_base}/rest/api/content"
        params = {"limit": 10}
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
            results["content"] = {
                "success": True,
                "total": content.get("size", 0),
                "pages": len(pages)
            }
            print(f"✓ Success: Found {len(pages)} pages")
            if pages:
                print("  Sample pages:")
                for page in pages[:5]:
                    print(f"    - {page.get('title', 'Unknown')} (ID: {page.get('id')})")
        else:
            results["content"] = {"success": False, "status": response.status_code}
            print(f"✗ Failed: {response.status_code}")
            print(f"  {response.text[:200]}")
    except Exception as e:
        results["content"] = {"success": False, "error": str(e)}
        print(f"✗ Error: {e}")
    
    # Test 3: List spaces
    print("\nTest 3: GET /rest/api/space")
    print("-" * 60)
    print(f"  Trying: {api_base}/rest/api/space")
    try:
        api_url = f"{api_base}/rest/api/space"
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
            results["spaces"] = {
                "success": True,
                "total": len(spaces)
            }
            print(f"✓ Success: Found {len(spaces)} spaces")
            if spaces:
                print("  Sample spaces:")
                for space in spaces[:5]:
                    print(f"    - {space.get('name', 'Unknown')} (Key: {space.get('key')})")
        else:
            results["spaces"] = {"success": False, "status": response.status_code}
            print(f"✗ Failed: {response.status_code}")
            print(f"  {response.text[:200]}")
    except Exception as e:
        results["spaces"] = {"success": False, "error": str(e)}
        print(f"✗ Error: {e}")
    
    # Test 4: Search for ARD/PRD/TRD
    print("\nTest 4: Search for ARD/PRD/TRD pages")
    print("-" * 60)
    print(f"  Trying: {api_base}/rest/api/content")
    try:
        api_url = f"{api_base}/rest/api/content"
        params = {"limit": 50}
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, api_token),
            headers={"Accept": "application/json"},
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            content = response.json()
            all_pages = content.get("results", [])
            
            # Filter for ARD/PRD/TRD
            doc_pages = [
                p for p in all_pages
                if any(term in p.get("title", "").upper() 
                      for term in ["ARD", "PRD", "TRD"])
            ]
            
            results["documents"] = {
                "success": True,
                "total_pages": len(all_pages),
                "ard_prd_trd": len(doc_pages)
            }
            print(f"✓ Success: Found {len(doc_pages)} ARD/PRD/TRD pages out of {len(all_pages)} total")
            if doc_pages:
                print("  Document pages:")
                for page in doc_pages[:10]:
                    title = page.get("title", "Unknown")
                    page_id = page.get("id")
                    print(f"    - {title} (ID: {page_id})")
        else:
            results["documents"] = {"success": False, "status": response.status_code}
            print(f"✗ Failed: {response.status_code}")
    except Exception as e:
        results["documents"] = {"success": False, "error": str(e)}
        print(f"✗ Error: {e}")
    
    # Summary
    print("\n" + "="*60)
    print("Summary")
    print("="*60)
    for test_name, result in results.items():
        status = "✓" if result.get("success") else "✗"
        print(f"{status} {test_name}")
    
    all_passed = all(r.get("success") for r in results.values())
    return all_passed


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent / "src"))
    from src.config import Config
    
    url = Config.get_confluence_url()
    username = Config.get_confluence_username()
    api_token = Config.get_confluence_api_token()
    
    if not username or not api_token:
        print("Error: CONFLUENCE_USERNAME and CONFLUENCE_API_TOKEN must be set in .env file")
        sys.exit(1)
    
    success = test_confluence_direct(url, username, api_token)
    sys.exit(0 if success else 1)

