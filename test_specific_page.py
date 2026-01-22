"""
Test fetching a specific Confluence page

Tests access to: https://slicepay.atlassian.net/wiki/spaces/HOR/pages/3024814095/UPI+Lite+-+ARD
"""

import sys
import requests
from requests.auth import HTTPBasicAuth
import json

def fetch_confluence_page(page_id, url_base, username, api_token):
    """
    Fetch a specific Confluence page by ID.
    
    Args:
        page_id: Confluence page ID
        url_base: Base URL (https://slicepay.atlassian.net/wiki)
        username: Username
        api_token: API token
        
    Returns:
        Page content dictionary
    """
    # API endpoint: GET /rest/api/content/{pageId}
    api_url = f"{url_base}/rest/api/content/{page_id}"
    
    # Expand to get full content
    params = {
        "expand": "body.storage,space,version,metadata.labels,ancestors"
    }
    
    print(f"Fetching page ID: {page_id}")
    print(f"URL: {api_url}")
    print()
    
    try:
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, api_token),
            headers={"Accept": "application/json"},
            params=params,
            timeout=30
        )
        
        if response.status_code == 200:
            page_data = response.json()
            return page_data
        else:
            print(f"Error: Status {response.status_code}")
            print(response.text[:500])
            return None
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def display_page_info(page_data):
    """Display page information."""
    if not page_data:
        return
    
    print("="*60)
    print("Page Information")
    print("="*60)
    
    # Basic info
    print(f"Title: {page_data.get('title', 'Unknown')}")
    print(f"Page ID: {page_data.get('id', 'Unknown')}")
    print(f"Type: {page_data.get('type', 'Unknown')}")
    
    # Space info
    space = page_data.get('space', {})
    print(f"Space: {space.get('name', 'Unknown')} (Key: {space.get('key', 'Unknown')})")
    
    # Version info
    version = page_data.get('version', {})
    print(f"Version: {version.get('number', 'Unknown')}")
    print(f"Last Modified: {version.get('when', 'Unknown')}")
    
    # URL
    links = page_data.get('_links', {})
    webui = links.get('webui', '')
    if webui:
        print(f"URL: https://slicepay.atlassian.net{webui}")
    
    # Labels
    metadata = page_data.get('metadata', {})
    labels = metadata.get('labels', {}).get('results', [])
    if labels:
        print(f"Labels: {', '.join([l.get('name') for l in labels])}")
    
    # Content
    print("\n" + "="*60)
    print("Page Content")
    print("="*60)
    
    body = page_data.get('body', {})
    storage = body.get('storage', {})
    content = storage.get('value', '')
    
    if content:
        # Display first 2000 characters
        print(content[:2000])
        if len(content) > 2000:
            print(f"\n... (content truncated, total length: {len(content)} characters)")
        
        # Save full content to file
        output_file = f"data/raw/confluence_page_{page_data.get('id')}.html"
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        Path(output_file).write_text(content, encoding='utf-8')
        print(f"\n✓ Full content saved to: {output_file}")
    else:
        print("No content found")
    
    # Ancestors (parent pages)
    ancestors = page_data.get('ancestors', [])
    if ancestors:
        print("\n" + "="*60)
        print("Parent Pages")
        print("="*60)
        for ancestor in ancestors:
            print(f"  - {ancestor.get('title', 'Unknown')} (ID: {ancestor.get('id')})")


if __name__ == "__main__":
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parent / "src"))
    from src.config import Config
    
    # Page details from URL
    page_id = "3024814095"
    space_key = "HOR"
    page_title = "UPI Lite - ARD"
    
    # Credentials from environment variables
    url_base = Config.get_confluence_url()
    username = Config.get_confluence_username()
    api_token = Config.get_confluence_api_token()
    
    if not username or not api_token:
        print("Error: CONFLUENCE_USERNAME and CONFLUENCE_API_TOKEN must be set in .env file")
        sys.exit(1)
    
    print("Fetching Confluence Page")
    print("="*60)
    print(f"Page ID: {page_id}")
    print(f"Space: {space_key}")
    print(f"Title: {page_title}")
    print()
    
    page_data = fetch_confluence_page(page_id, url_base, username, api_token)
    
    if page_data:
        display_page_info(page_data)
        
        # Also save as JSON for inspection
        json_file = f"data/raw/confluence_page_{page_id}_metadata.json"
        Path(json_file).parent.mkdir(parents=True, exist_ok=True)
        Path(json_file).write_text(
            json.dumps(page_data, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )
        print(f"\n✓ Metadata saved to: {json_file}")
    else:
        print("\n✗ Failed to fetch page")
        sys.exit(1)

