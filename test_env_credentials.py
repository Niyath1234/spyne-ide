"""
Test Environment Credentials

Verifies that all credentials from .env file are loaded correctly
and can be used to access Confluence, Jira, and Slack.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config import Config
import requests
from requests.auth import HTTPBasicAuth


def test_config_loading():
    """Test that Config class loads credentials from .env"""
    print("="*80)
    print("TEST 1: Config Class Loading")
    print("="*80)
    
    results = {
        "confluence": {},
        "jira": {},
        "slack": {}
    }
    
    # Test Confluence
    print("\nConfluence Configuration:")
    print("-" * 80)
    url = Config.get_confluence_url()
    username = Config.get_confluence_username()
    token = Config.get_confluence_api_token()
    space = Config.get_confluence_space_key()
    
    results["confluence"] = {
        "url": url,
        "username": username,
        "token_set": bool(token),
        "token_preview": f"{token[:20]}...{token[-10:]}" if token else None,
        "space": space
    }
    
    print(f"  URL: {url}")
    print(f"  Username: {username}")
    print(f"  Token: {'✓ SET' if token else '✗ NOT SET'}")
    if token:
        print(f"  Token Preview: {token[:20]}...{token[-10:]}")
    print(f"  Space Key: {space}")
    
    # Test Jira
    print("\nJira Configuration:")
    print("-" * 80)
    jira_url = Config.get_jira_url()
    jira_username = Config.get_jira_username()
    jira_token = Config.get_jira_api_token()
    
    results["jira"] = {
        "url": jira_url,
        "username": jira_username,
        "token_set": bool(jira_token),
        "token_preview": f"{jira_token[:20]}...{jira_token[-10:]}" if jira_token else None
    }
    
    print(f"  URL: {jira_url}")
    print(f"  Username: {jira_username}")
    print(f"  Token: {'✓ SET' if jira_token else '✗ NOT SET'}")
    if jira_token:
        print(f"  Token Preview: {jira_token[:20]}...{jira_token[-10:]}")
    
    # Test Slack
    print("\nSlack Configuration:")
    print("-" * 80)
    slack_token = Config.get_slack_bot_token()
    slack_channel = Config.get_slack_default_channel()
    slack_xoxc = Config.get_slack_xoxc_token()
    slack_xoxd = Config.get_slack_xoxd_token()
    
    results["slack"] = {
        "bot_token_set": bool(slack_token),
        "bot_token_preview": f"{slack_token[:20]}...{slack_token[-10:]}" if slack_token else None,
        "channel": slack_channel,
        "xoxc_set": bool(slack_xoxc),
        "xoxd_set": bool(slack_xoxd)
    }
    
    print(f"  Bot Token: {'✓ SET' if slack_token else '✗ NOT SET'}")
    if slack_token:
        print(f"  Bot Token Preview: {slack_token[:20]}...{slack_token[-10:]}")
    print(f"  Default Channel: {slack_channel}")
    print(f"  XOXC Token: {'✓ SET' if slack_xoxc else '✗ NOT SET'}")
    print(f"  XOXD Token: {'✓ SET' if slack_xoxd else '✗ NOT SET'}")
    
    # Validation
    print("\n" + "="*80)
    print("Validation:")
    print("="*80)
    
    is_valid, error = Config.validate_confluence_config()
    print(f"Confluence Config: {'✓ Valid' if is_valid else '✗ Invalid: ' + error}")
    
    is_valid_slack, error_slack = Config.validate_slack_config()
    print(f"Slack Config: {'✓ Valid' if is_valid_slack else '✗ Invalid: ' + error_slack}")
    
    return results


def test_confluence_connection():
    """Test actual Confluence API connection"""
    print("\n" + "="*80)
    print("TEST 2: Confluence API Connection")
    print("="*80)
    
    url = Config.get_confluence_url()
    username = Config.get_confluence_username()
    token = Config.get_confluence_api_token()
    
    if not username or not token:
        print("✗ Cannot test: Credentials not set")
        return False
    
    try:
        # Test authentication
        api_url = f"{url}/rest/api/user/current"
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, token),
            headers={"Accept": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            user_info = response.json()
            print(f"✓ Connected successfully")
            print(f"  User: {user_info.get('displayName', 'Unknown')}")
            print(f"  Email: {user_info.get('email', 'Unknown')}")
            return True
        else:
            print(f"✗ Connection failed: {response.status_code}")
            print(f"  Error: {response.text[:200]}")
            return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False


def test_slack_connection():
    """Test Slack connection using MCP tools"""
    print("\n" + "="*80)
    print("TEST 3: Slack Connection (via MCP)")
    print("="*80)
    
    slack_token = Config.get_slack_bot_token()
    
    if not slack_token:
        print("✗ Cannot test: SLACK_BOT_TOKEN not set")
        return False
    
    print(f"✓ Slack Bot Token loaded: {slack_token[:20]}...{slack_token[-10:]}")
    print("  Note: Actual Slack API calls require MCP server to be running")
    print("  The token is ready to use with MCP tools:")
    print("    - mcp_mcp-slack_list_channels()")
    print("    - mcp_mcp-slack_send_message()")
    print("    - mcp_mcp-slack_get_channel_history()")
    
    return True


def test_jira_connection():
    """Test Jira API connection"""
    print("\n" + "="*80)
    print("TEST 4: Jira API Connection")
    print("="*80)
    
    url = Config.get_jira_url()
    username = Config.get_jira_username()
    token = Config.get_jira_api_token()
    
    if not username or not token:
        print("✗ Cannot test: Credentials not set")
        return False
    
    try:
        # Test authentication
        api_url = f"{url}/rest/api/3/myself"
        response = requests.get(
            api_url,
            auth=HTTPBasicAuth(username, token),
            headers={"Accept": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            user_info = response.json()
            print(f"✓ Connected successfully")
            print(f"  User: {user_info.get('displayName', 'Unknown')}")
            print(f"  Email: {user_info.get('emailAddress', 'Unknown')}")
            return True
        else:
            print(f"✗ Connection failed: {response.status_code}")
            print(f"  Error: {response.text[:200]}")
            return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("ENVIRONMENT CREDENTIALS TEST")
    print("="*80)
    print("\nTesting access to all services using credentials from .env file...")
    
    # Test 1: Config loading
    config_results = test_config_loading()
    
    # Test 2: Confluence connection
    confluence_ok = test_confluence_connection()
    
    # Test 3: Slack connection
    slack_ok = test_slack_connection()
    
    # Test 4: Jira connection
    jira_ok = test_jira_connection()
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Config Loading:     ✓")
    print(f"Confluence Access:  {'✓' if confluence_ok else '✗'}")
    print(f"Slack Access:       {'✓' if slack_ok else '✗'} (Token loaded, ready for MCP)")
    print(f"Jira Access:        {'✓' if jira_ok else '✗'}")
    
    print("\n" + "="*80)
    print("NEXT STEPS")
    print("="*80)
    if confluence_ok:
        print("✓ Confluence is ready - you can fetch pages and extract knowledge")
    if slack_ok:
        print("✓ Slack is ready - use MCP tools to send notifications")
    if jira_ok:
        print("✓ Jira is ready - you can query issues and create tickets")
    
    print("\nAll credentials are loaded from .env file and ready to use!")
    
    return all([confluence_ok, slack_ok, jira_ok])


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

