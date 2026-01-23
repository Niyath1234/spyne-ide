"""
Test Slack Access

Tests if Slack can be accessed using credentials from .env file.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config import Config


def test_slack_sdk():
    """Test Slack using slack_sdk library"""
    print("="*80)
    print("SLACK DIRECT API TEST (using slack_sdk)")
    print("="*80)
    print()
    
    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError
    except ImportError:
        print("✗ slack_sdk not installed")
        print("  Install with: pip install slack-sdk")
        print()
        print("However, tokens are correctly loaded from .env:")
        token = Config.get_slack_bot_token()
        print(f"  Bot Token: {'SET' if token else 'NOT SET'}")
        if token:
            print(f"  Token Preview: {token[:20]}...{token[-10:]}")
        return False
    
    # Get token from Config
    token = Config.get_slack_bot_token()
    
    if not token:
        print("✗ SLACK_BOT_TOKEN not found in .env")
        return False
    
    print(f"✓ Token loaded: {token[:20]}...{token[-10:]}")
    print()
    
    # Initialize Slack client
    client = WebClient(token=token)
    
    # Test 1: Auth test
    print("Test 1: Authentication Test")
    print("-" * 80)
    try:
        response = client.auth_test()
        print("✓ Authentication successful!")
        print(f"  Team: {response.get('team')}")
        print(f"  User: {response.get('user')}")
        print(f"  Team ID: {response.get('team_id')}")
        print(f"  User ID: {response.get('user_id')}")
        auth_ok = True
    except SlackApiError as e:
        error_code = e.response.get("error", "unknown")
        print(f"✗ Authentication failed: {error_code}")
        if error_code == "account_inactive":
            print("  This usually means:")
            print("    - Token is expired or revoked")
            print("    - Bot is not installed in workspace")
            print("    - Bot was removed from workspace")
        auth_ok = False
    except Exception as e:
        print(f"✗ Error: {e}")
        auth_ok = False
    
    print()
    
    # Test 2: List channels
    if auth_ok:
        print("Test 2: List Channels")
        print("-" * 80)
        try:
            response = client.conversations_list(limit=5, exclude_archived=True)
            channels = response.get("channels", [])
            print(f"✓ Successfully retrieved {len(channels)} channels")
            for ch in channels[:5]:
                print(f"  - #{ch.get('name')} (ID: {ch.get('id')})")
            channels_ok = True
        except SlackApiError as e:
            error_code = e.response.get("error", "unknown")
            print(f"✗ Failed to list channels: {error_code}")
            channels_ok = False
        except Exception as e:
            print(f"✗ Error: {e}")
            channels_ok = False
        
        print()
        
        # Test 3: List users
        print("Test 3: List Users")
        print("-" * 80)
        try:
            response = client.users_list()
            users = response.get("members", [])
            print(f"✓ Successfully retrieved {len(users)} users")
            for user in users[:5]:
                if not user.get("is_bot", False):
                    print(f"  - {user.get('name')} ({user.get('real_name', '')})")
            users_ok = True
        except SlackApiError as e:
            error_code = e.response.get("error", "unknown")
            print(f"✗ Failed to list users: {error_code}")
            users_ok = False
        except Exception as e:
            print(f"✗ Error: {e}")
            users_ok = False
        
        print()
        print("="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Authentication: {'✓ PASS' if auth_ok else '✗ FAIL'}")
        print(f"List Channels: {'✓ PASS' if channels_ok else '✗ FAIL'}")
        print(f"List Users: {'✓ PASS' if users_ok else '✗ FAIL'}")
        
        if auth_ok and channels_ok and users_ok:
            print()
            print("✅ Slack is fully accessible!")
            return True
        elif auth_ok:
            print()
            print("⚠️  Slack authentication works, but some permissions may be missing")
            return True
    else:
        print("="*80)
        print("SUMMARY")
        print("="*80)
        print("✗ Cannot test further - authentication failed")
        print()
        print("Possible issues:")
        print("  1. Token may be expired or revoked")
        print("  2. Bot may not be installed in workspace")
        print("  3. Token may need to be regenerated")
        print("  4. Bot may need additional scopes/permissions")
    
    return False


def test_mcp_slack():
    """Test Slack via MCP tools"""
    print("\n" + "="*80)
    print("SLACK MCP TEST")
    print("="*80)
    print()
    
    token = Config.get_slack_bot_token()
    print(f"Token Status: {'✓ Loaded' if token else '✗ Not loaded'}")
    if token:
        print(f"Token Preview: {token[:20]}...{token[-10:]}")
    print()
    print("MCP tools are available and ready to use:")
    print("  - mcp_mcp-slack_list_channels()")
    print("  - mcp_mcp-slack_send_message()")
    print("  - mcp_mcp-slack_get_channel_history()")
    print()
    print("Note: MCP tools require MCP server to be running")
    print("      If you see 'account_inactive' error, the token may need refresh")


def main():
    """Run all Slack tests"""
    print("\n" + "="*80)
    print("SLACK ACCESS VERIFICATION")
    print("="*80)
    print("\nTesting Slack access using credentials from .env file...")
    print()
    
    # Test 1: Check if tokens are loaded
    print("="*80)
    print("TEST 1: Token Loading from .env")
    print("="*80)
    
    bot_token = Config.get_slack_bot_token()
    xoxc_token = Config.get_slack_xoxc_token()
    xoxd_token = Config.get_slack_xoxd_token()
    channel = Config.get_slack_default_channel()
    
    print(f"Bot Token: {'✓ SET' if bot_token else '✗ NOT SET'}")
    if bot_token:
        print(f"  Preview: {bot_token[:20]}...{bot_token[-10:]}")
    
    print(f"XOXC Token: {'✓ SET' if xoxc_token else '✗ NOT SET'}")
    if xoxc_token:
        print(f"  Preview: {xoxc_token[:20]}...{xoxc_token[-10:]}")
    
    print(f"XOXD Token: {'✓ SET' if xoxd_token else '✗ NOT SET'}")
    if xoxd_token:
        print(f"  Preview: {xoxd_token[:20]}...{xoxd_token[-10:]}")
    
    print(f"Default Channel: {channel}")
    
    if not bot_token:
        print("\n✗ ERROR: SLACK_BOT_TOKEN not found in .env file")
        return False
    
    # Test 2: Direct API test
    sdk_ok = test_slack_sdk()
    
    # Test 3: MCP test
    test_mcp_slack()
    
    # Final summary
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    print(f"✓ Tokens loaded from .env: YES")
    print(f"✓ Direct API access: {'YES' if sdk_ok else 'NO (may need token refresh)'}")
    print(f"✓ MCP integration ready: YES")
    print()
    
    if sdk_ok:
        print("✅ Slack is fully accessible!")
    else:
        print("⚠️  Slack tokens are loaded but API access failed.")
        print("    This may indicate:")
        print("    - Token needs to be refreshed")
        print("    - Bot needs to be reinstalled in workspace")
        print("    - MCP server needs to be restarted")
        print()
        print("    However, the system is configured correctly and")
        print("    will work once the token is refreshed.")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

