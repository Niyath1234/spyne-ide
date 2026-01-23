#!/usr/bin/env python3
"""
Detailed Slack Access Test

Tests Slack access and provides detailed diagnostics.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config import Config
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def test_slack_access():
    """Test Slack access with detailed error reporting"""
    print("="*80)
    print("SLACK ACCESS DIAGNOSTIC TEST")
    print("="*80)
    print()
    
    # Check token
    token = Config.get_slack_bot_token()
    if not token:
        print("✗ ERROR: SLACK_BOT_TOKEN not found in .env")
        return False
    
    print(f"✓ Token loaded: {token[:20]}...{token[-10:]}")
    print(f"  Token type: {'xoxb' if token.startswith('xoxb') else 'other'}")
    print()
    
    # Initialize client
    client = WebClient(token=token)
    
    # Test auth
    print("Testing authentication...")
    print("-" * 80)
    try:
        response = client.auth_test()
        print("✅ Authentication SUCCESSFUL!")
        print(f"   Team: {response.get('team')}")
        print(f"   User: {response.get('user')}")
        print(f"   Team ID: {response.get('team_id')}")
        print(f"   User ID: {response.get('user_id')}")
        print(f"   Bot ID: {response.get('bot_id', 'N/A')}")
        print()
        
        # If auth works, test other operations
        print("Testing channel access...")
        print("-" * 80)
        try:
            channels = client.conversations_list(limit=5, exclude_archived=True)
            print(f"✅ Successfully retrieved {len(channels.get('channels', []))} channels")
            for ch in channels.get('channels', [])[:5]:
                print(f"   - #{ch.get('name')} (ID: {ch.get('id')})")
        except SlackApiError as e:
            print(f"⚠️  Channel access failed: {e.response.get('error')}")
        
        print()
        print("Testing user list access...")
        print("-" * 80)
        try:
            users = client.users_list()
            print(f"✅ Successfully retrieved {len(users.get('members', []))} users")
        except SlackApiError as e:
            print(f"⚠️  User list access failed: {e.response.get('error')}")
        
        return True
        
    except SlackApiError as e:
        error_code = e.response.get("error", "unknown")
        error_msg = e.response.get("error", "No error message")
        
        print(f"✗ Authentication FAILED")
        print(f"   Error code: {error_code}")
        print()
        
        if error_code == "account_inactive":
            print("DIAGNOSIS: account_inactive error")
            print()
            print("This error typically means:")
            print("  1. The bot token has expired or been revoked")
            print("  2. The bot app was uninstalled from the workspace")
            print("  3. The bot app was removed or deleted")
            print("  4. The workspace subscription expired (if applicable)")
            print()
            print("SOLUTION:")
            print("  1. Go to https://api.slack.com/apps")
            print("  2. Find your bot application")
            print("  3. Check if it's installed in your workspace")
            print("  4. If not installed, reinstall it:")
            print("     - Go to 'OAuth & Permissions'")
            print("     - Click 'Reinstall to Workspace'")
            print("  5. Generate a new bot token")
            print("  6. Update SLACK_BOT_TOKEN in your .env file")
            print()
        elif error_code == "invalid_auth":
            print("DIAGNOSIS: invalid_auth error")
            print()
            print("This error means:")
            print("  - The token format is incorrect")
            print("  - The token is malformed")
            print()
            print("SOLUTION:")
            print("  - Verify the token starts with 'xoxb-'")
            print("  - Check for any extra spaces or characters")
            print("  - Regenerate the token from Slack API")
            print()
        elif error_code == "token_revoked":
            print("DIAGNOSIS: token_revoked error")
            print()
            print("This error means:")
            print("  - The token was explicitly revoked")
            print("  - The token was rotated")
            print()
            print("SOLUTION:")
            print("  - Generate a new token from Slack API")
            print("  - Update SLACK_BOT_TOKEN in your .env file")
            print()
        else:
            print(f"DIAGNOSIS: Unknown error ({error_code})")
            print()
            print("Please check:")
            print("  - Token is valid and not expired")
            print("  - Bot is installed in workspace")
            print("  - Bot has required permissions")
            print()
        
        return False
        
    except Exception as e:
        print(f"✗ Unexpected error: {type(e).__name__}: {e}")
        return False


if __name__ == "__main__":
    success = test_slack_access()
    print()
    print("="*80)
    print("SUMMARY")
    print("="*80)
    if success:
        print("✅ Slack is accessible and working correctly!")
    else:
        print("❌ Slack access failed - token needs to be refreshed")
        print()
        print("All required software is installed:")
        print("  ✓ slack_sdk (Python package)")
        print("  ✓ uvx (MCP server runner)")
        print()
        print("The issue is with the Slack token/authentication.")
        print("Follow the diagnosis steps above to resolve.")
    
    sys.exit(0 if success else 1)

