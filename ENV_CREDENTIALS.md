# Environment Variables - Complete Credentials Reference

**⚠️ SECURITY WARNING: This file contains actual API tokens and credentials.**
**DO NOT commit this file to git or share it publicly.**

This document contains the exact values needed to fill your `.env` file.

---

## Complete .env File Contents

Copy the following content exactly into your `.env` file:

```bash
# RCA Engine Environment Variables
# This file contains actual credentials - DO NOT COMMIT TO GIT

# ============================================================================
# CONFLUENCE CONFIGURATION (Required)
# ============================================================================
CONFLUENCE_URL=https://slicepay.atlassian.net/wiki
CONFLUENCE_USERNAME=niyath.nair@slicebank.com
CONFLUENCE_API_TOKEN=YOUR_CONFLUENCE_API_TOKEN_HERE
CONFLUENCE_SPACE_KEY=HOR

# ============================================================================
# JIRA CONFIGURATION (Optional - if using Jira integration)
# ============================================================================
JIRA_URL=https://slicepay.atlassian.net
JIRA_USERNAME=niyath.nair@slicebank.com
JIRA_API_TOKEN=YOUR_JIRA_API_TOKEN_HERE

# ============================================================================
# SLACK CONFIGURATION (Required for Slack notifications)
# ============================================================================
# Bot Token (Recommended - starts with xoxb-)
SLACK_BOT_TOKEN=YOUR_SLACK_BOT_TOKEN_HERE

# Default channel for notifications
SLACK_DEFAULT_CHANNEL=#general

# Workspace name (optional)
SLACK_WORKSPACE_NAME=

# Alternative Slack MCP Tokens (if not using bot token)
# XOXC Token (Browser-based - starts with xoxc-)
SLACK_MCP_XOXC_TOKEN=YOUR_SLACK_XOXC_TOKEN_HERE

# XOXD Token (Desktop-based - starts with xoxd-)
SLACK_MCP_XOXD_TOKEN=YOUR_SLACK_XOXD_TOKEN_HERE

# ============================================================================
# DATABASE CONFIGURATION (Optional)
# ============================================================================
DATABASE_URL=

# ============================================================================
# VECTOR DATABASE CONFIGURATION (Optional)
# ============================================================================
VECTOR_DB_PATH=./data/vector_db
VECTOR_DB_INDEX_PATH=./data/vector_index

# ============================================================================
# KNOWLEDGE BASE PATHS (Optional - defaults shown)
# ============================================================================
KNOWLEDGE_BASE_PATH=metadata/knowledge_base.json
KNOWLEDGE_REGISTER_PATH=metadata/knowledge_register.json
PRODUCT_INDEX_PATH=metadata/product_index.json

# ============================================================================
# LLM CONFIGURATION (Optional - if using LLM features)
# ============================================================================
OPENAI_API_KEY=
ANTHROPIC_API_KEY=

# ============================================================================
# OTHER CONFIGURATION (Optional)
# ============================================================================
LOG_LEVEL=INFO
DEBUG=false
```

---

## Quick Setup Instructions

1. **Copy the content above** into a file named `.env` in the project root
2. **Verify `.env` is in `.gitignore`** (it should already be there)
3. **Test the configuration:**
   ```bash
   python -c "from src.config import Config; print('✓ Config loaded successfully')"
   ```

---

## Credential Details

### Confluence Credentials

| Variable | Value |
|----------|-------|
| **URL** | `https://slicepay.atlassian.net/wiki` |
| **Username** | `niyath.nair@slicebank.com` |
| **API Token** | `YOUR_CONFLUENCE_API_TOKEN_HERE` |
| **Space Key** | `HOR` |

**Token Type:** Atlassian API Token  
**How to Get:** https://id.atlassian.com/manage-profile/security/api-tokens

### Jira Credentials

| Variable | Value |
|----------|-------|
| **URL** | `https://slicepay.atlassian.net` |
| **Username** | `niyath.nair@slicebank.com` |
| **API Token** | Same as Confluence API Token |

**Note:** Jira and Confluence use the same API token for the same Atlassian instance.

### Slack Credentials

#### Bot Token (Primary - Recommended)
| Variable | Value |
|----------|-------|
| **SLACK_BOT_TOKEN** | `YOUR_SLACK_BOT_TOKEN_HERE` |

**Token Type:** Bot User OAuth Token  
**Prefix:** `xoxb-`  
**How to Get:** https://api.slack.com/apps → Your App → OAuth & Permissions → Bot User OAuth Token

#### XOXC Token (Browser-based - Alternative)
| Variable | Value |
|----------|-------|
| **SLACK_MCP_XOXC_TOKEN** | `YOUR_SLACK_XOXC_TOKEN_HERE` |

**Token Type:** Browser Session Token  
**Prefix:** `xoxc-`  
**Extracted from:** Browser local storage

#### XOXD Token (Desktop-based - Alternative)
| Variable | Value |
|----------|-------|
| **SLACK_MCP_XOXD_TOKEN** | `YOUR_SLACK_XOXD_TOKEN_HERE` |

**Token Type:** Desktop App Token  
**Prefix:** `xoxd-`  
**URL Encoded:** Yes (contains `%2B`, `%2F`, `%3D`)  
**Extracted from:** Desktop app local storage

**Decoded XOXD Token:**
```
YOUR_SLACK_XOXD_TOKEN_HERE
```

#### Slack Configuration
| Variable | Value |
|----------|-------|
| **SLACK_DEFAULT_CHANNEL** | `#general` |
| **SLACK_WORKSPACE_NAME** | (Leave empty or set your workspace name) |

---

## Token Reference

### Confluence/Jira API Token
```
YOUR_CONFLUENCE_API_TOKEN_HERE
```

### Slack Bot Token
```
YOUR_SLACK_BOT_TOKEN_HERE
```

### Slack XOXC Token
```
YOUR_SLACK_XOXC_TOKEN_HERE
```

### Slack XOXD Token (URL Encoded)
```
YOUR_SLACK_XOXD_TOKEN_HERE
```

---

## Verification Commands

After creating your `.env` file, verify it's working:

```bash
# Test Config loading
python -c "from src.config import Config; print('Confluence URL:', Config.get_confluence_url()); print('Username:', Config.get_confluence_username())"

# Test Confluence connection
python test_confluence_connection.py

# Test Slack token (if configured)
python -c "from src.config import Config; token = Config.get_slack_bot_token(); print('Slack token:', 'SET' if token else 'NOT SET')"
```

---

## Security Checklist

- [x] `.env` file created with all credentials
- [x] `.env` is in `.gitignore` (verified)
- [x] `.env.example` contains placeholders (no real tokens)
- [ ] This file (`ENV_CREDENTIALS.md`) should NOT be committed to git
- [ ] Keep this file secure and local only
- [ ] Rotate tokens periodically
- [ ] Never share tokens in public channels

---

## Troubleshooting

### "CONFLUENCE_USERNAME is not set"
- Check that `.env` file exists in project root
- Verify the variable name is exactly `CONFLUENCE_USERNAME`
- Ensure no extra spaces around `=`

### "CONFLUENCE_API_TOKEN is not set"
- Verify the token is on a single line (no line breaks)
- Check for any hidden characters
- Ensure the token starts with `ATATT3`

### "SLACK_BOT_TOKEN is not set"
- Verify the token starts with `xoxb-`
- Check that it's on a single line
- Ensure no quotes around the value

---

## Notes

- All tokens are valid and active
- Confluence and Jira share the same API token
- Slack bot token is the primary method (recommended)
- XOXC and XOXD tokens are alternatives for MCP integration
- XOXD token is URL-encoded (keep it as-is in .env)

---

**Last Updated:** 2026-01-22  
**Status:** All credentials configured and tested

