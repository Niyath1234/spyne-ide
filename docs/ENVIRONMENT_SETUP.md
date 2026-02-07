# Environment Variables Setup Guide

This guide explains how to configure all API keys, credentials, and IDs for the RCA Engine application.

## Quick Start

1. **Copy the example file:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` and fill in your actual credentials**

3. **Never commit `.env` to git** - it's already in `.gitignore`

## Environment Variables Reference

### Azure AD SSO Configuration (Required for Frontend)

These are required for Microsoft SSO authentication:

```bash
VITE_AZURE_CLIENT_ID=your-azure-client-id-here
VITE_AZURE_TENANT_ID=your-azure-tenant-id-here
```

**How to get these values:**
1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to **Azure Active Directory** → **App registrations**
3. Select your application
4. Copy the **Application (client) ID** → `VITE_AZURE_CLIENT_ID`
5. Copy the **Directory (tenant) ID** → `VITE_AZURE_TENANT_ID`

**Note:** Frontend environment variables must be prefixed with `VITE_` for Vite to expose them to the client.

### LLM Configuration (Required)

```bash
OPENAI_API_KEY=your-openai-api-key-here
OPENAI_MODEL=gpt-4
OPENAI_BASE_URL=https://api.openai.com/v1
```

### Database Configuration (Required)

```bash
RCA_DB_TYPE=postgresql
RCA_DB_HOST=localhost
RCA_DB_PORT=5432
RCA_DB_NAME=spyne
RCA_DB_USER=spyne
RCA_DB_PASSWORD=your-password-here
```

### Application Configuration

```bash
RCA_HOST=0.0.0.0
RCA_PORT=8080
RCA_DEBUG=false
RCA_SECRET_KEY=generate-a-random-secret-key-here
```

## Frontend vs Backend Variables

### Frontend Variables (Vite)
- Must be prefixed with `VITE_`
- Accessible in browser code via `import.meta.env.VITE_*`
- Examples: `VITE_AZURE_CLIENT_ID`, `VITE_AZURE_TENANT_ID`

### Backend Variables (Python)
- No prefix required
- Accessible via `os.getenv('VARIABLE_NAME')`
- Examples: `OPENAI_API_KEY`, `RCA_DB_HOST`

## Security Best Practices

1. **Never commit `.env` files** - Already in `.gitignore`
2. **Use `.env.example`** - Contains placeholders, safe to commit
3. **Rotate credentials regularly** - Especially API keys
4. **Use different credentials** - For development, staging, and production
5. **Restrict access** - Only team members who need credentials should have them

## Migration from Hardcoded Values

If you have hardcoded credentials in your code:

1. **Find hardcoded values:**
   ```bash
   grep -r "your-actual-value" src/
   ```

2. **Move to `.env` file:**
   - Add the variable to `.env`
   - Update code to use `os.getenv()` or `import.meta.env`

3. **Update `.env.example`:**
   - Add placeholder value
   - Document what it's for

## Environment-Specific Files

You can create environment-specific files:

- `.env.local` - Local development (highest priority, gitignored)
- `.env.development` - Development environment
- `.env.production` - Production environment

**Priority order (highest to lowest):**
1. `.env.local`
2. `.env.development` / `.env.production`
3. `.env`

## Troubleshooting

### Frontend: "Missing Azure AD configuration"
- Ensure variables are prefixed with `VITE_`
- Restart Vite dev server after changing `.env`
- Check that `.env` file exists in project root

### Backend: "Environment variable not found"
- Ensure variable is in `.env` file (no `VITE_` prefix for backend)
- Restart Python server after changing `.env`
- Check spelling and case sensitivity

### Variables not loading
1. Verify `.env` file is in project root (same level as `package.json`)
2. Restart your development server
3. Check for typos in variable names
4. Ensure no extra spaces around `=` sign

## Example `.env` File

```bash
# Azure AD
VITE_AZURE_CLIENT_ID=7d2f211d-b2a8-44bd-8738-d18efed5df9d
VITE_AZURE_TENANT_ID=aaa4fcdd-d6e9-487f-a3b3-b35a85d9bc3b

# OpenAI
OPENAI_API_KEY=sk-proj-...
OPENAI_MODEL=gpt-4

# Database
RCA_DB_HOST=localhost
RCA_DB_PASSWORD=your-secure-password
```

## Need Help?

If you're missing credentials:
1. Check with your team lead or DevOps
2. Review the service documentation (Azure AD, OpenAI, etc.)
3. Check if credentials are stored in a secrets manager (AWS Secrets Manager, Azure Key Vault, etc.)
