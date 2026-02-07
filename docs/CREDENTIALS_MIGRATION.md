# Credentials Migration Summary

All API keys, secrets, and IDs have been moved to a centralized `.env` file.

## ✅ What Was Changed

### 1. Frontend Authentication (Azure AD)
- **Before:** Hardcoded in `frontend/src/auth/authConfig.ts`
- **After:** Loaded from environment variables `VITE_AZURE_CLIENT_ID` and `VITE_AZURE_TENANT_ID`
- **File Updated:** `frontend/src/auth/authConfig.ts`

### 2. Environment Configuration
- **Created:** Comprehensive `.env.example` with all credential placeholders
- **Updated:** `.env` file with actual credentials (gitignored)
- **Documentation:** Created `docs/ENVIRONMENT_SETUP.md`

### 3. Vite Configuration
- **Updated:** `frontend/vite.config.ts` to expose `VITE_` prefixed variables
- **Note:** Frontend variables must use `VITE_` prefix to be accessible in browser

## 📁 File Structure

```
RCA-Engine/
├── .env                    # Your actual credentials (GITIGNORED)
├── .env.example            # Template with placeholders (COMMITTED)
├── .env.template           # Template with your current values (GITIGNORED)
└── docs/
    ├── ENVIRONMENT_SETUP.md      # Setup guide
    └── CREDENTIALS_MIGRATION.md  # This file
```

## 🔐 Credentials Location

All credentials are now in `.env` file at the project root:

```bash
# Azure AD SSO
VITE_AZURE_CLIENT_ID=7d2f211d-b2a8-44bd-8738-d18efed5df9d
VITE_AZURE_TENANT_ID=aaa4fcdd-d6e9-487f-a3b3-b35a85d9bc3b

# OpenAI
OPENAI_API_KEY=sk-proj-...

# Database
RCA_DB_PASSWORD=...
```

## 🚀 How to Use

### For New Team Members:
1. Copy `.env.example` to `.env`
2. Fill in actual values
3. Never commit `.env` to git

### For Development:
- Frontend reads from `.env` automatically (via Vite)
- Backend reads from `.env` automatically (via Python `os.getenv()`)
- Restart dev servers after changing `.env`

## ✅ Verification

- ✅ `.env` is in `.gitignore`
- ✅ `.env.example` has placeholders (safe to commit)
- ✅ Frontend code uses `import.meta.env.VITE_*`
- ✅ Backend code uses `os.getenv()`
- ✅ Build succeeds with environment variables

## 🔄 Migration Checklist

- [x] Moved Azure AD credentials to `.env`
- [x] Updated frontend to read from environment variables
- [x] Created `.env.example` template
- [x] Verified `.gitignore` includes `.env`
- [x] Updated Vite config to expose variables
- [x] Created documentation
- [x] Tested build process

## 📝 Next Steps

1. **Share credentials securely** with team members (use password manager, not email)
2. **Rotate credentials** if they were previously committed to git
3. **Set up different `.env` files** for different environments (dev/staging/prod)
4. **Review git history** - if credentials were committed, rotate them immediately

## ⚠️ Important Notes

- **Never commit `.env`** - It's already in `.gitignore`
- **Frontend variables** must be prefixed with `VITE_`
- **Restart servers** after changing `.env`
- **Use `.env.example`** as a template for new environments
