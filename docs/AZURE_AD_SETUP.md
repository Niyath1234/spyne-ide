# Azure AD SSO Setup Guide

## Error: AADSTS500113 - No reply address registered

This error occurs when the redirect URI (reply URL) is not registered in your Azure AD app registration.

## Steps to Fix

### 1. Go to Azure Portal
1. Navigate to [Azure Portal](https://portal.azure.com)
2. Go to **Azure Active Directory** → **App registrations**
3. Find your application with Client ID: `7d2f211d-b2a8-44bd-8738-d18efed5df9d`

### 2. Configure Authentication
1. Click on your application
2. Go to **Authentication** in the left sidebar
3. Under **Platform configurations**, click **Add a platform**
4. Select **Single-page application (SPA)**

### 3. Add Redirect URIs
Add the following redirect URIs:

**For Local Development:**
- `http://localhost:5173`
- `http://localhost:5173/`
- `http://localhost:3000` (if using different port)
- `http://localhost:3000/` (if using different port)

**For Production:**
- `https://your-production-domain.com`
- `https://your-production-domain.com/`

### 4. Configure Implicit Grant (if needed)
Under **Implicit grant and hybrid flows**, ensure:
- ✅ **Access tokens** is checked
- ✅ **ID tokens** is checked

### 5. Save Configuration
Click **Configure** to save your changes.

## Important Notes

- The redirect URI must **exactly match** the URL where your application is running
- For popup authentication, the redirect URI is still required (even though the popup handles the redirect internally)
- After making changes, wait 1-2 minutes for Azure AD to propagate the changes
- Clear your browser cache and cookies if you still see errors after configuration

## Testing

After configuration:
1. Clear your browser cache
2. Restart your development server
3. Try logging in again

## Common Redirect URIs

Based on your setup, you likely need:
- `http://localhost:5173` (Vite default)
- `http://localhost:5173/`
- Your production URL (when deployed)

## Troubleshooting

If you still see errors:
1. Verify the redirect URI in Azure AD matches exactly (including trailing slash)
2. Check that your app registration is using the correct platform type (SPA)
3. Ensure the Client ID and Tenant ID in `authConfig.ts` match your Azure AD app registration
4. Wait a few minutes for Azure AD changes to propagate
