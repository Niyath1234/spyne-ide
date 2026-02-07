import { PublicClientApplication, type Configuration, type RedirectRequest } from '@azure/msal-browser';

/**
 * Get Azure AD configuration from environment variables
 * These are loaded from .env file (Vite requires VITE_ prefix)
 */
const getAzureConfig = () => {
  const clientId = import.meta.env.VITE_AZURE_CLIENT_ID;
  const tenantId = import.meta.env.VITE_AZURE_TENANT_ID;

  if (!clientId || !tenantId) {
    throw new Error(
      'Missing Azure AD configuration. Please set VITE_AZURE_CLIENT_ID and VITE_AZURE_TENANT_ID in your .env file.'
    );
  }

  return {
    clientId,
    authority: `https://login.microsoftonline.com/${tenantId}`,
  };
};

const azureConfig = getAzureConfig();

/**
 * Configuration object to be passed to MSAL instance on creation.
 * For a full list of MSAL.js configuration parameters, visit:
 * https://github.com/AzureAD/microsoft-authentication-library-for-js/blob/dev/lib/msal-browser/docs/configuration.md
 */
export const msalConfig: Configuration = {
  auth: {
    clientId: azureConfig.clientId,
    authority: azureConfig.authority,
    redirectUri: window.location.origin, // Must be registered as a SPA redirectURI on your app registration
    postLogoutRedirectUri: window.location.origin, // Redirect URI after logout
  },
  cache: {
    cacheLocation: 'localStorage', // Use localStorage instead of sessionStorage
  },
};

/**
 * Scopes you add here will be prompted for user consent during sign-in.
 * By default, MSAL.js will add OIDC scopes (openid, profile, email) to any login request.
 * For more information about OIDC scopes, visit:
 * https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-permissions-and-consent#openid-connect-scopes
 */
export const loginRequest: RedirectRequest = {
  scopes: ['openid', 'profile', 'email', 'User.Read'],
};

// Create MSAL instance
export const msalInstance = new PublicClientApplication(msalConfig);
