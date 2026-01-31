"""
Authentication Middleware

Extracts user context from JWT tokens or sessions.
"""

import logging
from typing import Optional, Dict, Any
from functools import wraps

from flask import request, g, abort
from backend.auth.authenticator import JWTAuthenticator
from backend.models.table_state import UserRole

logger = logging.getLogger(__name__)


class AuthMiddleware:
    """Authentication middleware for Flask."""
    
    def __init__(self, secret_key: str):
        """
        Initialize auth middleware.
        
        Args:
            secret_key: Secret key for JWT verification
        """
        self.authenticator = JWTAuthenticator(secret_key)
    
    def extract_user_context(self) -> Optional[Dict[str, Any]]:
        """
        Extract user context from request.
        
        Checks:
        1. Authorization header (Bearer token)
        2. Session cookie
        3. API key header
        
        Returns:
            User context dict or None if not authenticated
        """
        # Try Bearer token in Authorization header
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            token = auth_header[7:]  # Remove 'Bearer ' prefix
            result = self.authenticator.authenticate(token)
            if result.get('success'):
                return {
                    'user_id': result.get('user_id'),
                    'email': result.get('email'),
                    'roles': result.get('roles', []),
                    'permissions': result.get('permissions', []),
                    'auth_method': 'jwt'
                }
        
        # Try session (if using Flask sessions)
        if hasattr(request, 'session'):
            user_id = request.session.get('user_id')
            if user_id:
                return {
                    'user_id': user_id,
                    'email': request.session.get('email'),
                    'roles': request.session.get('roles', []),
                    'permissions': request.session.get('permissions', []),
                    'auth_method': 'session'
                }
        
        # Try API key header (for service-to-service)
        api_key = request.headers.get('X-API-Key')
        if api_key:
            # In production, validate API key against database
            # For now, return service account context
            return {
                'user_id': 'service',
                'email': 'service@spyne.local',
                'roles': ['ADMIN'],  # Service accounts have admin by default
                'permissions': [],
                'auth_method': 'api_key'
            }
        
        return None
    
    def get_user_role(self, user_context: Optional[Dict[str, Any]]) -> UserRole:
        """
        Get user role from context.
        
        Args:
            user_context: User context dict
        
        Returns:
            UserRole enum (defaults to VIEWER if not found)
        """
        if not user_context:
            return UserRole.VIEWER
        
        roles = user_context.get('roles', [])
        
        # Check for role in priority order (highest first)
        if 'ADMIN' in roles or 'admin' in roles:
            return UserRole.ADMIN
        if 'ENGINEER' in roles or 'engineer' in roles:
            return UserRole.ENGINEER
        if 'ANALYST' in roles or 'analyst' in roles:
            return UserRole.ANALYST
        
        return UserRole.VIEWER
    
    def require_auth(self, f):
        """
        Decorator to require authentication.
        
        Usage:
            @auth_middleware.require_auth
            def my_endpoint():
                user_id = g.user_id
                ...
        """
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_context = self.extract_user_context()
            
            if not user_context:
                abort(401, description="Authentication required")
            
            # Set user context in Flask g
            g.user_id = user_context.get('user_id')
            g.user_email = user_context.get('email')
            g.user_roles = user_context.get('roles', [])
            g.user_permissions = user_context.get('permissions', [])
            g.user_role = self.get_user_role(user_context)
            g.auth_method = user_context.get('auth_method')
            
            return f(*args, **kwargs)
        
        return decorated_function
    
    def require_role(self, *allowed_roles: UserRole):
        """
        Decorator to require specific role(s).
        
        Usage:
            @auth_middleware.require_role(UserRole.ADMIN)
            def admin_endpoint():
                ...
        """
        def decorator(f):
            @wraps(f)
            @self.require_auth
            def decorated_function(*args, **kwargs):
                user_role = getattr(g, 'user_role', UserRole.VIEWER)
                
                if user_role not in allowed_roles:
                    abort(403, description=f"Role {user_role.value} not authorized. Required: {[r.value for r in allowed_roles]}")
                
                return f(*args, **kwargs)
            
            return decorated_function
        return decorator
    
    def require_permission(self, permission: str):
        """
        Decorator to require specific permission.
        
        Usage:
            @auth_middleware.require_permission('can_promote')
            def promote_endpoint():
                ...
        """
        def decorator(f):
            @wraps(f)
            @self.require_auth
            def decorated_function(*args, **kwargs):
                user_permissions = getattr(g, 'user_permissions', [])
                user_role = getattr(g, 'user_role', UserRole.VIEWER)
                
                # Check explicit permission or role-based permission
                from backend.models.table_state import RolePermissions
                
                if permission not in user_permissions:
                    if not RolePermissions.can(user_role, permission):
                        abort(403, description=f"Permission '{permission}' required")
                
                return f(*args, **kwargs)
            
            return decorated_function
        return decorator


def init_auth_middleware(app, secret_key: Optional[str] = None):
    """
    Initialize auth middleware for Flask app.
    
    Args:
        app: Flask application
        secret_key: Secret key (defaults to app.config['SECRET_KEY'])
    """
    if secret_key is None:
        secret_key = app.config.get('SECRET_KEY')
        if not secret_key:
            raise ValueError("SECRET_KEY must be set in app config or provided")
    
    middleware = AuthMiddleware(secret_key)
    app.auth_middleware = middleware
    
    # Add before_request handler to extract user context
    @app.before_request
    def extract_user_context():
        """Extract user context for all requests (optional auth)."""
        user_context = middleware.extract_user_context()
        
        if user_context:
            g.user_id = user_context.get('user_id')
            g.user_email = user_context.get('email')
            g.user_roles = user_context.get('roles', [])
            g.user_permissions = user_context.get('permissions', [])
            g.user_role = middleware.get_user_role(user_context)
            g.auth_method = user_context.get('auth_method')
        else:
            # Set defaults for unauthenticated requests
            g.user_id = None
            g.user_email = None
            g.user_roles = []
            g.user_permissions = []
            g.user_role = UserRole.VIEWER
            g.auth_method = None
    
    logger.info("Authentication middleware initialized")
    
    return middleware

