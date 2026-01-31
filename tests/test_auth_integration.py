"""
Integration tests for authentication and authorization
"""

import pytest
from unittest.mock import patch, MagicMock
from flask import Flask

from backend.auth.middleware import AuthMiddleware, init_auth_middleware
from backend.auth.authenticator import JWTAuthenticator
from backend.models.table_state import UserRole


class TestAuthIntegration:
    """Integration tests for authentication."""
    
    @pytest.fixture
    def app(self):
        """Create Flask app for testing."""
        app = Flask(__name__)
        app.config['SECRET_KEY'] = 'test-secret-key'
        app.config['TESTING'] = True
        return app
    
    @pytest.fixture
    def middleware(self):
        """Create auth middleware."""
        return AuthMiddleware('test-secret-key')
    
    @pytest.fixture
    def authenticator(self):
        """Create JWT authenticator."""
        return JWTAuthenticator('test-secret-key')
    
    def test_jwt_token_generation_and_verification(self, authenticator):
        """Test JWT token generation and verification."""
        # Generate token
        token = authenticator.generate_token(
            user_id='user123',
            email='test@example.com',
            roles=['ADMIN'],
            permissions=['can_promote', 'can_deprecate']
        )
        
        assert token is not None
        assert isinstance(token, str)
        
        # Verify token
        result = authenticator.authenticate(token)
        
        assert result['success'] is True
        assert result['user_id'] == 'user123'
        assert result['email'] == 'test@example.com'
        assert 'ADMIN' in result['roles']
    
    def test_jwt_token_expiration(self, authenticator):
        """Test JWT token expiration."""
        # Generate token with short expiry
        authenticator_short = JWTAuthenticator('test-secret-key', token_expiry_hours=0.0001)
        token = authenticator_short.generate_token(
            user_id='user123',
            email='test@example.com'
        )
        
        import time
        time.sleep(0.1)  # Wait for expiration
        
        # Verify expired token
        result = authenticator_short.authenticate(token)
        assert result['success'] is False
        assert 'expired' in result['error'].lower()
    
    def test_extract_user_context_from_bearer_token(self, middleware, authenticator):
        """Test extracting user context from Bearer token."""
        from flask import Flask, request
        from unittest.mock import Mock
        
        app = Flask(__name__)
        
        token = authenticator.generate_token(
            user_id='user123',
            email='test@example.com',
            roles=['ENGINEER']
        )
        
        with app.test_request_context(
            headers={'Authorization': f'Bearer {token}'}
        ):
            user_context = middleware.extract_user_context()
            
            assert user_context is not None
            assert user_context['user_id'] == 'user123'
            assert user_context['email'] == 'test@example.com'
            assert user_context['auth_method'] == 'jwt'
    
    def test_extract_user_context_from_api_key(self, middleware):
        """Test extracting user context from API key."""
        from flask import Flask
        
        app = Flask(__name__)
        
        with app.test_request_context(
            headers={'X-API-Key': 'test-api-key'}
        ):
            user_context = middleware.extract_user_context()
            
            assert user_context is not None
            assert user_context['user_id'] == 'service'
            assert user_context['auth_method'] == 'api_key'
            assert 'ADMIN' in user_context['roles']
    
    def test_get_user_role(self, middleware):
        """Test getting user role from context."""
        # Admin role
        admin_context = {
            'roles': ['ADMIN'],
            'user_id': 'admin123'
        }
        assert middleware.get_user_role(admin_context) == UserRole.ADMIN
        
        # Engineer role
        engineer_context = {
            'roles': ['ENGINEER'],
            'user_id': 'engineer123'
        }
        assert middleware.get_user_role(engineer_context) == UserRole.ENGINEER
        
        # Analyst role
        analyst_context = {
            'roles': ['ANALYST'],
            'user_id': 'analyst123'
        }
        assert middleware.get_user_role(analyst_context) == UserRole.ANALYST
        
        # Default to VIEWER
        assert middleware.get_user_role(None) == UserRole.VIEWER
        assert middleware.get_user_role({'roles': []}) == UserRole.VIEWER
    
    def test_require_auth_decorator(self, middleware, authenticator):
        """Test require_auth decorator."""
        from flask import Flask, jsonify
        
        app = Flask(__name__)
        
        token = authenticator.generate_token(
            user_id='user123',
            email='test@example.com'
        )
        
        @app.route('/protected')
        @middleware.require_auth
        def protected():
            from flask import g
            return jsonify({'user_id': g.user_id})
        
        # Test with valid token
        with app.test_client() as client:
            response = client.get(
                '/protected',
                headers={'Authorization': f'Bearer {token}'}
            )
            assert response.status_code == 200
            assert response.json['user_id'] == 'user123'
        
        # Test without token
        with app.test_client() as client:
            response = client.get('/protected')
            assert response.status_code == 401
    
    def test_require_role_decorator(self, middleware, authenticator):
        """Test require_role decorator."""
        from flask import Flask, jsonify
        
        app = Flask(__name__)
        
        admin_token = authenticator.generate_token(
            user_id='admin123',
            email='admin@test.com',
            roles=['ADMIN']
        )
        
        viewer_token = authenticator.generate_token(
            user_id='viewer123',
            email='viewer@test.com',
            roles=['VIEWER']
        )
        
        @app.route('/admin-only')
        @middleware.require_role(UserRole.ADMIN)
        def admin_only():
            return jsonify({'message': 'success'})
        
        # Test with admin token
        with app.test_client() as client:
            response = client.get(
                '/admin-only',
                headers={'Authorization': f'Bearer {admin_token}'}
            )
            assert response.status_code == 200
        
        # Test with viewer token
        with app.test_client() as client:
            response = client.get(
                '/admin-only',
                headers={'Authorization': f'Bearer {viewer_token}'}
            )
            assert response.status_code == 403
    
    def test_init_auth_middleware(self, app):
        """Test initializing auth middleware in Flask app."""
        middleware = init_auth_middleware(app, secret_key='test-secret-key')
        
        assert hasattr(app, 'auth_middleware')
        assert app.auth_middleware is middleware
        
        # Test that before_request handler is registered
        assert len(app.before_request_funcs.get(None, [])) > 0

