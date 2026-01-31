"""
Unit tests for TableStore
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from backend.stores.table_store import TableStore
from backend.stores.db_connection import DatabaseConnection
from backend.models.table_state import TableState, TableStateInfo, UserRole


class TestTableStore:
    """Test TableStore implementation."""
    
    @pytest.fixture
    def store(self):
        """Create TableStore instance."""
        return TableStore()
    
    @pytest.fixture
    def mock_connection(self):
        """Mock database connection."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    def test_get_table_by_id(self, store, mock_connection):
        """Test getting table by ID."""
        conn, cursor = mock_connection
        
        cursor.fetchone.return_value = {
            'name': 'customers',
            'state': 'ACTIVE',
            'version': 'v2',
            'owner': 'admin@test.com',
            'supersedes': 'v1',
            'deprecated_at': None,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.get_table_by_id('customers')
            
            assert result is not None
            assert result['name'] == 'customers'
            assert result['state'] == 'ACTIVE'
    
    def test_get_table_by_id_not_found(self, store, mock_connection):
        """Test getting non-existent table."""
        conn, cursor = mock_connection
        
        cursor.fetchone.return_value = None
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.get_table_by_id('nonexistent')
            
            assert result is None
    
    def test_get_table_with_state(self, store, mock_connection):
        """Test getting table with specific state."""
        conn, cursor = mock_connection
        
        cursor.fetchone.return_value = {
            'name': 'customers',
            'state': 'SHADOW',
            'version': 'v3',
            'owner': 'admin@test.com',
            'supersedes': None,
            'deprecated_at': None,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.get_table('customers', TableState.SHADOW)
            
            assert result is not None
            assert isinstance(result, TableStateInfo)
            assert result.name == 'customers'
            assert result.state == TableState.SHADOW
    
    def test_promote_table_success(self, store, mock_connection):
        """Test successful table promotion."""
        conn, cursor = mock_connection
        
        cursor.fetchone.side_effect = [
            None,  # Check for existing ACTIVE (none found)
            {  # Updated table
                'name': 'customers',
                'state': 'ACTIVE',
                'version': 'v2',
                'owner': 'admin@test.com',
                'supersedes': 'v1',
                'deprecated_at': None,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
        ]
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.promote_table(
                table_id='customers',
                from_state=TableState.SHADOW,
                to_state=TableState.ACTIVE,
                changed_by='admin@test.com'
            )
            
            assert result['state'] == 'ACTIVE'
            assert cursor.execute.call_count >= 3  # Check existing + update + history
    
    def test_promote_table_invalid_transition(self, store):
        """Test promotion with invalid state transition."""
        with pytest.raises(ValueError, match="Invalid transition"):
            store.promote_table(
                table_id='customers',
                from_state=TableState.READ_ONLY,
                to_state=TableState.ACTIVE,
                changed_by='admin@test.com'
            )
    
    def test_promote_table_existing_active(self, store, mock_connection):
        """Test promotion when another ACTIVE version exists."""
        conn, cursor = mock_connection
        
        cursor.fetchone.return_value = {'name': 'customers'}  # Existing ACTIVE found
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            with pytest.raises(ValueError, match="Another ACTIVE version already exists"):
                store.promote_table(
                    table_id='customers',
                    from_state=TableState.SHADOW,
                    to_state=TableState.ACTIVE,
                    changed_by='admin@test.com'
                )
    
    def test_deprecate_table(self, store, mock_connection):
        """Test table deprecation."""
        conn, cursor = mock_connection
        
        cursor.fetchone.side_effect = [
            {'state': 'ACTIVE'},  # Current state
            {  # Updated table
                'name': 'customers',
                'state': 'DEPRECATED',
                'version': 'v2',
                'owner': 'admin@test.com',
                'supersedes': None,
                'deprecated_at': datetime.now(),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
        ]
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.deprecate_table(
                table_id='customers',
                reason='Replaced by customers_v3',
                changed_by='admin@test.com'
            )
            
            assert result['state'] == 'DEPRECATED'
            assert result['deprecated_at'] is not None
    
    def test_restore_table(self, store, mock_connection):
        """Test restoring deprecated table."""
        conn, cursor = mock_connection
        
        cursor.fetchone.side_effect = [
            {'state': 'DEPRECATED'},  # Current state check
            None,  # No existing ACTIVE
            {  # Updated table
                'name': 'customers',
                'state': 'ACTIVE',
                'version': 'v2',
                'owner': 'admin@test.com',
                'supersedes': None,
                'deprecated_at': None,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
        ]
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.restore_table(
                table_id='customers',
                changed_by='admin@test.com'
            )
            
            assert result['state'] == 'ACTIVE'
            assert result['deprecated_at'] is None
    
    def test_restore_table_not_deprecated(self, store, mock_connection):
        """Test restoring non-deprecated table."""
        conn, cursor = mock_connection
        
        cursor.fetchone.return_value = {'state': 'ACTIVE'}
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            with pytest.raises(ValueError, match="is not DEPRECATED"):
                store.restore_table(
                    table_id='customers',
                    changed_by='admin@test.com'
                )

