"""
Unit tests for ContractStore
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from backend.stores.contract_store import ContractStore
from backend.stores.db_connection import DatabaseConnection


class TestContractStore:
    """Test ContractStore implementation."""
    
    @pytest.fixture
    def store(self):
        """Create ContractStore instance."""
        return ContractStore()
    
    @pytest.fixture
    def mock_connection(self):
        """Mock database connection."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor
    
    def test_register_contract_success(self, store, mock_connection):
        """Test successful contract registration."""
        conn, cursor = mock_connection
        
        # Mock contract data
        cursor.fetchone.return_value = None  # No existing contract
        cursor.fetchone.side_effect = [
            None,  # First call: check existing
            {  # Second call: return inserted contract
                'id': '123',
                'contract_id': 'customers_v1',
                'endpoint': '/api/v1/customers',
                'table_name': 'customers',
                'ingestion_semantics': {'mode': 'upsert', 'idempotency_key': ['id']},
                'version': 'v1',
                'state': 'SHADOW',
                'owner': 'admin@test.com',
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
        ]
        
        ingestion_semantics = {
            'mode': 'upsert',
            'idempotency_key': ['api_id', 'event_id'],
            'event_time_column': 'event_time',
            'processing_time_column': 'ingested_at',
            'dedupe_window': '24h',
            'conflict_resolution': 'latest_wins'
        }
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.register_contract(
                endpoint='/api/v1/customers',
                table_name='customers',
                ingestion_semantics=ingestion_semantics,
                owner='admin@test.com'
            )
            
            assert result['contract_id'] == 'customers_v1'
            assert result['table_name'] == 'customers'
            assert cursor.execute.call_count >= 2  # Check existing + insert
    
    def test_register_contract_missing_semantics(self, store):
        """Test contract registration with missing ingestion semantics."""
        ingestion_semantics = {
            'mode': 'upsert',
            # Missing required fields
        }
        
        with pytest.raises(ValueError, match="Missing required ingestion_semantics field"):
            store.register_contract(
                endpoint='/api/v1/customers',
                table_name='customers',
                ingestion_semantics=ingestion_semantics,
                owner='admin@test.com'
            )
    
    def test_register_contract_duplicate(self, store, mock_connection):
        """Test contract registration with duplicate contract_id."""
        conn, cursor = mock_connection
        
        # Mock existing contract
        cursor.fetchone.return_value = {'contract_id': 'customers_v1'}
        
        ingestion_semantics = {
            'mode': 'upsert',
            'idempotency_key': ['id'],
            'event_time_column': 'event_time',
            'processing_time_column': 'ingested_at',
            'dedupe_window': '24h',
            'conflict_resolution': 'latest_wins'
        }
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            with pytest.raises(ValueError, match="already exists"):
                store.register_contract(
                    endpoint='/api/v1/customers',
                    table_name='customers',
                    ingestion_semantics=ingestion_semantics,
                    owner='admin@test.com'
                )
    
    def test_get_current_schema(self, store, mock_connection):
        """Test getting current schema."""
        conn, cursor = mock_connection
        
        cursor.fetchone.side_effect = [
            {'table_name': 'customers'},  # Contract lookup
            {  # Schema lookup
                'schema_snapshot': {'columns': ['id', 'name']},
                'version': 'v2',
                'created_at': datetime.now()
            }
        ]
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.get_current_schema('customers_v1')
            
            assert result is not None
            assert 'schema' in result
            assert result['version'] == 'v2'
    
    def test_get_current_schema_not_found(self, store, mock_connection):
        """Test getting current schema when contract not found."""
        conn, cursor = mock_connection
        
        cursor.fetchone.return_value = None
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.get_current_schema('nonexistent_v1')
            
            assert result is None
    
    def test_get_schema_version(self, store, mock_connection):
        """Test getting specific schema version."""
        conn, cursor = mock_connection
        
        cursor.fetchone.side_effect = [
            {'table_name': 'customers'},  # Contract lookup
            {  # Schema version lookup
                'schema_snapshot': {'columns': ['id', 'name']},
                'version': 'v1',
                'created_at': datetime.now(),
                'deprecated_at': None
            }
        ]
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.get_schema_version('customers_v1', 'v1')
            
            assert result is not None
            assert result['version'] == 'v1'
    
    def test_replay_ingestion_dry_run(self, store, mock_connection):
        """Test replay ingestion in dry-run mode."""
        conn, cursor = mock_connection
        
        cursor.fetchone.side_effect = [
            {  # Contract lookup
                'endpoint': '/api/v1/customers',
                'table_name': 'customers',
                'ingestion_semantics': {'mode': 'upsert'}
            },
            {  # Stats lookup
                'total_rows': 1000,
                'total_duplicates': 50
            }
        ]
        
        time_range = {
            'start': '2024-01-01T00:00:00Z',
            'end': '2024-01-31T23:59:59Z'
        }
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.replay_ingestion(
                contract_id='customers_v1',
                time_range=time_range,
                dedupe_strategy='idempotency_key',
                dry_run=True,
                preview_rows=100
            )
            
            assert result['dry_run'] is True
            assert 'estimated_rows' in result
            assert 'estimated_duplicates' in result
    
    def test_backfill_ingestion_dry_run(self, store, mock_connection):
        """Test backfill ingestion in dry-run mode."""
        conn, cursor = mock_connection
        
        cursor.fetchone.return_value = {
            'endpoint': '/api/v1/customers',
            'table_name': 'customers',
            'ingestion_semantics': {'mode': 'upsert'}
        }
        
        time_range = {
            'start': '2023-01-01T00:00:00Z',
            'end': '2023-12-31T23:59:59Z'
        }
        
        with patch.object(DatabaseConnection, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = conn
            
            result = store.backfill_ingestion(
                contract_id='customers_v1',
                source='archive',
                time_range=time_range,
                batch_size=1000,
                dedupe_strategy='idempotency_key',
                dry_run=True
            )
            
            assert result['dry_run'] is True
            assert 'estimated_rows' in result
            assert result['source'] == 'archive'

