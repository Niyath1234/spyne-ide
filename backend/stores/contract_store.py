"""
Contract Store

Database access for contract management with ingestion semantics.
"""

import uuid
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging
import json

from .db_connection import DatabaseConnection
from backend.observability.metrics import metrics_collector

logger = logging.getLogger(__name__)


class ContractStore:
    """Store for contract management."""
    
    def __init__(self, db_connection=None):
        """
        Initialize contract store with database connection.
        
        Args:
            db_connection: Optional database connection (uses pool if None)
        """
        self.db = db_connection
        if self.db is None:
            DatabaseConnection.initialize_pool()
    
    def register_contract(
        self,
        endpoint: str,
        table_name: str,
        ingestion_semantics: Dict[str, Any],
        owner: str,
        version: str = "v1"
    ) -> Dict[str, Any]:
        """
        Register a new contract.
        
        Args:
            endpoint: API endpoint URL
            table_name: Target table name
            ingestion_semantics: Ingestion semantics dict (required fields validated)
            owner: Owner email/user identifier
            version: Contract version (default: v1)
        
        Returns:
            Registered contract dict with contract_id
        """
        # Validate ingestion semantics
        required_fields = ['mode', 'idempotency_key', 'event_time_column', 
                          'processing_time_column', 'dedupe_window', 'conflict_resolution']
        for field in required_fields:
            if field not in ingestion_semantics:
                raise ValueError(f"Missing required ingestion_semantics field: {field}")
        
        contract_id = f"{table_name}_{version}"
        
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if contract already exists
            cursor.execute("""
                SELECT contract_id FROM contracts 
                WHERE contract_id = %s
            """, (contract_id,))
            
            if cursor.fetchone():
                raise ValueError(f"Contract {contract_id} already exists")
            
            # Insert contract
            cursor.execute("""
                INSERT INTO contracts (
                    contract_id, endpoint, table_name, ingestion_semantics,
                    version, state, owner, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s::jsonb, %s, 'SHADOW', %s, NOW(), NOW()
                )
                RETURNING id, contract_id, endpoint, table_name, ingestion_semantics,
                          version, state, owner, created_at, updated_at
            """, (
                contract_id,
                endpoint,
                table_name,
                json.dumps(ingestion_semantics),
                version,
                owner
            ))
            
            result = cursor.fetchone()
            conn.commit()
            
            logger.info(f"Registered contract {contract_id} for table {table_name}")
            
            return dict(result)
    
    def get_current_schema(self, contract_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current schema for contract.
        
        Args:
            contract_id: Contract identifier
        
        Returns:
            Schema snapshot dict or None if not found
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get contract to find table_name
            cursor.execute("""
                SELECT table_name FROM contracts WHERE contract_id = %s
            """, (contract_id,))
            
            contract = cursor.fetchone()
            if not contract:
                return None
            
            table_name = contract['table_name']
            
            # Get active schema version
            cursor.execute("""
                SELECT schema_snapshot, version, created_at
                FROM table_versions
                WHERE table_name = %s AND state = 'ACTIVE'
                ORDER BY created_at DESC
                LIMIT 1
            """, (table_name,))
            
            result = cursor.fetchone()
            if result:
                return {
                    'schema': result['schema_snapshot'],
                    'version': result['version'],
                    'created_at': result['created_at'].isoformat() if result['created_at'] else None
                }
            
            return None
    
    def get_schema_version(
        self,
        contract_id: str,
        version: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get schema for specific version.
        
        Args:
            contract_id: Contract identifier
            version: Schema version
        
        Returns:
            Schema snapshot dict or None if not found
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get contract to find table_name
            cursor.execute("""
                SELECT table_name FROM contracts WHERE contract_id = %s
            """, (contract_id,))
            
            contract = cursor.fetchone()
            if not contract:
                return None
            
            table_name = contract['table_name']
            
            # Get specific schema version
            cursor.execute("""
                SELECT schema_snapshot, version, created_at, deprecated_at
                FROM table_versions
                WHERE table_name = %s AND version = %s
            """, (table_name, version))
            
            result = cursor.fetchone()
            if result:
                return {
                    'schema': result['schema_snapshot'],
                    'version': result['version'],
                    'created_at': result['created_at'].isoformat() if result['created_at'] else None,
                    'deprecated_at': result['deprecated_at'].isoformat() if result['deprecated_at'] else None
                }
            
            return None
    
    def replay_ingestion(
        self,
        contract_id: str,
        time_range: Dict[str, str],
        dedupe_strategy: str,
        dry_run: bool,
        preview_rows: int = 100
    ) -> Dict[str, Any]:
        """
        Replay ingestion for time range.
        
        Args:
            contract_id: Contract identifier
            time_range: Dict with 'start' and 'end' ISO timestamps
            dedupe_strategy: Deduplication strategy ('idempotency_key' or 'time_window')
            dry_run: If True, return preview only
            preview_rows: Number of preview rows to return
        
        Returns:
            Replay result dict with estimated_rows, estimated_duplicates, preview
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get contract and ingestion semantics
            cursor.execute("""
                SELECT endpoint, table_name, ingestion_semantics
                FROM contracts
                WHERE contract_id = %s
            """, (contract_id,))
            
            contract = cursor.fetchone()
            if not contract:
                raise ValueError(f"Contract {contract_id} not found")
            
            ingestion_semantics = contract['ingestion_semantics']
            table_name = contract['table_name']
            
            # In a real implementation, this would:
            # 1. Query the source API endpoint for the time range
            # 2. Apply deduplication based on strategy
            # 3. Return preview or ingest based on dry_run
            
            # For now, return estimated values based on ingestion history
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(rows_duplicated) as total_duplicates
                FROM ingestion_history
                WHERE contract_id = %s
                  AND ingestion_time >= %s::timestamp
                  AND ingestion_time <= %s::timestamp
            """, (
                contract_id,
                time_range.get('start'),
                time_range.get('end')
            ))
            
            stats = cursor.fetchone()
            estimated_rows = stats['total_rows'] or 0
            estimated_duplicates = stats['total_duplicates'] or 0
            
            # Preview would come from actual API query in real implementation
            preview = []
            
            result = {
                'dry_run': dry_run,
                'estimated_rows': estimated_rows,
                'estimated_duplicates': estimated_duplicates,
                'preview': preview,
                'time_range': time_range,
                'dedupe_strategy': dedupe_strategy
            }
            
            if not dry_run:
                # In real implementation, would perform actual ingestion here
                logger.info(f"Replay ingestion for {contract_id} would be performed here")
                metrics_collector.record_replay(contract_id)
            
            return result
    
    def backfill_ingestion(
        self,
        contract_id: str,
        source: str,
        time_range: Dict[str, str],
        batch_size: int,
        dedupe_strategy: str,
        dry_run: bool
    ) -> Dict[str, Any]:
        """
        Backfill ingestion from archive.
        
        Args:
            contract_id: Contract identifier
            source: Archive source identifier
            time_range: Dict with 'start' and 'end' ISO timestamps
            batch_size: Batch size for processing
            dedupe_strategy: Deduplication strategy
            dry_run: If True, return preview only
        
        Returns:
            Backfill result dict with estimated_rows, estimated_duplicates
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get contract
            cursor.execute("""
                SELECT endpoint, table_name, ingestion_semantics
                FROM contracts
                WHERE contract_id = %s
            """, (contract_id,))
            
            contract = cursor.fetchone()
            if not contract:
                raise ValueError(f"Contract {contract_id} not found")
            
            # In real implementation, this would:
            # 1. Query archive source for time range
            # 2. Process in batches
            # 3. Apply deduplication
            # 4. Ingest or return preview
            
            # Estimate based on time range (simplified)
            estimated_rows = 0  # Would be calculated from archive
            estimated_duplicates = 0
            
            result = {
                'dry_run': dry_run,
                'estimated_rows': estimated_rows,
                'estimated_duplicates': estimated_duplicates,
                'source': source,
                'time_range': time_range,
                'batch_size': batch_size,
                'dedupe_strategy': dedupe_strategy
            }
            
            if not dry_run:
                # In real implementation, would perform actual backfill here
                logger.info(f"Backfill ingestion for {contract_id} would be performed here")
                metrics_collector.record_backfill_rows(contract_id, estimated_rows)
            
            return result

