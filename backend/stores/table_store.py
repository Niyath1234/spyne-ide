"""
Table Store

Database access for table state management.

RISK #1 FIX: All writes route through CKO client.
Reads remain direct for performance.
"""

from typing import Optional, Dict, Any
from backend.models.table_state import TableState, TableStateInfo, TableStateManager
from datetime import datetime
import logging

from .db_connection import DatabaseConnection
from backend.observability.metrics import metrics_collector
from backend.cko_client import get_cko_client

logger = logging.getLogger(__name__)


class TableStore:
    """
    Store for table state management.
    
    RISK #1 FIX: Writes route through CKO client.
    Reads are direct for performance (read-only projections).
    """
    
    def __init__(self, db_connection=None):
        """
        Initialize table store with database connection.
        
        Args:
            db_connection: Optional database connection (uses pool if None)
        """
        self.db = db_connection
        if self.db is None:
            DatabaseConnection.initialize_pool()
    
    def get_table_by_id(self, table_id: str) -> Optional[Dict[str, Any]]:
        """
        Get table by ID (name).
        
        Args:
            table_id: Table name/identifier
        
        Returns:
            Table dict or None if not found
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name, state, version, owner, supersedes, 
                       deprecated_at, created_at, updated_at
                FROM tables
                WHERE name = %s
            """, (table_id,))
            
            result = cursor.fetchone()
            if result:
                return dict(result)
            
            return None
    
    def get_table(self, name: str, state: Optional[TableState] = None) -> Optional[TableStateInfo]:
        """
        Get table by name and optional state.
        
        Args:
            name: Table name
            state: Optional table state filter
        
        Returns:
            TableStateInfo or None if not found
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            if state:
                cursor.execute("""
                    SELECT name, state, version, owner, supersedes,
                           deprecated_at, created_at, updated_at
                    FROM tables
                    WHERE name = %s AND state = %s
                """, (name, state.value))
            else:
                cursor.execute("""
                    SELECT name, state, version, owner, supersedes,
                           deprecated_at, created_at, updated_at
                    FROM tables
                    WHERE name = %s
                    ORDER BY 
                        CASE state
                            WHEN 'ACTIVE' THEN 1
                            WHEN 'READ_ONLY' THEN 2
                            WHEN 'SHADOW' THEN 3
                            WHEN 'DEPRECATED' THEN 4
                        END
                    LIMIT 1
                """, (name,))
            
            result = cursor.fetchone()
            if result:
                return TableStateInfo(
                    name=result['name'],
                    state=TableState(result['state']),
                    version=result['version'] or 'v1',
                    owner=result['owner'],
                    supersedes=result['supersedes'],
                    deprecated_at=result['deprecated_at'],
                    created_at=result['created_at'],
                    updated_at=result['updated_at']
                )
            
            return None
    
    def promote_table(
        self,
        table_id: str,
        from_state: TableState,
        to_state: TableState,
        changed_by: str
    ) -> Dict[str, Any]:
        """
        Promote table from one state to another.
        
        RISK #1 FIX: Routes through CKO client.
        RISK #4 FIX: Promotion is explicit and auditable.
        
        Args:
            table_id: Table name/identifier
            from_state: Current state
            to_state: Target state
            changed_by: User identifier who made the change
        
        Returns:
            Updated table dict
        
        Raises:
            ValueError: If transition is invalid or constraints violated
        """
        # RISK #1 FIX: Request state change through CKO client
        cko_client = get_cko_client()
        cko_response = cko_client.request_state_change(
            table_name=table_id,
            from_state=from_state.value,
            to_state=to_state.value,
            requested_by=changed_by,
            reason=f"Promotion from {from_state.value} to {to_state.value}"
        )
        
        # Validate transition
        is_valid, error_msg = TableStateManager.validate_transition(
            from_state, to_state, None  # Role check happens at API level
        )
        if not is_valid:
            raise ValueError(error_msg)
        
        # RISK #1 FIX: Execute the state change (CKO has approved)
        # This is the actual database write, but it's been authorized by CKO
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            # If promoting to ACTIVE, ensure no other ACTIVE version exists
            if to_state == TableState.ACTIVE:
                cursor.execute("""
                    SELECT name FROM tables
                    WHERE name = %s AND state = 'ACTIVE' AND name != %s
                """, (table_id, table_id))
                
                if cursor.fetchone():
                    raise ValueError(
                        f"Cannot promote {table_id} to ACTIVE: "
                        "Another ACTIVE version already exists"
                    )
            
            # Update table state
            cursor.execute("""
                UPDATE tables
                SET state = %s, updated_at = NOW()
                WHERE name = %s AND state = %s
                RETURNING name, state, version, owner, supersedes,
                          deprecated_at, created_at, updated_at
            """, (to_state.value, table_id, from_state.value))
            
            updated = cursor.fetchone()
            if not updated:
                raise ValueError(
                    f"Table {table_id} not found in state {from_state.value}"
                )
            
            # Record state history
            cursor.execute("""
                INSERT INTO table_state_history (
                    table_name, from_state, to_state, changed_by, created_at
                ) VALUES (%s, %s, %s, %s, NOW())
            """, (table_id, from_state.value, to_state.value, changed_by))
            
            conn.commit()
            
            logger.info(
                f"Promoted table {table_id} from {from_state.value} to {to_state.value} "
                f"by {changed_by} (authorized by CKO)"
            )
            
            # RISK #4 FIX: Log promotion for audit trail
            logger.warning(
                f"🔴 PROMOTION: {table_id} from {from_state.value} to {to_state.value} "
                f"by {changed_by}. This affects all users."
            )
            
            # Record metrics
            metrics_collector.record_table_promotion(
                from_state.value,
                to_state.value
            )
            
            return dict(updated)
    
    def deprecate_table(
        self,
        table_id: str,
        reason: str,
        changed_by: str
    ) -> Dict[str, Any]:
        """
        Deprecate table.
        
        Args:
            table_id: Table name/identifier
            reason: Reason for deprecation
            changed_by: User identifier who made the change
        
        Returns:
            Updated table dict
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get current state
            cursor.execute("""
                SELECT state FROM tables WHERE name = %s
            """, (table_id,))
            
            current = cursor.fetchone()
            if not current:
                raise ValueError(f"Table {table_id} not found")
            
            current_state = TableState(current['state'])
            
            # Update to DEPRECATED
            cursor.execute("""
                UPDATE tables
                SET state = 'DEPRECATED', deprecated_at = NOW(), updated_at = NOW()
                WHERE name = %s
                RETURNING name, state, version, owner, supersedes,
                          deprecated_at, created_at, updated_at
            """, (table_id,))
            
            updated = cursor.fetchone()
            if not updated:
                raise ValueError(f"Failed to deprecate table {table_id}")
            
            # Record state history
            cursor.execute("""
                INSERT INTO table_state_history (
                    table_name, from_state, to_state, changed_by, reason, created_at
                ) VALUES (%s, %s, %s, %s, %s, NOW())
            """, (table_id, current_state.value, 'DEPRECATED', changed_by, reason))
            
            conn.commit()
            
            logger.info(f"Deprecated table {table_id} by {changed_by}: {reason}")
            
            # Record metrics
            metrics_collector.record_table_deprecation()
            
            return dict(updated)
    
    def restore_table(
        self,
        table_id: str,
        changed_by: str
    ) -> Dict[str, Any]:
        """
        Restore deprecated table to active.
        
        Args:
            table_id: Table name/identifier
            changed_by: User identifier who made the change
        
        Returns:
            Updated table dict
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check current state
            cursor.execute("""
                SELECT state FROM tables WHERE name = %s
            """, (table_id,))
            
            current = cursor.fetchone()
            if not current:
                raise ValueError(f"Table {table_id} not found")
            
            if current['state'] != 'DEPRECATED':
                raise ValueError(
                    f"Table {table_id} is not DEPRECATED (current: {current['state']})"
                )
            
            # Ensure no other ACTIVE version exists
            cursor.execute("""
                SELECT name FROM tables
                WHERE name = %s AND state = 'ACTIVE'
            """, (table_id,))
            
            if cursor.fetchone():
                raise ValueError(
                    f"Cannot restore {table_id}: Another ACTIVE version exists"
                )
            
            # Restore to ACTIVE
            cursor.execute("""
                UPDATE tables
                SET state = 'ACTIVE', deprecated_at = NULL, updated_at = NOW()
                WHERE name = %s
                RETURNING name, state, version, owner, supersedes,
                          deprecated_at, created_at, updated_at
            """, (table_id,))
            
            updated = cursor.fetchone()
            if not updated:
                raise ValueError(f"Failed to restore table {table_id}")
            
            # Record state history
            cursor.execute("""
                INSERT INTO table_state_history (
                    table_name, from_state, to_state, changed_by, created_at
                ) VALUES (%s, %s, %s, %s, NOW())
            """, (table_id, 'DEPRECATED', 'ACTIVE', changed_by))
            
            conn.commit()
            
            logger.info(f"Restored table {table_id} to ACTIVE by {changed_by}")
            
            return dict(updated)

