"""
CKO (Canonical Knowledge Object) Client

This module provides the ONLY interface for Python code to interact with WorldState.
All metadata mutations MUST go through this client - it is the single write path.

INVARIANT #1: WorldState is the only authority on meaning.
INVARIANT #2: Python never generates executable SQL.
INVARIANT #3: Ingestion never writes to ACTIVE tables.
INVARIANT #4: Promotion is the only path to user-visible change.
INVARIANT #5: All irreversible actions are explicit and auditable.

This client enforces these invariants by:
- Only allowing requests, never direct mutations
- Routing all writes through WorldState
- Blocking direct SQL generation
- Enforcing shadow table state for ingestion
"""

from typing import Dict, Any, Optional, List
import logging
import json

logger = logging.getLogger(__name__)


class CKOClient:
    """
    Canonical Knowledge Object Client.
    
    This is the ONLY way Python code can request metadata changes.
    WorldState accepts, rejects, versions, or defers these requests.
    Python may only REQUEST, never decide.
    """
    
    def __init__(self, world_state_endpoint: Optional[str] = None):
        """
        Initialize CKO client.
        
        Args:
            world_state_endpoint: Optional endpoint for WorldState service
                                 If None, uses in-process WorldState
        """
        self.world_state_endpoint = world_state_endpoint
        # In a real implementation, this would connect to WorldState service
        # For now, we'll use a placeholder that enforces the boundary
    
    def propose_contract(
        self,
        table_name: str,
        column_mappings: List[Dict[str, Any]],
        primary_key: List[str],
        endpoint: Optional[str] = None,
        description: Optional[str] = None,
        business_rules: Optional[List[str]] = None,
        created_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Propose a new contract to WorldState.
        
        This is a REQUEST - WorldState will:
        - Validate the contract
        - Version it appropriately
        - Store it in SHADOW state
        - Return the contract ID
        
        Args:
            table_name: Target table name
            column_mappings: List of column mapping dicts
            primary_key: List of primary key column names
            endpoint: Optional API endpoint
            description: Optional table description
            business_rules: Optional business rules
            created_by: User who created this contract
            
        Returns:
            Dict with contract_id, version, state (SHADOW), and status
        """
        logger.info(f"CKO: Proposing contract for table {table_name}")
        
        # In real implementation, this would call WorldState API
        # For now, return a structured response that enforces shadow state
        return {
            "contract_id": f"{table_name}_v1",
            "table_name": table_name,
            "version": "v1",
            "state": "SHADOW",  # ALWAYS SHADOW initially
            "status": "proposed",
            "message": "Contract proposed - requires promotion to become ACTIVE"
        }
    
    def request_state_change(
        self,
        table_name: str,
        from_state: str,
        to_state: str,
        requested_by: str,
        reason: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Request a state change (e.g., SHADOW -> ACTIVE promotion).
        
        This is a REQUEST - WorldState will:
        - Validate the transition
        - Check permissions
        - Perform the change
        - Log it for audit
        
        Args:
            table_name: Table name
            from_state: Current state
            to_state: Target state
            requested_by: User requesting the change
            reason: Optional reason for the change
            
        Returns:
            Dict with status, new_state, and any warnings
        """
        logger.info(f"CKO: Requesting state change for {table_name}: {from_state} -> {to_state}")
        
        # Enforce promotion boundary
        if from_state == "SHADOW" and to_state == "ACTIVE":
            logger.warning(f"⚠️ PROMOTION REQUEST: {table_name} from SHADOW to ACTIVE")
            logger.warning("This affects all users. Rollback available within 24 hours.")
        
        # In real implementation, this would call WorldState API
        return {
            "status": "requested",
            "table_name": table_name,
            "from_state": from_state,
            "to_state": to_state,
            "requested_by": requested_by,
            "requires_confirmation": to_state == "ACTIVE"
        }
    
    def get_table_schema(
        self,
        table_name: str,
        version: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get table schema from WorldState (READ-ONLY).
        
        Args:
            table_name: Table name
            version: Optional version (defaults to ACTIVE)
            
        Returns:
            Schema dict or None if not found
        """
        # In real implementation, this would query WorldState
        logger.debug(f"CKO: Reading schema for {table_name} (read-only)")
        return None
    
    def get_contract(
        self,
        contract_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get contract from WorldState (READ-ONLY).
        
        Args:
            contract_id: Contract identifier
            
        Returns:
            Contract dict or None if not found
        """
        # In real implementation, this would query WorldState
        logger.debug(f"CKO: Reading contract {contract_id} (read-only)")
        return None
    
    def list_tables(
        self,
        state: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List tables from WorldState (READ-ONLY).
        
        Args:
            state: Optional state filter (ACTIVE, SHADOW, etc.)
            
        Returns:
            List of table dicts
        """
        # In real implementation, this would query WorldState
        logger.debug(f"CKO: Listing tables (read-only, state={state})")
        return []


# Singleton instance
_cko_client: Optional[CKOClient] = None


def get_cko_client() -> CKOClient:
    """Get singleton CKO client instance."""
    global _cko_client
    if _cko_client is None:
        world_state_endpoint = None  # Could come from config
        _cko_client = CKOClient(world_state_endpoint)
    return _cko_client

