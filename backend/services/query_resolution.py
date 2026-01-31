"""
Query Resolution Engine

Implements the query resolution logic from EXECUTION_PLAN.md:
- Prefer ACTIVE tables
- Fallback to READ_ONLY tables
- Ignore SHADOW tables (never auto-queried)
- Include DEPRECATED only if explicitly pinned
"""

from typing import List, Optional, Dict, Any
import logging

from backend.models.table_state import TableState, TableStateInfo

logger = logging.getLogger(__name__)


class TableNotFoundError(Exception):
    """Raised when a table cannot be resolved."""
    pass


class QueryResolutionEngine:
    """Resolves table names to actual table objects based on state."""

    def __init__(self, table_store: Any):
        """
        Initialize query resolution engine.
        
        Args:
            table_store: Object with get_table(name, state) method
        """
        self.table_store = table_store

    def resolve_tables(
        self,
        query_tables: List[str],
        include_deprecated: bool = False
    ) -> List[TableStateInfo]:
        """
        Resolve table names to TableStateInfo objects.
        
        Resolution order:
        1. Try ACTIVE first (canonical tables)
        2. Fallback to READ_ONLY (external tables)
        3. Ignore SHADOW (never auto-queried)
        4. Include DEPRECATED only if explicitly requested
        
        Args:
            query_tables: List of table names to resolve
            include_deprecated: Whether to include deprecated tables
            
        Returns:
            List of resolved TableStateInfo objects
            
        Raises:
            TableNotFoundError: If a table cannot be resolved
        """
        resolved = []
        
        for table_name in query_tables:
            table = self._resolve_single_table(table_name, include_deprecated)
            if table:
                resolved.append(table)
            else:
                raise TableNotFoundError(
                    f"Table {table_name} not available. "
                    f"Check if table exists and is in ACTIVE or READ_ONLY state."
                )
        
        return resolved

    def _resolve_single_table(
        self,
        table_name: str,
        include_deprecated: bool = False
    ) -> Optional[TableStateInfo]:
        """Resolve a single table."""
        # Try ACTIVE first
        active = self.table_store.get_table(table_name, state=TableState.ACTIVE)
        if active:
            logger.debug(f"Resolved {table_name} as ACTIVE")
            return active
        
        # Fallback to READ_ONLY
        read_only = self.table_store.get_table(table_name, state=TableState.READ_ONLY)
        if read_only:
            logger.debug(f"Resolved {table_name} as READ_ONLY")
            return read_only
        
        # Include DEPRECATED only if explicitly requested
        if include_deprecated:
            deprecated = self.table_store.get_table(
                table_name, state=TableState.DEPRECATED
            )
            if deprecated:
                logger.debug(f"Resolved {table_name} as DEPRECATED (explicit)")
                return deprecated
        
        # SHADOW never auto-resolved
        shadow = self.table_store.get_table(table_name, state=TableState.SHADOW)
        if shadow:
            logger.warning(
                f"Table {table_name} is in SHADOW state and cannot be auto-queried. "
                f"Promote to ACTIVE first."
            )
        
        return None

    def get_resolution_explanation(
        self,
        query_tables: List[str],
        include_deprecated: bool = False
    ) -> Dict[str, Any]:
        """
        Get explanation of how tables were resolved.
        
        Returns:
            Dictionary with resolution details
        """
        explanation = {
            "tables": [],
            "warnings": [],
            "errors": []
        }
        
        for table_name in query_tables:
            try:
                table = self._resolve_single_table(table_name, include_deprecated)
                if table:
                    explanation["tables"].append({
                        "name": table.name,
                        "state": table.state.value,
                        "version": table.version,
                        "resolved": True
                    })
                else:
                    explanation["errors"].append({
                        "table": table_name,
                        "message": "Table not found or not available"
                    })
            except Exception as e:
                explanation["errors"].append({
                    "table": table_name,
                    "message": str(e)
                })
        
        return explanation

