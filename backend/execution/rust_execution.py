"""
Rust Execution Engine Integration

Python wrapper for Rust execution engines via PyO3 bindings.
"""

import os
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# Try to import Rust execution bindings
try:
    from spyne_execution import (
        PythonExecutionRouter,
        PythonQueryProfile,
        agent_select_engine_py,
    )
    RUST_EXECUTION_AVAILABLE = True
except ImportError:
    RUST_EXECUTION_AVAILABLE = False
    logger.warning("Rust execution engines not available. Install with: maturin develop")


class RustExecutionRouter:
    """Python wrapper for Rust ExecutionRouter."""
    
    def __init__(
        self,
        engines: Optional[List[str]] = None,
        data_dir: Optional[str] = None,
        trino_config: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize Rust execution router.
        
        Args:
            engines: List of engine names ['duckdb', 'trino', 'polars']
            data_dir: Optional data directory path (defaults to './data')
            trino_config: Optional dict with Trino config:
                - coordinator_url: Trino coordinator URL
                - catalog: Default catalog name
                - schema: Default schema name
                - user: Trino user name
        """
        if not RUST_EXECUTION_AVAILABLE:
            raise ImportError(
                "Rust execution engines not available. "
                "Install with: maturin develop"
            )
        
        engines = engines or ['duckdb']  # Default to DuckDB
        
        # Use environment variables for Trino if not provided
        if trino_config is None:
            trino_config = {}
            if os.getenv('TRINO_COORDINATOR_URL'):
                trino_config['coordinator_url'] = os.getenv('TRINO_COORDINATOR_URL')
            if os.getenv('TRINO_CATALOG'):
                trino_config['catalog'] = os.getenv('TRINO_CATALOG')
            if os.getenv('TRINO_SCHEMA'):
                trino_config['schema'] = os.getenv('TRINO_SCHEMA')
            if os.getenv('TRINO_USER'):
                trino_config['user'] = os.getenv('TRINO_USER')
        
        data_dir = data_dir or os.getenv('DATA_DIR', './data')
        
        try:
            self._router = PythonExecutionRouter(engines, data_dir, trino_config if trino_config else None)
            logger.info(f"Rust execution router initialized with engines: {engines}")
        except Exception as e:
            logger.error(f"Failed to initialize Rust execution router: {e}")
            raise
    
    def available_engines(self) -> List[str]:
        """Get list of available engines."""
        return self._router.available_engines()
    
    def suggest_engine(self, sql: str) -> List[Dict[str, Any]]:
        """
        Get engine suggestions for SQL query.
        
        Args:
            sql: SQL query string
            
        Returns:
            List of engine suggestions with scores and reasoning
        """
        try:
            suggestions = self._router.suggest_engine(sql)
            return [dict(s) for s in suggestions]
        except Exception as e:
            logger.error(f"Failed to suggest engine: {e}")
            raise
    
    def execute(
        self,
        sql: str,
        engine_selection: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Execute query with selected engine.
        
        Args:
            sql: SQL query string
            engine_selection: EngineSelection dict with:
                - engine_name: str
                - reasoning: list[str]
                - fallback_available: bool (optional)
        
        Returns:
            QueryResult dict with:
                - success: bool
                - data: list[dict]
                - columns: list[str]
                - rows_returned: int
                - rows_scanned: int
                - execution_time_ms: int
                - engine_metadata: dict
        """
        try:
            result = self._router.execute(sql, engine_selection)
            return dict(result)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def agent_select_engine(
        self,
        query: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Agent-based engine selection.
        
        Args:
            query: User query or SQL
            metadata: Optional metadata dict with:
                - prefer_speed: bool (optional)
        
        Returns:
            EngineSelection dict
        """
        try:
            selection = agent_select_engine_py(self._router, query, metadata)
            return dict(selection)
        except Exception as e:
            logger.error(f"Agent engine selection failed: {e}")
            raise

