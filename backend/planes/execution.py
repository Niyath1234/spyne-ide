"""
Execution Plane

Execute queries in sandboxed environment.
SLA: < 30s latency
Failure Mode: Timeout, return partial
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

# Try to import Rust execution
try:
    from backend.execution.rust_execution import RustExecutionRouter, RUST_EXECUTION_AVAILABLE
except ImportError:
    RUST_EXECUTION_AVAILABLE = False
    RustExecutionRouter = None
    logger.warning("Rust execution engines not available")


@dataclass
class ExecutionResult:
    """Result from execution plane."""
    success: bool
    execution_id: str
    rows_returned: int = 0
    rows_scanned: int = 0
    duration_ms: float = 0.0
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    partial: bool = False
    warning: Optional[str] = None
    timestamp: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            'success': self.success,
            'execution_id': self.execution_id,
            'rows_returned': self.rows_returned,
            'rows_scanned': self.rows_scanned,
            'duration_ms': self.duration_ms,
            'partial': self.partial,
            'timestamp': self.timestamp or datetime.utcnow().isoformat(),
        }
        if self.data:
            result['data'] = self.data
        if self.columns:
            result['columns'] = self.columns
        if self.error:
            result['error'] = self.error
        if self.error_code:
            result['error_code'] = self.error_code
        if self.warning:
            result['warning'] = self.warning
        return result


class ExecutionPlane:
    """Execute queries in sandboxed environment."""
    
    def __init__(
        self,
        sandbox=None,
        query_firewall=None,
        kill_switch=None,
        db_executor=None,
        use_rust_engines: Optional[bool] = None,
    ):
        """
        Initialize execution plane.
        
        Args:
            sandbox: Query sandbox instance
            query_firewall: Query firewall instance
            kill_switch: Kill switch instance
            db_executor: Database executor instance
            use_rust_engines: Whether to use Rust execution engines (defaults to env var)
        """
        self.sandbox = sandbox
        self.query_firewall = query_firewall
        self.kill_switch = kill_switch
        self.db_executor = db_executor
        
        # Initialize Rust execution router if available
        self.rust_router = None
        self.use_rust_engines = (
            use_rust_engines
            if use_rust_engines is not None
            else os.getenv('USE_RUST_EXECUTION_ENGINES', 'false').lower() == 'true'
        ) and RUST_EXECUTION_AVAILABLE
        
        if self.use_rust_engines:
            try:
                # Get available engines from environment
                available_engines = os.getenv('AVAILABLE_ENGINES', 'duckdb').split(',')
                available_engines = [e.strip() for e in available_engines if e.strip()]
                
                # Filter to only engines that are actually available
                engines = []
                for engine_name in available_engines:
                    if self._check_engine_available(engine_name):
                        engines.append(engine_name)
                
                if engines:
                    self.rust_router = RustExecutionRouter(engines=engines)
                    logger.info(f"Rust execution engines enabled: {engines}")
                else:
                    logger.warning("No Rust engines available, falling back to DB executor")
                    self.use_rust_engines = False
            except Exception as e:
                logger.warning(f"Failed to initialize Rust execution engines: {e}")
                self.use_rust_engines = False
    
    def _check_engine_available(self, engine_name: str) -> bool:
        """Check if engine is available."""
        if engine_name == 'trino':
            return bool(os.getenv('TRINO_COORDINATOR_URL'))
        elif engine_name in ['duckdb', 'polars']:
            return True  # Always available
        return False
    
    def execute_query(self, sql: str, db_config: Dict[str, Any],
                     user_id: Optional[str] = None) -> ExecutionResult:
        """
        Execute query with full sandboxing.
        
        Args:
            sql: SQL query string
            db_config: Database configuration
            user_id: User ID for kill switch checks
        
        Returns:
            ExecutionResult
        """
        execution_id = self._generate_execution_id()
        start_time = datetime.utcnow()
        
        # Step 1: Check kill switch
        if self.kill_switch and user_id:
            if self.kill_switch.check_kill_switch(user_id, execution_id):
                return ExecutionResult(
                    success=False,
                    execution_id=execution_id,
                    error='Query execution blocked by kill switch',
                    error_code='KILL_SWITCH_ACTIVE',
                    timestamp=datetime.utcnow().isoformat()
                )
        
        # Step 2: Query firewall check
        if self.query_firewall:
            firewall_result = self.query_firewall.check_query(sql)
            if not firewall_result['allowed']:
                return ExecutionResult(
                    success=False,
                    execution_id=execution_id,
                    error=firewall_result.get('reason', 'Query blocked by firewall'),
                    error_code='FIREWALL_BLOCKED',
                    timestamp=datetime.utcnow().isoformat()
                )
        
        # Step 3: Sandbox query (rewrite for safety)
        if self.sandbox:
            try:
                safe_sql = self.sandbox.execute_sandboxed(sql)
            except Exception as e:
                return ExecutionResult(
                    success=False,
                    execution_id=execution_id,
                    error=f'Sandbox error: {str(e)}',
                    error_code='SANDBOX_ERROR',
                    timestamp=datetime.utcnow().isoformat()
                )
        else:
            safe_sql = sql
        
        # Step 4: Execute query
        # Try Rust execution engines first if available
        if self.use_rust_engines and self.rust_router:
            try:
                # Agent selects engine
                engine_selection = self.rust_router.agent_select_engine(safe_sql)
                
                # Execute with selected engine
                rust_result = self.rust_router.execute(safe_sql, engine_selection)
                
                end_time = datetime.utcnow()
                duration_ms = (end_time - start_time).total_seconds() * 1000
                
                return ExecutionResult(
                    success=rust_result.get('success', True),
                    execution_id=execution_id,
                    rows_returned=rust_result.get('rows_returned', 0),
                    rows_scanned=rust_result.get('rows_scanned', 0),
                    duration_ms=rust_result.get('execution_time_ms', duration_ms),
                    data=rust_result.get('data', []),
                    columns=rust_result.get('columns', []),
                    partial=False,
                    warning=None,
                    timestamp=datetime.utcnow().isoformat()
                )
            except Exception as e:
                logger.warning(f"Rust execution failed, falling back to DB executor: {e}")
                # Fall through to DB executor
        
        # Fallback: Traditional database executor
        if not self.db_executor:
            return ExecutionResult(
                success=False,
                execution_id=execution_id,
                error='Database executor not available',
                error_code='EXECUTOR_UNAVAILABLE',
                timestamp=datetime.utcnow().isoformat()
            )
        
        try:
            result = self.db_executor.execute_readonly(safe_sql)
            
            end_time = datetime.utcnow()
            duration_ms = (end_time - start_time).total_seconds() * 1000
            
            return ExecutionResult(
                success=True,
                execution_id=execution_id,
                rows_returned=result.get('rows_returned', 0),
                rows_scanned=result.get('rows_scanned', 0),
                duration_ms=duration_ms,
                data=result.get('data', []),
                columns=result.get('columns', []),
                partial=result.get('partial', False),
                warning=result.get('warning'),
                timestamp=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration_ms = (end_time - start_time).total_seconds() * 1000
            
            return ExecutionResult(
                success=False,
                execution_id=execution_id,
                duration_ms=duration_ms,
                error=str(e),
                error_code='EXECUTION_ERROR',
                timestamp=datetime.utcnow().isoformat()
            )
    
    def _generate_execution_id(self) -> str:
        """Generate unique execution ID."""
        import uuid
        return str(uuid.uuid4())

