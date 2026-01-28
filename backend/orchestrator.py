"""
RCA Engine Orchestrator

Main orchestration layer that integrates all components.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import re

from backend.config.config_manager import get_config
from backend.invariants import (
    DeterminismEnforcer,
    LLMDatabaseBoundary,
    ReproducibilityEngine,
    FailClosedEnforcer
)
from backend.planes import (
    IngressPlane,
    PlanningPlane,
    ExecutionPlane,
    PresentationPlane
)
from backend.planning import MultiStepPlanner, PlanningGuardrails
from backend.planning.intent_extractor import IntentExtractor
from backend.planning.query_builder import QueryBuilder
from backend.planning.schema_selector import SchemaSelector
from backend.planning.metric_resolver import MetricResolver
from backend.execution import QuerySandbox, QueryFirewall, KillSwitch
from backend.execution.sandbox import SandboxConfig
from backend.observability import GoldenSignals, CorrelationID, StructuredLogger
from backend.security import DataExfiltrationProtection, PromptInjectionProtection
from backend.failure_handling import LLMFailureHandler, MetadataDriftHandler, PartialResultHandler
from backend.deployment import FeatureFlags, ShadowMode
from backend.implementations import PostgreSQLExecutor, MySQLExecutor, SQLiteExecutor, OpenAIProvider, MemoryCache, RedisCache
from backend.auth import JWTAuthenticator, TokenBucketRateLimiter
from backend.presentation import ResultFormatter, ResultExplainer
from backend.planning.validator import QueryValidator, TableValidator


class RCAEngineOrchestrator:
    """Main orchestrator for RCA Engine."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize RCA Engine orchestrator.
        
        Args:
            config_file: Optional config file path
        """
        self.config = get_config(config_file)
        
        # Initialize components
        self._initialize_invariants()
        self._initialize_planes()
        self._initialize_planning()
        self._initialize_execution()
        self._initialize_observability()
        self._initialize_security()
        self._initialize_failure_handling()
        self._initialize_deployment()
    
    def _initialize_invariants(self):
        """Initialize invariant enforcers."""
        invariants_config = self.config.get_invariants_config()
        
        self.determinism_enforcer = DeterminismEnforcer(
            max_rows=self.config.get('security.max_rows', 10000),
            default_limit=self.config.get('security.default_limit', 1000)
        ) if invariants_config.get('enforce_determinism', True) else None
        
        self.boundary_enforcer = LLMDatabaseBoundary(
            strict_mode=invariants_config.get('enforce_boundary', True)
        ) if invariants_config.get('enforce_boundary', True) else None
        
        self.reproducibility_engine = ReproducibilityEngine() if invariants_config.get(
            'enforce_reproducibility', True
        ) else None
        
        self.fail_closed_enforcer = FailClosedEnforcer() if invariants_config.get(
            'enforce_fail_closed', True
        ) else None
    
    def _initialize_planes(self):
        """Initialize four-plane architecture."""
        # Initialize cache
        cache_config = self.config.get_cache_config()
        if cache_config.get('enabled', True):
            cache_type = cache_config.get('type', 'memory')
            if cache_type == 'redis':
                redis_config = cache_config.get('redis', {})
                cache = RedisCache(
                    host=redis_config.get('host', 'localhost'),
                    port=redis_config.get('port', 6379),
                    db=redis_config.get('db', 0),
                    default_ttl=cache_config.get('ttl', 3600)
                )
            else:
                cache = MemoryCache(
                    max_size=cache_config.get('max_size', 1000),
                    default_ttl=cache_config.get('ttl', 3600)
                )
        else:
            cache = None
        
        # Initialize authenticator
        auth_secret = self.config.get('auth.secret_key', 'change-me-in-production')
        authenticator = JWTAuthenticator(secret_key=auth_secret) if auth_secret else None
        
        # Initialize rate limiter
        security_config = self.config.get_security_config()
        rate_limit_config = security_config.get('rate_limit', {})
        rate_limiter = None
        if rate_limit_config.get('enabled', True):
            rate_limiter = TokenBucketRateLimiter(
                requests_per_minute=rate_limit_config.get('requests_per_minute', 60),
                requests_per_hour=rate_limit_config.get('requests_per_hour', 1000)
            )
        
        # Initialize validators
        self.query_validator = QueryValidator(metadata=None)  # Will be updated with metadata
        self.table_validator = TableValidator(metadata=None)  # Will be updated with metadata
        
        # Ingress plane
        self.ingress_plane = IngressPlane(
            authenticator=authenticator,
            rate_limiter=rate_limiter,
            validator=self.query_validator
        )
        
        # Planning plane
        self.planning_plane = PlanningPlane(
            multi_step_planner=None,  # Will be set after planning init
            guardrails=None,  # Will be set after planning init
            cache=cache
        )
        
        # Execution plane
        self.execution_plane = ExecutionPlane(
            sandbox=None,  # Will be set after execution init
            query_firewall=None,  # Will be set after execution init
            kill_switch=None,  # Will be set after execution init
            db_executor=None  # Will be set after execution init
        )
        
        # Presentation plane
        llm_config = self.config.get_llm_config()
        llm_provider = None
        if llm_config.get('api_key'):
            llm_provider = OpenAIProvider(
                api_key=llm_config.get('api_key'),
                model=llm_config.get('model', 'gpt-4'),
                temperature=llm_config.get('temperature', 0.0),
                max_tokens=llm_config.get('max_tokens', 3000),
                timeout=llm_config.get('timeout', 120)
            )
        
        self.presentation_plane = PresentationPlane(
            formatter=ResultFormatter(),
            explainer=ResultExplainer(llm_provider=llm_provider),
            cache=cache
        )
    
    def _initialize_planning(self):
        """Initialize planning components."""
        # Initialize LLM provider for intent extraction
        llm_config = self.config.get_llm_config()
        llm_provider = None
        if llm_config.get('api_key'):
            llm_provider = OpenAIProvider(
                api_key=llm_config.get('api_key'),
                model=llm_config.get('model', 'gpt-4'),
                temperature=llm_config.get('temperature', 0.0),
                max_tokens=llm_config.get('max_tokens', 3000),
                timeout=llm_config.get('timeout', 120)
            )
        
        # Intent extractor
        intent_extractor = IntentExtractor(llm_provider=llm_provider)
        
        # Query builder
        query_builder = QueryBuilder()
        
        # Schema selector and metric resolver
        metadata_path = self.config.get('metadata.path', None)
        schema_selector = SchemaSelector(metadata_path=metadata_path)
        metric_resolver = MetricResolver(metadata_path=metadata_path)
        
        # Load known metrics from metric resolver
        known_metrics = metric_resolver.known_metrics
        
        # Update validators with metadata
        self.query_validator.metadata = self.metadata
        self.table_validator.metadata = self.metadata
        self.table_validator._build_table_registry()
        
        # Guardrails
        self.planning_guardrails = PlanningGuardrails(
            known_metrics=known_metrics,
            table_validator=self.table_validator
        )
        
        # Multi-step planner
        self.multi_step_planner = MultiStepPlanner(
            intent_extractor=intent_extractor,
            schema_selector=schema_selector,
            metric_resolver=metric_resolver,
            query_builder=query_builder,
            guardrails=self.planning_guardrails
        )
        
        # Update planning plane
        self.planning_plane.multi_step_planner = self.multi_step_planner
        self.planning_plane.guardrails = self.planning_guardrails
    
    def _initialize_execution(self):
        """Initialize execution components."""
        execution_config = self.config.get_execution_config()
        sandbox_config_dict = execution_config.get('sandbox', {})
        
        # Create SandboxConfig
        sandbox_config = SandboxConfig(
            max_execution_time=sandbox_config_dict.get('max_execution_time', 30),
            max_rows=sandbox_config_dict.get('max_rows', 10000),
            default_limit=sandbox_config_dict.get('default_limit', 1000),
            read_only_role=sandbox_config_dict.get('read_only_role', 'rca_readonly'),
            enforce_ordering=sandbox_config_dict.get('enforce_ordering', True),
            enforce_limit=sandbox_config_dict.get('enforce_limit', True),
        )
        
        # Sandbox
        db_config = self.config.get_database_config()
        self.query_sandbox = QuerySandbox(
            db_config=db_config,
            sandbox_config=sandbox_config
        )
        
        # Firewall
        firewall_config = execution_config.get('firewall', {})
        self.query_firewall = QueryFirewall() if firewall_config.get('enabled', True) else None
        
        # Kill switch
        kill_switch_config = execution_config.get('kill_switch', {})
        self.kill_switch = KillSwitch() if kill_switch_config.get('enabled', True) else None
        
        # Database executor
        db_type = db_config.get('type', 'postgresql').lower()
        if db_type in ['postgresql', 'postgres']:
            self.db_executor = PostgreSQLExecutor(db_config)
        elif db_type == 'mysql':
            self.db_executor = MySQLExecutor(db_config)
        elif db_type == 'sqlite':
            self.db_executor = SQLiteExecutor(db_config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        # Update execution plane
        self.execution_plane.sandbox = self.query_sandbox
        self.execution_plane.query_firewall = self.query_firewall
        self.execution_plane.kill_switch = self.kill_switch
        self.execution_plane.db_executor = self.db_executor
    
    def _initialize_observability(self):
        """Initialize observability components."""
        self.golden_signals = GoldenSignals()
        self.structured_logger = StructuredLogger('rca_engine')
    
    def _initialize_security(self):
        """Initialize security components."""
        security_config = self.config.get_security_config()
        
        self.data_exfiltration_protection = DataExfiltrationProtection(
            max_rows=security_config.get('max_rows', 10000),
            max_export_rows=security_config.get('max_export_rows', 1000)
        )
        
        self.prompt_injection_protection = PromptInjectionProtection()
    
    def _initialize_failure_handling(self):
        """Initialize failure handlers."""
        self.llm_failure_handler = LLMFailureHandler()
        self.metadata_drift_handler = MetadataDriftHandler()
        self.partial_result_handler = PartialResultHandler()
    
    def _initialize_deployment(self):
        """Initialize deployment utilities."""
        deployment_config = self.config.get_deployment_config()
        
        # Feature flags
        feature_flags_config = deployment_config.get('feature_flags', {})
        self.feature_flags = FeatureFlags(flags=feature_flags_config)
        
        # Shadow mode
        shadow_config = deployment_config.get('shadow_mode', {})
        self.shadow_mode = ShadowMode() if shadow_config.get('enabled', False) else None
    
    def process_query(self, user_query: str, user_id: Optional[str] = None,
                     context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Process user query through all planes.
        
        Supports conversational queries where users can incrementally modify queries:
        - "give me all khatabook customers"
        - "add a writeoff flag to it as a column"
        - "remove arc cases etc"
        
        Args:
            user_query: User's natural language query
            user_id: Optional user ID
            context: Optional context dictionary (can include 'session_id' for conversational context)
        
        Returns:
            Complete result dictionary
        """
        context = context or {}
        correlation_id = CorrelationID.create()
        
        # Get or create conversational context (always, like ChatGPT)
        session_id = context.get('session_id') or user_id or f"session_{correlation_id.planning_id}"
        from backend.conversational_context import get_context_manager
        context_manager = get_context_manager()
        conv_context = context_manager.get_or_create_context(session_id)
        
        # Add conversational context to planning context (always, not just for modifications)
        conversational_context = None
        if conv_context.current_intent:
            conversational_context = {
                'current_intent': conv_context.current_intent,
                'current_sql': conv_context.current_sql,
                'query_history': conv_context.query_history[-3:] if len(conv_context.query_history) > 0 else []
            }
        context['conversational_context'] = conversational_context
        
        try:
            # Step 1: Ingress Plane
            request = {
                'query': user_query,
                'token': context.get('token'),
                'format': context.get('format', 'json'),
                'options': context.get('options', {}),
            }
            
            ingress_result = self.ingress_plane.process_request(request)
            correlation_id.set_planning_id(ingress_result.request_id)
            
            self.structured_logger.log_request(correlation_id, request)
            
            if not ingress_result.success:
                return {
                    'success': False,
                    'error': ingress_result.error,
                    'error_code': ingress_result.error_code,
                    'correlation_id': correlation_id.to_dict(),
                }
            
            # Step 2: Planning Plane
            planning_start = datetime.utcnow()
            
            # Pass conversational context to planning plane
            planning_result = self.planning_plane.plan_query(
                ingress_result.validated_input['query'],
                {
                    'user_id': ingress_result.user_id,
                    'request_id': ingress_result.request_id,
                    'conversational_context': conversational_context,
                    **context
                }
            )
            planning_duration = (datetime.utcnow() - planning_start).total_seconds() * 1000
            
            # Store conversational context after successful planning
            if planning_result.success and planning_result.intent:
                conv_context.update(planning_result.intent, planning_result.sql)
            
            if planning_result.planning_id:
                correlation_id.set_planning_id(planning_result.planning_id)
            
            self.golden_signals.record_planning(planning_duration)
            self.structured_logger.log_planning_step(
                correlation_id,
                'planning_complete',
                {'duration_ms': planning_duration}
            )
            
            if not planning_result.success:
                self.golden_signals.record_failure('planning_failed')
                return {
                    'success': False,
                    'error': planning_result.error,
                    'error_code': planning_result.error_code,
                    'correlation_id': correlation_id.to_dict(),
                }
            
            # Step 3: Execution Plane
            execution_start = datetime.utcnow()
            execution_result = self.execution_plane.execute_query(
                planning_result.sql,
                self.config.get_database_config(),
                ingress_result.user_id
            )
            execution_duration = (datetime.utcnow() - execution_start).total_seconds() * 1000
            
            if execution_result.execution_id:
                correlation_id.set_execution_id(execution_result.execution_id)
            
            self.golden_signals.record_execution(
                execution_duration,
                execution_result.rows_scanned,
                execution_result.rows_returned
            )
            self.structured_logger.log_execution(
                correlation_id,
                planning_result.sql,
                execution_duration
            )
            
            if not execution_result.success:
                self.golden_signals.record_failure('execution_failed')
                return {
                    'success': False,
                    'error': execution_result.error,
                    'error_code': execution_result.error_code,
                    'correlation_id': correlation_id.to_dict(),
                }
            
            # Step 4: Presentation Plane
            presentation_result = self.presentation_plane.format_results(
                execution_result.to_dict(),
                format_type=ingress_result.validated_input.get('format', 'json'),
                generate_explanation=True
            )
            
            # Return complete result
            return {
                'success': True,
                'data': presentation_result.data,
                'content': presentation_result.content,
                'explanation': presentation_result.explanation,
                'correlation_id': correlation_id.to_dict(),
                'planning_id': planning_result.planning_id,
                'execution_id': execution_result.execution_id,
                'metrics': {
                    'planning_duration_ms': planning_duration,
                    'execution_duration_ms': execution_duration,
                    'rows_returned': execution_result.rows_returned,
                    'rows_scanned': execution_result.rows_scanned,
                }
            }
            
        except Exception as e:
            self.structured_logger.log_error(correlation_id, e, context)
            self.golden_signals.record_failure('orchestrator_error')
            return {
                'success': False,
                'error': str(e),
                'error_code': 'ORCHESTRATOR_ERROR',
                'correlation_id': correlation_id.to_dict(),
            }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return self.golden_signals.get_metrics()
    
    def get_health(self) -> Dict[str, Any]:
        """Get system health status."""
        return {
            'status': 'healthy',
            'components': {
                'ingress': 'operational',
                'planning': 'operational',
                'execution': 'operational',
                'presentation': 'operational',
            },
            'metrics': self.get_metrics(),
            'timestamp': datetime.utcnow().isoformat(),
        }

