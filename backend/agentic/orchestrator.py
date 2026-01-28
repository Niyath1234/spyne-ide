#!/usr/bin/env python3
"""
Agentic Semantic SQL Engine Orchestrator

Main orchestrator that chains all agents together.
Implements the golden invariant: If Metric Agent fails, system must refuse to answer.
"""

from typing import Dict, Any, Optional, Tuple, List
from pathlib import Path
import json

from .metric_registry import MetricRegistry
from .intent_agent import IntentAgent
from .metric_agent import MetricAgent
from .table_agent import TableAgent
from .filter_agent import FilterAgent
from .shape_agent import ShapeAgent
from .verifier_agent import VerifierAgent
from .sql_renderer import SQLRenderer


class AgenticSQLOrchestrator:
    """
    Agentic Semantic SQL Engine Orchestrator.
    
    This orchestrator chains all agents together:
    1. Intent Agent → classify query
    2. Metric Agent → resolve metrics (CRITICAL - fails if unresolved)
    3. Table Agent → confirm base table
    4. Filter Agent → generate filters
    5. Shape Agent → decide presentation
    6. Verifier Agent → check correctness
    7. SQL Renderer → generate SQL
    
    Golden Invariant: If Metric Agent fails, system must refuse to answer.
    """
    
    def __init__(self, llm_client, metadata: Dict[str, Any], 
                 knowledge_rules=None):
        """
        Initialize orchestrator.
        
        Args:
            llm_client: LLM client (from LLMQueryGenerator)
            metadata: Metadata dictionary
            knowledge_rules: Knowledge register rules (optional)
        """
        self.llm = llm_client
        self.metadata = metadata
        
        # Initialize Metric Registry (ground truth)
        semantic_registry = metadata.get('semantic_registry', {})
        self.metric_registry = MetricRegistry(registry_data=semantic_registry)
        
        # Initialize all agents
        self.intent_agent = IntentAgent(llm_client)
        self.metric_agent = MetricAgent(llm_client, self.metric_registry)
        self.table_agent = TableAgent(llm_client, metadata)
        self.filter_agent = FilterAgent(llm_client, metadata, knowledge_rules)
        self.shape_agent = ShapeAgent(llm_client)
        self.verifier_agent = VerifierAgent()
        self.sql_renderer = SQLRenderer()
        
        # Track execution steps for debugging
        self.execution_steps: List[str] = []
    
    def generate_sql(self, query: str) -> Dict[str, Any]:
        """
        Generate SQL using agentic pipeline.
        
        Args:
            query: User query
        
        Returns:
            {
                "success": True/False,
                "sql": "...",
                "intent": {...},
                "metric_output": {...},
                "table_output": {...},
                "filter_output": {...},
                "shape_output": {...},
                "verification": {...},
                "reasoning": [...]
            }
        """
        self.execution_steps = []
        self.execution_steps.append(f" Analyzing query: {query}")
        
        try:
            # Step 1: Intent Agent
            self.execution_steps.append(" Step 1: Intent Classification...")
            intent_output = self.intent_agent.classify(query)
            self.execution_steps.append(f"    Query type: {intent_output.get('query_type')}")
            self.execution_steps.append(f"    Requested metrics: {intent_output.get('requested_metrics')}")
            
            # Step 2: Metric Agent (CRITICAL)
            self.execution_steps.append(" Step 2: Metric Resolution...")
            try:
                metric_output = self.metric_agent.resolve(intent_output, query)
                self.execution_steps.append(f"    Status: {metric_output.get('status')}")
                self.execution_steps.append(f"    Resolved metrics: {len(metric_output.get('resolved_metrics', []))}")
                
                if metric_output.get('status') == 'UNRESOLVED':
                    raise ValueError("Metric resolution failed - cannot proceed")
                    
            except ValueError as e:
                # Golden Invariant: If Metric Agent fails, STOP THE PIPELINE
                self.execution_steps.append(f"    CRITICAL: {str(e)}")
                return {
                    "success": False,
                    "error": str(e),
                    "reasoning": self.execution_steps,
                    "stage": "metric_resolution"
                }
            
            # Step 3: Table Agent
            self.execution_steps.append("️  Step 3: Table Resolution...")
            table_output = self.table_agent.resolve(metric_output, query)
            self.execution_steps.append(f"    Base table: {table_output.get('base_table')}")
            
            # Step 4: Filter Agent
            self.execution_steps.append(" Step 4: Filter Generation...")
            filter_output = self.filter_agent.generate(intent_output, metric_output, table_output, query)
            self.execution_steps.append(f"    Filters: {len(filter_output.get('filters', []))}")
            
            # Step 5: Shape Agent
            self.execution_steps.append(" Step 5: Shape Generation...")
            shape_output = self.shape_agent.generate(intent_output, metric_output, query)
            self.execution_steps.append(f"    Dimensions: {len(shape_output.get('dimensions', []))}")
            
            # Step 6: Verifier Agent
            self.execution_steps.append(" Step 6: Verification...")
            verification = self.verifier_agent.verify(
                intent_output, metric_output, table_output, filter_output, shape_output
            )
            
            if verification.get('status') == 'REJECTED':
                self.execution_steps.append(f"    REJECTED: {verification.get('reason')}")
                return {
                    "success": False,
                    "error": verification.get('reason'),
                    "reasoning": self.execution_steps,
                    "stage": "verification",
                    "intent": intent_output,
                    "metric_output": metric_output,
                    "table_output": table_output,
                    "filter_output": filter_output,
                    "shape_output": shape_output
                }
            
            self.execution_steps.append(f"    ACCEPTED: {verification.get('reason')}")
            
            # Step 7: SQL Renderer
            self.execution_steps.append(" Step 7: SQL Rendering...")
            sql = self.sql_renderer.render(
                intent_output, metric_output, table_output, filter_output, shape_output
            )
            self.execution_steps.append(f"    SQL generated ({len(sql)} chars)")
            
            # Final SQL verification
            sql_verification = self.verifier_agent.verify_sql(sql, intent_output, metric_output)
            if sql_verification.get('status') == 'REJECTED':
                self.execution_steps.append(f"    SQL REJECTED: {sql_verification.get('reason')}")
                return {
                    "success": False,
                    "error": f"SQL verification failed: {sql_verification.get('reason')}",
                    "reasoning": self.execution_steps,
                    "stage": "sql_verification",
                    "sql": sql
                }
            
            self.execution_steps.append(f"    SQL VERIFIED: {sql_verification.get('reason')}")
            
            # Success!
            return {
                "success": True,
                "sql": sql,
                "intent": intent_output,
                "metric_output": metric_output,
                "table_output": table_output,
                "filter_output": filter_output,
                "shape_output": shape_output,
                "verification": verification,
                "sql_verification": sql_verification,
                "reasoning": self.execution_steps
            }
            
        except Exception as e:
            self.execution_steps.append(f" ERROR: {str(e)}")
            import traceback
            self.execution_steps.append(traceback.format_exc())
            
            return {
                "success": False,
                "error": str(e),
                "reasoning": self.execution_steps,
                "stage": "unknown"
            }

