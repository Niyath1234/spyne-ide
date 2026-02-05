"""
LangGraph Orchestration - Master pipeline connecting all intelligence nodes
"""
from typing import TypedDict, Dict, Any, Optional, List
from langgraph.graph import StateGraph, END
import logging

from ..planning.intent_engine import IntentEngine
from ..planning.resolution_engine import ResolutionEngine
from ..retrieval.semantic_search import SemanticRetriever
from ..planning.join_graph import JoinGraph
from ..planning.query_planner import QueryPlanner
from ..sql.generator import SQLGenerator
from ..sql.critic import SQLCritic
from ..sql.validator import SQLValidator
from ..sql.column_fixer import ColumnFixer
from ..trino.validator import TrinoValidator
from ..learning.memory import MemorySystem
from ..metadata.semantic_registry import SemanticRegistry
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class GraphState(TypedDict):
    """Global graph state passed between nodes"""
    user_query: str
    
    # Intent extraction
    intent: Dict[str, Any]
    
    # Resolution
    resolution: Dict[str, Any]
    
    # Retrieval
    retrieved_context: Dict[str, Any]
    
    # Join planning
    join_path: List[tuple]
    
    # Query planning
    query_plan: Dict[str, Any]
    
    # SQL generation
    generated_sql: str
    fixed_sql: str
    
    # Validation
    validation_errors: List[str]
    
    # Final output
    final_sql: str
    
    # Metadata
    available_metrics: List[str]


class LangGraphOrchestrator:
    """LangGraph orchestration of the entire pipeline"""
    
    def __init__(
        self,
        intent_engine: Optional[IntentEngine] = None,
        resolution_engine: Optional[ResolutionEngine] = None,
        semantic_retriever: Optional[SemanticRetriever] = None,
        join_graph: Optional[JoinGraph] = None,
        query_planner: Optional[QueryPlanner] = None,
        sql_generator: Optional[SQLGenerator] = None,
        sql_critic: Optional[SQLCritic] = None,
        sql_validator: Optional[SQLValidator] = None,
        trino_validator: Optional[TrinoValidator] = None,
        memory_system: Optional[MemorySystem] = None,
        semantic_registry: Optional[SemanticRegistry] = None
    ):
        """Initialize orchestrator with all components"""
        self.intent_engine = intent_engine or IntentEngine()
        self.resolution_engine = resolution_engine or ResolutionEngine()
        self.semantic_retriever = semantic_retriever or SemanticRetriever()
        self.join_graph = join_graph or JoinGraph()
        self.query_planner = query_planner or QueryPlanner()
        self.sql_generator = sql_generator or SQLGenerator()
        self.sql_critic = sql_critic or SQLCritic()
        self.sql_validator = sql_validator or SQLValidator()
        self.column_fixer = ColumnFixer(semantic_registry or SemanticRegistry())
        self.trino_validator = trino_validator
        self.memory_system = memory_system or MemorySystem()
        self.semantic_registry = semantic_registry or SemanticRegistry()
        
        # Load join graph from metadata
        self._load_join_graph()
        
        # Build graph
        self.graph = self._build_graph()
    
    def _load_join_graph(self):
        """Load join relationships from lineage.json"""
        try:
            metadata_dir = Path(__file__).parent.parent.parent.parent / "metadata"
            lineage_file = metadata_dir / "lineage.json"
            
            if lineage_file.exists():
                with open(lineage_file, 'r') as f:
                    lineage_data = json.load(f)
                
                edges = lineage_data.get('edges', [])
                for edge in edges:
                    from_table_full = edge.get('from', '')
                    to_table_full = edge.get('to', '')
                    join_condition = edge.get('on', '')
                    
                    # Extract short table names
                    from_table = from_table_full.split('.')[-1] if '.' in from_table_full else from_table_full
                    to_table = to_table_full.split('.')[-1] if '.' in to_table_full else to_table_full
                    
                    if from_table and to_table and join_condition:
                        self.join_graph.add_join(from_table, to_table, join_condition, 'LEFT')
                        logger.debug(f"Added join: {from_table} -> {to_table}: {join_condition}")
                
                logger.info(f"Loaded {len(edges)} join relationships into join graph")
            else:
                logger.warning(f"Lineage file not found: {lineage_file}")
        except Exception as e:
            logger.warning(f"Could not load join graph from lineage.json: {e}")
    
    def _build_graph(self) -> StateGraph:
        """Build LangGraph with all nodes"""
        graph = StateGraph(GraphState)
        
        # Add all nodes
        graph.add_node("intent", self._intent_node)
        graph.add_node("resolution", self._resolution_node)
        graph.add_node("retrieval", self._retrieval_node)
        graph.add_node("join", self._join_planner_node)
        graph.add_node("plan", self._query_plan_node)
        graph.add_node("sql_gen", self._sql_generation_node)
        graph.add_node("critic", self._sql_critic_node)
        graph.add_node("validator", self._sql_ast_validator_node)
        graph.add_node("trino", self._trino_validation_node)
        graph.add_node("memory", self._memory_node)
        
        # Set entry point
        graph.set_entry_point("intent")
        
        # Add edges (linear flow)
        graph.add_edge("intent", "resolution")
        graph.add_edge("resolution", "retrieval")
        graph.add_edge("retrieval", "join")
        graph.add_edge("join", "plan")
        graph.add_edge("plan", "sql_gen")
        graph.add_edge("sql_gen", "critic")
        graph.add_edge("critic", "validator")
        graph.add_edge("validator", "trino")
        graph.add_edge("trino", "memory")
        graph.add_edge("memory", END)
        
        return graph.compile()
    
    def _intent_node(self, state: GraphState) -> GraphState:
        """Node 1: Extract intent"""
        logger.info("Running intent node")
        intent = self.intent_engine.extract_intent(state["user_query"])
        state["intent"] = intent
        return state
    
    def _resolution_node(self, state: GraphState) -> GraphState:
        """Node 2: Classify query and determine resolution"""
        logger.info("Running resolution node")
        
        # Get available metrics
        metrics = self.semantic_registry.get_metrics()
        metric_names = [m['metric_name'] for m in metrics]
        state["available_metrics"] = metric_names
        
        resolution = self.resolution_engine.resolve(
            state["user_query"],
            state["intent"],
            metric_names
        )
        state["resolution"] = resolution
        
        # If IMPOSSIBLE, we could exit early, but for now continue
        if self.resolution_engine.is_impossible(resolution):
            logger.warning("Query marked as IMPOSSIBLE")
        
        return state
    
    def _retrieval_node(self, state: GraphState) -> GraphState:
        """Node 3: Semantic retrieval"""
        logger.info("Running retrieval node")
        context = self.semantic_retriever.retrieve(state["user_query"], top_k=10)
        
        # Enhance context with actual column details for retrieved tables
        enhanced_tables = []
        for table in context.get("tables", []):
            table_name = table.get("entity_name", "") or table.get("table_name", "")
            if table_name:
                # Get actual columns from metadata
                columns = self.semantic_registry.get_columns(table_name)
                if not columns:
                    # Try with short name
                    table_short = table_name.split('.')[-1] if '.' in table_name else table_name
                    columns = self.semantic_registry.get_columns(table_short)
                table["columns"] = columns
                logger.info(f"Retrieved {len(columns)} columns for table {table_name}")
                enhanced_tables.append(table)
            else:
                enhanced_tables.append(table)
        
        # Also ensure we have customer, orders, and lineitem tables if they're relevant
        intent_grain = state.get("intent", {}).get("grain", "").lower()
        intent_metric = state.get("intent", {}).get("metric", "").lower()
        
        if "customer" in intent_grain:
            # Ensure customer table is in context
            customer_cols = self.semantic_registry.get_columns("customer")
            if customer_cols and not any("customer" in (t.get("entity_name", "") or t.get("table_name", "")).lower() for t in enhanced_tables):
                enhanced_tables.append({
                    "entity_name": "customer",
                    "table_name": "customer",
                    "columns": customer_cols
                })
                logger.info("Added customer table to context")
        
        # Ensure lineitem is included if metric mentions extendedprice or discount
        if "extendedprice" in intent_metric or "discount" in intent_metric:
            lineitem_cols = self.semantic_registry.get_columns("lineitem")
            if lineitem_cols and not any("lineitem" in (t.get("entity_name", "") or t.get("table_name", "")).lower() for t in enhanced_tables):
                enhanced_tables.append({
                    "entity_name": "lineitem",
                    "table_name": "lineitem",
                    "columns": lineitem_cols
                })
                logger.info("Added lineitem table to context")
        
        # Also ensure orders table is available (needed for join path)
        orders_cols = self.semantic_registry.get_columns("orders")
        if orders_cols and not any("orders" in (t.get("entity_name", "") or t.get("table_name", "")).lower() for t in enhanced_tables):
            enhanced_tables.append({
                "entity_name": "orders",
                "table_name": "orders",
                "columns": orders_cols
            })
            logger.info("Added orders table to context")
        
        context["tables"] = enhanced_tables
        state["retrieved_context"] = context
        return state
    
    def _join_planner_node(self, state: GraphState) -> GraphState:
        """Node 4: Compute join path"""
        logger.info("=" * 60)
        logger.info("JOIN PLANNER NODE STARTING")
        logger.info("=" * 60)
        
        # Get base table from intent grain or retrieved context
        grain = state["intent"].get("grain", "")
        intent_metric = state["intent"].get("metric", "").lower()
        logger.info(f"Intent grain: {grain}")
        logger.info(f"Intent metric: {intent_metric}")
        base_table = None
        
        # EARLY FALLBACK: If customer grain + extendedprice/discount metric, find path immediately
        if grain.lower() == "customer" and ("extendedprice" in intent_metric or "discount" in intent_metric):
            logger.info("=" * 60)
            logger.info("EARLY FALLBACK: Customer grain + metric columns detected")
            logger.info("Finding path from customer to lineitem")
            logger.info("=" * 60)
            path = self.join_graph.find_join_path("customer", "lineitem")
            if path:
                logger.info(f"✓✓✓ EARLY FALLBACK SUCCESS: Found join path: {path}")
                state["join_path"] = path
                return state
            else:
                logger.warning("EARLY FALLBACK: No path found, continuing with normal flow")
        
        # Try to find base table from retrieved context tables
        tables = state["retrieved_context"].get("tables", [])
        logger.info(f"Join planner - Retrieved {len(tables)} tables")
        logger.info(f"Join planner - Tables: {[t.get('entity_name', '') or t.get('table_name', '') for t in tables]}")
        
        if grain:
            # Find table matching grain
            for table in tables:
                table_name = table.get("entity_name", "").lower() or table.get("table_name", "").lower()
                table_short = table_name.split('.')[-1] if '.' in table_name else table_name
                if grain.lower() in table_short or table_short in grain.lower():
                    base_table = table_short
                    logger.info(f"Found base_table '{base_table}' from grain '{grain}'")
                    break
            
            # If not found, try direct lookup
            if not base_table:
                base_table = grain.lower()
                logger.info(f"Using grain '{grain}' directly as base_table")
        
        # Find metric table from retrieved context
        # The metric table is where the metric columns (extendedprice, discount) are located
        metrics = state["retrieved_context"].get("metrics", [])
        metric_table = None
        
        # First try to get from metrics registry
        if metrics:
            metric_info = metrics[0].get("metadata", {})
            metric_table = metric_info.get("base_table")
        
        # If no metric found, infer from intent metric formula
        # Check which table contains the columns mentioned in the metric
        if not metric_table:
            intent_metric = state["intent"].get("metric", "").lower()
            logger.info(f"Inferring metric table from intent metric: {intent_metric}")
            tables = state["retrieved_context"].get("tables", [])
            
            # Look for tables that contain columns mentioned in the metric
            best_match = None
            best_match_score = 0
            
            for table in tables:
                table_name = table.get("entity_name", "").lower() or table.get("table_name", "").lower()
                table_short = table_name.split('.')[-1] if '.' in table_name else table_name
                columns = table.get("columns", [])
                
                logger.info(f"Checking table '{table_short}' with {len(columns)} columns")
                
                # Check if this table has columns mentioned in the metric
                metric_columns_found = []
                for col in columns:
                    col_name = col.get("column_name", "").lower()
                    if "extendedprice" in intent_metric and "extendedprice" in col_name:
                        metric_columns_found.append(col_name)
                    if "discount" in intent_metric and "discount" in col_name:
                        metric_columns_found.append(col_name)
                
                logger.info(f"Table '{table_short}' has {len(metric_columns_found)} matching columns: {metric_columns_found}")
                
                # Score: number of matching columns
                score = len(metric_columns_found)
                if score > best_match_score:
                    best_match = table_short
                    best_match_score = score
                    logger.info(f"New best match: '{best_match}' with score {score}")
            
            if best_match_score >= 2:  # Found both columns
                metric_table = best_match
                logger.info(f"Inferred metric table '{metric_table}' with score {best_match_score}")
            elif best_match_score >= 1:
                # At least one column found, use it
                metric_table = best_match
                logger.info(f"Inferred metric table '{metric_table}' with partial match (score {best_match_score})")
        
        join_path = []
        if base_table and metric_table and base_table != metric_table:
            # Find join path from base_table to metric_table
            # Note: find_join_path returns path from first arg to second arg
            path = self.join_graph.find_join_path(base_table, metric_table)
            if path:
                join_path = path
                logger.info(f"Found join path from {base_table} to {metric_table}: {path}")
            else:
                # Try reverse path
                path = self.join_graph.find_join_path(metric_table, base_table)
                if path:
                    # Reverse the path
                    join_path = [(t2, t1) for t1, t2 in reversed(path)]
                    logger.info(f"Found reverse join path: {join_path}")
        elif not base_table and metric_table:
            # If no grain specified, use metric table as base
            base_table = metric_table
        elif base_table and not metric_table:
            # If we have base table but no metric table, check if metric columns are in retrieved tables
            intent_metric = state["intent"].get("metric", "").lower()
            tables = state["retrieved_context"].get("tables", [])
            for table in tables:
                table_name = table.get("entity_name", "").lower() or table.get("table_name", "").lower()
                table_short = table_name.split('.')[-1] if '.' in table_name else table_name
                columns = table.get("columns", [])
                
                # Check if this table has the metric columns
                has_extendedprice = any("extendedprice" in col.get("column_name", "").lower() for col in columns)
                has_discount = any("discount" in col.get("column_name", "").lower() for col in columns)
                
                if has_extendedprice and has_discount:
                    metric_table = table_short
                    # Find path from base to metric table
                    path = self.join_graph.find_join_path(base_table, metric_table)
                    if path:
                        join_path = path
                        logger.info(f"Inferred metric table '{metric_table}' and found path: {path}")
                    break
        
        # CRITICAL FALLBACK: If we have customer grain and metric mentions extendedprice/discount, 
        # directly find path from customer to lineitem using join graph
        if not join_path:
            grain_lower = grain.lower() if grain else ""
            base_table_lower = base_table.lower() if base_table else ""
            
            logger.info(f"FALLBACK CHECK: base_table={base_table}, grain={grain}, intent_metric={intent_metric}")
            logger.info(f"FALLBACK CHECK: join_path is empty: {not join_path}")
            
            # If customer grain and metric has extendedprice/discount, find path directly
            is_customer_grain = grain_lower == "customer" or base_table_lower == "customer"
            has_metric_columns = "extendedprice" in intent_metric or "discount" in intent_metric
            
            logger.info(f"FALLBACK CONDITIONS: is_customer_grain={is_customer_grain}, has_metric_columns={has_metric_columns}")
            
            if is_customer_grain and has_metric_columns:
                logger.info("=" * 60)
                logger.info("FALLBACK TRIGGERED: Finding path from customer to lineitem")
                logger.info("=" * 60)
                path = self.join_graph.find_join_path("customer", "lineitem")
                logger.info(f"Join graph returned path: {path}")
                if path:
                    join_path = path
                    metric_table = "lineitem"
                    if not base_table:
                        base_table = "customer"
                    logger.info("=" * 60)
                    logger.info(f"✓✓✓ FALLBACK SUCCESS: Found join path: {path}")
                    logger.info(f"✓✓✓ Set base_table={base_table}, metric_table={metric_table}")
                    logger.info("=" * 60)
                else:
                    logger.error("FALLBACK FAILED: No path found from customer to lineitem")
            else:
                logger.info(f"FALLBACK NOT TRIGGERED: is_customer_grain={is_customer_grain}, has_metric_columns={has_metric_columns}")
        
        # CRITICAL: Ensure join_path is set even if empty (for debugging)
        logger.info("=" * 60)
        logger.info(f"JOIN PLANNER NODE COMPLETE")
        logger.info(f"  Base table: {base_table}")
        logger.info(f"  Metric table: {metric_table}")
        logger.info(f"  Final join_path: {join_path}")
        logger.info(f"  Join path length: {len(join_path)}")
        logger.info("=" * 60)
        
        state["join_path"] = join_path
        return state
    
    def _query_plan_node(self, state: GraphState) -> GraphState:
        """Node 5: Build structured query plan"""
        logger.info("=" * 60)
        logger.info("QUERY PLAN NODE STARTING")
        logger.info("=" * 60)
        
        # Enhance join_path with actual join conditions from join graph
        enhanced_join_path = []
        join_path = state.get("join_path", [])
        
        logger.info(f"Query plan node - join_path from state: {join_path}")
        logger.info(f"Query plan node - join_path type: {type(join_path)}, length: {len(join_path) if join_path else 0}")
        
        for t1, t2 in join_path:
            join_condition = self.join_graph.get_join_condition(t1, t2)
            if join_condition:
                enhanced_join_path.append({
                    'from_table': t1,
                    'to_table': t2,
                    'condition': join_condition.get('condition', ''),
                    'type': join_condition.get('type', 'LEFT')
                })
                logger.info(f"Added join: {t1} -> {t2} with condition: {join_condition.get('condition', '')}")
            else:
                logger.warning(f"No join condition found for {t1} -> {t2}")
                # Fallback if no join condition found
                enhanced_join_path.append({
                    'from_table': t1,
                    'to_table': t2,
                    'condition': f"{t1}.id = {t2}.{t1}_id",  # Generic fallback
                    'type': 'LEFT'
                })
        
        plan = self.query_planner.build_plan(
            state["intent"],
            state["retrieved_context"],
            join_path,
            join_graph=self.join_graph
        )
        
        # CRITICAL: Always add join_path to plan, even if empty
        join_path = state.get("join_path", [])
        logger.info("=" * 60)
        logger.info(f"QUERY PLAN NODE - join_path from state: {join_path}")
        logger.info(f"QUERY PLAN NODE - join_path type: {type(join_path)}, length: {len(join_path) if join_path else 0}")
        logger.info(f"QUERY PLAN NODE - enhanced_join_path: {enhanced_join_path}")
        logger.info("=" * 60)
        
        # Add enhanced join path to plan - CRITICAL: This must include ALL intermediate tables
        if enhanced_join_path:
            plan['joins'] = enhanced_join_path
            plan['join_path'] = join_path  # Also keep the raw path
            logger.info(f"✓ Query plan includes {len(enhanced_join_path)} enhanced joins: {[j.get('to_table') for j in enhanced_join_path]}")
        elif join_path:
            # If no enhanced joins but we have a raw path, add it
            plan['join_path'] = join_path
            logger.info(f"✓ Query plan includes raw join path: {join_path}")
            
            # Also create joins from raw path
            plan['joins'] = []
            for t1, t2 in join_path:
                join_condition = self.join_graph.get_join_condition(t1, t2)
                if join_condition:
                    plan['joins'].append({
                        'from_table': t1,
                        'to_table': t2,
                        'condition': join_condition.get('condition', ''),
                        'type': join_condition.get('type', 'LEFT')
                    })
                    logger.info(f"  Added join: {t1} -> {t2}: {join_condition.get('condition', '')}")
            logger.info(f"✓ Created {len(plan['joins'])} joins from raw path")
        else:
            logger.warning("✗ Query plan node - No join_path found in state!")
        
        logger.info(f"Final plan keys: {list(plan.keys())}")
        logger.info(f"Final plan join_path: {plan.get('join_path')}")
        logger.info(f"Final plan joins: {plan.get('joins')}")
        
        # Ensure base_table is set
        if not plan.get('base_table') and state.get("intent", {}).get("grain"):
            grain = state["intent"]["grain"]
            # Find base table from retrieved context
            tables = state["retrieved_context"].get("tables", [])
            for table in tables:
                table_name = table.get("entity_name", "").lower() or table.get("table_name", "").lower()
                if grain.lower() in table_name or table_name in grain.lower():
                    plan['base_table'] = table_name.split('.')[-1] if '.' in table_name else table_name
                    break
        
        logger.info(f"Query plan node - Final plan keys: {list(plan.keys())}")
        logger.info(f"Query plan node - Final plan join_path: {plan.get('join_path')}")
        
        state["query_plan"] = plan
        return state
    
    def _sql_generation_node(self, state: GraphState) -> GraphState:
        """Node 6: Generate SQL"""
        logger.info("Running SQL generation node")
        
        # Log join path for debugging
        join_path = state.get("join_path", [])
        query_plan = state.get("query_plan", {})
        logger.info(f"Join path for SQL generation: {join_path}")
        logger.info(f"Query plan joins: {query_plan.get('joins', [])}")
        
        sql = self.sql_generator.generate(state["query_plan"])
        
        # Fix column names to match actual metadata
        fixed_sql = self.column_fixer.fix_column_names(sql, state.get("query_plan"))
        state["generated_sql"] = fixed_sql
        return state
    
    def _sql_critic_node(self, state: GraphState) -> GraphState:
        """Node 7: Self-correct SQL"""
        logger.info("Running SQL critic node")
        critique = self.sql_critic.critique_and_fix(
            state["generated_sql"],
            state["query_plan"]
        )
        state["fixed_sql"] = critique.get("fixed_sql", state["generated_sql"])
        return state
    
    def _sql_ast_validator_node(self, state: GraphState) -> GraphState:
        """Node 8: Validate SQL AST"""
        logger.info("Running SQL AST validator node")
        validation = self.sql_validator.validate(state["fixed_sql"])
        
        if validation["fixed_sql"]:
            state["fixed_sql"] = validation["fixed_sql"]
        
        if validation["errors"]:
            state["validation_errors"] = validation["errors"]
        else:
            state["validation_errors"] = []
        
        return state
    
    def _trino_validation_node(self, state: GraphState) -> GraphState:
        """Node 9: Validate with Trino"""
        logger.info("Running Trino validation node")
        
        if not self.trino_validator:
            logger.warning("Trino validator not configured, skipping")
            state["final_sql"] = state["fixed_sql"]
            return state
        
        validation = self.trino_validator.validate(state["fixed_sql"])
        
        if not validation["valid"]:
            # Try to fix using LLM
            fix_result = self.trino_validator.validate_with_fix(
                state["fixed_sql"],
                validation["error"]
            )
            
            if fix_result["needs_fix"]:
                # Use SQL critic to fix based on error
                fixed = self.sql_critic.critique_and_fix(
                    state["fixed_sql"],
                    state["query_plan"]
                )
                state["fixed_sql"] = fixed.get("fixed_sql", state["fixed_sql"])
        
        state["final_sql"] = state["fixed_sql"]
        return state
    
    def _memory_node(self, state: GraphState) -> GraphState:
        """Node 10: Store successful query"""
        logger.info("Running memory node")
        
        # Store successful query (if valid)
        if state.get("final_sql") and not state.get("validation_errors"):
            self.memory_system.store_successful_query(
                state["user_query"],
                state["final_sql"]
            )
        
        return state
    
    def run(self, user_query: str) -> Dict[str, Any]:
        """
        Run the complete pipeline
        
        Args:
            user_query: Natural language query
            
        Returns:
            Dictionary with final SQL and all intermediate results
        """
        initial_state: GraphState = {
            "user_query": user_query,
            "intent": {},
            "resolution": {},
            "retrieved_context": {},
            "join_path": [],
            "query_plan": {},
            "generated_sql": "",
            "fixed_sql": "",
            "validation_errors": [],
            "final_sql": "",
            "available_metrics": []
        }
        
        try:
            final_state = self.graph.invoke(initial_state)
            
            return {
                "success": True,
                "sql": final_state.get("final_sql", ""),
                "intent": final_state.get("intent", {}),
                "resolution": final_state.get("resolution", {}),
                "query_plan": final_state.get("query_plan", {}),
                "validation_errors": final_state.get("validation_errors", [])
            }
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            return {
                "success": False,
                "error": str(e),
                "sql": ""
            }
