#!/usr/bin/env python3
"""
Query Builder API Endpoint

This module provides API endpoints for building SQL queries from natural language
using metadata and business rules from the semantic registry.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path to import test functions
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.metadata_provider import MetadataProvider

# Import test functions with fallback
try:
    from test_outstanding_daily_regeneration import (
        find_metric_by_query,
        find_dimensions_by_query,
        identify_required_joins,
        identify_required_filters,
        generate_sql_from_metadata,
        classify_query_intent,
        find_table_by_query,
    )
except ImportError as e:
    # Fallback implementations if test file doesn't exist
    def classify_query_intent(query: str) -> str:
        return "relational"
    
    def find_metric_by_query(registry: Dict[str, Any], query: str) -> Any:
        return None
    
    def find_dimensions_by_query(registry: Dict[str, Any], query: str, metric: Any, tables: Dict[str, Any]) -> list:
        return []
    
    def identify_required_joins(query: str, metric: Any, dimensions: list, filters: list, registry: Dict[str, Any], tables: Dict[str, Any]) -> list:
        return []
    
    def identify_required_filters(query: str, metric: Any, dimensions: list, registry: Dict[str, Any]) -> list:
        return []
    
    def generate_sql_from_metadata(query: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        return {"success": False, "error": "Metadata functions not available"}
    
    def find_table_by_query(tables: Dict[str, Any], query: str) -> Any:
        return None

def generate_sql_from_query(query: str, use_llm: bool = True, 
                            clarification_mode: bool = True) -> dict:
    """
    Generate SQL from natural language query using LLM with comprehensive context.
    
    Args:
        query: User query text
        use_llm: Whether to use LLM for generation
        clarification_mode: If True, ask clarifying questions for ambiguous queries
    
    Returns:
        Dictionary with SQL result or clarification questions
    """
    try:
        # Step 1: Check for clarification needs (if enabled)
        if clarification_mode:
            try:
                from backend.planning.clarification_agent import ClarificationAgent
                from backend.metadata_provider import MetadataProvider
                from backend.interfaces import LLMProvider
                
                metadata = MetadataProvider.load()
                
                # Try to get LLM provider if available
                llm_provider = None
                if use_llm:
                    try:
                        from backend.implementations.llm_provider import OpenAIProvider
                        llm_provider = OpenAIProvider()
                    except:
                        pass  # LLM not available, use rule-based questions
                
                clarification_agent = ClarificationAgent(
                    llm_provider=llm_provider,
                    metadata=metadata
                )
                
                # Analyze query for ambiguities
                clarification_result = clarification_agent.analyze_query(query, metadata=metadata)
                
                if clarification_result.needs_clarification:
                    # Return clarification response
                    return {
                        "success": False,
                        "needs_clarification": True,
                        "confidence": clarification_result.confidence,
                        "query": query,
                        "clarification": {
                            "message": f"I need a bit more information to understand your query. Please answer these {len(clarification_result.questions)} question(s):",
                            "questions": [q.to_dict() for q in clarification_result.questions]
                        },
                        "suggested_intent": clarification_result.suggested_intent
                    }
            except Exception as e:
                # If clarification check fails, log and continue
                import logging
                logging.warning(f"Clarification check failed: {e}, proceeding with normal flow")
        
        # Step 2: Try LLM-based generation first (with full context)
        if use_llm:
            try:
                from backend.llm_query_generator import generate_sql_with_llm
                result = generate_sql_with_llm(query, use_llm=True)
                if result.get("success"):
                    # Ensure reasoning_steps are included
                    if "reasoning_steps" not in result:
                        result["reasoning_steps"] = []
                    return result
                # Fall through to rule-based if LLM fails
            except Exception as e:
                # Fall back to rule-based if LLM not available
                import traceback
                import sys
                print(f"LLM generation failed, using fallback: {e}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                pass
        
        # Fallback: Rule-based generation
        # Load metadata
        metadata = MetadataProvider.load()
        registry = metadata["semantic_registry"]
        tables = metadata["tables"]
        
        if not registry or not tables:
            return {
                "success": False,
                "error": "Metadata not loaded. Please load prerequisites first.",
            }
        
        # Classify query intent
        intent = classify_query_intent(query)
        
        # Resolve metric (may be None for relational queries)
        metric = find_metric_by_query(registry, query)
        
        # For relational queries, metric can be None
        if intent == "relational" and not metric:
            # This is fine - relational queries don't need metrics
            pass
        elif intent == "metric" and not metric:
            return {
                "success": False,
                "error": f"Could not resolve metric from query: '{query}'",
                "suggestion": "Try mentioning 'principal outstanding', 'POS', or 'current POS'",
            }
        
        # Resolve dimensions
        dimensions = find_dimensions_by_query(registry, query, metric, tables)
        
        # Identify filters (now returns list of filter objects)
        filters = identify_required_filters(query, metric, dimensions, registry)
        
        # Identify joins (now takes query and filters)
        joins = identify_required_joins(query, metric, dimensions, filters, registry, tables)
        
        # Build intent dict for production-grade SQL builder
        base_table = metric.get("base_table") if metric else tables.get("tables", [{}])[0].get("name", "")
        if not base_table:
            # Try to find base table from dimensions or filters
            if dimensions:
                base_table = dimensions[0].get("base_table", "")
            elif filters:
                base_table = filters[0].get("table", "")
        
        intent_dict = {
            "query_type": intent,
            "base_table": base_table,
            "anchor_entity": base_table,
            "columns": [d.get("name") for d in dimensions] if dimensions else [],
            "joins": [
                {
                    "table": j.get("to_table", ""),
                    "type": "LEFT",
                    "on": j.get("on", ""),
                    "reason": f"Required join for {j.get('to_table', '')}"
                }
                for j in joins
            ],
            "filters": [
                {
                    "column": f.get("column", ""),
                    "table": f.get("table", base_table),
                    "operator": f.get("operator", "="),
                    "value": f.get("value"),
                    "reason": f.get("reason", "")
                }
                for f in filters
            ],
            "group_by": [d.get("name") for d in dimensions] if intent == "metric" and dimensions else [],
            "metric": {
                "name": metric.get("name"),
                "sql_expression": metric.get("sql_expression", "")
            } if metric else None
        }
        
        # Use production-grade SQL builder
        from backend.sql_builder import TableRelationshipResolver, IntentValidator, SQLBuilder
        
        # Enable learning by default - will ask user when join paths not found
        resolver = TableRelationshipResolver(metadata, enable_learning=True)
        validator = IntentValidator(resolver)
        
        # Validate and fix intent
        is_valid, errors, warnings = validator.validate(intent_dict)
        if not is_valid:
            fixed_intent, fix_confidence, fix_reasons = validator.fix_intent(intent_dict)
            if fix_confidence.value == "safe":
                intent_dict = fixed_intent
            elif fix_confidence.value == "ambiguous":
                intent_dict = fixed_intent
                warnings.append(f"Ambiguous fix applied: {', '.join(fix_reasons)}")
        
        # Build SQL
        builder = SQLBuilder(resolver)
        sql, explain_plan = builder.build(intent_dict, include_explain=True)
        
        result = {
            "success": True,
            "sql": sql,
            "metric": {
                "name": metric.get("name") if metric else None,
                "description": metric.get("description") if metric else None,
            } if metric else None,
            "dimensions": [
                {
                    "name": d.get("name"),
                    "description": d.get("description"),
                }
                for d in dimensions
            ],
            "joins": [
                {
                    "from_table": j.get("from_table"),
                    "to_table": j.get("to_table"),
                    "on": j.get("on"),
                }
                for j in joins
            ],
            "filters": filters,
            "method": "rule_based_with_production_builder"
        }
        
        if explain_plan:
            result["explain_plan"] = explain_plan
        
        if warnings:
            result["warnings"] = "\n".join([f"️  {w}" for w in warnings])
        
        # Add reasoning steps if available from LLM
        if "reasoning_steps" in locals() and reasoning_steps:
            result["reasoning_steps"] = reasoning_steps
        
        return result
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }

def load_prerequisites() -> dict:
    """Load all prerequisites (metadata)."""
    try:
        metadata = MetadataProvider.load()
        registry = metadata.get("semantic_registry", {})
        tables = metadata.get("tables", {})
        
        return {
            "success": True,
            "metadata": {
                "semantic_registry": registry,
                "tables": tables,
            },
            "loaded": {
                "metrics": len(registry.get("metrics", [])),
                "dimensions": len(registry.get("dimensions", [])),
                "tables": len(tables.get("tables", [])),
            },
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }

if __name__ == "__main__":
    # CLI mode for testing
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "load":
            result = load_prerequisites()
            print(json.dumps(result, indent=2))
        elif command == "generate":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "Query required"}))
                sys.exit(1)
            query = sys.argv[2]
            use_llm = len(sys.argv) > 3 and sys.argv[3] == "true"
            result = generate_sql_from_query(query, use_llm=use_llm)
            print(json.dumps(result, indent=2))
        else:
            print(json.dumps({"error": f"Unknown command: {command}"}))
    else:
        # Read from stdin (for HTTP POST)
        try:
            input_data = json.loads(sys.stdin.read())
            command = input_data.get("command")
            
            if command == "load":
                result = load_prerequisites()
            elif command == "generate":
                query = input_data.get("query")
                use_llm = input_data.get("use_llm", True)  # Default to True
                if not query:
                    result = {"error": "Query required"}
                else:
                    result = generate_sql_from_query(query, use_llm=use_llm)
            else:
                result = {"error": f"Unknown command: {command}"}
            
            print(json.dumps(result))
        except Exception as e:
            print(json.dumps({"error": str(e)}))

