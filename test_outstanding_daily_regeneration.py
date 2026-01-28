#!/usr/bin/env python3
"""
Mock implementation for query generation testing
Provides the functions needed by query_regeneration_api.py
"""

import json
import os
from typing import Dict, List, Any, Optional


def load_metadata() -> Dict[str, Any]:
    """Load metadata from the metadata directory."""
    try:
        metadata_dir = os.path.join(os.path.dirname(__file__), 'metadata')
        
        # Try to load semantic registry (check for both ad_hoc and regular versions)
        semantic_registry = None
        for filename in ['semantic_registry.json', 'ad_hoc_semantic_registry.json']:
            semantic_registry_path = os.path.join(metadata_dir, filename)
            if os.path.exists(semantic_registry_path):
                with open(semantic_registry_path, 'r') as f:
                    semantic_registry = json.load(f)
                break
        
        # Try to load tables (check for both ad_hoc and regular versions)
        tables = None
        for filename in ['tables.json', 'ad_hoc_tables.json']:
            tables_path = os.path.join(metadata_dir, filename)
            if os.path.exists(tables_path):
                with open(tables_path, 'r') as f:
                    tables = json.load(f)
                break
        
        if semantic_registry is None or tables is None:
            raise FileNotFoundError("Metadata files not found. Please ensure semantic_registry.json and tables.json exist in metadata/ directory.")
        
        return {
            "semantic_registry": semantic_registry,
            "tables": tables
        }
    except Exception as e:
        print(f"Warning: Could not load metadata: {e}")
        return {
            "semantic_registry": {"metrics": [], "dimensions": []},
            "tables": {"tables": []}
        }


def classify_query_intent(query: str) -> str:
    """Classify query intent as 'metric' or 'relational'."""
    query_lower = query.lower()
    
    # Metric indicators
    metric_keywords = ['sum', 'count', 'average', 'total', 'revenue', 'pos', 'outstanding', 'balance']
    
    # Relational indicators  
    relational_keywords = ['show', 'list', 'get', 'find', 'all', 'customers', 'orders', 'users']
    
    metric_score = sum(1 for keyword in metric_keywords if keyword in query_lower)
    relational_score = sum(1 for keyword in relational_keywords if keyword in query_lower)
    
    return "metric" if metric_score > relational_score else "relational"


def find_metric_by_query(registry: Dict[str, Any], query: str) -> Optional[Dict[str, Any]]:
    """Find the best matching metric for a query."""
    query_lower = query.lower()
    metrics = registry.get("metrics", [])
    
    for metric in metrics:
        name = metric.get("name", "").lower()
        description = metric.get("description", "").lower()
        
        # Simple keyword matching
        if any(keyword in query_lower for keyword in [name, "pos", "outstanding", "balance", "revenue"]):
            return metric
    
    return None


def find_dimensions_by_query(registry: Dict[str, Any], query: str, metric: Optional[Dict[str, Any]], tables: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Find dimensions relevant to the query."""
    query_lower = query.lower()
    dimensions = registry.get("dimensions", [])
    found_dimensions = []
    
    # Keywords to dimension mapping
    dimension_keywords = {
        "region": ["region", "area", "location"],
        "order_type": ["type", "category", "product", "khatabook", "bank", "digital"],
        "product_group": ["product", "group", "category"],
        "customer": ["customer", "user", "client"],
        "date": ["date", "time", "month", "year", "daily"]
    }
    
    for dimension in dimensions:
        dim_name = dimension.get("name", "").lower()
        
        # Check if dimension keywords are in query
        keywords = dimension_keywords.get(dim_name, [dim_name])
        if any(keyword in query_lower for keyword in keywords):
            found_dimensions.append(dimension)
    
    return found_dimensions


def identify_required_filters(query: str, metric: Optional[Dict[str, Any]], dimensions: List[Dict[str, Any]], registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Identify required filters based on query."""
    filters = []
    query_lower = query.lower()
    
    # Khatabook filter
    if "khatabook" in query_lower:
        filters.append({
            "column": "order_type",
            "table": "kb_adh433",
            "operator": "=",
            "value": "khatabook",
            "reason": "Query mentions khatabook"
        })
    
    # Bank filter
    if "bank" in query_lower:
        filters.append({
            "column": "order_type", 
            "table": "nondigital_adh433_v1",
            "operator": "=",
            "value": "bank",
            "reason": "Query mentions bank"
        })
    
    # Time filters
    if any(time_word in query_lower for time_word in ["last month", "current month", "this month"]):
        filters.append({
            "column": "da_date",
            "table": "outstanding_daily",
            "operator": ">=",
            "value": "2024-02-01",
            "reason": "Time range filter"
        })
    
    return filters


def identify_required_joins(query: str, metric: Optional[Dict[str, Any]], dimensions: List[Dict[str, Any]], filters: List[Dict[str, Any]], registry: Dict[str, Any], tables: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Identify required joins."""
    joins = []
    
    # If we have filters or dimensions from different tables, we need joins
    table_names = set()
    
    if metric:
        base_table = metric.get("base_table")
        if base_table and base_table != "varies_by_product":
            table_names.add(base_table)
    
    for dim in dimensions:
        dim_table = dim.get("base_table")
        if dim_table and dim_table != "varies_by_product":
            table_names.add(dim_table)
    
    for filter_obj in filters:
        filter_table = filter_obj.get("table")
        if filter_table:
            table_names.add(filter_table)
    
    # Simple join logic - if we have multiple tables, create joins
    table_list = list(table_names)
    for i in range(len(table_list) - 1):
        joins.append({
            "from_table": table_list[i],
            "to_table": table_list[i + 1],
            "on": f"{table_list[i]}.id = {table_list[i + 1]}.id",  # Generic join condition
        })
    
    return joins


def find_table_by_query(tables: Dict[str, Any], query: str) -> Optional[Dict[str, Any]]:
    """Find the best matching table for a query."""
    query_lower = query.lower()
    table_list = tables.get("tables", [])
    
    # Table name to keywords mapping
    table_keywords = {
        "outstanding_daily": ["outstanding", "daily", "pos"],
        "kb_adh433": ["khatabook", "kb"],
        "da_adh433": ["da", "digital"],
        "nondigital_adh433_v1": ["bank", "nondigital"]
    }
    
    for table in table_list:
        table_name = table.get("name", "").lower()
        keywords = table_keywords.get(table_name, [table_name])
        
        if any(keyword in query_lower for keyword in keywords):
            return table
    
    # Default to first table if no match
    return table_list[0] if table_list else None


def generate_sql_from_metadata(query: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Generate SQL from metadata (simplified version)."""
    try:
        registry = metadata.get("semantic_registry", {})
        tables = metadata.get("tables", {})
        
        # Classify intent
        intent = classify_query_intent(query)
        
        # Find components
        metric = find_metric_by_query(registry, query) if intent == "metric" else None
        dimensions = find_dimensions_by_query(registry, query, metric, tables)
        filters = identify_required_filters(query, metric, dimensions, registry)
        joins = identify_required_joins(query, metric, dimensions, filters, registry, tables)
        
        # Build basic SQL
        if metric:
            select_clause = f"SELECT {metric.get('sql_expression', 'COUNT(*)')} as {metric.get('name', 'metric')}"
        else:
            select_clause = "SELECT *"
        
        # Add dimensions to SELECT
        if dimensions:
            dim_cols = [f"{dim.get('sql_expression', dim.get('name'))}" for dim in dimensions]
            if metric:
                select_clause += ", " + ", ".join(dim_cols)
            else:
                select_clause = "SELECT " + ", ".join(dim_cols)
        
        # FROM clause
        base_table = "outstanding_daily"  # Default table
        if metric and metric.get("base_table") != "varies_by_product":
            base_table = metric.get("base_table")
        elif dimensions and dimensions[0].get("base_table") != "varies_by_product":
            base_table = dimensions[0].get("base_table")
        
        from_clause = f"FROM {base_table}"
        
        # WHERE clause
        where_conditions = []
        for filter_obj in filters:
            condition = f"{filter_obj.get('column')} {filter_obj.get('operator')} '{filter_obj.get('value')}'"
            where_conditions.append(condition)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # GROUP BY clause
        group_by_clause = ""
        if metric and dimensions:
            group_cols = [dim.get('name') for dim in dimensions]
            group_by_clause = f"GROUP BY {', '.join(group_cols)}"
        
        # Combine SQL
        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        
        sql = " ".join(sql_parts)
        
        return {
            "success": True,
            "sql": sql,
            "intent": intent,
            "metric": metric,
            "dimensions": dimensions,
            "filters": filters,
            "joins": joins,
            "method": "rule_based_metadata"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


if __name__ == "__main__":
    # Test the functions
    metadata = load_metadata()
    print("Metadata loaded successfully")
    print(f"Metrics: {len(metadata.get('semantic_registry', {}).get('metrics', []))}")
    print(f"Dimensions: {len(metadata.get('semantic_registry', {}).get('dimensions', []))}")
    
    # Test query
    test_query = "show me khatabook customers"
    result = generate_sql_from_metadata(test_query, metadata)
    print(f"\nTest query: {test_query}")
    print(f"Result: {result}")
