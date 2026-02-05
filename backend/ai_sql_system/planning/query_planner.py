"""
Query Plan Builder - Create structured query plan from intent and context
"""
from typing import Dict, Any, Optional, List
import json
import os
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)


class QueryPlanner:
    """Builds structured query plan from intent and retrieved context"""
    
    def __init__(self, llm_client: Optional[OpenAI] = None):
        """
        Initialize query planner
        
        Args:
            llm_client: OpenAI client
        """
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OPENAI_API_KEY not set. LLM features will not work.")
            self.client = None
        else:
            self.client = llm_client or OpenAI(api_key=api_key)
        self.model = os.getenv('OPENAI_MODEL') or os.getenv('LLM_MODEL') or os.getenv('RCA_LLM_MODEL') or 'gpt-4'
    
    def build_plan(self, intent: Dict[str, Any], retrieved_context: Dict[str, Any],
                   join_path: List[tuple], join_graph: Optional[Any] = None) -> Dict[str, Any]:
        """
        Build structured query plan
        
        Args:
            intent: Extracted intent
            retrieved_context: Retrieved metadata context
            join_path: List of (table1, table2) tuples for joins
            
        Returns:
            Structured query plan:
            {
                'base_table': str,
                'joins': List[Dict],
                'metric_sql': str,
                'group_by': List[str],
                'filters': List[str],
                'time_filter': Optional[str]
            }
        """
        # Format join path for prompt with join conditions
        join_path_str = ""
        if join_path and join_graph:
            join_details = []
            for t1, t2 in join_path:
                join_condition = join_graph.get_join_condition(t1, t2)
                if join_condition:
                    join_details.append(f"{t1} -> {t2}: {join_condition.get('condition', '')}")
                else:
                    join_details.append(f"{t1} -> {t2}")
            join_path_str = "\n".join(join_details)
        elif join_path:
            join_path_str = "\n".join([f"{t1} -> {t2}" for t1, t2 in join_path])
        
        # Format context with detailed column information
        tables_info = []
        for table in retrieved_context.get('tables', []):
            table_name = table.get('entity_name', '') or table.get('table_name', '')
            # Extract short table name for display
            table_short = table_name.split('.')[-1] if '.' in table_name else table_name
            columns = table.get('columns', [])
            if columns:
                columns_list = [f"{col.get('column_name', '')}" for col in columns]
                columns_str = ", ".join(columns_list)
                tables_info.append(f"Table: {table_short} (full: {table_name})\n  Available Columns: {columns_str}")
            else:
                tables_info.append(f"Table: {table_short} (full: {table_name})\n  Available Columns: (none found)")
        
        tables_str = "\n".join(tables_info) if tables_info else "No tables retrieved"
        metrics_str = ", ".join([m.get('entity_name', '') or m.get('metric_name', '') 
                                for m in retrieved_context.get('metrics', [])])
        
        # Extract grain to help identify the right table and column
        grain = intent.get('grain', '')
        metric = intent.get('metric', '')
        
        # Create column mapping hints for common terms
        column_mapping_hints = []
        if "extendedprice" in metric.lower():
            # Find extendedprice column
            for table in retrieved_context.get('tables', []):
                for col in table.get('columns', []):
                    col_name = col.get('column_name', '').lower()
                    if 'extendedprice' in col_name or 'extended' in col_name:
                        column_mapping_hints.append(f"User term 'extendedprice' maps to: {table.get('entity_name', '')}.{col.get('column_name', '')}")
        
        if "discount" in metric.lower():
            # Find discount column
            for table in retrieved_context.get('tables', []):
                for col in table.get('columns', []):
                    col_name = col.get('column_name', '').lower()
                    if 'discount' in col_name:
                        column_mapping_hints.append(f"User term 'discount' maps to: {table.get('entity_name', '')}.{col.get('column_name', '')}")
        
        if "customer" in grain.lower():
            # Find customer key column
            for table in retrieved_context.get('tables', []):
                if 'customer' in table.get('entity_name', '').lower():
                    for col in table.get('columns', []):
                        col_name = col.get('column_name', '').lower()
                        if 'custkey' in col_name or ('key' in col_name and 'primary' in col.get('description', '').lower()):
                            column_mapping_hints.append(f"Customer grain key: {table.get('entity_name', '')}.{col.get('column_name', '')}")
        
        mapping_hints_str = "\n".join(column_mapping_hints) if column_mapping_hints else "No specific mappings found"
        
        prompt = f"""You are a query planner.

Using:
- user intent
- retrieved tables/columns (USE EXACT COLUMN NAMES FROM BELOW)
- join path

Create a structured query plan.

CRITICAL RULES:
1. Use ONLY the exact column names provided in the tables section below
2. Do NOT invent or guess column names
3. Map user terms to actual column names using the hints below
4. Use exact table names and column names as shown below

COLUMN MAPPING HINTS:
{mapping_hints_str}

Return JSON:
{{
  "base_table": "exact_table_name",
  "joins": [
    {{"table": "exact_table_name", "type": "LEFT", "on": "table1.exact_column_name = table2.exact_column_name"}}
  ],
  "metric_sql": "SUM(table.exact_column_name * (1 - table.exact_column_name))",
  "group_by": ["table.exact_column_name"],
  "filters": ["..."],
  "time_filter": "..."
}}

Intent: {json.dumps(intent)}
Grain: {grain}
Metric Formula: {metric}

Retrieved Tables and Columns:
{tables_str}

Retrieved Metrics: {metrics_str}
Join Path: {join_path_str}

REMEMBER: Every column name must match exactly what is listed above. Use the mapping hints to translate user terms."""
        
        if not self.client:
            logger.error("OpenAI client not initialized. Set OPENAI_API_KEY environment variable.")
            return {
                'base_table': None,
                'joins': [],
                'metric_sql': '',
                'group_by': [],
                'filters': [],
                'time_filter': None
            }
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a query planner. Return only valid JSON. Use exact table/column names from context."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            
            content = response.choices[0].message.content.strip()
            
            # Clean JSON
            if content.startswith('```'):
                content = content.split('```')[1]
                if content.startswith('json'):
                    content = content[4:]
                content = content.strip()
            
            plan = json.loads(content)
            logger.info(f"Built query plan: base_table={plan.get('base_table')}")
            return plan
            
        except Exception as e:
            logger.error(f"Error building query plan: {e}")
            # Return minimal plan
            return {
                'base_table': None,
                'joins': [],
                'metric_sql': '',
                'group_by': [],
                'filters': [],
                'time_filter': None
            }
