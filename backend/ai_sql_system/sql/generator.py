"""
SQL Generator - Convert query plan to Trino SQL
"""
from typing import Dict, Any, Optional
import os
from openai import OpenAI
import logging
from .deterministic_builder import DeterministicSQLBuilder

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generates Trino SQL from structured query plan"""
    
    def __init__(self, llm_client: Optional[OpenAI] = None):
        """
        Initialize SQL generator
        
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
        self.deterministic_builder = DeterministicSQLBuilder()
    
    def generate(self, query_plan: Dict[str, Any]) -> str:
        """
        Generate Trino SQL from query plan
        
        Args:
            query_plan: Structured query plan
            
        Returns:
            Generated SQL query string
        """
        import json
        
        # Extract joins from query plan
        joins = query_plan.get('joins', [])
        join_path = query_plan.get('join_path', [])
        base_table = query_plan.get('base_table', '')
        
        logger.info(f"SQL Generator - Query plan keys: {list(query_plan.keys())}")
        logger.info(f"SQL Generator - Query plan joins: {joins}")
        logger.info(f"SQL Generator - Join path: {join_path}")
        logger.info(f"SQL Generator - Base table: {base_table}")
        
        # If we have a join_path OR joins, use deterministic builder to ensure correct path
        has_join_info = (join_path and len(join_path) > 0) or (joins and len(joins) > 0)
        
        if has_join_info:
            logger.info(f"Using deterministic SQL builder - join_path: {join_path}, joins: {len(joins) if joins else 0}")
            try:
                deterministic_sql = self.deterministic_builder.build_sql(query_plan)
                if deterministic_sql and len(deterministic_sql.strip()) > 0:
                    logger.info(f"Deterministic SQL built successfully ({len(deterministic_sql)} chars)")
                    logger.info(f"Deterministic SQL preview: {deterministic_sql[:200]}")
                    return deterministic_sql
                else:
                    logger.warning("Deterministic builder returned empty SQL, falling back to LLM")
            except Exception as e:
                logger.error(f"Deterministic builder failed: {e}", exc_info=True)
                import traceback
                logger.error(traceback.format_exc())
        else:
            logger.info("No join_path or joins found in query plan, using LLM generator")
        
        # Build detailed join instructions with explicit table order
        joins_instructions = []
        all_tables_in_path = []
        
        if joins and isinstance(joins[0], dict):
            # Enhanced join format with conditions
            current_table = base_table
            for i, j in enumerate(joins):
                to_table = j.get('to_table', '')
                join_type = j.get('type', 'LEFT')
                condition = j.get('condition', '')
                
                # Extract table alias (first letter)
                alias = to_table[0] if to_table else ''
                
                joins_instructions.append(
                    f"Step {i+1}: {join_type} JOIN {to_table} AS {alias} ON {condition}"
                )
                all_tables_in_path.append(to_table)
                current_table = to_table
        elif join_path:
            # Use raw join path - build explicit join sequence
            current_table = base_table
            for i, (t1, t2) in enumerate(join_path):
                # Get join condition from query plan if available
                condition = ""
                if joins and isinstance(joins[0], dict):
                    for j in joins:
                        if j.get('from_table') == t1 and j.get('to_table') == t2:
                            condition = j.get('condition', '')
                            break
                
                if not condition:
                    # Fallback: construct condition from table names
                    condition = f"{t1[0]}.{t1}_id = {t2[0]}.{t1}_id"
                
                joins_instructions.append(
                    f"Step {i+1}: LEFT JOIN {t2} AS {t2[0]} ON {condition}"
                )
                all_tables_in_path.append(t2)
                current_table = t2
        
        joins_str = "\n".join(joins_instructions) if joins_instructions else "No joins specified"
        
        # Build explicit instruction about join order
        join_order_note = ""
        if join_path:
            join_order_note = f"\n\nCRITICAL JOIN ORDER (DO NOT SKIP ANY TABLES):\nFROM {base_table}\n" + "\n".join([
                f"THEN {join_type} JOIN {t2} AS {t2[0] if t2 else ''} ON {condition}"
                for i, (t1, t2) in enumerate(join_path)
                for join_type, condition in [('LEFT', joins[i].get('condition', '') if joins and i < len(joins) and isinstance(joins[0], dict) else f"{t1[0]}.{t1}_id = {t2[0]}.{t1}_id")]
            ])
        
        # Build explicit table list
        table_list = f"Tables in join path: {base_table} -> " + " -> ".join([t2 for _, t2 in join_path]) if join_path else f"Base table: {base_table}"
        
        prompt = f"""You are a senior analytics engineer.

Generate Trino SQL using ONLY this query plan.
CRITICAL RULES:
1. Use EXACT table names and column names from the query plan
2. Do NOT invent or guess column names
3. Use proper aggregation and GROUP BY
4. Use table aliases consistently (e.g., customer c, orders o, lineitem l)
5. MUST include ALL joins from the join path in the EXACT order specified - DO NOT SKIP INTERMEDIATE TABLES
6. If the join path is customer -> orders -> lineitem, you MUST join orders first, then lineitem
7. Use the EXACT join conditions provided
8. Return SQL only, no explanations

Query plan:
{json.dumps(query_plan, indent=2)}

{table_list}

Join Instructions (FOLLOW EXACTLY):
{joins_str}
{join_order_note}

IMPORTANT: 
- Every column name in your SQL must match exactly what is in the query plan above
- Include ALL tables in the join path in order - do not skip any intermediate tables
- If join path shows customer -> orders -> lineitem, your SQL MUST have: FROM customer JOIN orders ... JOIN lineitem ...
- Use the exact join conditions provided"""
        
        if not self.client:
            logger.error("OpenAI client not initialized. Set OPENAI_API_KEY environment variable.")
            return ""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a Trino SQL expert. Generate valid Trino SQL only. Do not invent columns or joins."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000
            )
            
            sql = response.choices[0].message.content.strip()
            
            # Remove markdown code fences if present
            if sql.startswith('```'):
                sql = sql.split('```')[1]
                if sql.startswith('sql'):
                    sql = sql[3:]
                sql = sql.strip()
            
            logger.info(f"Generated SQL ({len(sql)} chars)")
            return sql
            
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return ""
