"""
SQL Critic - Self-correct SQL in second LLM pass
"""
from typing import Dict, Any, Optional
import os
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)


class SQLCritic:
    """Reviews and fixes SQL queries"""
    
    def __init__(self, llm_client: Optional[OpenAI] = None):
        """
        Initialize SQL critic
        
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
    
    def critique_and_fix(self, sql: str, query_plan: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Review SQL and fix issues
        
        Args:
            sql: Generated SQL to review
            query_plan: Original query plan (optional, for context)
            
        Returns:
            Dictionary with:
            {
                'fixed_sql': str,
                'issues_found': List[str],
                'fixes_applied': List[str]
            }
        """
        import json
        
        plan_context = ""
        if query_plan:
            plan_context = f"\n\nOriginal query plan:\n{json.dumps(query_plan, indent=2)}"
        
        prompt = f"""You are a strict SQL reviewer.

Check this SQL for:
- invalid joins
- missing GROUP BY
- wrong aggregation
- invalid columns
- syntax errors

Fix issues and return corrected SQL.

SQL:
{sql}{plan_context}

Return JSON:
{{
  "fixed_sql": "...",
  "issues_found": ["..."],
  "fixes_applied": ["..."]
}}"""
        
        if not self.client:
            logger.error("OpenAI client not initialized. Set OPENAI_API_KEY environment variable.")
            return {
                'fixed_sql': sql,  # Return original on error
                'issues_found': [],
                'fixes_applied': []
            }
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a strict SQL reviewer. Return only valid JSON with fixed SQL."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000
            )
            
            content = response.choices[0].message.content.strip()
            
            # Clean JSON
            if content.startswith('```'):
                content = content.split('```')[1]
                if content.startswith('json'):
                    content = content[4:]
                content = content.strip()
            
            result = json.loads(content)
            
            logger.info(f"SQL critic found {len(result.get('issues_found', []))} issues")
            return result
            
        except Exception as e:
            logger.error(f"Error in SQL critic: {e}")
            return {
                'fixed_sql': sql,  # Return original on error
                'issues_found': [],
                'fixes_applied': []
            }
