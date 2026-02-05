"""
Intent Engine - Extract metric, grain, filters, time from user query
"""
from typing import Dict, Any, Optional
import json
import os
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)


class IntentEngine:
    """Extracts structured intent from natural language queries"""
    
    def __init__(self, llm_client: Optional[OpenAI] = None):
        """
        Initialize intent engine
        
        Args:
            llm_client: OpenAI client (defaults to env var)
        """
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OPENAI_API_KEY not set. LLM features will not work.")
            self.client = None
        else:
            self.client = llm_client or OpenAI(api_key=api_key)
        # Check multiple env var names for model
        self.model = os.getenv('OPENAI_MODEL') or os.getenv('LLM_MODEL') or os.getenv('RCA_LLM_MODEL') or 'gpt-4'
    
    def extract_intent(self, user_query: str) -> Dict[str, Any]:
        """
        Extract intent from user query
        
        Args:
            user_query: Natural language query
            
        Returns:
            Dictionary with extracted intent:
            {
                'metric': str,
                'grain': str,
                'filters': List[str],
                'time_range': Optional[str],
                'top_n': Optional[int]
            }
        """
        prompt = f"""You are a data analytics intent parser.

Extract from the query:
- metric (what is being measured)
- grain/entity (what level of aggregation: customer, order, product, etc.)
- filters (any constraints or conditions)
- time range (if mentioned: last_month, last_week, this_year, etc.)
- ranking (top_n if mentioned)

Return JSON only. Do not include any other text.

Query: {user_query}

Return JSON:
{{
  "metric": "...",
  "grain": "...",
  "filters": [],
  "time_range": null,
  "top_n": null
}}"""
        
        if not self.client:
            logger.error("OpenAI client not initialized. Set OPENAI_API_KEY environment variable.")
            return {
                'metric': None,
                'grain': None,
                'filters': [],
                'time_range': None,
                'top_n': None
            }
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a precise data analytics intent parser. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            content = response.choices[0].message.content.strip()
            
            # Clean JSON (remove markdown code fences if present)
            if content.startswith('```'):
                content = content.split('```')[1]
                if content.startswith('json'):
                    content = content[4:]
                content = content.strip()
            
            intent = json.loads(content)
            logger.info(f"Extracted intent: {intent}")
            return intent
            
        except Exception as e:
            logger.error(f"Error extracting intent: {e}")
            # Return minimal intent on error
            return {
                'metric': None,
                'grain': None,
                'filters': [],
                'time_range': None,
                'top_n': None
            }
