"""
Resolution Engine - Classify query and decide system behavior
"""
from typing import Dict, Any, Optional
import json
import os
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)


class ResolutionEngine:
    """Classifies queries and determines resolution strategy"""
    
    RESOLUTION_TYPES = {
        'EXACT_MATCH': 'Metric exists exactly as requested',
        'DERIVABLE': 'Can be computed from other columns/metrics',
        'CLOSE_MATCH': 'Similar metric exists',
        'AMBIGUOUS': 'Needs clarification',
        'IMPOSSIBLE': 'No data available'
    }
    
    def __init__(self, llm_client: Optional[OpenAI] = None):
        """
        Initialize resolution engine
        
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
    
    def resolve(self, user_query: str, intent: Dict[str, Any], 
                available_metrics: Optional[list] = None) -> Dict[str, Any]:
        """
        Classify query and determine resolution strategy
        
        Args:
            user_query: Original user query
            intent: Extracted intent from IntentEngine
            available_metrics: List of available metrics (optional)
            
        Returns:
            Dictionary with resolution:
            {
                'type': 'EXACT_MATCH' | 'DERIVABLE' | 'CLOSE_MATCH' | 'AMBIGUOUS' | 'IMPOSSIBLE',
                'reason': str,
                'confidence': float (0-1)
            }
        """
        metrics_context = ""
        if available_metrics:
            metrics_context = f"\nAvailable metrics: {', '.join(available_metrics)}"
        
        prompt = f"""You are a senior data analyst.

Given user query and extracted intent, classify the query:

1. EXACT_MATCH - Metric exists exactly as requested
2. DERIVABLE - Can compute from other columns/metrics (e.g., profit = revenue - cost)
3. CLOSE_MATCH - Similar metric exists
4. AMBIGUOUS - Needs clarification (multiple interpretations possible)
5. IMPOSSIBLE - No data available

Return JSON:
{{
  "type": "...",
  "reason": "...",
  "confidence": 0.0-1.0
}}

Query: {user_query}
Intent: {json.dumps(intent)}{metrics_context}"""
        
        if not self.client:
            logger.error("OpenAI client not initialized. Set OPENAI_API_KEY environment variable.")
            return {
                'type': 'AMBIGUOUS',
                'reason': 'OpenAI API key not configured',
                'confidence': 0.0
            }
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a senior data analyst. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=300
            )
            
            content = response.choices[0].message.content.strip()
            
            # Clean JSON
            if content.startswith('```'):
                content = content.split('```')[1]
                if content.startswith('json'):
                    content = content[4:]
                content = content.strip()
            
            resolution = json.loads(content)
            
            # Validate resolution type
            if resolution['type'] not in self.RESOLUTION_TYPES:
                logger.warning(f"Invalid resolution type: {resolution['type']}, defaulting to AMBIGUOUS")
                resolution['type'] = 'AMBIGUOUS'
            
            logger.info(f"Resolution: {resolution['type']} (confidence: {resolution.get('confidence', 0)})")
            return resolution
            
        except Exception as e:
            logger.error(f"Error in resolution: {e}")
            return {
                'type': 'AMBIGUOUS',
                'reason': f'Error during resolution: {str(e)}',
                'confidence': 0.0
            }
    
    def is_impossible(self, resolution: Dict[str, Any]) -> bool:
        """Check if resolution is IMPOSSIBLE"""
        return resolution.get('type') == 'IMPOSSIBLE'
