"""
Rust SQL Generation Client

RISK #2 FIX: This is the ONLY way Python can get SQL.
Python sends intent, Rust generates SQL.

This client communicates with the Rust SQL generation API.
"""

from typing import Dict, Any, Optional, List
import logging
import json
import requests
import os

logger = logging.getLogger(__name__)


class RustSqlClient:
    """
    Client for Rust SQL generation API.
    
    RISK #2 FIX: Python NEVER generates SQL directly.
    All SQL generation goes through Rust.
    """
    
    def __init__(self, rust_api_url: Optional[str] = None):
        """
        Initialize Rust SQL client.
        
        Args:
            rust_api_url: URL of Rust SQL generation API
                         Defaults to RUST_SQL_API_URL env var or localhost
        """
        self.api_url = rust_api_url or os.getenv(
            'RUST_SQL_API_URL',
            'http://localhost:8080/api/sql/generate'
        )
    
    def generate_sql_from_intent(
        self,
        intent: str,
        entities: List[str],
        constraints: List[str],
        preferences: Optional[List[str]] = None,
        metric_name: Optional[str] = None,
        dimensions: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Generate SQL from intent.
        
        RISK #2 FIX: This is the ONLY way to get SQL.
        Python sends intent, Rust generates SQL.
        
        Args:
            intent: Natural language intent description
            entities: List of semantic entities (not table names)
            constraints: List of constraints (not SQL WHERE clauses)
            preferences: Optional preferences/hints
            metric_name: Optional metric name
            dimensions: Optional dimensions for grouping
            
        Returns:
            Dict with sql, logical_plan, tables_used, warnings, explanation
        """
        logger.info(f"Rust SQL Client: Generating SQL from intent: {intent}")
        
        # Build request payload
        payload = {
            "intent": intent,
            "entities": entities,
            "constraints": constraints,
            "preferences": preferences or [],
            "metric_name": metric_name,
            "dimensions": dimensions
        }
        
        try:
            # Call Rust API
            response = requests.post(
                self.api_url,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            
            logger.info(
                f"Rust SQL Client: Generated SQL for {len(result.get('tables_used', []))} tables"
            )
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Rust SQL Client: Failed to generate SQL: {e}")
            raise Exception(f"SQL generation failed: {e}")
    
    def generate_sql_from_intent_dict(self, intent_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate SQL from intent dictionary.
        
        Convenience method that accepts the intent format from planning.
        
        Args:
            intent_dict: Intent dictionary (from QueryIntent.to_dict())
            
        Returns:
            Dict with sql, logical_plan, tables_used, warnings, explanation
        """
        return self.generate_sql_from_intent(
            intent=intent_dict.get('intent', ''),
            entities=intent_dict.get('entities', []),
            constraints=intent_dict.get('constraints', []),
            preferences=intent_dict.get('preferences'),
            metric_name=intent_dict.get('metric_name'),
            dimensions=intent_dict.get('dimensions')
        )


# Singleton instance
_rust_sql_client: Optional[RustSqlClient] = None


def get_rust_sql_client() -> RustSqlClient:
    """Get singleton Rust SQL client instance."""
    global _rust_sql_client
    if _rust_sql_client is None:
        rust_api_url = os.getenv('RUST_SQL_API_URL')
        _rust_sql_client = RustSqlClient(rust_api_url)
    return _rust_sql_client

