"""
Trino Validator - Validate SQL queries against Trino
"""
from typing import Dict, Any, Optional
from .client import TrinoClient
import logging

logger = logging.getLogger(__name__)


class TrinoValidator:
    """Validates SQL queries using Trino EXPLAIN and test execution"""
    
    def __init__(self, trino_client: TrinoClient):
        """
        Initialize Trino validator
        
        Args:
            trino_client: TrinoClient instance
        """
        self.client = trino_client
    
    def validate(self, sql: str) -> Dict[str, Any]:
        """
        Validate SQL query using EXPLAIN and test execution
        
        Args:
            sql: SQL query to validate
            
        Returns:
            Dictionary with validation results:
            {
                'valid': bool,
                'explain_success': bool,
                'execution_success': bool,
                'error': Optional[str],
                'explain_output': Optional[str]
            }
        """
        result = {
            'valid': False,
            'explain_success': False,
            'execution_success': False,
            'error': None,
            'explain_output': None
        }
        
        # Step 1: Try EXPLAIN
        try:
            explain_output = self.client.explain_query(sql)
            result['explain_success'] = True
            result['explain_output'] = explain_output
            logger.info("EXPLAIN succeeded")
        except Exception as e:
            result['error'] = f"EXPLAIN failed: {str(e)}"
            logger.error(f"EXPLAIN validation failed: {e}")
            return result
        
        # Step 2: Try LIMIT 1 execution
        try:
            self.client.run_limit_query(sql, limit=1)
            result['execution_success'] = True
            logger.info("Test execution succeeded")
        except Exception as e:
            result['error'] = f"Execution failed: {str(e)}"
            logger.error(f"Test execution failed: {e}")
            return result
        
        result['valid'] = True
        return result
    
    def validate_with_fix(self, sql: str, error_message: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate SQL and return error message for LLM fixing
        
        Args:
            sql: SQL query to validate
            error_message: Optional error message from previous attempt
            
        Returns:
            Dictionary with validation results and error details for LLM
        """
        validation = self.validate(sql)
        
        if not validation['valid']:
            return {
                'needs_fix': True,
                'error': validation['error'],
                'sql': sql,
                'suggestion': f"SQL failed validation: {validation['error']}"
            }
        
        return {
            'needs_fix': False,
            'sql': sql
        }
