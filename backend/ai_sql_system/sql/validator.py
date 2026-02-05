"""
SQL AST Validator - Validate SQL using sqlglot
"""
import sqlglot
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class SQLValidator:
    """Validates SQL syntax and structure using sqlglot"""
    
    def __init__(self):
        """Initialize SQL validator"""
        pass
    
    def validate(self, sql: str, dialect: str = 'trino') -> Dict[str, Any]:
        """
        Validate SQL syntax and structure
        
        Args:
            sql: SQL query to validate
            dialect: SQL dialect (default: 'trino')
            
        Returns:
            Dictionary with validation results:
            {
                'valid': bool,
                'errors': List[str],
                'warnings': List[str],
                'fixed_sql': Optional[str]
            }
        """
        result = {
            'valid': False,
            'errors': [],
            'warnings': [],
            'fixed_sql': None
        }
        
        try:
            # Parse SQL
            parsed = sqlglot.parse_one(sql, dialect=dialect)
            
            # Check for parse errors
            if parsed.errors:
                result['errors'].extend([str(e) for e in parsed.errors])
                return result
            
            # Try to fix common issues
            try:
                fixed = sqlglot.transpile(sql, dialect=dialect, pretty=True)[0]
                if fixed != sql:
                    result['warnings'].append("SQL was auto-corrected")
                    result['fixed_sql'] = fixed
            except Exception as e:
                result['warnings'].append(f"Could not auto-fix: {str(e)}")
            
            # Basic structure checks
            if not self._has_select(parsed):
                result['errors'].append("Query must have SELECT clause")
            
            if not self._has_from(parsed):
                result['errors'].append("Query must have FROM clause")
            
            # If no errors, SQL is valid
            if not result['errors']:
                result['valid'] = True
            
            logger.info(f"SQL validation: valid={result['valid']}, errors={len(result['errors'])}")
            return result
            
        except sqlglot.errors.ParseError as e:
            result['errors'].append(f"Parse error: {str(e)}")
            logger.error(f"SQL parse error: {e}")
            return result
        except Exception as e:
            result['errors'].append(f"Validation error: {str(e)}")
            logger.error(f"SQL validation error: {e}")
            return result
    
    def _has_select(self, parsed) -> bool:
        """Check if parsed SQL has SELECT"""
        try:
            return parsed.kind == 'select' or hasattr(parsed, 'expressions')
        except:
            return False
    
    def _has_from(self, parsed) -> bool:
        """Check if parsed SQL has FROM"""
        try:
            return hasattr(parsed, 'from') and parsed.from_ is not None
        except:
            return False
    
    def auto_fix(self, sql: str, dialect: str = 'trino') -> Optional[str]:
        """
        Attempt to auto-fix SQL issues
        
        Args:
            sql: SQL to fix
            dialect: SQL dialect
            
        Returns:
            Fixed SQL or None if cannot fix
        """
        validation = self.validate(sql, dialect)
        
        if validation['fixed_sql']:
            return validation['fixed_sql']
        
        if validation['valid']:
            return sql
        
        return None
