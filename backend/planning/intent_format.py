"""
Intent Format - Python Planning Output

RISK #2 FIX: Python planning outputs INTENT ONLY.
No SQL. No joins. No table names (only entities).

This format is consumed by Rust to generate logical plans and SQL.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import json


@dataclass
class QueryIntent:
    """
    Query intent - what the user wants, not how to execute it.
    
    RISK #2 FIX: This is the ONLY format Python planning should output.
    Rust owns the translation from intent → logical plan → SQL.
    """
    
    # Core intent
    intent: str  # Natural language description: "top customers by revenue"
    
    # Entities (not table names - semantic concepts)
    entities: List[str]  # e.g., ["customer", "order"]
    
    # Constraints (business rules, not SQL WHERE clauses)
    constraints: List[str]  # e.g., ["last 30 days", "active customers only"]
    
    # Preferences (hints, not requirements)
    preferences: Optional[List[str]] = None  # e.g., ["left join", "prefer index"]
    
    # Optional: Metric name if this is a metric query
    metric_name: Optional[str] = None  # e.g., "revenue"
    
    # Optional: Dimensions for grouping
    dimensions: Optional[List[str]] = None  # e.g., ["customer_id", "region"]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


def validate_intent(intent: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate that intent follows the intent-only format.
    
    RISK #2 FIX: Rejects any intent that contains SQL or table names.
    
    Args:
        intent: Intent dictionary to validate
        
    Returns:
        (is_valid, error_message)
    """
    # Must have core fields
    if 'intent' not in intent:
        return False, "Missing required field: intent"
    
    if 'entities' not in intent:
        return False, "Missing required field: entities"
    
    if 'constraints' not in intent:
        return False, "Missing required field: constraints"
    
    # Must NOT have SQL-related fields
    forbidden_fields = [
        'sql', 'query', 'table_name', 'table_names', 'joins',
        'join_type', 'where', 'group_by', 'order_by', 'select'
    ]
    
    for field in forbidden_fields:
        if field in intent:
            return False, f"Forbidden field '{field}' found. Python may not generate SQL."
    
    # Entities must be semantic concepts, not table names
    entities = intent.get('entities', [])
    for entity in entities:
        # Table names typically have underscores and are lowercase
        # This is a heuristic - could be improved
        if '_' in entity and entity.islower():
            return False, (
                f"Entity '{entity}' looks like a table name. "
                "Use semantic concepts instead (e.g., 'customer' not 'customers_table')."
            )
    
    return True, None


# Example valid intent
EXAMPLE_INTENT = QueryIntent(
    intent="top customers by revenue",
    entities=["customer", "order"],
    constraints=["last 30 days"],
    preferences=["left join"],
    metric_name="revenue",
    dimensions=["customer_id"]
)

