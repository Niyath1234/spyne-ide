#!/usr/bin/env python3
"""
Interactive Join Path Learner

When a join path cannot be found, this module:
1. Asks the user how to join the tables
2. Stores the learned join information
3. Uses it in future queries
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime


class JoinLearner:
    """Learns and stores join paths from user input."""
    
    def __init__(self, storage_path: Optional[str] = None):
        """
        Initialize the join learner.
        
        Args:
            storage_path: Path to JSON file storing learned joins (default: metadata/learned_joins.json)
        """
        if storage_path is None:
            storage_path = Path(__file__).parent.parent / "metadata" / "learned_joins.json"
        
        self.storage_path = Path(storage_path)
        self.learned_joins: Dict[str, List[Dict[str, Any]]] = {}
        self._load_learned_joins()
    
    def _load_learned_joins(self):
        """Load learned joins from storage."""
        if self.storage_path.exists():
            try:
                with open(self.storage_path, 'r') as f:
                    self.learned_joins = json.load(f)
            except Exception as e:
                print(f"️  Warning: Could not load learned joins: {e}")
                self.learned_joins = {}
        else:
            self.learned_joins = {}
    
    def _save_learned_joins(self):
        """Save learned joins to storage."""
        try:
            # Ensure directory exists
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.storage_path, 'w') as f:
                json.dump(self.learned_joins, f, indent=2)
        except Exception as e:
            print(f"️  Warning: Could not save learned joins: {e}")
    
    def _normalize_table_name(self, table: str) -> str:
        """Normalize table name for storage."""
        # Remove schema prefix if present
        if '.' in table:
            table = table.split('.')[-1]
        return table.lower().strip()
    
    def _create_join_key(self, from_table: str, to_table: str) -> str:
        """Create a canonical key for join storage."""
        from_norm = self._normalize_table_name(from_table)
        to_norm = self._normalize_table_name(to_table)
        # Sort for canonical form
        return tuple(sorted([from_norm, to_norm]))
    
    def get_learned_join(self, from_table: str, to_table: str) -> Optional[Dict[str, Any]]:
        """
        Get a learned join path between two tables.
        
        Returns:
            Join information dict or None if not found
        """
        join_key = self._create_join_key(from_table, to_table)
        from_norm = self._normalize_table_name(from_table)
        to_norm = self._normalize_table_name(to_table)
        
        # Check both directions
        if str(join_key) in self.learned_joins:
            joins = self.learned_joins[str(join_key)]
            # Find the one matching direction
            for join in joins:
                if (self._normalize_table_name(join.get('from_table', '')) == from_norm and
                    self._normalize_table_name(join.get('to_table', '')) == to_norm):
                    return join
                # Also check reverse direction
                if (self._normalize_table_name(join.get('from_table', '')) == to_norm and
                    self._normalize_table_name(join.get('to_table', '')) == from_norm):
                    # Return reversed join
                    return {
                        'from_table': from_table,
                        'to_table': to_table,
                        'on': self._reverse_join_on(join.get('on', ''), join.get('to_table', ''), join.get('from_table', '')),
                        'join_type': join.get('join_type', 'LEFT'),
                        'relationship_type': join.get('relationship_type', 'many_to_one'),
                        'cardinality_safe': join.get('cardinality_safe', True),
                        'learned': True,
                        'learned_date': join.get('learned_date', '')
                    }
        
        return None
    
    def _reverse_join_on(self, on_clause: str, old_from: str, old_to: str) -> str:
        """Reverse a join ON clause."""
        # Simple reversal: swap table names in the ON clause
        # This is a basic implementation - could be improved
        return on_clause.replace(f"{old_to}.", "TEMP_TO.").replace(f"{old_from}.", f"{old_to}.").replace("TEMP_TO.", f"{old_from}.")
    
    def ask_user_for_join(self, from_table: str, to_table: str, context: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Interactively ask user how to join two tables.
        
        Args:
            from_table: Source table name
            to_table: Target table name
            context: Optional context about why this join is needed
        
        Returns:
            Join information dict or None if user cancels
        """
        print("\n" + "=" * 80)
        print(" JOIN PATH NOT FOUND - NEED YOUR HELP")
        print("=" * 80)
        
        if context:
            print(f"\n Context: {context}")
        
        print(f"\n How do I join these two tables? Any idea?")
        print(f"   Table 1: {from_table}")
        print(f"   Table 2: {to_table}")
        print("\n I need to know:")
        print("   1. Join condition (e.g., 'customers.customer_id = loans.customer_id')")
        print("   2. Join type (LEFT, INNER, RIGHT) - default: LEFT")
        print("   3. Relationship type (one_to_one, one_to_many, many_to_one) - default: many_to_one")
        print("\n   Or type 'skip' to skip this join")
        print("   Or type 'cancel' to cancel the query")
        
        try:
            # Get join condition
            join_on = input("\n Join condition (ON clause): ").strip()
            
            if join_on.lower() in ['skip', 'cancel', '']:
                print("⏭️  Skipping this join")
                return None
            
            # Get join type
            join_type = input(" Join type (LEFT/INNER/RIGHT) [default: LEFT]: ").strip().upper()
            if not join_type or join_type not in ['LEFT', 'INNER', 'RIGHT']:
                join_type = 'LEFT'
            
            # Get relationship type
            rel_type = input(" Relationship (one_to_one/one_to_many/many_to_one) [default: many_to_one]: ").strip().lower()
            if not rel_type or rel_type not in ['one_to_one', 'one_to_many', 'many_to_one']:
                rel_type = 'many_to_one'
            
            # Determine cardinality safety
            cardinality_safe = rel_type in ['one_to_one', 'many_to_one']
            
            # Create join info
            join_info = {
                'from_table': from_table,
                'to_table': to_table,
                'on': join_on,
                'join_type': join_type,
                'relationship_type': rel_type,
                'cardinality_safe': cardinality_safe,
                'learned': True,
                'learned_date': datetime.now().isoformat(),
                'context': context
            }
            
            # Store it
            self.learned_joins[str(self._create_join_key(from_table, to_table))] = [join_info]
            self._save_learned_joins()
            
            print(f"\n Join learned and saved!")
            print(f"   {from_table} --[{join_type}]--> {to_table}")
            print(f"   ON: {join_on}")
            
            return join_info
            
        except (EOFError, KeyboardInterrupt):
            print("\n\n⏭️  Skipping join (user cancelled)")
            return None
        except Exception as e:
            print(f"\n Error learning join: {e}")
            return None
    
    def add_learned_join(self, from_table: str, to_table: str, on_clause: str, 
                         join_type: str = 'LEFT', relationship_type: str = 'many_to_one',
                         context: Optional[str] = None):
        """
        Programmatically add a learned join (for testing or bulk import).
        
        Args:
            from_table: Source table
            to_table: Target table
            on_clause: JOIN ON condition
            join_type: Type of join (LEFT/INNER/RIGHT)
            relationship_type: Relationship type
            context: Optional context
        """
        join_key = self._create_join_key(from_table, to_table)
        
        join_info = {
            'from_table': from_table,
            'to_table': to_table,
            'on': on_clause,
            'join_type': join_type,
            'relationship_type': relationship_type,
            'cardinality_safe': relationship_type in ['one_to_one', 'many_to_one'],
            'learned': True,
            'learned_date': datetime.now().isoformat(),
            'context': context
        }
        
        if str(join_key) not in self.learned_joins:
            self.learned_joins[str(join_key)] = []
        
        self.learned_joins[str(join_key)].append(join_info)
        self._save_learned_joins()
    
    def get_all_learned_joins(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all learned joins."""
        return self.learned_joins.copy()
    
    def clear_learned_joins(self):
        """Clear all learned joins (use with caution)."""
        self.learned_joins = {}
        self._save_learned_joins()
        print(" All learned joins cleared")


# Global instance (singleton pattern)
_join_learner_instance: Optional[JoinLearner] = None


def get_join_learner(storage_path: Optional[str] = None) -> JoinLearner:
    """Get or create the global join learner instance."""
    global _join_learner_instance
    if _join_learner_instance is None:
        _join_learner_instance = JoinLearner(storage_path)
    return _join_learner_instance

