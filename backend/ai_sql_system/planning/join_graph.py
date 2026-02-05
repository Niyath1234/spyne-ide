"""
Join Graph Engine - Compute join paths deterministically using NetworkX
"""
import networkx as nx
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class JoinGraph:
    """Manages table relationships and finds join paths"""
    
    def __init__(self):
        """Initialize join graph"""
        self.graph = nx.Graph()
        self.join_conditions = {}  # (table1, table2) -> join condition
    
    def add_table(self, table_name: str):
        """Add table to graph"""
        if table_name not in self.graph:
            self.graph.add_node(table_name)
    
    def add_join(self, table1: str, table2: str, join_condition: str, 
                 join_type: str = 'LEFT'):
        """
        Add join relationship between two tables
        
        Args:
            table1: First table name
            table2: Second table name
            join_condition: SQL join condition (e.g., "table1.id = table2.table1_id")
            join_type: Join type ('LEFT', 'INNER', etc.)
        """
        self.add_table(table1)
        self.add_table(table2)
        
        self.graph.add_edge(table1, table2)
        
        # Store join condition (bidirectional)
        self.join_conditions[(table1, table2)] = {
            'condition': join_condition,
            'type': join_type
        }
        self.join_conditions[(table2, table1)] = {
            'condition': join_condition,
            'type': join_type
        }
    
    def find_join_path(self, table_a: str, table_b: str) -> Optional[List[Tuple[str, str]]]:
        """
        Find shortest join path between two tables
        
        Args:
            table_a: Source table
            table_b: Target table
            
        Returns:
            List of (table1, table2) tuples representing the path, or None if no path
        """
        if table_a not in self.graph or table_b not in self.graph:
            logger.warning(f"Tables not in graph: {table_a}, {table_b}")
            return None
        
        try:
            path = nx.shortest_path(self.graph, table_a, table_b)
            
            # Convert path to edges
            edges = []
            for i in range(len(path) - 1):
                edges.append((path[i], path[i + 1]))
            
            logger.info(f"Found join path from {table_a} to {table_b}: {edges}")
            return edges
        except nx.NetworkXNoPath:
            logger.warning(f"No path found from {table_a} to {table_b}")
            return None
    
    def get_all_related_tables(self, table: str) -> List[str]:
        """
        Get all tables directly connected to a table
        
        Args:
            table: Table name
            
        Returns:
            List of connected table names
        """
        if table not in self.graph:
            return []
        
        return list(self.graph.neighbors(table))
    
    def get_join_condition(self, table1: str, table2: str) -> Optional[Dict[str, Any]]:
        """
        Get join condition between two tables
        
        Args:
            table1: First table
            table2: Second table
            
        Returns:
            Dictionary with 'condition' and 'type', or None
        """
        return self.join_conditions.get((table1, table2))
    
    def build_from_metadata(self, metadata: Dict[str, Any]):
        """
        Build join graph from metadata
        
        Args:
            metadata: Dictionary with 'lineage' or 'joins' information
        """
        # Extract joins from metadata
        joins = metadata.get('lineage', {}).get('joins', [])
        
        for join in joins:
            table1 = join.get('from_table')
            table2 = join.get('to_table')
            condition = join.get('condition', '')
            join_type = join.get('type', 'LEFT')
            
            if table1 and table2:
                self.add_join(table1, table2, condition, join_type)
        
        logger.info(f"Built join graph with {len(self.graph.nodes)} tables and {len(self.graph.edges)} relationships")
