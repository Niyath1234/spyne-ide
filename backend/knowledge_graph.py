#!/usr/bin/env python3
"""
Knowledge Graph

Structured knowledge representation with semantic relationships.
Enables efficient traversal and retrieval of related knowledge.
"""

from typing import Dict, List, Any, Optional, Set
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum


class NodeType(Enum):
    """Types of nodes in the knowledge graph."""
    TABLE = "table"
    COLUMN = "column"
    METRIC = "metric"
    DIMENSION = "dimension"
    BUSINESS_TERM = "business_term"
    RULE = "rule"
    JOIN = "join"


class RelationshipType(Enum):
    """Types of relationships in the knowledge graph."""
    HAS_COLUMN = "has_column"
    HAS_RULE = "has_rule"
    JOINS_TO = "joins_to"
    RELATES_TO = "relates_to"
    USES_DIMENSION = "uses_dimension"
    USES_METRIC = "uses_metric"
    RELATED_COLUMN = "related_column"
    ALIAS_OF = "alias_of"
    DEPENDS_ON = "depends_on"


@dataclass
class KnowledgeNode:
    """A node in the knowledge graph."""
    id: str
    type: NodeType
    properties: Dict[str, Any] = field(default_factory=dict)
    relationships: Dict[RelationshipType, List[str]] = field(default_factory=lambda: defaultdict(list))
    embedding: Optional[List[float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_relationship(self, rel_type: RelationshipType, target_id: str):
        """Add a relationship to another node."""
        if target_id not in self.relationships[rel_type]:
            self.relationships[rel_type].append(target_id)
    
    def get_related_nodes(self, rel_type: Optional[RelationshipType] = None) -> List[str]:
        """Get related node IDs."""
        if rel_type:
            return self.relationships.get(rel_type, [])
        return [node_id for nodes in self.relationships.values() for node_id in nodes]


class KnowledgeGraph:
    """Knowledge graph for structured knowledge representation."""
    
    def __init__(self):
        """Initialize knowledge graph."""
        self.nodes: Dict[str, KnowledgeNode] = {}
        self.indexes: Dict[str, Dict[str, List[str]]] = {
            'name': defaultdict(list),
            'type': defaultdict(list),
            'table': defaultdict(list),
        }
    
    def add_node(self, node: KnowledgeNode):
        """Add a node to the graph."""
        self.nodes[node.id] = node
        
        # Update indexes
        self.indexes['type'][node.type.value].append(node.id)
        if 'name' in node.properties:
            self.indexes['name'][node.properties['name'].lower()].append(node.id)
        if 'table' in node.properties:
            self.indexes['table'][node.properties['table'].lower()].append(node.id)
    
    def get_node(self, node_id: str) -> Optional[KnowledgeNode]:
        """Get a node by ID."""
        return self.nodes.get(node_id)
    
    def find_nodes_by_name(self, name: str) -> List[KnowledgeNode]:
        """Find nodes by name (fuzzy match)."""
        name_lower = name.lower()
        node_ids = set()
        
        # Exact match
        node_ids.update(self.indexes['name'].get(name_lower, []))
        
        # Partial match
        for indexed_name, ids in self.indexes['name'].items():
            if name_lower in indexed_name or indexed_name in name_lower:
                node_ids.update(ids)
        
        return [self.nodes[nid] for nid in node_ids if nid in self.nodes]
    
    def find_nodes_by_type(self, node_type: NodeType) -> List[KnowledgeNode]:
        """Find all nodes of a specific type."""
        node_ids = self.indexes['type'].get(node_type.value, [])
        return [self.nodes[nid] for nid in node_ids if nid in self.nodes]
    
    def traverse(self, start_node_id: str, rel_type: Optional[RelationshipType] = None, 
                 max_depth: int = 2, visited: Optional[Set[str]] = None) -> List[KnowledgeNode]:
        """
        Traverse the graph from a starting node.
        
        Args:
            start_node_id: Starting node ID
            rel_type: Optional relationship type to follow
            max_depth: Maximum traversal depth
            visited: Set of visited node IDs (for cycle detection)
        
        Returns:
            List of nodes reached during traversal
        """
        if visited is None:
            visited = set()
        
        if start_node_id in visited or max_depth <= 0:
            return []
        
        start_node = self.get_node(start_node_id)
        if not start_node:
            return []
        
        visited.add(start_node_id)
        result = [start_node]
        
        # Get related nodes
        related_ids = start_node.get_related_nodes(rel_type)
        
        # Traverse to related nodes
        for related_id in related_ids:
            if related_id not in visited:
                result.extend(self.traverse(related_id, rel_type, max_depth - 1, visited))
        
        return result
    
    def get_subgraph(self, node_ids: List[str], include_relationships: bool = True) -> Dict[str, Any]:
        """
        Get a subgraph containing specified nodes and their relationships.
        
        Args:
            node_ids: List of node IDs to include
            include_relationships: Whether to include related nodes
        
        Returns:
            Dictionary with nodes and edges
        """
        subgraph_nodes = {}
        subgraph_edges = []
        included = set(node_ids)
        
        # Add requested nodes
        for node_id in node_ids:
            node = self.get_node(node_id)
            if node:
                subgraph_nodes[node_id] = {
                    'id': node.id,
                    'type': node.type.value,
                    'properties': node.properties,
                    'metadata': node.metadata
                }
        
        # Add relationships if requested
        if include_relationships:
            for node_id in node_ids:
                node = self.get_node(node_id)
                if node:
                    for rel_type, target_ids in node.relationships.items():
                        for target_id in target_ids:
                            if target_id in self.nodes:
                                if target_id not in included:
                                    included.add(target_id)
                                    target_node = self.nodes[target_id]
                                    subgraph_nodes[target_id] = {
                                        'id': target_node.id,
                                        'type': target_node.type.value,
                                        'properties': target_node.properties,
                                        'metadata': target_node.metadata
                                    }
                                
                                subgraph_edges.append({
                                    'from': node_id,
                                    'to': target_id,
                                    'type': rel_type.value
                                })
        
        return {
            'nodes': subgraph_nodes,
            'edges': subgraph_edges
        }
    
    def build_from_metadata(self, metadata: Dict[str, Any]):
        """
        Build knowledge graph from metadata.
        
        Args:
            metadata: Metadata dictionary with tables, metrics, dimensions, etc.
        """
        # Build table nodes
        tables = metadata.get('tables', {}).get('tables', [])
        for table in tables:
            table_name = table.get('name')
            if table_name:
                table_node = KnowledgeNode(
                    id=f"table:{table_name}",
                    type=NodeType.TABLE,
                    properties={
                        'name': table_name,
                        'system': table.get('system'),
                        'entity': table.get('entity'),
                        'description': table.get('description', ''),
                        'primary_key': table.get('primary_key', []),
                        'time_column': table.get('time_column')
                    }
                )
                self.add_node(table_node)
                
                # Add column nodes
                for col in table.get('columns', []):
                    col_name = col.get('name', '')
                    if col_name:
                        col_node = KnowledgeNode(
                            id=f"column:{table_name}.{col_name}",
                            type=NodeType.COLUMN,
                            properties={
                                'name': col_name,
                                'table': table_name,
                                'data_type': col.get('data_type', ''),
                                'description': col.get('description', '')
                            },
                            metadata={
                                'distinct_values': col.get('distinct_values'),
                                'sample_values': col.get('sample_values')
                            }
                        )
                        self.add_node(col_node)
                        
                        # Add relationship: table has_column column
                        table_node.add_relationship(RelationshipType.HAS_COLUMN, col_node.id)
        
        # Build metric nodes
        registry = metadata.get('semantic_registry', {})
        for metric in registry.get('metrics', []):
            metric_name = metric.get('name')
            if metric_name:
                metric_node = KnowledgeNode(
                    id=f"metric:{metric_name}",
                    type=NodeType.METRIC,
                    properties={
                        'name': metric_name,
                        'description': metric.get('description', ''),
                        'base_table': metric.get('base_table', ''),
                        'sql_expression': metric.get('sql_expression', '')
                    }
                )
                self.add_node(metric_node)
                
                # Add relationship to base table
                base_table = metric.get('base_table')
                if base_table:
                    table_node_id = f"table:{base_table}"
                    if table_node_id in self.nodes:
                        self.nodes[table_node_id].add_relationship(RelationshipType.USES_METRIC, metric_node.id)
        
        # Build dimension nodes
        for dim in registry.get('dimensions', []):
            dim_name = dim.get('name')
            if dim_name:
                dim_node = KnowledgeNode(
                    id=f"dimension:{dim_name}",
                    type=NodeType.DIMENSION,
                    properties={
                        'name': dim_name,
                        'description': dim.get('description', ''),
                        'base_table': dim.get('base_table', ''),
                        'column': dim.get('column', ''),
                        'sql_expression': dim.get('sql_expression', '')
                    }
                )
                self.add_node(dim_node)
        
        # Build join/lineage edges
        lineage = metadata.get('lineage', {})
        for edge in lineage.get('edges', []):
            from_table = edge.get('from')
            to_table = edge.get('to')
            
            if from_table and to_table:
                from_node_id = f"table:{from_table}"
                to_node_id = f"table:{to_table}"
                
                if from_node_id in self.nodes and to_node_id in self.nodes:
                    self.nodes[from_node_id].add_relationship(RelationshipType.JOINS_TO, to_node_id)
        
        # Build knowledge base term nodes
        kb = metadata.get('knowledge_base', {})
        for term_name, term_data in kb.get('terms', {}).items():
            term_node = KnowledgeNode(
                id=f"term:{term_name}",
                type=NodeType.BUSINESS_TERM,
                properties={
                    'name': term_name,
                    'definition': term_data.get('definition', ''),
                    'aliases': term_data.get('aliases', []),
                    'business_meaning': term_data.get('business_meaning', '')
                }
            )
            self.add_node(term_node)
            
            # Add relationships to related tables
            for related_table in term_data.get('related_tables', []):
                table_node_id = f"table:{related_table}"
                if table_node_id in self.nodes:
                    self.nodes[table_node_id].add_relationship(RelationshipType.RELATES_TO, term_node.id)
                    term_node.add_relationship(RelationshipType.RELATES_TO, table_node_id)
        
        # Build rule nodes and relationships
        rules = metadata.get('rules', [])
        for rule in rules:
            # Handle both dict and string formats
            if isinstance(rule, str):
                # String format - create a simple rule node
                rule_id = f"rule:{len(self.find_nodes_by_type(NodeType.RULE))}"
                rule_node = KnowledgeNode(
                    id=rule_id,
                    type=NodeType.RULE,
                    properties={
                        'id': rule_id,
                        'description': rule,
                        'source_table': ''
                    },
                    metadata={'rule_data': rule}
                )
                self.add_node(rule_node)
            elif isinstance(rule, dict):
                # Dict format - extract properties
                rule_id = rule.get('id', f"rule:{len(self.find_nodes_by_type(NodeType.RULE))}")
                computation = rule.get('computation', {})
                if isinstance(computation, dict):
                    description = computation.get('description', rule.get('description', ''))
                    source_table = computation.get('source_table', '')
                else:
                    description = rule.get('description', '')
                    source_table = ''
                
                rule_node = KnowledgeNode(
                    id=rule_id,
                    type=NodeType.RULE,
                    properties={
                        'id': rule_id,
                        'description': description,
                        'source_table': source_table
                    },
                    metadata={'rule_data': rule}
                )
                self.add_node(rule_node)
            
            # Add relationship to source table
            source_table = rule.get('computation', {}).get('source_table')
            if source_table:
                table_node_id = f"table:{source_table}"
                if table_node_id in self.nodes:
                    self.nodes[table_node_id].add_relationship(RelationshipType.HAS_RULE, rule_node.id)


# Global instance
_knowledge_graph: Optional[KnowledgeGraph] = None


def get_knowledge_graph(metadata: Optional[Dict[str, Any]] = None) -> KnowledgeGraph:
    """Get or create global knowledge graph."""
    global _knowledge_graph
    if _knowledge_graph is None:
        _knowledge_graph = KnowledgeGraph()
        if metadata:
            _knowledge_graph.build_from_metadata(metadata)
    return _knowledge_graph

