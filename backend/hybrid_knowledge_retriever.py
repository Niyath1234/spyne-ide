#!/usr/bin/env python3
"""
Hybrid Knowledge Retriever

Combines RAG (semantic search) with knowledge graph (structured search)
for optimal knowledge retrieval.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from backend.knowledge_graph import KnowledgeGraph, NodeType, RelationshipType, get_knowledge_graph
from backend.knowledge_base_client import get_knowledge_base_client

logger = logging.getLogger(__name__)


@dataclass
class RetrievedKnowledge:
    """Represents retrieved knowledge with relevance score."""
    node_id: str
    node_type: str
    content: Dict[str, Any]
    relevance_score: float
    source: str  # 'rag', 'graph', 'rule', 'metadata'
    relationships: List[str] = None


class HybridKnowledgeRetriever:
    """Hybrid knowledge retriever combining RAG and graph search."""
    
    def __init__(self, knowledge_graph: Optional[KnowledgeGraph] = None):
        """
        Initialize hybrid knowledge retriever.
        
        Args:
            knowledge_graph: Optional knowledge graph instance
        """
        self.knowledge_graph = knowledge_graph
        try:
            self.kb_client = get_knowledge_base_client()
        except Exception:
            self.kb_client = None
    
    def retrieve_for_query(self, query: str, metadata: Dict[str, Any],
                          max_results: int = 20) -> List[RetrievedKnowledge]:
        """
        Retrieve knowledge for a query using hybrid approach with parallel execution.
        
        Args:
            query: User query
            metadata: Metadata dictionary
            max_results: Maximum number of results
        
        Returns:
            List of retrieved knowledge items with relevance scores
        """
        # Initialize knowledge graph if needed
        if not self.knowledge_graph:
            self.knowledge_graph = get_knowledge_graph(metadata)
            if not self.knowledge_graph.nodes:
                self.knowledge_graph.build_from_metadata(metadata)
        
        retrieved = []
        
        # Parallel retrieval
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(self._rag_search, query, max_results // 2): 'rag',
                executor.submit(self._graph_search, query, max_results // 2): 'graph',
                executor.submit(self._rule_search, query, metadata): 'rule'
            }
            
            for future in as_completed(futures):
                source_type = futures[future]
                try:
                    results = future.result()
                    retrieved.extend(results)
                except Exception as e:
                    logger.warning(f"{source_type} search failed: {e}", exc_info=True)
                    # Continue with other sources
        
        # Validate results
        validated = [k for k in retrieved if self._validate_knowledge(k, query)]
        
        # Deduplicate and rank by relevance
        unique_results = self._deduplicate_and_rank(validated, max_results)
        
        return unique_results
    
    def _validate_knowledge(self, knowledge: RetrievedKnowledge, query: str) -> bool:
        """
        Validate knowledge relevance.
        
        Returns:
            True if knowledge should be included, False otherwise.
        """
        # Check relevance score
        if knowledge.relevance_score < 0.3:
            return False
        
        # Check content quality
        if not knowledge.content:
            return False
        
        content_str = str(knowledge.content)
        if len(content_str) < 10:  # Too short
            return False
        
        # Check if content relates to query (simple keyword check)
        query_lower = query.lower()
        content_lower = content_str.lower()
        
        # At least one query word should appear in content
        query_words = set(query_lower.split())
        content_words = set(content_lower.split())
        
        if not query_words.intersection(content_words):
            # No overlap - might be irrelevant
            # But don't filter if relevance is high
            if knowledge.relevance_score < 0.5:
                return False
        
        return True
    
    def _rag_search(self, query: str, max_results: int) -> List[RetrievedKnowledge]:
        """Semantic search using RAG."""
        results = []
        
        if not self.kb_client:
            return results
        
        try:
            rag_response = self.kb_client.rag_retrieve(query, top_k=max_results)
            rag_results = rag_response.get('results', [])
            
            for i, result in enumerate(rag_results):
                # Extract concept information
                concept = result.get('concept', {})
                concept_id = concept.get('id', f"rag_{i}")
                concept_name = concept.get('name', '')
                
                # Calculate relevance (higher rank = more relevant)
                relevance = 1.0 - (i / max(max_results, 1))
                
                results.append(RetrievedKnowledge(
                    node_id=concept_id,
                    node_type='concept',
                    content={
                        'name': concept_name,
                        'definition': concept.get('definition', ''),
                        'text': result.get('text', '')
                    },
                    relevance_score=relevance,
                    source='rag'
                ))
        except Exception as e:
            print(f"RAG search failed: {e}")
        
        return results
    
    def _graph_search(self, query: str, max_results: int) -> List[RetrievedKnowledge]:
        """Structured search using knowledge graph."""
        results = []
        
        if not self.knowledge_graph:
            return results
        
        # Extract keywords from query
        query_lower = query.lower()
        keywords = query_lower.split()
        
        # Find nodes matching keywords
        matched_nodes = set()
        for keyword in keywords:
            if len(keyword) < 3:  # Skip short words
                continue
            
            # Search by name
            nodes = self.knowledge_graph.find_nodes_by_name(keyword)
            for node in nodes:
                matched_nodes.add(node.id)
        
        # Traverse from matched nodes
        all_related = set()
        for node_id in list(matched_nodes)[:5]:  # Limit starting nodes
            related = self.knowledge_graph.traverse(node_id, max_depth=2)
            for node in related:
                all_related.add(node.id)
        
        # Convert to RetrievedKnowledge
        for i, node_id in enumerate(list(all_related)[:max_results]):
            node = self.knowledge_graph.get_node(node_id)
            if node:
                # Calculate relevance based on match quality
                relevance = 0.8 - (i / max(max_results, 1)) * 0.3
                
                results.append(RetrievedKnowledge(
                    node_id=node_id,
                    node_type=node.type.value,
                    content={
                        'properties': node.properties,
                        'metadata': node.metadata
                    },
                    relevance_score=relevance,
                    source='graph',
                    relationships=node.get_related_nodes()
                ))
        
        return results
    
    def _rule_search(self, query: str, metadata: Dict[str, Any]) -> List[RetrievedKnowledge]:
        """Search for applicable rules."""
        results = []
        
        if not self.knowledge_graph:
            return results
        
        # Extract column/table names from query
        query_lower = query.lower()
        
        # Find columns mentioned in query
        column_nodes = self.knowledge_graph.find_nodes_by_type(NodeType.COLUMN)
        for col_node in column_nodes:
            col_name = col_node.properties.get('name', '').lower()
            if col_name in query_lower:
                # Find rules for this column
                table_name = col_node.properties.get('table', '')
                table_node_id = f"table:{table_name}"
                table_node = self.knowledge_graph.get_node(table_node_id)
                
                if table_node:
                    rule_ids = table_node.get_related_nodes(RelationshipType.HAS_RULE)
                    for rule_id in rule_ids:
                        rule_node = self.knowledge_graph.get_node(rule_id)
                        if rule_node:
                            results.append(RetrievedKnowledge(
                                node_id=rule_id,
                                node_type='rule',
                                content=rule_node.metadata.get('rule_data', {}),
                                relevance_score=0.9,  # Rules are highly relevant
                                source='rule'
                            ))
        
        return results
    
    def _deduplicate_and_rank(self, results: List[RetrievedKnowledge], 
                              max_results: int) -> List[RetrievedKnowledge]:
        """Deduplicate and rank results by relevance."""
        # Group by node_id
        seen = {}
        for result in results:
            if result.node_id not in seen:
                seen[result.node_id] = result
            else:
                # Merge if duplicate (take higher relevance)
                existing = seen[result.node_id]
                if result.relevance_score > existing.relevance_score:
                    seen[result.node_id] = result
        
        # Sort by relevance score (descending)
        ranked = sorted(seen.values(), key=lambda x: x.relevance_score, reverse=True)
        
        return ranked[:max_results]
    
    def build_optimized_context(self, query: str, metadata: Dict[str, Any],
                               max_knowledge_items: int = 30) -> str:
        """
        Build optimized context using hybrid retrieval.
        
        Args:
            query: User query
            metadata: Metadata dictionary
            max_knowledge_items: Maximum knowledge items to include
        
        Returns:
            Formatted context string
        """
        # Retrieve knowledge
        retrieved = self.retrieve_for_query(query, metadata, max_knowledge_items)
        
        # Build context sections
        context_parts = []
        context_parts.append("=" * 80)
        context_parts.append("RELEVANT KNOWLEDGE (Hybrid RAG + Graph Search)")
        context_parts.append("=" * 80)
        
        # Group by type
        by_type = {}
        for item in retrieved:
            item_type = item.node_type
            if item_type not in by_type:
                by_type[item_type] = []
            by_type[item_type].append(item)
        
        # Add sections by type
        for item_type, items in sorted(by_type.items()):
            context_parts.append(f"\n{item_type.upper()} ({len(items)} items):")
            for item in items[:10]:  # Limit per type
                context_parts.append(f"  [{item.relevance_score:.2f}] {item.node_id}")
                if item.content:
                    if isinstance(item.content, dict):
                        for key, value in list(item.content.items())[:3]:  # Limit properties
                            context_parts.append(f"    {key}: {str(value)[:100]}")
                    else:
                        context_parts.append(f"    {str(item.content)[:200]}")
        
        return "\n".join(context_parts)

