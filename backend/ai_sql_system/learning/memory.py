"""
Memory System - Store and retrieve successful queries for learning
"""
from typing import List, Dict, Any, Optional
from ..metadata.ingestion import MetadataIngestion
from ..retrieval.semantic_search import SemanticRetriever
import logging

logger = logging.getLogger(__name__)


class MemorySystem:
    """Stores successful queries and retrieves similar ones"""
    
    def __init__(self, ingestion: Optional[MetadataIngestion] = None,
                 retriever: Optional[SemanticRetriever] = None):
        """
        Initialize memory system
        
        Args:
            ingestion: MetadataIngestion instance
            retriever: SemanticRetriever instance
        """
        self.ingestion = ingestion or MetadataIngestion()
        self.retriever = retriever or SemanticRetriever()
    
    def store_successful_query(self, user_query: str, final_sql: str, execution_time_ms: Optional[int] = None):
        """
        Store successful query for future reference
        
        Args:
            user_query: Original user query
            final_sql: Final SQL that worked
            execution_time_ms: Execution time in milliseconds
        """
        self.ingestion.store_query_memory(user_query, final_sql, success=True, execution_time_ms=execution_time_ms)
        
        # Also store embedding for semantic search
        # In production, generate embedding and store
        # For now, just store the text
        logger.info(f"Stored successful query: {user_query[:50]}...")
    
    def retrieve_similar_queries(self, user_query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Retrieve similar past queries
        
        Args:
            user_query: Current user query
            top_k: Number of similar queries to retrieve
            
        Returns:
            List of similar queries with SQL
        """
        # Use semantic retriever to find similar queries
        context = self.retriever.retrieve(user_query, top_k=top_k)
        
        similar_queries = []
        for query_result in context.get('past_queries', []):
            # Retrieve full query details from database
            conn = self.ingestion._get_connection()
            cur = conn.cursor()
            
            try:
                cur.execute("""
                    SELECT user_query, final_sql, success, execution_time_ms
                    FROM query_memory
                    WHERE id = %s
                """, (query_result.get('entity_id'),))
                
                row = cur.fetchone()
                if row:
                    similar_queries.append({
                        'user_query': row[0],
                        'final_sql': row[1],
                        'success': row[2],
                        'execution_time_ms': row[3],
                        'similarity': query_result.get('similarity', 0.0)
                    })
            except Exception as e:
                logger.error(f"Error retrieving query: {e}")
            finally:
                cur.close()
                conn.close()
        
        logger.info(f"Retrieved {len(similar_queries)} similar queries")
        return similar_queries
    
    def get_best_sql_for_query(self, user_query: str) -> Optional[str]:
        """
        Get best SQL for a similar query
        
        Args:
            user_query: User query
            
        Returns:
            Best SQL string or None
        """
        similar = self.retrieve_similar_queries(user_query, top_k=1)
        
        if similar and similar[0]['success']:
            return similar[0]['final_sql']
        
        return None
