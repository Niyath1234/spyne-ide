"""
Vector Store - pgvector operations for semantic search
"""
import os
import psycopg2
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class VectorStore:
    """Vector store using pgvector for semantic search"""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize vector store
        
        Args:
            connection_string: Postgres connection string
        """
        self.conn_string = connection_string or os.getenv(
            'POSTGRES_CONNECTION_STRING',
            'postgresql://postgres:postgres@localhost:5432/rca_engine'
        )
    
    def _get_connection(self):
        """Get Postgres connection"""
        return psycopg2.connect(self.conn_string)
    
    def search_similar(self, query_embedding: List[float], entity_type: Optional[str] = None, 
                      top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Search for similar entities using cosine similarity
        
        Args:
            query_embedding: Query embedding vector
            entity_type: Filter by entity type ('column', 'metric', 'table', 'query')
            top_k: Number of results to return
            
        Returns:
            List of similar entities with similarity scores
        """
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'
            
            if entity_type:
                sql = """
                    SELECT 
                        entity_type,
                        entity_id,
                        entity_name,
                        metadata,
                        1 - (embedding <=> %s::vector) as similarity
                    FROM metadata_embeddings
                    WHERE entity_type = %s
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """
                cur.execute(sql, (embedding_str, entity_type, embedding_str, top_k))
            else:
                sql = """
                    SELECT 
                        entity_type,
                        entity_id,
                        entity_name,
                        metadata,
                        1 - (embedding <=> %s::vector) as similarity
                    FROM metadata_embeddings
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """
                cur.execute(sql, (embedding_str, embedding_str, top_k))
            
            results = cur.fetchall()
            
            return [
                {
                    'entity_type': row[0],
                    'entity_id': row[1],
                    'entity_name': row[2],
                    'metadata': row[3],
                    'similarity': float(row[4])
                }
                for row in results
            ]
        except Exception as e:
            logger.error(f"Error in vector search: {e}")
            return []
        finally:
            cur.close()
            conn.close()
    
    def get_by_id(self, entity_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get entity by type and ID"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT entity_type, entity_id, entity_name, embedding, metadata
                FROM metadata_embeddings
                WHERE entity_type = %s AND entity_id = %s
            """, (entity_type, entity_id))
            
            row = cur.fetchone()
            if row:
                return {
                    'entity_type': row[0],
                    'entity_id': row[1],
                    'entity_name': row[2],
                    'embedding': row[3],
                    'metadata': row[4]
                }
            return None
        except Exception as e:
            logger.error(f"Error getting entity: {e}")
            return None
        finally:
            cur.close()
            conn.close()
