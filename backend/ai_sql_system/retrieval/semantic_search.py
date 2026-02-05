"""
Semantic Search - Retrieve relevant metadata using embeddings
"""
from typing import List, Dict, Any, Optional
from ..metadata.semantic_registry import SemanticRegistry
import logging

logger = logging.getLogger(__name__)


class SemanticRetriever:
    """Retrieves relevant metadata using semantic search"""
    
    def __init__(self, semantic_registry: Optional[SemanticRegistry] = None):
        """
        Initialize semantic retriever
        
        Args:
            semantic_registry: SemanticRegistry instance
        """
        self.registry = semantic_registry or SemanticRegistry()
        # In production, use sentence-transformers or OpenAI embeddings
        # For now, we'll use a simple approach - in production integrate with embedding model
        self._embedding_model = None
    
    def _get_embedding(self, text: str) -> List[float]:
        """
        Get embedding for text using sentence-transformers
        
        Uses BAAI/bge-large-en-v1.5 model (1024 dimensions)
        Falls back to OpenAI embeddings if available
        """
        import os
        
        # Try OpenAI embeddings first (if available)
        openai_key = os.getenv('OPENAI_API_KEY')
        if openai_key:
            try:
                from openai import OpenAI
                client = OpenAI(api_key=openai_key)
                response = client.embeddings.create(
                    model="text-embedding-3-large",
                    input=text
                )
                return response.data[0].embedding
            except Exception as e:
                logger.warning(f"OpenAI embedding failed, using local model: {e}")
        
        # Fallback to sentence-transformers
        try:
            from sentence_transformers import SentenceTransformer
            
            # Lazy load model (cache it)
            if not hasattr(self, '_embedding_model'):
                logger.info("Loading sentence-transformers model...")
                self._embedding_model = SentenceTransformer('BAAI/bge-large-en-v1.5')
            
            embedding = self._embedding_model.encode(text, normalize_embeddings=True)
            return embedding.tolist()
        except ImportError:
            logger.warning("sentence-transformers not installed, using placeholder")
            return [0.0] * 1024
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return [0.0] * 1024
    
    def retrieve(self, query: str, top_k: int = 10) -> Dict[str, Any]:
        """
        Retrieve relevant metadata for a query
        
        Args:
            query: User query text
            top_k: Number of results to return
            
        Returns:
            Dictionary with retrieved context:
            {
                'tables': [...],
                'metrics': [...],
                'columns': [...],
                'past_queries': [...]
            }
        """
        query_embedding = self._get_embedding(query)
        
        # Search across all entity types
        results = self.registry.search_semantic(query_embedding, top_k=top_k)
        
        # Organize results by type
        context = {
            'tables': [],
            'metrics': [],
            'columns': [],
            'past_queries': []
        }
        
        for result in results:
            entity_type = result['entity_type']
            if entity_type == 'table':
                context['tables'].append(result)
            elif entity_type == 'metric':
                context['metrics'].append(result)
            elif entity_type == 'column':
                context['columns'].append(result)
            elif entity_type == 'query':
                context['past_queries'].append(result)
        
        logger.info(f"Retrieved {len(results)} relevant entities for query")
        return context
    
    def retrieve_metric(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve specific metric"""
        return self.registry.get_metric(metric_name)
    
    def retrieve_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Retrieve columns for a table"""
        return self.registry.get_columns(table_name)
