"""
Advanced Retrieval Pipeline with Reranking

Implements the complete query flow:
1. Hybrid Search (Dense + Sparse) -> Top 20 chunks
2. Cohere Rerank -> Top 5 chunks
3. GPT-4o -> Final answer with citations
"""

import os
from typing import List, Dict, Optional, Any
from pathlib import Path

try:
    from openai import OpenAI
except ImportError:
    print("Warning: openai not installed.")
    OpenAI = None

try:
    import cohere
except ImportError:
    print("Warning: cohere not installed.")
    cohere = None

try:
    from .vector_db import HybridVectorDB
except ImportError:
    # Fallback for direct execution
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))
    from vector_db import HybridVectorDB


class DocumentRetrievalEngine:
    """
    Complete retrieval and answer generation pipeline.
    
    Combines:
    - Hybrid vector search (semantic + keyword)
    - Reranking (Cohere) for precision
    - LLM generation (GPT-4o) with citations
    """
    
    def __init__(
        self,
        vector_db: Optional[HybridVectorDB] = None,
        openai_api_key: Optional[str] = None,
        cohere_api_key: Optional[str] = None,
        index_name: str = "technical-docs"
    ):
        """
        Initialize the retrieval engine.
        
        Args:
            vector_db: Pre-initialized HybridVectorDB instance
            openai_api_key: OpenAI API key (or set OPENAI_API_KEY env var)
            cohere_api_key: Cohere API key (or set COHERE_API_KEY env var)
            index_name: Pinecone index name
        """
        # Initialize vector DB
        if vector_db:
            self.vector_db = vector_db
        else:
            self.vector_db = HybridVectorDB(index_name=index_name)
        
        # Initialize OpenAI
        if OpenAI is None:
            raise ImportError("openai is required. Install with: pip install openai")
        
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY must be provided or set as environment variable")
        
        self.openai_client = OpenAI(api_key=self.openai_api_key)
        self.llm_model = "gpt-4o"
        
        # Initialize Cohere
        if cohere is None:
            raise ImportError("cohere is required. Install with: pip install cohere")
        
        self.cohere_api_key = cohere_api_key or os.getenv("COHERE_API_KEY")
        if not self.cohere_api_key:
            raise ValueError("COHERE_API_KEY must be provided or set as environment variable")
        
        self.cohere_client = cohere.Client(api_key=self.cohere_api_key)
        self.rerank_model = "rerank-english-v3.0"
    
    def rerank(
        self,
        query: str,
        documents: List[Dict[str, Any]],
        top_n: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Rerank documents using Cohere.
        
        Args:
            query: Search query
            documents: List of document dictionaries with 'text' field
            top_n: Number of top documents to return
            
        Returns:
            Reranked list of documents with rerank scores
        """
        if not documents:
            return []
        
        # Prepare documents for Cohere
        doc_texts = [doc.get("text", "") for doc in documents]
        
        # Rerank
        rerank_response = self.cohere_client.rerank(
            model=self.rerank_model,
            query=query,
            documents=doc_texts,
            top_n=top_n,
            return_documents=True
        )
        
        # Map rerank results back to original documents
        reranked_docs = []
        for result in rerank_response.results:
            original_doc = documents[result.index]
            reranked_docs.append({
                **original_doc,
                "rerank_score": result.relevance_score,
                "rerank_rank": result.index + 1
            })
        
        return reranked_docs
    
    def generate_answer(
        self,
        query: str,
        context_chunks: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Generate answer using GPT-4o with context and citations.
        
        Args:
            query: User question
            context_chunks: List of context chunks with metadata
            
        Returns:
            Dictionary with answer, citations, and metadata
        """
        # Build context with citations
        context_parts = []
        citations = []
        
        for i, chunk in enumerate(context_chunks, 1):
            file_name = chunk.get("file_name", "Unknown")
            doc_type = chunk.get("document_type", "")
            header = chunk.get("current_header", "")
            parent_headers = chunk.get("parent_headers", [])
            text = chunk.get("text", "")
            
            # Build citation reference
            citation_ref = f"[{i}]"
            citation_text = f"{file_name}"
            if doc_type:
                citation_text += f" ({doc_type})"
            if parent_headers:
                citation_text += f" > {' > '.join(parent_headers)}"
            if header:
                citation_text += f" > {header}"
            
            citations.append({
                "ref": citation_ref,
                "source": citation_text,
                "file": file_name,
                "document_type": doc_type,
                "header": header,
                "parent_headers": parent_headers
            })
            
            # Add to context
            context_parts.append(
                f"{citation_ref} {citation_text}\n{text}\n"
            )
        
        context = "\n\n".join(context_parts)
        
        # Build prompt
        system_prompt = """You are a Senior Technical Architect analyzing technical documentation (ARDs, PRDs, TRDs).

Your task:
1. Answer the user's question using ONLY the provided documentation context.
2. If the answer is not in the context, explicitly state "I don't have enough information in the provided documentation to answer this question."
3. Always cite your sources using the citation references [1], [2], etc.
4. Preserve hierarchical relationships (e.g., "Sub-requirement 2.1.3 belongs to Feature 2.1").
5. Be precise and avoid hallucination. If you're uncertain, say so.

Format your response with:
- A clear, direct answer
- Citations in brackets [1], [2], etc.
- Any relevant context about parent-child relationships"""
        
        user_prompt = f"""Context from technical documentation:

{context}

---

Question: {query}

Answer:"""
        
        # Generate answer
        response = self.openai_client.chat.completions.create(
            model=self.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,  # Low temperature for factual accuracy
            max_tokens=1000
        )
        
        answer = response.choices[0].message.content
        
        return {
            "answer": answer,
            "citations": citations,
            "context_chunks_used": len(context_chunks),
            "model": self.llm_model
        }
    
    def query(
        self,
        question: str,
        document_type: Optional[str] = None,
        reference_id: Optional[str] = None,
        project: Optional[str] = None,
        top_k_retrieval: int = 20,
        top_k_rerank: int = 5,
        alpha: float = 0.7
    ) -> Dict[str, Any]:
        """
        Complete query pipeline: Retrieve -> Rerank -> Generate.
        
        Args:
            question: User question
            document_type: Filter by document type (ARD/PRD/TRD)
            top_k_retrieval: Number of chunks to retrieve initially
            top_k_rerank: Number of chunks to rerank and use
            alpha: Hybrid search alpha (0.0 = sparse, 1.0 = dense)
            
        Returns:
            Complete response with answer, citations, and metadata
        """
        print(f"Query: {question}")
        print("-" * 60)
        
        # Step 1: Hybrid Search
        print(f"Step 1: Retrieving top {top_k_retrieval} chunks...")
        if reference_id:
            print(f"  Filtering by reference_id: {reference_id}")
        if project:
            print(f"  Filtering by project: {project}")
        retrieved_chunks = self.vector_db.hybrid_search(
            query=question,
            top_k=top_k_retrieval,
            document_type=document_type,
            reference_id=reference_id,
            project=project,
            alpha=alpha
        )
        
        if not retrieved_chunks:
            return {
                "answer": "I couldn't find any relevant documentation to answer your question.",
                "citations": [],
                "retrieved_chunks": 0,
                "reranked_chunks": 0
            }
        
        print(f"  ✓ Retrieved {len(retrieved_chunks)} chunks")
        
        # Step 2: Rerank
        print(f"Step 2: Reranking to top {top_k_rerank} chunks...")
        reranked_chunks = self.rerank(
            query=question,
            documents=retrieved_chunks,
            top_n=top_k_rerank
        )
        
        print(f"  ✓ Reranked to {len(reranked_chunks)} chunks")
        
        # Display rerank scores
        print("\nRerank Scores:")
        for i, chunk in enumerate(reranked_chunks, 1):
            print(f"  {i}. Score: {chunk.get('rerank_score', 0):.4f} | "
                  f"{chunk.get('file_name', 'Unknown')} | "
                  f"{chunk.get('current_header', 'No header')[:50]}")
        
        # Step 3: Generate Answer
        print(f"\nStep 3: Generating answer with {self.llm_model}...")
        result = self.generate_answer(
            query=question,
            context_chunks=reranked_chunks
        )
        
        print("  ✓ Answer generated")
        
        # Combine results
        return {
            **result,
            "question": question,
            "retrieved_chunks": len(retrieved_chunks),
            "reranked_chunks": len(reranked_chunks),
            "rerank_scores": [chunk.get("rerank_score") for chunk in reranked_chunks]
        }


def main():
    """Main entry point for the engine."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Query technical documents")
    parser.add_argument("--question", type=str, required=True, help="Question to ask")
    parser.add_argument("--doc-type", type=str, help="Filter by document type (ARD/PRD/TRD)")
    parser.add_argument("--index-name", type=str, default="technical-docs", help="Pinecone index name")
    parser.add_argument("--top-k-retrieval", type=int, default=20, help="Initial retrieval count")
    parser.add_argument("--top-k-rerank", type=int, default=5, help="Final chunks after reranking")
    
    args = parser.parse_args()
    
    engine = DocumentRetrievalEngine(index_name=args.index_name)
    
    result = engine.query(
        question=args.question,
        document_type=args.doc_type,
        top_k_retrieval=args.top_k_retrieval,
        top_k_rerank=args.top_k_rerank
    )
    
    print("\n" + "="*60)
    print("ANSWER:")
    print("="*60)
    print(result["answer"])
    
    if result["citations"]:
        print("\n" + "="*60)
        print("CITATIONS:")
        print("="*60)
        for citation in result["citations"]:
            print(f"{citation['ref']} {citation['source']}")


if __name__ == "__main__":
    main()

