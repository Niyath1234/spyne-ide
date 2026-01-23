//! Vector Store Abstraction
//! 
//! Simple in-memory vector store for schema embeddings.

use crate::error::{RcaError, Result};
use std::collections::HashMap;

/// Vector embedding (simple f32 vector)
pub type Embedding = Vec<f32>;

/// Document in the vector store
#[derive(Debug, Clone)]
pub struct Document {
    pub id: String,
    pub text: String,
    pub metadata: HashMap<String, String>,
    pub embedding: Option<Embedding>,
}

/// Search result from vector store
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub document: Document,
    pub score: f32,
}

/// Simple in-memory vector store
pub struct InMemoryVectorStore {
    documents: HashMap<String, Document>,
}

impl InMemoryVectorStore {
    pub fn new() -> Self {
        Self {
            documents: HashMap::new(),
        }
    }

    pub fn add_document(&mut self, document: Document) {
        self.documents.insert(document.id.clone(), document);
    }

    pub fn add_documents(&mut self, documents: Vec<Document>) {
        for doc in documents {
            self.add_document(doc);
        }
    }

    /// Search for similar documents using cosine similarity
    pub fn search(&self, query_embedding: &Embedding, top_k: usize) -> Result<Vec<SearchResult>> {
        let mut results: Vec<SearchResult> = Vec::new();

        for doc in self.documents.values() {
            if let Some(ref embedding) = doc.embedding {
                let score = cosine_similarity(query_embedding, embedding);
                results.push(SearchResult {
                    document: doc.clone(),
                    score,
                });
            }
        }

        // Sort by score descending
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        // Return top K
        results.truncate(top_k);
        Ok(results)
    }

    pub fn get_document(&self, id: &str) -> Option<&Document> {
        self.documents.get(id)
    }

    pub fn len(&self) -> usize {
        self.documents.len()
    }
}

impl Default for InMemoryVectorStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }

    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot_product / (norm_a * norm_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert_eq!(cosine_similarity(&a, &b), 1.0);

        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
    }
}

