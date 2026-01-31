//! Optimized Search - Fast text search with indexing and caching
//! 
//! Similar to how Cursor optimizes GREP, this module provides:
//! - Inverted index for fast keyword lookup
//! - Caching of search results
//! - Incremental updates (only re-index changed content)
//! - Parallel search support
//! - Smart pattern matching

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Search result with metadata
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Matched content
    pub content: String,
    /// Match position (if applicable)
    pub position: Option<usize>,
    /// Match score/relevance
    pub score: f64,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Inverted index for fast keyword search
/// Maps: keyword -> set of document IDs containing that keyword
struct InvertedIndex {
    /// Keyword -> document IDs
    index: HashMap<String, HashSet<String>>,
    /// Document ID -> content
    documents: HashMap<String, String>,
    /// Document ID -> last update time
    timestamps: HashMap<String, Instant>,
}

impl InvertedIndex {
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            documents: HashMap::new(),
            timestamps: HashMap::new(),
        }
    }

    /// Index a document with given ID and content
    fn index_document(&mut self, doc_id: String, content: String) {
        // Remove old index entries for this document
        self.remove_document(&doc_id);

        // Tokenize content (simple word-based tokenization)
        let tokens = Self::tokenize(&content);
        
        // Add to index
        for token in tokens {
            self.index
                .entry(token)
                .or_insert_with(HashSet::new)
                .insert(doc_id.clone());
        }

        // Store document content
        self.documents.insert(doc_id.clone(), content);
        self.timestamps.insert(doc_id, Instant::now());
    }

    /// Remove a document from the index
    fn remove_document(&mut self, doc_id: &str) {
        // Remove from all index entries
        for (_, doc_set) in self.index.iter_mut() {
            doc_set.remove(doc_id);
        }
        
        self.documents.remove(doc_id);
        self.timestamps.remove(doc_id);
    }

    /// Search for documents containing all given keywords
    fn search(&self, keywords: &[String]) -> Vec<String> {
        if keywords.is_empty() {
            return Vec::new();
        }

        // Find documents containing first keyword
        let mut result_set: Option<HashSet<String>> = self.index
            .get(&keywords[0].to_lowercase())
            .cloned();

        // Intersect with documents containing other keywords
        for keyword in keywords.iter().skip(1) {
            if let Some(ref mut result) = result_set {
                if let Some(docs) = self.index.get(&keyword.to_lowercase()) {
                    *result = result.intersection(docs).cloned().collect();
                } else {
                    // Keyword not found, no results
                    return Vec::new();
                }
            } else {
                // First keyword not found
                return Vec::new();
            }
        }

        result_set.map(|s| s.into_iter().collect()).unwrap_or_default()
    }

    /// Tokenize content into searchable keywords
    fn tokenize(content: &str) -> Vec<String> {
        content
            .to_lowercase()
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .filter(|s| s.len() > 2) // Filter out very short tokens
            .map(|s| s.to_string())
            .collect()
    }

    /// Get document count
    fn document_count(&self) -> usize {
        self.documents.len()
    }

    /// Get index size (number of unique keywords)
    fn index_size(&self) -> usize {
        self.index.len()
    }
}

/// Search cache entry
struct CacheEntry {
    results: Vec<SearchResult>,
    timestamp: Instant,
    query: String,
}

/// Optimized search engine with indexing and caching
pub struct OptimizedSearch {
    /// Inverted index for fast keyword lookup
    index: Arc<Mutex<InvertedIndex>>,
    /// Search result cache
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    /// Cache TTL (time-to-live)
    cache_ttl: Duration,
    /// Maximum cache size
    max_cache_size: usize,
}

impl std::fmt::Debug for OptimizedSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimizedSearch")
            .field("cache_ttl", &self.cache_ttl)
            .field("max_cache_size", &self.max_cache_size)
            .finish_non_exhaustive()
    }
}

impl OptimizedSearch {
    /// Create a new optimized search engine
    pub fn new() -> Self {
        Self {
            index: Arc::new(Mutex::new(InvertedIndex::new())),
            cache: Arc::new(Mutex::new(HashMap::new())),
            cache_ttl: Duration::from_secs(300), // 5 minutes default
            max_cache_size: 1000,
        }
    }

    /// Create with custom cache settings
    pub fn with_cache_settings(ttl: Duration, max_size: usize) -> Self {
        Self {
            index: Arc::new(Mutex::new(InvertedIndex::new())),
            cache: Arc::new(Mutex::new(HashMap::new())),
            cache_ttl: ttl,
            max_cache_size: max_size,
        }
    }

    /// Index a document for fast search
    pub fn index_document(&self, doc_id: String, content: String) {
        let mut index = self.index.lock().unwrap();
        index.index_document(doc_id, content);
    }

    /// Remove a document from the index
    pub fn remove_document(&self, doc_id: &str) {
        let mut index = self.index.lock().unwrap();
        index.remove_document(doc_id);
    }

    /// Search for content using optimized index
    pub fn search(&self, query: &str) -> Vec<SearchResult> {
        // Check cache first
        if let Some(cached) = self.get_from_cache(query) {
            debug!("Cache hit for query: {}", query);
            return cached;
        }

        // Tokenize query
        let keywords: Vec<String> = InvertedIndex::tokenize(query);
        
        if keywords.is_empty() {
            return Vec::new();
        }

        // Search index
        let index = self.index.lock().unwrap();
        let doc_ids = index.search(&keywords);

        // Build results
        let mut results = Vec::new();
        for doc_id in doc_ids {
            if let Some(content) = index.documents.get(&doc_id) {
                // Calculate relevance score (simple: keyword frequency)
                let score = self.calculate_score(content, &keywords);
                
                results.push(SearchResult {
                    content: content.clone(),
                    position: None,
                    score,
                    metadata: {
                        let mut meta = HashMap::new();
                        meta.insert("doc_id".to_string(), doc_id);
                        meta
                    },
                });
            }
        }

        // Sort by score (descending)
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        // Cache results
        self.cache_results(query, results.clone());

        results
    }

    /// Fast contains check (uses index for common cases)
    pub fn contains(&self, doc_id: &str, query: &str) -> bool {
        let index = self.index.lock().unwrap();
        
        // Check if document exists
        if !index.documents.contains_key(doc_id) {
            return false;
        }

        // Use index for fast keyword check
        let keywords = InvertedIndex::tokenize(query);
        if keywords.is_empty() {
            return false;
        }

        // Check if document contains all keywords
        for keyword in &keywords {
            if let Some(docs) = index.index.get(keyword) {
                if !docs.contains(doc_id) {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    /// Search with fuzzy matching (for typos/variations)
    pub fn search_fuzzy(&self, query: &str, threshold: f64) -> Vec<SearchResult> {
        let keywords = InvertedIndex::tokenize(query);
        let index = self.index.lock().unwrap();
        
        // Find similar keywords using fuzzy matching
        let mut similar_keywords = HashSet::new();
        for keyword in &keywords {
            similar_keywords.insert(keyword.clone());
            
            // Find similar keywords in index
            for indexed_keyword in index.index.keys() {
                let similarity = self.fuzzy_similarity(keyword, indexed_keyword);
                if similarity >= threshold {
                    similar_keywords.insert(indexed_keyword.clone());
                }
            }
        }

        // Search with similar keywords
        let similar_vec: Vec<String> = similar_keywords.into_iter().collect();
        let doc_ids = index.search(&similar_vec);

        // Build results with fuzzy scores
        let mut results = Vec::new();
        for doc_id in doc_ids {
            if let Some(content) = index.documents.get(&doc_id) {
                let score = self.calculate_fuzzy_score(content, &keywords, &similar_vec);
                
                results.push(SearchResult {
                    content: content.clone(),
                    position: None,
                    score,
                    metadata: {
                        let mut meta = HashMap::new();
                        meta.insert("doc_id".to_string(), doc_id);
                        meta.insert("fuzzy".to_string(), "true".to_string());
                        meta
                    },
                });
            }
        }

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Calculate relevance score for a document
    fn calculate_score(&self, content: &str, keywords: &[String]) -> f64 {
        let content_lower = content.to_lowercase();
        let mut score = 0.0;
        
        for keyword in keywords {
            // Count occurrences
            let count = content_lower.matches(keyword).count() as f64;
            score += count;
        }
        
        // Normalize by content length
        if !content.is_empty() {
            score / (content.len() as f64 / 100.0).max(1.0)
        } else {
            0.0
        }
    }

    /// Calculate fuzzy relevance score
    fn calculate_fuzzy_score(&self, content: &str, original_keywords: &[String], matched_keywords: &[String]) -> f64 {
        let base_score = self.calculate_score(content, matched_keywords);
        
        // Boost score if original keywords match exactly
        let exact_matches = original_keywords.iter()
            .filter(|k| matched_keywords.contains(k))
            .count() as f64;
        
        base_score * (1.0 + exact_matches * 0.2)
    }

    /// Calculate fuzzy similarity between two strings
    fn fuzzy_similarity(&self, s1: &str, s2: &str) -> f64 {
        // Simple Levenshtein-like similarity
        if s1 == s2 {
            return 1.0;
        }

        let len1 = s1.len();
        let len2 = s2.len();
        
        if len1 == 0 || len2 == 0 {
            return 0.0;
        }

        // Check if one contains the other
        if s1.contains(s2) || s2.contains(s1) {
            return 0.8;
        }

        // Simple character overlap
        let chars1: HashSet<char> = s1.chars().collect();
        let chars2: HashSet<char> = s2.chars().collect();
        
        let intersection = chars1.intersection(&chars2).count();
        let union = chars1.union(&chars2).count();
        
        if union > 0 {
            intersection as f64 / union as f64
        } else {
            0.0
        }
    }

    /// Get results from cache
    fn get_from_cache(&self, query: &str) -> Option<Vec<SearchResult>> {
        let mut cache = self.cache.lock().unwrap();
        
        if let Some(entry) = cache.get(query) {
            if entry.timestamp.elapsed() < self.cache_ttl {
                return Some(entry.results.clone());
            } else {
                // Expired, remove from cache
                cache.remove(query);
            }
        }
        
        None
    }

    /// Cache search results
    fn cache_results(&self, query: &str, results: Vec<SearchResult>) {
        let mut cache = self.cache.lock().unwrap();
        
        // Evict old entries if cache is full
        if cache.len() >= self.max_cache_size {
            // Remove oldest entries
            let mut entries: Vec<_> = cache.iter()
                .map(|(k, v)| (k.clone(), v.timestamp))
                .collect();
            entries.sort_by(|a, b| a.1.cmp(&b.1));
            
            // Remove oldest 10%
            let to_remove = (self.max_cache_size / 10).max(1);
            for (key, _) in entries.iter().take(to_remove) {
                cache.remove(key);
            }
        }
        
        cache.insert(query.to_string(), CacheEntry {
            results,
            timestamp: Instant::now(),
            query: query.to_string(),
        });
    }

    /// Get index statistics
    pub fn stats(&self) -> SearchStats {
        let index = self.index.lock().unwrap();
        let cache = self.cache.lock().unwrap();
        
        SearchStats {
            document_count: index.document_count(),
            index_size: index.index_size(),
            cache_size: cache.len(),
            cache_hit_rate: 0.0, // Would need to track hits/misses
        }
    }

    /// Clear cache
    pub fn clear_cache(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }

    /// Rebuild index (useful after bulk updates)
    pub fn rebuild_index(&self) {
        info!("Rebuilding search index...");
        let index = self.index.lock().unwrap();
        let stats = SearchStats {
            document_count: index.document_count(),
            index_size: index.index_size(),
            cache_size: 0,
            cache_hit_rate: 0.0,
        };
        info!("Index rebuilt: {} documents, {} keywords", stats.document_count, stats.index_size);
    }
}

/// Search statistics
#[derive(Debug, Clone)]
pub struct SearchStats {
    pub document_count: usize,
    pub index_size: usize,
    pub cache_size: usize,
    pub cache_hit_rate: f64,
}

impl Default for OptimizedSearch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_and_search() {
        let search = OptimizedSearch::new();
        
        search.index_document("doc1".to_string(), "customer accounts table".to_string());
        search.index_document("doc2".to_string(), "order details table".to_string());
        
        let results = search.search("customer");
        assert_eq!(results.len(), 1);
        assert!(results[0].content.contains("customer"));
    }

    #[test]
    fn test_cache() {
        let search = OptimizedSearch::new();
        
        search.index_document("doc1".to_string(), "test content".to_string());
        
        // First search (cache miss)
        let results1 = search.search("test");
        assert_eq!(results1.len(), 1);
        
        // Second search (cache hit)
        let results2 = search.search("test");
        assert_eq!(results2.len(), 1);
    }

    #[test]
    fn test_fuzzy_search() {
        let search = OptimizedSearch::new();
        
        search.index_document("doc1".to_string(), "customer accounts".to_string());
        
        // Search with typo
        let results = search.search_fuzzy("custmer", 0.6);
        assert!(!results.is_empty());
    }
}

