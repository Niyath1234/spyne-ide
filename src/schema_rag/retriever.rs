//! Schema RAG Retriever
//! 
//! Retrieves relevant schema information using vector search.

use crate::error::{RcaError, Result};
use crate::metadata::{ColumnMetadata, Metadata, Table};
use crate::schema_rag::embedder::SchemaEmbedder;
use crate::schema_rag::vector_store::{Document, InMemoryVectorStore, SearchResult};
use crate::semantic::registry::SemanticRegistry;
use crate::execution_loop::query_plan::QueryPlan;
use std::collections::HashMap;
use std::sync::Arc;

/// Retrieved schema information
#[derive(Debug, Clone)]
pub struct RetrievedSchema {
    pub tables: Vec<Table>,
    pub columns: Vec<(String, ColumnMetadata)>, // (table_name, column)
    pub metrics: Vec<String>,
    pub dimensions: Vec<String>,
}

/// Schema RAG retriever
pub struct SchemaRAG {
    vector_store: InMemoryVectorStore,
    embedder: SchemaEmbedder,
    metadata: Metadata,
    semantic_registry: Arc<dyn SemanticRegistry>,
    // Index maps for reverse lookup
    table_id_to_name: HashMap<String, String>,
    column_id_to_info: HashMap<String, (String, String)>, // (table_name, column_name)
    metric_id_to_name: HashMap<String, String>,
    dimension_id_to_name: HashMap<String, String>,
}

impl SchemaRAG {
    pub fn new(
        embedder: SchemaEmbedder,
        metadata: Metadata,
        semantic_registry: Arc<dyn SemanticRegistry>,
    ) -> Self {
        Self {
            vector_store: InMemoryVectorStore::new(1536), // Default dimension for OpenAI embeddings
            embedder,
            metadata,
            semantic_registry,
            table_id_to_name: HashMap::new(),
            column_id_to_info: HashMap::new(),
            metric_id_to_name: HashMap::new(),
            dimension_id_to_name: HashMap::new(),
        }
    }

    /// Initialize the vector store by embedding all metadata
    pub async fn initialize(&mut self) -> Result<()> {
        // Embed tables
        for table in &self.metadata.tables {
            let embedding = self.embedder.embed_table(table).await?;
            let doc = Document {
                id: format!("table:{}", table.name),
                text: format!("Table {} in system {} for entity {}", table.name, table.system, table.entity),
                metadata: {
                    let mut m = HashMap::new();
                    m.insert("type".to_string(), "table".to_string());
                    m.insert("name".to_string(), table.name.clone());
                    m.insert("system".to_string(), table.system.clone());
                    m.insert("entity".to_string(), table.entity.clone());
                    m
                },
                embedding: Some(embedding),
            };
            self.table_id_to_name.insert(doc.id.clone(), table.name.clone());
            self.vector_store.add_document(doc);
        }

        // Embed columns
        for table in &self.metadata.tables {
            if let Some(ref columns) = table.columns {
                for column in columns {
                    let embedding = self.embedder.embed_column(&table.name, column).await?;
                    let doc = Document {
                        id: format!("column:{}:{}", table.name, column.name),
                        text: format!("Column {}.{} in table {}", table.name, column.name, table.name),
                        metadata: {
                            let mut m = HashMap::new();
                            m.insert("type".to_string(), "column".to_string());
                            m.insert("table".to_string(), table.name.clone());
                            m.insert("name".to_string(), column.name.clone());
                            m
                        },
                        embedding: Some(embedding),
                    };
                    self.column_id_to_info.insert(
                        doc.id.clone(),
                        (table.name.clone(), column.name.clone()),
                    );
                    self.vector_store.add_document(doc);
                }
            }
        }

        // Embed metrics
        for metric_name in self.semantic_registry.list_metrics() {
            if let Some(metric) = self.semantic_registry.metric(&metric_name) {
                let embedding = self.embedder.embed_metric(metric.as_ref()).await?;
                let doc = Document {
                    id: format!("metric:{}", metric_name),
                    text: format!("Metric {}: {}", metric_name, metric.description()),
                    metadata: {
                        let mut m = HashMap::new();
                        m.insert("type".to_string(), "metric".to_string());
                        m.insert("name".to_string(), metric_name.clone());
                        m
                    },
                    embedding: Some(embedding),
                };
                self.metric_id_to_name.insert(doc.id.clone(), metric_name.clone());
                self.vector_store.add_document(doc);
            }
        }

        // Embed dimensions
        for dimension_name in self.semantic_registry.list_dimensions() {
            if let Some(dimension) = self.semantic_registry.dimension(&dimension_name) {
                let embedding = self.embedder.embed_dimension(dimension.as_ref()).await?;
                let doc = Document {
                    id: format!("dimension:{}", dimension_name),
                    text: format!("Dimension {}: {}", dimension_name, dimension.description()),
                    metadata: {
                        let mut m = HashMap::new();
                        m.insert("type".to_string(), "dimension".to_string());
                        m.insert("name".to_string(), dimension_name.clone());
                        m
                    },
                    embedding: Some(embedding),
                };
                self.dimension_id_to_name.insert(doc.id.clone(), dimension_name.clone());
                self.vector_store.add_document(doc);
            }
        }

        Ok(())
    }

    /// Get the number of documents in the vector store
    pub fn vector_store_len(&self) -> usize {
        self.vector_store.len()
    }

    /// Retrieve relevant schema for a query (legacy method, no filtering)
    pub async fn retrieve_relevant_schema(
        &self,
        query: &str,
        top_k: usize,
    ) -> Result<RetrievedSchema> {
        self.retrieve_relevant_schema_with_plan(query, top_k, None, 0.0).await
    }

    /// Retrieve relevant schema with multi-layered filtering based on query plan
    pub async fn retrieve_relevant_schema_with_plan(
        &self,
        query: &str,
        top_k: usize,
        plan: Option<&QueryPlan>,
        min_similarity: f32,
    ) -> Result<RetrievedSchema> {
        // Stage 1: Entity-based filtering (if plan provided)
        let candidate_tables: Vec<&Table> = if let Some(plan) = plan {
            self.metadata.tables
                .iter()
                .filter(|t| plan.matches_entity(&t.entity))
                .collect()
        } else {
            self.metadata.tables.iter().collect()
        };

        // Stage 2: Embed the query
        let query_embedding = self.embedder.embed_text(query).await?;

        // Stage 3: Vector search (within candidates if plan provided, otherwise all)
        let search_results = if let Some(plan) = plan {
            if !candidate_tables.is_empty() {
                // Search only within candidate tables/metrics
                self.search_within_candidates(&query_embedding, &candidate_tables, plan, top_k * 2, min_similarity).await?
            } else {
                // No candidates, fallback to full search
                self.vector_store.search(&query_embedding, top_k)?
            }
        } else {
            // No plan, fallback to full search
            self.vector_store.search(&query_embedding, top_k)?
        };

        // Stage 4: Filter by query type and similarity threshold
        let mut tables: Vec<Table> = Vec::new();
        let mut columns: Vec<(String, ColumnMetadata)> = Vec::new();
        let mut metrics: Vec<String> = Vec::new();
        let mut dimensions: Vec<String> = Vec::new();

        let mut seen_tables: std::collections::HashSet<String> = std::collections::HashSet::new();

        for result in search_results {
            // Apply similarity threshold
            if result.score < min_similarity {
                continue;
            }

            let doc_type = result.document.metadata.get("type")
                .map(|s| s.as_str())
                .unwrap_or("unknown");

            // Filter by query type if plan provided
            if let Some(plan) = plan {
                match doc_type {
                    "metric" => {
                        if !plan.should_show_metrics() {
                            continue;
                        }
                    }
                    "table" => {
                        if !plan.should_show_tables() {
                            continue;
                        }
                    }
                    _ => {}
                }
            }

            match doc_type {
                "table" => {
                    if let Some(table_name) = result.document.metadata.get("name") {
                        if !seen_tables.contains(table_name) {
                            if let Some(table) = self.metadata.tables.iter()
                                .find(|t| t.name == *table_name) {
                                tables.push(table.clone());
                                seen_tables.insert(table_name.clone());
                            }
                        }
                    }
                }
                "column" => {
                    if let (Some(table_name), Some(column_name)) = (
                        result.document.metadata.get("table"),
                        result.document.metadata.get("name"),
                    ) {
                        if let Some(table) = self.metadata.tables.iter()
                            .find(|t| t.name == *table_name) {
                            if let Some(column) = table.columns.as_ref()
                                .and_then(|cols| cols.iter().find(|c| c.name == *column_name)) {
                                columns.push((table_name.clone(), column.clone()));
                                if !seen_tables.contains(table_name) {
                                    tables.push(table.clone());
                                    seen_tables.insert(table_name.clone());
                                }
                            }
                        }
                    }
                }
                "metric" => {
                    if let Some(metric_name) = result.document.metadata.get("name") {
                        if !metrics.contains(metric_name) {
                            metrics.push(metric_name.clone());
                        }
                    }
                }
                "dimension" => {
                    if let Some(dimension_name) = result.document.metadata.get("name") {
                        if !dimensions.contains(dimension_name) {
                            dimensions.push(dimension_name.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(RetrievedSchema {
            tables,
            columns,
            metrics,
            dimensions,
        })
    }

    /// Search within candidate tables/metrics (hierarchical search)
    async fn search_within_candidates(
        &self,
        query_embedding: &crate::schema_rag::vector_store::Embedding,
        candidate_tables: &[&Table],
        plan: &QueryPlan,
        top_k: usize,
        min_similarity: f32,
    ) -> Result<Vec<SearchResult>> {
        let mut results: Vec<SearchResult> = Vec::new();
        
        // Build candidate IDs
        let candidate_table_names: std::collections::HashSet<String> = candidate_tables
            .iter()
            .map(|t| t.name.clone())
            .collect();

        // Search all documents but filter to candidates
        let all_results = self.vector_store.search(query_embedding, top_k * 3)?;
        
        for result in all_results {
            // Apply similarity threshold
            if result.score < min_similarity {
                continue;
            }

            let doc_type = result.document.metadata.get("type")
                .map(|s| s.as_str())
                .unwrap_or("unknown");

            match doc_type {
                "table" => {
                    if let Some(table_name) = result.document.metadata.get("name") {
                        if candidate_table_names.contains(table_name) {
                            results.push(result);
                        }
                    }
                }
                "column" => {
                    if let Some(table_name) = result.document.metadata.get("table") {
                        if candidate_table_names.contains(table_name) {
                            results.push(result);
                        }
                    }
                }
                "metric" => {
                    if plan.should_show_metrics() {
                        // Check if metric's base table is in candidates
                        if let Some(metric_name) = result.document.metadata.get("name") {
                            if let Some(metric) = self.semantic_registry.metric(metric_name) {
                                let base_table = metric.base_table();
                                // Check if base table matches entity filter
                                if let Some(base_table_obj) = self.metadata.tables.iter()
                                    .find(|t| t.name == base_table) {
                                    if plan.matches_entity(&base_table_obj.entity) {
                                        results.push(result);
                                    }
                                }
                            }
                        }
                    }
                }
                "dimension" => {
                    // Include dimensions (they're usually small)
                    results.push(result);
                }
                _ => {}
            }

            if results.len() >= top_k {
                break;
            }
        }

        // Sort by score and limit
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(top_k);
        
        Ok(results)
    }
}

