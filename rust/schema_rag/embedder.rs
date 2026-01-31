//! Schema Embedder
//! 
//! Embeds table/column metadata and semantic metrics using OpenAI embeddings API.

use crate::error::{RcaError, Result};
use crate::metadata::{ColumnMetadata, Table};
use crate::schema_rag::vector_store::Embedding;
use crate::semantic::metric::MetricDefinition;
use crate::semantic::dimension::DimensionDefinition;
use std::collections::HashMap;

/// Embedding client using OpenAI API
pub struct SchemaEmbedder {
    api_key: String,
    base_url: String,
    model: String, // e.g., "text-embedding-3-small"
}

impl SchemaEmbedder {
    pub fn new(api_key: String, base_url: String, model: String) -> Self {
        Self {
            api_key,
            base_url,
            model,
        }
    }

    /// Embed a table's metadata
    pub async fn embed_table(&self, table: &Table) -> Result<Embedding> {
        let text = self.table_to_text(table);
        self.embed_text(&text).await
    }

    /// Embed a column's metadata
    pub async fn embed_column(&self, table_name: &str, column: &ColumnMetadata) -> Result<Embedding> {
        let text = self.column_to_text(table_name, column);
        self.embed_text(&text).await
    }

    /// Embed a metric definition
    pub async fn embed_metric(&self, metric: &dyn MetricDefinition) -> Result<Embedding> {
        let text = format!(
            "Metric: {}. Description: {}. Base table: {}",
            metric.name(),
            metric.description(),
            metric.base_table()
        );
        self.embed_text(&text).await
    }

    /// Embed a dimension definition
    pub async fn embed_dimension(&self, dimension: &dyn DimensionDefinition) -> Result<Embedding> {
        let text = format!(
            "Dimension: {}. Description: {}. Base table: {}. Column: {}",
            dimension.name(),
            dimension.description(),
            dimension.base_table(),
            dimension.column()
        );
        self.embed_text(&text).await
    }

    /// Embed arbitrary text using OpenAI API
    pub async fn embed_text(&self, text: &str) -> Result<Embedding> {
        // Handle dummy mode
        if self.api_key == "dummy-api-key" {
            // Return a dummy embedding (1536 dimensions for text-embedding-3-small)
            return Ok(vec![0.1; 1536]);
        }

        let client = reqwest::Client::new();
        
        let body = serde_json::json!({
            "model": self.model,
            "input": text,
        });

        let response = client
            .post(&format!("{}/embeddings", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| RcaError::Llm(format!("Embedding API call failed: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(RcaError::Llm(format!("Embedding API error ({}): {}", status, error_text)));
        }

        let response_json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| RcaError::Llm(format!("Failed to parse embedding response: {}", e)))?;

        // Extract embedding from response
        let data = response_json.get("data")
            .and_then(|d| d.as_array())
            .and_then(|arr| arr.first())
            .ok_or_else(|| RcaError::Llm("No embedding data in response".to_string()))?;

        let embedding: Vec<f32> = data.get("embedding")
            .and_then(|e| e.as_array())
            .ok_or_else(|| RcaError::Llm("No embedding vector in response".to_string()))?
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect();

        Ok(embedding)
    }

    /// Convert table to searchable text
    fn table_to_text(&self, table: &Table) -> String {
        let mut parts = Vec::new();
        parts.push(format!("Table: {}", table.name));
        parts.push(format!("System: {}", table.system));
        parts.push(format!("Entity: {}", table.entity));
        
        if let Some(ref description) = table.columns.as_ref().and_then(|cols| {
            cols.first().and_then(|c| c.description.as_ref())
        }) {
            parts.push(format!("Description: {}", description));
        }

        if let Some(ref columns) = table.columns {
            let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            parts.push(format!("Columns: {}", column_names.join(", ")));
        }

        if let Some(ref time_col) = table.time_column {
            parts.push(format!("Time column: {}", time_col));
        }

        parts.join(". ")
    }

    /// Convert column to searchable text
    fn column_to_text(&self, table_name: &str, column: &ColumnMetadata) -> String {
        let mut parts = Vec::new();
        parts.push(format!("Column: {}.{}", table_name, column.name));
        
        if let Some(ref description) = column.description {
            parts.push(format!("Description: {}", description));
        }

        if let Some(ref data_type) = column.data_type {
            parts.push(format!("Type: {}", data_type));
        }

        parts.join(". ")
    }
}





