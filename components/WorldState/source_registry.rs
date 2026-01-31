//! API Source Registry - Tracks available API endpoints and their discovered columns
//! 
//! This registry maintains a "data pool" of all available columns from all API sources,
//! allowing contracts to reference columns from multiple endpoints.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A column discovered from an API source
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolColumn {
    /// API column name (e.g., "customer_id")
    pub column_name: String,
    
    /// Inferred data type
    pub data_type: String,
    
    /// Is nullable (true if we've seen null values)
    pub nullable: bool,
    
    /// Sample values seen (for semantic tagging and validation)
    pub sample_values: Vec<String>,
    
    /// Which endpoint this column came from
    pub source_endpoint: String,
    
    /// When this column was first discovered
    pub discovered_at: u64,
    
    /// When this column was last seen/updated
    pub last_seen_at: u64,
}

impl PoolColumn {
    /// Create a new pool column
    pub fn new(
        column_name: String,
        data_type: String,
        nullable: bool,
        source_endpoint: String,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            column_name,
            data_type,
            nullable,
            sample_values: Vec::new(),
            source_endpoint,
            discovered_at: now,
            last_seen_at: now,
        }
    }
    
    /// Get unique pool column ID (format: "{endpoint}::{column_name}")
    pub fn pool_column_id(&self) -> String {
        format!("{}::{}", self.source_endpoint, self.column_name)
    }
    
    /// Update column with new data (merge types, update nullable, add samples)
    pub fn update(&mut self, data_type: String, nullable: bool, sample: Option<String>) {
        // Merge types (use more general type)
        self.data_type = Self::merge_types(&self.data_type, &data_type);
        self.nullable = self.nullable || nullable;
        self.last_seen_at = Self::now_timestamp();
        
        // Add sample if provided and not already present
        if let Some(sample) = sample {
            if !self.sample_values.contains(&sample) && self.sample_values.len() < 10 {
                self.sample_values.push(sample);
            }
        }
    }
    
    /// Merge two types (return the more general type)
    fn merge_types(type1: &str, type2: &str) -> String {
        if type1 == type2 {
            return type1.to_string();
        }
        
        // Type hierarchy: INT < FLOAT < VARCHAR
        match (type1.to_uppercase().as_str(), type2.to_uppercase().as_str()) {
            ("INT" | "INT64" | "INT32", "FLOAT" | "FLOAT64" | "FLOAT32") => "FLOAT64".to_string(),
            ("FLOAT" | "FLOAT64" | "FLOAT32", "INT" | "INT64" | "INT32") => "FLOAT64".to_string(),
            (_, "VARCHAR" | "STRING" | "TEXT") => "VARCHAR".to_string(),
            ("VARCHAR" | "STRING" | "TEXT", _) => "VARCHAR".to_string(),
            _ => type1.to_string(), // Default to first type
        }
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// An API source (endpoint) with its discovered columns
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiSource {
    /// API endpoint URL
    pub endpoint: String,
    
    /// Columns discovered from this endpoint
    pub discovered_columns: Vec<PoolColumn>,
    
    /// When this source was first discovered
    pub first_discovered_at: u64,
    
    /// When this source was last accessed
    pub last_discovered_at: u64,
    
    /// Number of times this source has been accessed
    pub access_count: u64,
}

impl ApiSource {
    pub fn new(endpoint: String) -> Self {
        let now = Self::now_timestamp();
        Self {
            endpoint,
            discovered_columns: Vec::new(),
            first_discovered_at: now,
            last_discovered_at: now,
            access_count: 0,
        }
    }
    
    /// Register or update a column from this source
    pub fn register_column(&mut self, column: PoolColumn) {
        // Check if column already exists
        if let Some(existing) = self.discovered_columns.iter_mut()
            .find(|c| c.column_name == column.column_name) {
            // Update existing column
            existing.update(
                column.data_type.clone(),
                column.nullable,
                column.sample_values.first().cloned(),
            );
        } else {
            // Add new column
            self.discovered_columns.push(column);
        }
        self.last_discovered_at = Self::now_timestamp();
        self.access_count += 1;
    }
    
    /// Get a column by name
    pub fn get_column(&self, column_name: &str) -> Option<&PoolColumn> {
        self.discovered_columns.iter()
            .find(|c| c.column_name == column_name)
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// API Source Registry - maintains the data pool of all available API columns
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiSourceRegistry {
    /// Endpoint → ApiSource mapping
    sources: HashMap<String, ApiSource>,
    
    /// Pool column ID → PoolColumn mapping (for fast lookup across all sources)
    pool_columns: HashMap<String, PoolColumn>,
}

impl ApiSourceRegistry {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            pool_columns: HashMap::new(),
        }
    }
    
    /// Register or update an API source and its columns
    pub fn register_source(&mut self, endpoint: String, columns: Vec<PoolColumn>) {
        let source = self.sources.entry(endpoint.clone())
            .or_insert_with(|| ApiSource::new(endpoint.clone()));
        
        for column in columns {
            let pool_id = column.pool_column_id();
            source.register_column(column.clone());
            self.pool_columns.insert(pool_id, column);
        }
    }
    
    /// Register a single column from an endpoint
    pub fn register_column(&mut self, endpoint: String, column: PoolColumn) {
        let source = self.sources.entry(endpoint.clone())
            .or_insert_with(|| ApiSource::new(endpoint.clone()));
        
        let pool_id = column.pool_column_id();
        source.register_column(column.clone());
        self.pool_columns.insert(pool_id, column);
    }
    
    /// Get an API source by endpoint
    pub fn get_source(&self, endpoint: &str) -> Option<&ApiSource> {
        self.sources.get(endpoint)
    }
    
    /// Get all sources
    pub fn list_sources(&self) -> Vec<&ApiSource> {
        self.sources.values().collect()
    }
    
    /// Get a pool column by its ID
    pub fn get_pool_column(&self, pool_column_id: &str) -> Option<&PoolColumn> {
        self.pool_columns.get(pool_column_id)
    }
    
    /// Get all columns from a specific endpoint
    pub fn get_columns_for_endpoint(&self, endpoint: &str) -> Vec<&PoolColumn> {
        self.sources.get(endpoint)
            .map(|source| source.discovered_columns.iter().collect())
            .unwrap_or_default()
    }
    
    /// Get all pool columns (across all sources)
    pub fn list_all_columns(&self) -> Vec<&PoolColumn> {
        self.pool_columns.values().collect()
    }
    
    /// Find columns by name (across all sources)
    pub fn find_columns_by_name(&self, column_name: &str) -> Vec<&PoolColumn> {
        self.pool_columns.values()
            .filter(|c| c.column_name == column_name)
            .collect()
    }
    
    /// Check if an endpoint is registered
    pub fn has_endpoint(&self, endpoint: &str) -> bool {
        self.sources.contains_key(endpoint)
    }
    
    /// Remove an endpoint and its columns
    pub fn remove_endpoint(&mut self, endpoint: &str) {
        if let Some(source) = self.sources.remove(endpoint) {
            // Remove all pool columns from this source
            for column in source.discovered_columns {
                let pool_id = column.pool_column_id();
                self.pool_columns.remove(&pool_id);
            }
        }
    }
}

impl Default for ApiSourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

