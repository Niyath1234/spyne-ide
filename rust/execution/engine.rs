//! Execution Engine Trait - Core contract for all execution engines
//! 
//! This defines the minimal interface that all execution engines must implement.
//! Engines are pluggable and selected by the Agent as part of its chain of thought.

use crate::error::{RcaError, Result};
use crate::execution::profile::QueryProfile;
use crate::execution::result::QueryResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Execution context passed to engines
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionContext {
    /// User context (for access control)
    pub user: UserContext,
    
    /// Timeout in milliseconds
    pub timeout_ms: u64,
    
    /// Row limit (for preview queries)
    pub row_limit: Option<u64>,
    
    /// Whether this is a preview query
    pub preview: bool,
    
    /// Additional engine-specific parameters
    pub params: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserContext {
    pub user_id: String,
    pub roles: Vec<String>,
}

/// Engine capabilities - what features an engine supports
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EngineCapabilities {
    /// Supports Common Table Expressions (WITH clauses)
    pub supports_ctes: bool,
    
    /// Supports window functions (ROW_NUMBER, RANK, etc.)
    pub supports_window_functions: bool,
    
    /// Supports correlated subqueries
    pub supports_correlated_subqueries: bool,
    
    /// Supports federated queries (multiple data sources)
    pub supports_federated_sources: bool,
    
    /// Maximum data scan size in GB (None = unlimited)
    pub max_data_scan_gb: Option<u64>,
    
    /// Supports streaming results
    pub supports_streaming: bool,
    
    /// Supports transactions
    pub supports_transactions: bool,
}

/// Engine suggestion for agent reasoning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSuggestion {
    pub engine: String,
    pub can_handle: bool,
    pub score: i32,
    pub reasons: Vec<String>,
    pub capabilities: EngineCapabilities,
    /// Estimated execution time in milliseconds (if available)
    pub estimated_time_ms: Option<u64>,
}

/// Execution engine trait - all engines must implement this
#[async_trait]
pub trait ExecutionEngine: Send + Sync {
    /// Engine name (e.g., "duckdb", "trino", "polars")
    fn name(&self) -> &'static str;
    
    /// Engine capabilities
    fn capabilities(&self) -> &EngineCapabilities;
    
    /// Validate that this engine can execute the given query profile
    fn validate(&self, profile: &QueryProfile) -> Result<()>;
    
    /// Execute SQL query
    async fn execute(
        &self,
        sql: &str,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult>;
    
    /// Check if engine is available/healthy
    async fn health_check(&self) -> Result<bool>;
}

/// Engine selection result with reasoning (from agent)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSelection {
    pub engine_name: String,
    pub reasoning: Vec<String>, // Agent's chain of thought reasoning
    pub fallback_available: bool,
}

impl EngineCapabilities {
    /// DuckDB capabilities
    pub fn duckdb() -> Self {
        Self {
            supports_ctes: true,
            supports_window_functions: true,
            supports_correlated_subqueries: true,
            supports_federated_sources: false, // Single-source only
            max_data_scan_gb: Some(100), // Reasonable limit for embedded engine
            supports_streaming: false,
            supports_transactions: true,
        }
    }
    
    /// Trino capabilities
    pub fn trino() -> Self {
        Self {
            supports_ctes: true,
            supports_window_functions: true,
            supports_correlated_subqueries: true,
            supports_federated_sources: true, // Trino's main strength
            max_data_scan_gb: None, // Cluster-dependent, no hard limit
            supports_streaming: true,
            supports_transactions: false, // Trino doesn't support transactions
        }
    }
    
    /// Polars capabilities (preview-only)
    pub fn polars() -> Self {
        Self {
            supports_ctes: false, // Polars doesn't support CTEs in SQL
            supports_window_functions: true,
            supports_correlated_subqueries: false,
            supports_federated_sources: false,
            max_data_scan_gb: Some(10), // Small limit for preview
            supports_streaming: false,
            supports_transactions: false,
        }
    }
}
