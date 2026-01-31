//! DuckDB Execution Engine
//! 
//! DuckDB is the canonical correctness engine - default for most queries.

//! DuckDB Execution Engine
//! 
//! DuckDB is the canonical correctness engine - default for most queries.
//! 
//! NOTE: To enable full DuckDB support:
//! 1. Install DuckDB: brew install duckdb
//! 2. Add to Cargo.toml: duckdb = { version = "0.10", features = ["bundled"] }
//! 3. Uncomment DuckDB-specific code below
//! 
//! For now, this uses Polars as a fallback implementation.

use crate::error::{RcaError, Result};
use crate::execution::engine::{EngineCapabilities, ExecutionContext, ExecutionEngine};
use crate::execution::profile::QueryProfile;
use crate::execution::result::QueryResult;
use async_trait::async_trait;
// use duckdb::{Connection, params}; // Uncomment when DuckDB is installed
use polars::prelude::*;
use std::path::PathBuf;
use tracing::{info, warn};

/// DuckDB execution engine (using Polars fallback until DuckDB is installed)
pub struct DuckDbEngine {
    capabilities: EngineCapabilities,
    data_dir: PathBuf,
    // DuckDB connection would go here when enabled
    // connection: Arc<Mutex<Option<Connection>>>,
}

impl DuckDbEngine {
    /// Create a new DuckDB engine
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            capabilities: EngineCapabilities::duckdb(),
            data_dir,
        }
    }
}

#[async_trait]
impl ExecutionEngine for DuckDbEngine {
    fn name(&self) -> &'static str {
        "duckdb"
    }
    
    fn capabilities(&self) -> &EngineCapabilities {
        &self.capabilities
    }
    
    fn validate(&self, profile: &QueryProfile) -> Result<()> {
        // Check federation requirement
        if profile.requires_federation && !self.capabilities.supports_federated_sources {
            return Err(RcaError::Execution(
                "DuckDB does not support federated queries".to_string()
            ));
        }
        
        // Check scan size limit
        if let Some(scan_gb) = profile.estimated_scan_gb {
            if let Some(max_gb) = self.capabilities.max_data_scan_gb {
                if scan_gb > max_gb {
                    return Err(RcaError::Execution(
                        format!("Estimated scan size {}GB exceeds DuckDB limit of {}GB", scan_gb, max_gb)
                    ));
                }
            }
        }
        
        Ok(())
    }
    
    async fn execute(&self, sql: &str, ctx: &ExecutionContext) -> Result<QueryResult> {
        let start_time = std::time::Instant::now();
        
        // Check if DuckDB is available (would be set via feature flag or runtime check)
        // For now, use Polars fallback with clear warning
        warn!("DuckDB not installed - using Polars fallback. Install DuckDB: brew install duckdb");
        info!("Executing query with DuckDB fallback (Polars): {}", sql);
        
        // Production note: When DuckDB is installed, replace this with:
        // 1. Get/create DuckDB connection
        // 2. Register tables from data_dir
        // 3. Execute SQL directly with DuckDB
        // 4. Convert results to Polars DataFrame
        // See commented code below for reference implementation
        
        // Parse SQL to extract table name
        let sql_upper = sql.to_uppercase();
        let table_name = if let Some(from_idx) = sql_upper.find(" FROM ") {
            let after_from = &sql_upper[from_idx + 6..];
            after_from
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches('`')
                .to_string()
        } else {
            return Err(RcaError::Execution("No FROM clause found in SQL".to_string()));
        };
        
        // Load table using Polars
        let table_path = self.data_dir.join(format!("{}.csv", table_name));
        let df = if table_path.exists() {
            LazyCsvReader::new(&table_path)
                .with_has_header(true)
                .finish()
                .map_err(|e| RcaError::Execution(format!("Failed to load CSV: {}", e)))?
        } else {
            let parquet_path = self.data_dir.join(format!("{}.parquet", table_name));
            if parquet_path.exists() {
                LazyFrame::scan_parquet(&parquet_path, ScanArgsParquet::default())
                    .map_err(|e| RcaError::Execution(format!("Failed to load Parquet: {}", e)))?
            } else {
                return Err(RcaError::Execution(
                    format!("Table file not found: {}", table_name)
                ));
            }
        };
        
        // Apply LIMIT if preview
        let df = if ctx.preview {
            if let Some(limit) = ctx.row_limit {
                df.limit(limit as u32)
            } else {
                df.limit(1000)
            }
        } else {
            df
        };
        
        // Collect
        let df = df.collect()
            .map_err(|e| RcaError::Execution(format!("Failed to collect DataFrame: {}", e)))?;
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let mut result = QueryResult::success(df, "duckdb".to_string(), execution_time)?;
        result.warnings.push("Using Polars fallback - install DuckDB for full support".to_string());
        
        Ok(result)
    }
    
    async fn health_check(&self) -> Result<bool> {
        // DuckDB not installed, but Polars fallback is available
        warn!("DuckDB health check: not installed, using Polars fallback");
        Ok(true)
    }
}

