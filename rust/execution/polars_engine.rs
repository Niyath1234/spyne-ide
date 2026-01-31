//! Polars Execution Engine (Preview Only)
//! 
//! Polars is used for small, simple queries as a preview engine.
//! Not recommended for production use.

use crate::error::{RcaError, Result};
use crate::execution::engine::{EngineCapabilities, ExecutionContext, ExecutionEngine};
use crate::execution::profile::QueryProfile;
use crate::execution::result::QueryResult;
use async_trait::async_trait;
use polars::prelude::*;
use std::path::PathBuf;
use tracing::{info, warn};

/// Polars execution engine (preview-only)
pub struct PolarsEngine {
    capabilities: EngineCapabilities,
    data_dir: PathBuf,
}

impl PolarsEngine {
    /// Create a new Polars engine
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            capabilities: EngineCapabilities::polars(),
            data_dir,
        }
    }
}

#[async_trait]
impl ExecutionEngine for PolarsEngine {
    fn name(&self) -> &'static str {
        "polars"
    }
    
    fn capabilities(&self) -> &EngineCapabilities {
        &self.capabilities
    }
    
    fn validate(&self, profile: &QueryProfile) -> Result<()> {
        // Polars limitations
        if profile.uses_ctes {
            return Err(RcaError::Execution(
                "Polars does not support CTEs".to_string()
            ));
        }
        
        if profile.requires_federation {
            return Err(RcaError::Execution(
                "Polars does not support federated queries".to_string()
            ));
        }
        
        if let Some(scan_gb) = profile.estimated_scan_gb {
            if let Some(max_gb) = self.capabilities.max_data_scan_gb {
                if scan_gb > max_gb {
                    return Err(RcaError::Execution(
                        format!("Estimated scan size {}GB exceeds Polars preview limit of {}GB", scan_gb, max_gb)
                    ));
                }
            }
        }
        
        warn!("Using Polars preview engine - results may be limited");
        
        Ok(())
    }
    
    async fn execute(&self, sql: &str, ctx: &ExecutionContext) -> Result<QueryResult> {
        let start_time = std::time::Instant::now();
        info!("Executing query with Polars (preview): {}", sql);
        
        // Polars SQL support is limited - this is a simplified implementation
        // In production, you'd use Polars' SQL context or convert to DataFrame operations
        
        // For now, we'll parse SQL to extract table name and basic operations
        let sql_upper = sql.to_uppercase();
        
        // Extract table name
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
        
        // Load table
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
        
        let mut result = QueryResult::success(df, "polars".to_string(), execution_time)?;
        result.warnings.push("Polars preview engine - limited SQL support".to_string());
        
        Ok(result)
    }
    
    async fn health_check(&self) -> Result<bool> {
        // Polars is always available (in-process)
        Ok(true)
    }
}

