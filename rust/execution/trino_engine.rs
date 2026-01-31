//! Trino Execution Engine
//! 
//! Trino is used for federated queries and large-scale distributed execution.

use crate::error::{RcaError, Result};
use crate::execution::engine::{EngineCapabilities, ExecutionContext, ExecutionEngine};
use crate::execution::profile::QueryProfile;
use crate::execution::result::QueryResult;
use async_trait::async_trait;
use polars::prelude::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn, error};

/// Trino execution engine
pub struct TrinoEngine {
    capabilities: EngineCapabilities,
    coordinator_url: String,
    catalog: String,
    schema: String,
    client: Client,
    user: String,
}

// Trino REST API: POST /v1/statement with SQL in body (plain text, not JSON)
// Headers: X-Trino-User, X-Trino-Catalog, X-Trino-Schema

#[derive(Debug, Deserialize)]
struct TrinoQueryResponse {
    id: String,
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    #[serde(rename = "stats")]
    stats: Option<TrinoStats>,
    #[serde(rename = "error")]
    error: Option<TrinoError>,
}

#[derive(Debug, Deserialize)]
struct TrinoStats {
    #[serde(rename = "state")]
    state: String,
    #[serde(rename = "totalRows")]
    total_rows: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TrinoError {
    message: String,
    #[serde(rename = "errorCode")]
    error_code: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct TrinoDataResponse {
    data: Option<Vec<Vec<serde_json::Value>>>,
    columns: Option<Vec<TrinoColumn>>,
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    #[serde(rename = "error")]
    error: Option<TrinoError>,
}

#[derive(Debug, Deserialize)]
struct TrinoColumn {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
}

impl TrinoEngine {
    /// Create a new Trino engine
    /// 
    /// # Arguments
    /// * `coordinator_url` - Trino coordinator URL (e.g., "http://localhost:8080")
    /// * `catalog` - Default catalog name
    /// * `schema` - Default schema name
    /// * `user` - Trino user name
    pub fn new(
        coordinator_url: String,
        catalog: String,
        schema: String,
        user: String,
    ) -> Self {
        // Create HTTP client with proper configuration
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .expect("Failed to create HTTP client for Trino");
        
        Self {
            capabilities: EngineCapabilities::trino(),
            coordinator_url: coordinator_url.trim_end_matches('/').to_string(),
            catalog,
            schema,
            client,
            user,
        }
    }
    
    /// Create Trino engine from environment variables
    pub fn from_env() -> Result<Self> {
        let coordinator_url = std::env::var("TRINO_COORDINATOR_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        let catalog = std::env::var("TRINO_CATALOG")
            .unwrap_or_else(|_| "memory".to_string());
        let schema = std::env::var("TRINO_SCHEMA")
            .unwrap_or_else(|_| "default".to_string());
        let user = std::env::var("TRINO_USER")
            .unwrap_or_else(|_| "admin".to_string());
        
        Ok(Self::new(coordinator_url, catalog, schema, user))
    }
    
    /// Adapt SQL to Trino dialect (minimal changes needed)
    fn adapt_sql(&self, sql: &str) -> String {
        // Trino is mostly ANSI SQL compatible
        // Only minor adaptations needed
        
        let mut adapted = sql.to_string();
        
        // Replace CURRENT_DATE() with current_date (Trino function call style)
        adapted = adapted.replace("CURRENT_DATE()", "current_date");
        adapted = adapted.replace("CURRENT_TIMESTAMP()", "current_timestamp");
        
        // Trino uses different timestamp casting syntax in some cases
        // But most ANSI SQL should work as-is
        
        adapted
    }
    
    /// Submit query to Trino REST API
    /// 
    /// Trino REST API expects:
    /// - POST /v1/statement
    /// - Body: SQL query as plain text
    /// - Headers: X-Trino-User, X-Trino-Catalog, X-Trino-Schema
    async fn submit_query(&self, sql: &str) -> Result<TrinoQueryResponse> {
        let url = format!("{}/v1/statement", self.coordinator_url);
        
        let response = self.client
            .post(&url)
            .header("X-Trino-User", &self.user)
            .header("X-Trino-Catalog", &self.catalog)
            .header("X-Trino-Schema", &self.schema)
            .header("Content-Type", "text/plain")
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| RcaError::Execution(format!("Failed to submit Trino query: {}", e)))?;
        
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(RcaError::Execution(
                format!("Trino query failed with status {}: {}", status, text)
            ));
        }
        
        let query_response: TrinoQueryResponse = response.json().await
            .map_err(|e| RcaError::Execution(format!("Failed to parse Trino response: {}", e)))?;
        
        if let Some(error) = &query_response.error {
            return Err(RcaError::Execution(
                format!("Trino error: {} (code: {:?})", error.message, error.error_code)
            ));
        }
        
        Ok(query_response)
    }
    
    /// Fetch query results with retry logic
    async fn fetch_results(&self, next_uri: &str) -> Result<TrinoDataResponse> {
        // Trino returns nextUri in responses - use it directly
        let url = if next_uri.starts_with("http") {
            next_uri.to_string()
        } else {
            format!("{}{}", self.coordinator_url, next_uri)
        };
        
        // Retry logic for transient failures
        let mut retries = 3;
        loop {
            let response = match self.client
                .get(&url)
                .header("X-Trino-User", &self.user)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    if retries > 0 && e.is_timeout() {
                        retries -= 1;
                        warn!("Trino fetch timeout, retrying... ({} retries left)", retries);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    return Err(RcaError::Execution(format!("Failed to fetch Trino results: {}", e)));
                }
            };
            
            if !response.status().is_success() {
                let status = response.status();
                // Retry on 5xx errors
                if status.is_server_error() && retries > 0 {
                    retries -= 1;
                    warn!("Trino server error {}, retrying... ({} retries left)", status, retries);
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    continue;
                }
                let text = response.text().await.unwrap_or_default();
                return Err(RcaError::Execution(
                    format!("Trino fetch failed with status {}: {}", status, text)
                ));
            }
            
            let data_response: TrinoDataResponse = match response.json().await {
                Ok(d) => d,
                Err(e) => {
                    if retries > 0 {
                        retries -= 1;
                        warn!("Trino JSON parse error, retrying... ({} retries left)", retries);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    return Err(RcaError::Execution(format!("Failed to parse Trino data: {}", e)));
                }
            };
            
            if let Some(error) = &data_response.error {
                // Don't retry on query errors (4xx), only on system errors
                if error.error_code.map(|c| c >= 500).unwrap_or(false) && retries > 0 {
                    retries -= 1;
                    warn!("Trino query error {}, retrying... ({} retries left)", error.message, retries);
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    continue;
                }
                return Err(RcaError::Execution(
                    format!("Trino error: {} (code: {:?})", error.message, error.error_code)
                ));
            }
            
            return Ok(data_response);
        }
    }
    
    /// Convert Trino results to Polars DataFrame with proper null handling
    fn trino_to_dataframe(
        &self,
        columns: &[TrinoColumn],
        data: &[Vec<serde_json::Value>],
    ) -> Result<DataFrame> {
        if columns.is_empty() {
            return Err(RcaError::Execution("No columns in Trino response".to_string()));
        }
        
        let mut series_vec = Vec::new();
        
        for (col_idx, col) in columns.iter().enumerate() {
            let values: Vec<serde_json::Value> = data.iter()
                .map(|row| row.get(col_idx).cloned().unwrap_or(serde_json::Value::Null))
                .collect();
            
            // Create series based on column type with proper null handling
            let series = match col.column_type.as_str() {
                "bigint" | "integer" | "smallint" | "tinyint" => {
                    // Use Option<i64> to handle nulls properly
                    let nums: Vec<Option<i64>> = values.iter()
                        .map(|v| {
                            if v.is_null() {
                                None
                            } else {
                                v.as_i64()
                            }
                        })
                        .collect();
                    Series::new(&col.name, nums)
                }
                "double" | "real" | "decimal" => {
                    // Use Option<f64> to handle nulls properly
                    let nums: Vec<Option<f64>> = values.iter()
                        .map(|v| {
                            if v.is_null() {
                                None
                            } else {
                                v.as_f64()
                            }
                        })
                        .collect();
                    Series::new(&col.name, nums)
                }
                "boolean" => {
                    // Use Option<bool> to handle nulls properly
                    let bools: Vec<Option<bool>> = values.iter()
                        .map(|v| {
                            if v.is_null() {
                                None
                            } else {
                                v.as_bool()
                            }
                        })
                        .collect();
                    Series::new(&col.name, bools)
                }
                "date" | "timestamp" | "timestamp with time zone" => {
                    // Convert to string for date/timestamp types
                    let strings: Vec<String> = values.iter()
                        .map(|v| {
                            if v.is_null() {
                                String::new()
                            } else {
                                v.to_string()
                            }
                        })
                        .collect();
                    Series::new(&col.name, strings)
                }
                _ => {
                    // String type (varchar, char, etc.)
                    let strings: Vec<String> = values.iter()
                        .map(|v| {
                            if v.is_null() {
                                String::new()
                            } else if v.is_string() {
                                v.as_str().unwrap_or("").to_string()
                            } else {
                                v.to_string()
                            }
                        })
                        .collect();
                    Series::new(&col.name, strings)
                }
            };
            
            series_vec.push(series);
        }
        
        DataFrame::new(series_vec)
            .map_err(|e| RcaError::Execution(format!("Failed to create DataFrame: {}", e)))
    }
}

#[async_trait]
impl ExecutionEngine for TrinoEngine {
    fn name(&self) -> &'static str {
        "trino"
    }
    
    fn capabilities(&self) -> &EngineCapabilities {
        &self.capabilities
    }
    
    fn validate(&self, profile: &QueryProfile) -> Result<()> {
        // Trino can handle everything, but check if it's really needed
        if !profile.requires_federation {
            if let Some(scan_gb) = profile.estimated_scan_gb {
                if scan_gb < 100 {
                    warn!("Small query ({:?}GB) could use DuckDB instead of Trino", scan_gb);
                }
            }
        }
        
        Ok(())
    }
    
    async fn execute(&self, sql: &str, ctx: &ExecutionContext) -> Result<QueryResult> {
        let start_time = std::time::Instant::now();
        info!("Executing query with Trino: {}", sql);
        
        // Apply SQL dialect adapter
        let adapted_sql = self.adapt_sql(sql);
        
        // Apply LIMIT if preview
        let final_sql = if ctx.preview {
            if let Some(limit) = ctx.row_limit {
                format!("{} LIMIT {}", adapted_sql.trim_end_matches(';'), limit)
            } else {
                format!("{} LIMIT 1000", adapted_sql.trim_end_matches(';'))
            }
        } else {
            adapted_sql
        };
        
        // Submit query to Trino
        let query_response = self.submit_query(&final_sql).await?;
        let query_id = query_response.id.clone();
        info!("Trino query submitted: {}", query_id);
        
        // Poll for results using nextUri (Trino uses async polling)
        let mut all_data = Vec::new();
        let mut columns: Option<Vec<TrinoColumn>> = None;
        let mut next_uri = query_response.next_uri;
        let mut poll_count = 0;
        const MAX_POLLS: usize = 10000; // Prevent infinite loops
        
        while let Some(ref uri) = next_uri {
            // Check timeout before each poll
            if start_time.elapsed().as_millis() as u64 > ctx.timeout_ms {
                return Err(RcaError::Execution(
                    format!("Trino query timeout after {}ms", ctx.timeout_ms)
                ));
            }
            
            // Prevent infinite polling
            poll_count += 1;
            if poll_count > MAX_POLLS {
                return Err(RcaError::Execution(
                    format!("Trino query exceeded max polls ({})", MAX_POLLS)
                ));
            }
            
            let response = self.fetch_results(uri).await?;
            
            // Extract columns from first response
            if let Some(cols) = response.columns {
                if columns.is_none() {
                    columns = Some(cols);
                }
            }
            
            // Accumulate data
            if let Some(data) = response.data {
                all_data.extend(data);
            }
            
            next_uri = response.next_uri;
            
            // Exponential backoff for polling (start with 100ms, max 1s)
            let delay_ms = (100 * (poll_count.min(10))) as u64;
            tokio::time::sleep(Duration::from_millis(delay_ms.min(1000))).await;
        }
        
        let columns = columns.ok_or_else(|| RcaError::Execution("No columns in Trino response".to_string()))?;
        
        // Convert to DataFrame
        let df = self.trino_to_dataframe(&columns, &all_data)?;
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        let mut result = QueryResult::success(df, "trino".to_string(), execution_time)?;
        result.engine_metadata.insert(
            "query_id".to_string(),
            serde_json::Value::String(query_id),
        );
        
        Ok(result)
    }
    
    async fn health_check(&self) -> Result<bool> {
        // Health check - verify Trino coordinator is accessible
        let url = format!("{}/v1/info", self.coordinator_url);
        match self.client.get(&url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    Ok(true)
                } else {
                    error!("Trino health check failed: status {}", response.status());
                    Ok(false)
                }
            }
            Err(e) => {
                error!("Trino health check failed: {}", e);
                Ok(false)
            }
        }
    }
}

