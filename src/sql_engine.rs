//! SQL Engine Tool Module
//! 
//! Provides SQL execution capability for dynamic graph traversal.
//! Uses DuckDB as the embedded SQL engine (lightweight, fast, supports Parquet/CSV).
//! 
//! This enables the "Traverse → Test → Observe → Decide" pattern:
//! - Agent chooses a node (Table, Join, Filter, Rule, Metric)
//! - Runs a small SQL probe at that node
//! - Observes the result
//! - Decides next step dynamically

use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use tracing::{info, debug, warn};

/// SQL Engine for executing small probe queries
pub struct SqlEngine {
    metadata: Metadata,
    data_dir: PathBuf,
    // DuckDB connection (will be initialized on first use)
    // Note: For now, we'll use Polars with SQL-like operations
    // In production, you'd use duckdb crate: duckdb = "0.10"
}

/// Result of a SQL probe query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProbeResult {
    /// Number of rows returned
    pub row_count: usize,
    
    /// Sample rows (first N rows)
    pub sample_rows: Vec<HashMap<String, serde_json::Value>>,
    
    /// Column names
    pub columns: Vec<String>,
    
    /// Summary statistics (if applicable)
    pub summary: Option<ProbeSummary>,
    
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    
    /// Any warnings or issues
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeSummary {
    /// Distinct count of key columns
    pub distinct_keys: Option<usize>,
    
    /// Null counts per column
    pub null_counts: HashMap<String, usize>,
    
    /// Value ranges (min/max for numeric columns)
    pub value_ranges: HashMap<String, ValueRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueRange {
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
}

impl SqlEngine {
    /// Create a new SQL engine
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self {
            metadata,
            data_dir,
        }
    }
    
    /// Execute a SQL probe query
    /// 
    /// This runs a small SQL query to test a node and observe results.
    /// Used in the dynamic traversal pattern.
    pub async fn execute_probe(&self, sql: &str, max_rows: Option<usize>) -> Result<SqlProbeResult> {
        let start_time = std::time::Instant::now();
        info!("🔍 Executing SQL probe: {}", sql);
        
        // Parse SQL to determine what we're querying
        // For now, we'll use Polars to execute SQL-like operations
        // In production, integrate DuckDB for full SQL support
        
        let result = self.execute_with_polars(sql, max_rows.unwrap_or(100)).await?;
        
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        info!("✅ Probe completed in {}ms, returned {} rows", execution_time, result.row_count);
        
        Ok(result)
    }
    
    /// Probe a filter condition using simple expression parsing
    pub async fn probe_filter(&self, table: &str, condition: &str) -> Result<SqlProbeResult> {
        let df = self.load_table(table).await?;
        let expr = match self.parse_filter_expr(condition) {
            Ok(expr) => expr,
            Err(e) => {
                let mut fallback = self.execute_with_polars(
                    &format!("SELECT * FROM {} LIMIT 100", table),
                    100,
                ).await?;
                fallback.warnings.push(format!("Filter parse failed: {}", e));
                return Ok(fallback);
            }
        };
        
        let filtered = df
            .lazy()
            .filter(expr)
            .limit(100)
            .collect()?;
        
        let columns: Vec<String> = filtered.get_column_names().iter().map(|s| s.to_string()).collect();
        let sample_rows = self.dataframe_to_rows(&filtered, 100)?;
        let summary = self.calculate_summary(&filtered)?;
        
        Ok(SqlProbeResult {
            row_count: filtered.height(),
            sample_rows,
            columns,
            summary: Some(summary),
            execution_time_ms: 0,
            warnings: Vec::new(),
        })
    }
    
    /// Execute SQL using Polars (fallback until DuckDB is integrated)
    async fn execute_with_polars(&self, sql: &str, max_rows: usize) -> Result<SqlProbeResult> {
        // Parse SQL to extract table name and columns
        // This is a simplified parser - in production, use proper SQL parser or DuckDB
        
        let sql_lower = sql.to_lowercase().trim().to_string();
        
        // Try to extract table name from SQL
        let table_name = self.extract_table_name(&sql_lower)?;
        
        // Load the table
        let table = self.metadata
            .tables
            .iter()
            .find(|t| t.name == table_name)
            .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", table_name)))?;
        
        let table_path = PathBuf::from(&self.data_dir).join(&table.path);
        
        // Read table
        let df = if table_path.extension().and_then(|e| e.to_str()) == Some("csv") {
            LazyCsvReader::new(&table_path)
                .with_try_parse_dates(true)
                .with_infer_schema_length(Some(1000))
                .finish()
                .map_err(|e| RcaError::Execution(format!("Failed to read CSV: {}", e)))?
                .limit(max_rows as u32)
                .collect()
                .map_err(|e| RcaError::Execution(format!("Failed to collect: {}", e)))?
        } else {
            LazyFrame::scan_parquet(&table_path, ScanArgsParquet::default())
                .map_err(|e| RcaError::Execution(format!("Failed to scan parquet: {}", e)))?
                .limit(max_rows as u32)
                .collect()?
        };
        
        // Extract columns
        let columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        
        // Convert to sample rows
        let sample_rows = self.dataframe_to_rows(&df, max_rows)?;
        
        // Calculate summary
        let summary = self.calculate_summary(&df)?;
        
        Ok(SqlProbeResult {
            row_count: df.height(),
            sample_rows,
            columns,
            summary: Some(summary),
            execution_time_ms: 0, // Will be set by caller
            warnings: Vec::new(),
        })
    }
    
    fn parse_filter_expr(&self, condition: &str) -> Result<Expr> {
        let condition = condition.trim();
        let upper = condition.to_uppercase();
        if upper.contains(" AND ") || upper.contains(" OR ") {
            return Err(RcaError::Execution(format!("Compound conditions not supported: {}", condition)));
        }
        let operators = [">=", "<=", "!=", "=", ">", "<"];
        for op in operators {
            if let Some((left, right)) = condition.split_once(op) {
                let column = left.trim();
                let value = right.trim();
                return Ok(self.build_comparison_expr(column, op, value));
            }
        }
        Err(RcaError::Execution(format!("Unsupported filter condition: {}", condition)))
    }
    
    fn build_comparison_expr(&self, column: &str, op: &str, raw_value: &str) -> Expr {
        let value_expr = self.parse_value_expr(raw_value);
        match op {
            "=" => col(column).eq(value_expr),
            "!=" => col(column).neq(value_expr),
            ">" => col(column).gt(value_expr),
            "<" => col(column).lt(value_expr),
            ">=" => col(column).gt_eq(value_expr),
            "<=" => col(column).lt_eq(value_expr),
            _ => col(column).eq(value_expr),
        }
    }
    
    fn parse_value_expr(&self, raw_value: &str) -> Expr {
        let trimmed = raw_value.trim().trim_matches('\'').trim_matches('"');
        if let Ok(int_val) = trimmed.parse::<i64>() {
            lit(int_val)
        } else if let Ok(float_val) = trimmed.parse::<f64>() {
            lit(float_val)
        } else if trimmed.eq_ignore_ascii_case("true") {
            lit(true)
        } else if trimmed.eq_ignore_ascii_case("false") {
            lit(false)
        } else {
            lit(trimmed.to_string())
        }
    }
    
    /// Extract table name from SQL (simplified parser)
    fn extract_table_name(&self, sql: &str) -> Result<String> {
        // Look for "from <table>" pattern
        if let Some(from_idx) = sql.find("from") {
            let after_from = &sql[from_idx + 4..].trim();
            // Take first word after "from"
            let table = after_from
                .split_whitespace()
                .next()
                .ok_or_else(|| RcaError::Execution("No table name found after FROM".to_string()))?;
            
            // Remove quotes if present
            let table = table.trim_matches('"').trim_matches('\'').trim_matches('`');
            
            return Ok(table.to_string());
        }
        
        Err(RcaError::Execution("Could not extract table name from SQL".to_string()))
    }
    
    /// Convert DataFrame to JSON rows
    fn dataframe_to_rows(&self, df: &DataFrame, max_rows: usize) -> Result<Vec<HashMap<String, serde_json::Value>>> {
        let mut rows = Vec::new();
        let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        
        // Take first max_rows
        let df_limited = df.head(Some(max_rows));
        
        // Convert each row
        for row_idx in 0..df_limited.height() {
            let mut row_map = HashMap::new();
            
            for col_name in &column_names {
                let series = df_limited.column(col_name)?;
                let value = self.series_value_to_json(series, row_idx)?;
                row_map.insert(col_name.clone(), value);
            }
            
            rows.push(row_map);
        }
        
        Ok(rows)
    }
    
    /// Convert a single value from a Polars Series to JSON
    fn series_value_to_json(&self, series: &Series, row_idx: usize) -> Result<serde_json::Value> {
        use polars::prelude::*;
        
        // Check if null
        let null_mask = series.is_null();
        if null_mask.get(row_idx).unwrap_or(false) {
            return Ok(serde_json::Value::Null);
        }
        
        // Get value based on dtype
        let dtype = series.dtype();
        
        // Get value - series.get() returns AnyValue, need to extract based on type
        let any_val = series.get(row_idx).map_err(|_| RcaError::Execution("Failed to get value from series".to_string()))?;
        
        match dtype {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                if let Ok(val) = any_val.try_extract::<i64>() {
                    Ok(serde_json::Value::Number(serde_json::Number::from(val)))
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                if let Ok(val) = any_val.try_extract::<u64>() {
                    Ok(serde_json::Value::Number(serde_json::Number::from(val)))
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            DataType::Float32 | DataType::Float64 => {
                if let Ok(val) = any_val.try_extract::<f64>() {
                    if let Some(num) = serde_json::Number::from_f64(val) {
                        Ok(serde_json::Value::Number(num))
                    } else {
                        Ok(serde_json::Value::Null)
                    }
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            DataType::String => {
                // For String type, use to_string() or try_extract
                match any_val.to_string().as_str() {
                    "" => Ok(serde_json::Value::Null),
                    s => Ok(serde_json::Value::String(s.to_string())),
                }
            }
            DataType::Boolean => {
                // Try to extract boolean value - use get_str and parse since try_extract doesn't work for bool
                if let Some(s) = any_val.get_str() {
                    match s {
                        "true" | "1" | "yes" => Ok(serde_json::Value::Bool(true)),
                        "false" | "0" | "no" => Ok(serde_json::Value::Bool(false)),
                        _ => Ok(serde_json::Value::Null),
                    }
                } else {
                    // Try direct extraction as u8 and convert
                    if let Ok(val) = any_val.try_extract::<u8>() {
                        Ok(serde_json::Value::Bool(val != 0))
                    } else {
                        Ok(serde_json::Value::Null)
                    }
                }
            }
            DataType::Date => {
                // Convert date to string
                Ok(serde_json::Value::String(format!("{:?}", any_val)))
            }
            DataType::Datetime(_, _) => {
                Ok(serde_json::Value::String(format!("{:?}", any_val)))
            }
            _ => {
                // Fallback: convert to string
                Ok(serde_json::Value::String(format!("{:?}", any_val)))
            }
        }
    }
    
    /// Calculate summary statistics for probe result
    fn calculate_summary(&self, df: &DataFrame) -> Result<ProbeSummary> {
        let mut null_counts = HashMap::new();
        let mut value_ranges = HashMap::new();
        
        for col_name in df.get_column_names() {
            let series = df.column(col_name)?;
            
            // Count nulls
            let null_count = series.null_count();
            null_counts.insert(col_name.to_string(), null_count);
            
            // Calculate ranges for numeric columns
            if series.dtype().is_numeric() {
                if let (Ok(Some(min)), Ok(Some(max))) = (series.min(), series.max()) {
                    value_ranges.insert(
                        col_name.to_string(),
                        ValueRange {
                            min: Some(serde_json::Value::Number(serde_json::Number::from_f64(min).unwrap())),
                            max: Some(serde_json::Value::Number(serde_json::Number::from_f64(max).unwrap())),
                        },
                    );
                }
            }
        }
        
        Ok(ProbeSummary {
            distinct_keys: None, // Would need to know which columns are keys
            null_counts,
            value_ranges,
        })
    }
    
    /// Execute a join probe
    /// 
    /// Tests if a join between two tables would succeed
    pub async fn probe_join(
        &self,
        left_table: &str,
        right_table: &str,
        join_keys: &HashMap<String, String>, // left_col -> right_col mapping
        join_type: &str,
    ) -> Result<SqlProbeResult> {
        info!("🔗 Probing join: {} JOIN {} ON {:?}", left_table, right_table, join_keys);
        
        // Build SQL-like query to test the join
        // For now, use Polars to execute the join
        
        // Load both tables
        let left_df = self.load_table(left_table).await?;
        let right_df = self.load_table(right_table).await?;
        
        // Perform join using Polars
        let join_type_polars = match join_type.to_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Left, // Polars doesn't have Right, swap tables instead
            "outer" | "full" => JoinType::Outer,
            _ => JoinType::Inner,
        };
        
        // Swap tables for right join
        let (left_df_actual, right_df_actual) = if join_type.to_lowercase() == "right" {
            (right_df.clone(), left_df.clone())
        } else {
            (left_df.clone(), right_df.clone())
        };
        
        // Build join keys
        let left_keys: Vec<Expr> = join_keys.keys().map(|k| col(k)).collect();
        let right_keys: Vec<Expr> = join_keys.values().map(|k| col(k)).collect();
        
        let joined = left_df_actual
            .lazy()
            .join(
                right_df_actual.lazy(),
                left_keys,
                right_keys,
                JoinArgs::new(join_type_polars),
            )
            .limit(100) // Limit probe results
            .collect()?;
        
        // Convert to probe result
        let columns: Vec<String> = joined.get_column_names().iter().map(|s| s.to_string()).collect();
        let sample_rows = self.dataframe_to_rows(&joined, 100)?;
        let summary = self.calculate_summary(&joined)?;
        
        Ok(SqlProbeResult {
            row_count: joined.height(),
            sample_rows,
            columns,
            summary: Some(summary),
            execution_time_ms: 0,
            warnings: Vec::new(),
        })
    }
    
    /// Probe join failures by returning rows from the left side with no match on the right
    pub async fn probe_join_failures(
        &self,
        left_table: &str,
        right_table: &str,
        join_keys: &HashMap<String, String>, // left_col -> right_col mapping
        join_type: &str,
    ) -> Result<SqlProbeResult> {
        if join_keys.is_empty() {
            return Err(RcaError::Execution("Join keys are required for join failure probe".to_string()));
        }
        
        // Load both tables
        let left_df = self.load_table(left_table).await?;
        let right_df = self.load_table(right_table).await?;
        
        // Always probe missing matches on the right side
        let join_type_polars = match join_type.to_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Left,
            "outer" | "full" => JoinType::Left,
            _ => JoinType::Left,
        };
        
        let left_keys: Vec<Expr> = join_keys.keys().map(|k| col(k)).collect();
        let right_keys: Vec<Expr> = join_keys.values().map(|k| col(k)).collect();
        
        let left_cols: std::collections::HashSet<String> = left_df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        
        let right_key_columns: Vec<String> = join_keys.values().map(|col_name| {
            if left_cols.contains(col_name) {
                format!("{}_right", col_name)
            } else {
                col_name.clone()
            }
        }).collect();
        
        let joined = left_df
            .lazy()
            .join(
                right_df.lazy(),
                left_keys,
                right_keys,
                JoinArgs::new(join_type_polars),
            );
        
        let mut null_filter: Option<Expr> = None;
        for col_name in right_key_columns {
            let expr = col(&col_name).is_null();
            null_filter = Some(match null_filter {
                Some(existing) => existing.or(expr),
                None => expr,
            });
        }
        
        let filtered = if let Some(filter_expr) = null_filter {
            joined.filter(filter_expr)
        } else {
            joined
        };
        
        let result_df = filtered.limit(100).collect()?;
        
        let columns: Vec<String> = result_df.get_column_names().iter().map(|s| s.to_string()).collect();
        let sample_rows = self.dataframe_to_rows(&result_df, 100)?;
        let summary = self.calculate_summary(&result_df)?;
        
        Ok(SqlProbeResult {
            row_count: result_df.height(),
            sample_rows,
            columns,
            summary: Some(summary),
            execution_time_ms: 0,
            warnings: Vec::new(),
        })
    }
    
    /// Load a table as DataFrame
    async fn load_table(&self, table_name: &str) -> Result<DataFrame> {
        let table = self.metadata
            .tables
            .iter()
            .find(|t| t.name == table_name)
            .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", table_name)))?;
        
        let table_path = PathBuf::from(&self.data_dir).join(&table.path);
        
        if table_path.extension().and_then(|e| e.to_str()) == Some("csv") {
            Ok(LazyCsvReader::new(&table_path)
                .with_try_parse_dates(true)
                .with_infer_schema_length(Some(1000))
                .finish()
                .map_err(|e| RcaError::Execution(format!("Failed to read CSV: {}", e)))?
                .collect()
                .map_err(|e| RcaError::Execution(format!("Failed to collect: {}", e)))?)
        } else {
            Ok(LazyFrame::scan_parquet(&table_path, ScanArgsParquet::default())
                .map_err(|e| RcaError::Execution(format!("Failed to scan parquet: {}", e)))?
                .collect()?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Note: Tests would require mock metadata and data files
}

