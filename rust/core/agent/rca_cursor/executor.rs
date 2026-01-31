//! Execution Engine for RcaCursor
//! 
//! Executes ExecutionPlan nodes deterministically with grain-normalized output.
//! Guarantees exactly one row per grain_key after aggregation.

use crate::core::agent::rca_cursor::planner::{ExecutionNode, ExecutionPlan, JoinStrategy, SamplingStrategy, SamplingMethod, SamplingSize, StopConditions};
use crate::core::engine::{materialize::RowMaterializationEngine, logical_plan::FilterExpr, storage::{DataSource, create_table_reader}};
use crate::metadata::Metadata;
use crate::error::{RcaError, Result};
use polars::prelude::*;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use rand::Rng;

/// Small table threshold for broadcast join (default: 10MB)
const SMALL_TABLE_THRESHOLD_BYTES: u64 = 10 * 1024 * 1024;

/// Execution result with grain-normalized output
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// Output schema
    pub schema: Schema,
    /// Number of rows (should equal number of distinct grain_key values)
    pub row_count: usize,
    /// Output dataframe (grain-normalized: one row per grain_key)
    pub dataframe: DataFrame,
    /// Grain key column name
    pub grain_key: String,
    /// Execution metadata
    pub metadata: ExecutionMetadata,
}

/// Execution metadata
#[derive(Debug, Clone)]
pub struct ExecutionMetadata {
    /// Execution time
    pub execution_time: Duration,
    /// Rows scanned
    pub rows_scanned: usize,
    /// Memory used in MB (estimated)
    pub memory_mb: f64,
    /// Nodes executed
    pub nodes_executed: usize,
    /// Filter selectivity (if applicable)
    pub filter_selectivity: Option<f64>,
    /// Join selectivity (if applicable)
    pub join_selectivity: Option<f64>,
}

/// Execution engine
pub struct ExecutionEngine {
    metadata: Metadata,
    data_dir: PathBuf,
    materialization_engine: RowMaterializationEngine,
}

impl ExecutionEngine {
    /// Create a new execution engine
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        let materialization_engine = RowMaterializationEngine::new(metadata.clone(), data_dir.clone());
        Self {
            metadata,
            data_dir,
            materialization_engine,
        }
    }

    /// Execute an execution plan
    /// 
    /// Executes all nodes in the plan and returns a grain-normalized result.
    /// Guarantees exactly one row per grain_key.
    pub async fn execute(
        &self,
        execution_plan: &ExecutionPlan,
        grain_key: &str,
    ) -> Result<ExecutionResult> {
        let start_time = Instant::now();
        let mut current_df: Option<DataFrame> = None;
        let mut rows_scanned = 0;
        let mut nodes_executed = 0;
        let mut filter_selectivity: Option<f64> = None;
        let mut join_selectivity: Option<f64> = None;

        // Check stop conditions before starting
        self.check_stop_conditions(&execution_plan.stop_conditions, 0, start_time)?;

        // Execute nodes in order
        for node in &execution_plan.nodes {
            nodes_executed += 1;

            // Check stop conditions before each node
            let current_rows = current_df.as_ref().map(|df| df.height()).unwrap_or(0);
            self.check_stop_conditions(&execution_plan.stop_conditions, current_rows, start_time)?;

            match node {
                ExecutionNode::Scan { table, filters, sampling } => {
                    let df = self.execute_scan(table, filters, sampling.as_ref()).await?;
                    rows_scanned += df.height();
                    current_df = Some(df);
                }
                ExecutionNode::Join { left_table: _, right_table, keys, join_type, strategy } => {
                    let left_df = current_df.take().ok_or_else(|| {
                        RcaError::Execution("No left dataframe for join".to_string())
                    })?;
                    let right_df = self.execute_scan(right_table, &[], None).await?;
                    
                    let joined_df = self.execute_join(
                        &left_df,
                        &right_df,
                        keys,
                        join_type,
                        strategy,
                    ).await?;
                    
                    join_selectivity = Some(joined_df.height() as f64 / (left_df.height() * right_df.height()).max(1) as f64);
                    current_df = Some(joined_df);
                }
                ExecutionNode::Filter { expr, pushdown } => {
                    if *pushdown {
                        // Filter was already pushed down to scan, skip
                        continue;
                    }
                    
                    let df = current_df.take().ok_or_else(|| {
                        RcaError::Execution("No dataframe for filter".to_string())
                    })?;
                    
                    let filtered_df = self.execute_filter(&df, expr).await?;
                    filter_selectivity = Some(filtered_df.height() as f64 / df.height().max(1) as f64);
                    current_df = Some(filtered_df);
                }
                ExecutionNode::Aggregate { group_by, aggregations } => {
                    let df = current_df.take().ok_or_else(|| {
                        RcaError::Execution("No dataframe for aggregation".to_string())
                    })?;
                    
                    let aggregated_df = self.execute_aggregate(&df, group_by, aggregations).await?;
                    current_df = Some(aggregated_df);
                }
            }
        }

        // Ensure grain-normalized output
        let final_df = current_df.ok_or_else(|| {
            RcaError::Execution("No result dataframe produced".to_string())
        })?;

        // Verify grain normalization: ensure grain_key exists and is unique
        if final_df.column(grain_key).is_err() {
            return Err(RcaError::Execution(format!(
                "Grain key '{}' not found in result dataframe",
                grain_key
            )));
        }

        // Sort by grain_key for deterministic ordering
        let sorted_df = final_df
            .lazy()
            .sort_by_exprs(vec![col(grain_key)], SortMultipleOptions::default())
            .collect()?;

        // Verify uniqueness (should be one row per grain_key)
        let distinct_count = sorted_df
            .clone()
            .lazy()
            .select([col(grain_key)])
            .unique(None, UniqueKeepStrategy::First)
            .collect()?
            .height();

        if distinct_count != sorted_df.height() {
            return Err(RcaError::Execution(format!(
                "Grain normalization failed: {} distinct grain keys but {} rows",
                distinct_count,
                sorted_df.height()
            )));
        }

        let execution_time = start_time.elapsed();
        let memory_mb = self.estimate_memory_usage(&sorted_df);

        Ok(ExecutionResult {
            schema: sorted_df.schema().clone(),
            row_count: sorted_df.height(),
            dataframe: sorted_df,
            grain_key: grain_key.to_string(),
            metadata: ExecutionMetadata {
                execution_time,
                rows_scanned,
                memory_mb,
                nodes_executed,
                filter_selectivity,
                join_selectivity,
            },
        })
    }

    /// Execute a scan node
    async fn execute_scan(
        &self,
        table: &str,
        filters: &[String],
        sampling: Option<&SamplingStrategy>,
    ) -> Result<DataFrame> {
        // Find table metadata
        let table_meta = self.metadata
            .tables
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", table)))?;

        // Build path
        let table_path = self.resolve_table_path(&table_meta.path)?;

        // Parse filter expressions
        let filter_exprs: Vec<FilterExpr> = filters.iter()
            .filter_map(|f| self.parse_filter_string(f).ok())
            .collect();

        // Create data source and reader
        let source = DataSource::from_path(&table_path);
        let reader = create_table_reader(&source)?;

        // Read with filters (pushdown)
        let mut df = reader.read(Some(&filter_exprs))?;

        // Apply sampling if specified
        if let Some(sampling) = sampling {
            df = self.apply_sampling(&df, sampling)?;
        }

        Ok(df)
    }

    /// Execute a join operation with optimizations
    /// 
    /// Automatically selects optimal join strategy:
    /// - Broadcast join for small tables (<10MB)
    /// - Hash join for larger tables (default)
    /// - Validates join keys and checks for nulls
    async fn execute_join(
        &self,
        left_df: &DataFrame,
        right_df: &DataFrame,
        keys: &[String],
        join_type: &str,
        strategy: &JoinStrategy,
    ) -> Result<DataFrame> {
        // Handle right join by swapping tables (Polars doesn't have Right join)
        let (actual_left_df, actual_right_df, actual_join_type) = match join_type.to_lowercase().as_str() {
            "right" => {
                // Swap tables and use left join instead
                (right_df, left_df, "left")
            }
            _ => (left_df, right_df, join_type)
        };

        // Validate join keys
        for key in keys {
            if actual_left_df.column(key).is_err() {
                return Err(RcaError::Execution(format!(
                    "Join key '{}' not found in left dataframe",
                    key
                )));
            }
            if actual_right_df.column(key).is_err() {
                return Err(RcaError::Execution(format!(
                    "Join key '{}' not found in right dataframe",
                    key
                )));
            }
        }

        // Check for nulls in join keys (warn but don't fail)
        self.validate_join_keys_for_nulls(actual_left_df, actual_right_df, keys)?;

        // Convert join type string to Polars JoinType
        let polars_join_type = match actual_join_type.to_lowercase().as_str() {
            "inner" => polars::prelude::JoinType::Inner,
            "left" => polars::prelude::JoinType::Left,
            "outer" | "full" => polars::prelude::JoinType::Outer,
            _ => polars::prelude::JoinType::Inner,
        };

        // Determine optimal join strategy if not explicitly specified
        let effective_strategy = if matches!(strategy, JoinStrategy::HashJoin) {
            // Auto-detect: use broadcast if right table is small
            let right_size_bytes = self.estimate_dataframe_size(actual_right_df);
            if right_size_bytes < SMALL_TABLE_THRESHOLD_BYTES {
                JoinStrategy::BroadcastJoin
            } else {
                JoinStrategy::HashJoin
            }
        } else {
            strategy.clone()
        };

        // Build join keys
        let left_keys: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
        let right_keys: Vec<Expr> = keys.iter().map(|k| col(k)).collect();

        // Clone dataframes for join (lazy() consumes)
        let left_df_clone = actual_left_df.clone();
        let right_df_clone = actual_right_df.clone();

        // Execute join based on strategy
        match effective_strategy {
            JoinStrategy::HashJoin => {
                // Hash join - Polars default, efficient for large tables
                let left_lazy = left_df_clone.lazy();
                let right_lazy = right_df_clone.lazy();
                let join_args = JoinArgs::new(polars_join_type);
                left_lazy
                    .join(right_lazy, left_keys, right_keys, join_args)
                    .collect()
                    .map_err(|e| RcaError::Execution(format!("Hash join failed: {}", e)))
            }
            JoinStrategy::BroadcastJoin => {
                // Broadcast join - efficient for small right table
                // Polars automatically broadcasts small tables, but we can optimize further
                let left_lazy = left_df_clone.lazy();
                let right_lazy = right_df_clone.lazy();
                
                // For broadcast join, ensure right table is small
                // Polars will handle broadcasting automatically
                let join_args = JoinArgs::new(polars_join_type);
                left_lazy
                    .join(right_lazy, left_keys, right_keys, join_args)
                    .collect()
                    .map_err(|e| RcaError::Execution(format!("Broadcast join failed: {}", e)))
            }
            JoinStrategy::NestedLoopJoin => {
                // Nested loop join - only for very small tables
                // Polars doesn't have explicit nested loop, use regular join
                let left_lazy = left_df_clone.lazy();
                let right_lazy = right_df_clone.lazy();
                let join_args = JoinArgs::new(polars_join_type);
                left_lazy
                    .join(right_lazy, left_keys, right_keys, join_args)
                    .collect()
                    .map_err(|e| RcaError::Execution(format!("Join failed: {}", e)))
            }
        }
    }
    
    /// Validate join keys for null values
    fn validate_join_keys_for_nulls(
        &self,
        left_df: &DataFrame,
        right_df: &DataFrame,
        keys: &[String],
    ) -> Result<()> {
        for key in keys {
            // Check left dataframe
            if let Ok(series) = left_df.column(key) {
                let null_count = series.null_count();
                if null_count > 0 {
                    // Warn but don't fail - nulls in join keys can cause issues
                    // In production, this could be logged as a warning
                }
            }
            
            // Check right dataframe
            if let Ok(series) = right_df.column(key) {
                let null_count = series.null_count();
                if null_count > 0 {
                    // Warn but don't fail
                }
            }
        }
        Ok(())
    }
    
    /// Estimate dataframe size in bytes
    fn estimate_dataframe_size(&self, df: &DataFrame) -> u64 {
        // Rough estimate: rows * columns * avg_bytes_per_cell
        // More accurate would be to sum actual column sizes
        let rows = df.height();
        let cols = df.width();
        let avg_bytes_per_cell = 100; // Conservative estimate
        (rows * cols * avg_bytes_per_cell) as u64
    }

    /// Execute a filter operation
    async fn execute_filter(
        &self,
        df: &DataFrame,
        expr: &str,
    ) -> Result<DataFrame> {
        // Parse filter expression
        let filter_expr = self.parse_filter_string(expr)?;
        let polars_expr = filter_expr.to_polars_expr()?;

        // Apply filter (clone for lazy())
        df.clone()
            .lazy()
            .filter(polars_expr)
            .collect()
            .map_err(|e| RcaError::Execution(format!("Filter failed: {}", e)))
    }

    /// Execute an aggregation operation
    async fn execute_aggregate(
        &self,
        df: &DataFrame,
        group_by: &[String],
        aggregations: &[String],
    ) -> Result<DataFrame> {
        // Build group by expressions
        let group_by_exprs: Vec<Expr> = group_by.iter().map(|c| col(c)).collect();

        // Parse aggregation expressions
        // Format: "metric_name: Sum(column_name)" or "metric_name: Count(column_name)"
        let mut agg_exprs: Vec<Expr> = Vec::new();
        let mut agg_names: Vec<String> = Vec::new();

        for agg_str in aggregations {
            // Parse aggregation string
            // Simplified parser - assumes format "name: AggType(column)"
            if let Some((name, expr_part)) = agg_str.split_once(':') {
                let name = name.trim();
                let expr_part = expr_part.trim();
                
                // Extract aggregation type and column
                if let Some(agg_type_end) = expr_part.find('(') {
                    let agg_type = &expr_part[..agg_type_end];
                    let column_start = agg_type_end + 1;
                    if let Some(column_end) = expr_part[column_start..].find(')') {
                        let column = &expr_part[column_start..column_start + column_end];
                        
                        let expr = match agg_type.trim() {
                            "Sum" => col(column).sum(),
                            "Count" => col(column).count(),
                            "Avg" | "Mean" => col(column).mean(),
                            "Min" => col(column).min(),
                            "Max" => col(column).max(),
                            _ => return Err(RcaError::Execution(format!(
                                "Unsupported aggregation type: {}",
                                agg_type
                            ))),
                        };
                        
                        agg_exprs.push(expr.alias(name));
                        agg_names.push(name.to_string());
                    }
                }
            }
        }

        // Perform aggregation (clone for lazy())
        let aggregated = df
            .clone()
            .lazy()
            .group_by(group_by_exprs)
            .agg(agg_exprs)
            .collect()?;

        Ok(aggregated)
    }

    /// Apply sampling strategy
    fn apply_sampling(
        &self,
        df: &DataFrame,
        sampling: &SamplingStrategy,
    ) -> Result<DataFrame> {
        let total_rows = df.height();
        if total_rows == 0 {
            return Ok(df.clone());
        }

        let sample_size = match &sampling.size {
            SamplingSize::Rows(n) => *n,
            SamplingSize::Ratio(ratio) => {
                ((total_rows as f64) * ratio).ceil() as usize
            }
        };

        let sample_size = sample_size.min(total_rows);

        match sampling.method {
            SamplingMethod::Random => {
                // Random sampling - use lazy frame with random index column
                let total_rows = df.height();
                if sample_size >= total_rows {
                    return Ok(df.clone());
                }
                
                // Add a random column, sort by it, take first N, then drop the column
                let mut rng = rand::thread_rng();
                let random_values: Vec<f64> = (0..total_rows)
                    .map(|_| rng.gen())
                    .collect();
                
                let mut df_with_random = df.clone();
                df_with_random.with_column(Series::new("__random__", random_values))
                    .map_err(|e| RcaError::Execution(format!("Failed to add random column: {}", e)))?;
                
                let sampled_df = df_with_random
                    .lazy()
                    .sort_by_exprs([col("__random__")], SortMultipleOptions::default())
                    .limit(sample_size as u32)
                    .drop(["__random__"])
                    .collect()
                    .map_err(|e| RcaError::Execution(format!("Random sampling failed: {}", e)))?;
                
                Ok(sampled_df)
            }
            SamplingMethod::Stratified => {
                // Stratified sampling by grain key (if available)
                // For now, fall back to random sampling
                let total_rows = df.height();
                if sample_size >= total_rows {
                    return Ok(df.clone());
                }
                
                let mut rng = rand::thread_rng();
                let random_values: Vec<f64> = (0..total_rows)
                    .map(|_| rng.gen())
                    .collect();
                
                let mut df_with_random = df.clone();
                df_with_random.with_column(Series::new("__random__", random_values))
                    .map_err(|e| RcaError::Execution(format!("Failed to add random column: {}", e)))?;
                
                let sampled_df = df_with_random
                    .lazy()
                    .sort_by_exprs([col("__random__")], SortMultipleOptions::default())
                    .limit(sample_size as u32)
                    .drop(["__random__"])
                    .collect()
                    .map_err(|e| RcaError::Execution(format!("Stratified sampling failed: {}", e)))?;
                
                Ok(sampled_df)
            }
            SamplingMethod::TopN => {
                // Top-N sampling (highest value rows)
                // This requires knowing which column to sort by
                // For now, take first N rows (simplified)
                Ok(df.head(Some(sample_size)))
            }
        }
    }

    /// Parse filter string to FilterExpr
    fn parse_filter_string(&self, expr: &str) -> Result<FilterExpr> {
        // Simple parser - handles formats like "column = value", "column > value", etc.
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() < 3 {
            return Err(RcaError::Execution(format!("Invalid filter expression: {}", expr)));
        }

        let column = parts[0].to_string();
        let operator = parts[1].to_string();
        let value_str = parts[2..].join(" ");

        // Try to parse value as JSON
        let value = if value_str == "NULL" || value_str == "null" {
            None
        } else {
            serde_json::from_str(&value_str).ok()
        };

        Ok(FilterExpr {
            column,
            operator,
            value,
            values: None,
        })
    }

    /// Resolve table path
    fn resolve_table_path(&self, table_path: &str) -> Result<PathBuf> {
        if table_path.starts_with("tables/") {
            let project_root = self.data_dir.parent()
                .ok_or_else(|| RcaError::Execution(format!(
                    "Cannot determine project root from data_dir: {:?}",
                    self.data_dir
                )))?;
            Ok(project_root.join(table_path))
        } else if table_path.starts_with('/') {
            Ok(PathBuf::from(table_path))
        } else {
            Ok(self.data_dir.join(table_path))
        }
    }

    /// Check stop conditions
    fn check_stop_conditions(
        &self,
        conditions: &StopConditions,
        current_rows: usize,
        start_time: Instant,
    ) -> Result<()> {
        // Check max rows
        if let Some(max_rows) = conditions.max_rows {
            if current_rows > max_rows {
                return Err(RcaError::Execution(format!(
                    "Stop condition: exceeded max_rows ({})",
                    max_rows
                )));
            }
        }

        // Check max time
        if let Some(max_time) = conditions.max_time {
            if start_time.elapsed() > max_time {
                return Err(RcaError::Execution(format!(
                    "Stop condition: exceeded max_time ({:?})",
                    max_time
                )));
            }
        }

        // Note: confidence_threshold and cost_threshold are checked elsewhere
        // (they require context from the execution state)

        Ok(())
    }

    /// Estimate memory usage in MB
    fn estimate_memory_usage(&self, df: &DataFrame) -> f64 {
        // Rough estimate: rows * columns * avg_bytes_per_cell
        let rows = df.height();
        let cols = df.width();
        let avg_bytes_per_cell = 100; // Rough estimate
        (rows * cols * avg_bytes_per_cell) as f64 / (1024.0 * 1024.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_engine_creation() {
        // Test would require mock metadata
    }
}

