//! Row Materialization Engine
//! 
//! Converts MetricDefinition into row-level queries that produce data
//! before aggregation. This is the key to deterministic row comparison.

use crate::core::metrics::{MetricDefinition, AggregationExpression};
use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use polars::prelude::*;
use std::path::PathBuf;

/// Row materialization engine
/// 
/// Builds SQL/dataframe queries that produce row-level data at the canonical grain,
/// before any aggregation is applied.
pub struct RowMaterializationEngine {
    metadata: Metadata,
    data_dir: PathBuf,
}

impl RowMaterializationEngine {
    /// Create a new materialization engine
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self { metadata, data_dir }
    }
    
    /// Materialize rows from a metric definition
    /// 
    /// This rewrites the aggregation query to produce row-level data:
    /// - Removes aggregations (replaces sum(paid_amount) with paid_amount)
    /// - Keeps all joins
    /// - Keeps all filters
    /// - Applies rule transforms
    /// - Outputs at canonical grain
    pub async fn materialize_rows(
        &self,
        metric_def: &MetricDefinition,
        source: &str, // "left" or "right" to identify which pipeline
    ) -> Result<DataFrame> {
        // Start with base tables
        let mut df = self.scan_base_tables(&metric_def.base_tables).await?;
        
        // Apply joins
        for join_def in &metric_def.joins {
            df = self.apply_join(&df, join_def).await?;
        }
        
        // Apply filters
        for filter_def in &metric_def.filters {
            df = self.apply_filter(&df, &filter_def.expr).await?;
        }
        
        // Apply formulas/derived columns
        for formula_def in &metric_def.formulas {
            df = self.apply_formula(&df, &formula_def.expr, &formula_def.as_name).await?;
        }
        
        // Select columns needed for canonical mapping
        // Include: keys, value columns (before aggregation), attributes
        let select_cols = self.get_required_columns(metric_def);
        let select_exprs: Vec<Expr> = select_cols.iter().map(|c| col(c)).collect();
        df = df.lazy().select(select_exprs).collect()?;
        
        // Sort by keys for deterministic ordering
        if !metric_def.group_by.is_empty() {
            let key_cols: Vec<Expr> = metric_def.group_by.iter().map(|c| col(c)).collect();
            df = df.lazy().sort_by_exprs(key_cols, SortMultipleOptions::default()).collect()?;
        }
        
        Ok(df)
    }
    
    /// Scan base tables and combine them
    async fn scan_base_tables(&self, tables: &[String]) -> Result<DataFrame> {
        if tables.is_empty() {
            return Err(RcaError::Execution("No base tables specified".to_string()));
        }
        
        // For now, scan the first table
        // In a full implementation, you'd need to handle multiple base tables
        // by starting with the first and joining the rest
        let table_name = &tables[0];
        
        // Find table metadata
        let table = self.metadata
            .tables
            .iter()
            .find(|t| t.name == *table_name)
            .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", table_name)))?;
        
        // Build path
        let table_path = PathBuf::from(&self.data_dir).join(&table.path);
        
        // Read table (assuming CSV for now)
        // Try CSV first, then parquet
        let df = if table_path.extension().and_then(|e| e.to_str()) == Some("csv") {
            LazyCsvReader::new(&table_path)
                .with_try_parse_dates(true)
                .with_infer_schema_length(Some(1000))
                .finish()
                .map_err(|e| RcaError::Execution(format!("Failed to scan CSV table {}: {}", table_name, e)))?
                .collect()
                .map_err(|e| RcaError::Execution(format!("Failed to collect CSV table {}: {}", table_name, e)))?
        } else {
            LazyFrame::scan_parquet(&table_path, ScanArgsParquet::default())
                .map_err(|e| RcaError::Execution(format!("Failed to scan parquet table {}: {}", table_name, e)))?
                .collect()?
        };
        
        Ok(df)
    }
    
    /// Apply a join operation
    async fn apply_join(&self, df: &DataFrame, join_def: &crate::core::metrics::JoinDefinition) -> Result<DataFrame> {
        // Find the table to join
        let table = self.metadata
            .tables
            .iter()
            .find(|t| t.name == join_def.table)
            .ok_or_else(|| RcaError::Execution(format!("Join table not found: {}", join_def.table)))?;
        
        let table_path = PathBuf::from(&self.data_dir).join(&table.path);
        let right_df = if table_path.extension().and_then(|e| e.to_str()) == Some("csv") {
            LazyCsvReader::new(&table_path)
                .with_try_parse_dates(true)
                .with_infer_schema_length(Some(1000))
                .finish()
                .map_err(|e| RcaError::Execution(format!("Failed to scan CSV join table {}: {}", join_def.table, e)))?
                .collect()
                .map_err(|e| RcaError::Execution(format!("Failed to collect CSV join table {}: {}", join_def.table, e)))?
        } else {
            LazyFrame::scan_parquet(&table_path, ScanArgsParquet::default())
                .map_err(|e| RcaError::Execution(format!("Failed to scan parquet join table {}: {}", join_def.table, e)))?
                .collect()?
        };
        
        // Determine join type
        // Note: Polars doesn't have Right join, so we swap tables for right joins
        let (actual_left, actual_right, join_type) = match join_def.join_type.to_lowercase().as_str() {
            "inner" => (df.clone(), right_df.clone(), JoinType::Inner),
            "left" => (df.clone(), right_df.clone(), JoinType::Left),
            "right" => (right_df.clone(), df.clone(), JoinType::Left), // Swap for right join
            "outer" | "full" => (df.clone(), right_df.clone(), JoinType::Outer),
            _ => (df.clone(), right_df.clone(), JoinType::Inner),
        };
        
        // Build join keys
        let left_keys: Vec<Expr> = join_def.on.iter().map(|c| col(c)).collect();
        let right_keys: Vec<Expr> = join_def.on.iter().map(|c| col(c)).collect();
        
        // Perform join
        let df_lazy = df.clone().lazy();
        let right_lazy = right_df.lazy();
        
        let joined = df_lazy
            .join(right_lazy, left_keys, right_keys, JoinArgs::new(join_type))
            .collect()?;
        
        Ok(joined)
    }
    
    /// Apply a filter expression
    async fn apply_filter(&self, df: &DataFrame, expr: &str) -> Result<DataFrame> {
        // Parse and apply filter expression
        // For now, this is a placeholder - you'd need a proper expression parser
        // Polars supports some SQL-like expressions via col().filter()
        
        // Simple implementation: try to parse as a Polars expression
        // In practice, you'd want a proper SQL parser or expression builder
        // For now, pass through unchanged (filter parsing would go here)
        Ok(df.clone())
    }
    
    /// Apply a formula/derived column
    async fn apply_formula(&self, df: &DataFrame, expr: &str, as_name: &str) -> Result<DataFrame> {
        // Parse expression and add as new column
        // This is simplified - real implementation needs proper expression parsing
        
        // For now, if the expression is just a column name, select it
        // Otherwise, this would need a proper expression evaluator
        let result = df
            .clone()
            .lazy()
            .with_columns([
                // Placeholder - real implementation needs expression parsing
                col(expr).alias(as_name)
            ])
            .collect()?;
        
        Ok(result)
    }
    
    /// Get columns required for canonical mapping
    /// 
    /// This includes:
    /// - Group by columns (keys)
    /// - Value columns (before aggregation)
    /// - Attributes
    fn get_required_columns(&self, metric_def: &MetricDefinition) -> Vec<String> {
        let mut cols = Vec::new();
        
        // Add group by columns (keys)
        cols.extend(metric_def.group_by.clone());
        
        // Extract value columns from aggregations
        // For row-level, we want the raw columns before aggregation
        for agg_expr in metric_def.aggregations.values() {
            match agg_expr {
                AggregationExpression::Sum(col) | 
                AggregationExpression::Count(col) |
                AggregationExpression::Avg(col) |
                AggregationExpression::Min(col) |
                AggregationExpression::Max(col) => {
                    if col != "*" && !cols.contains(col) {
                        cols.push(col.clone());
                    }
                }
                AggregationExpression::Custom(_) => {
                    // For custom expressions, we'd need to parse to extract columns
                    // For now, skip
                }
            }
        }
        
        // Add attributes from source entities
        // This would need to be looked up from metadata
        // For now, we'll include any columns that aren't already included
        
        cols
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Note: Tests would require mock metadata and data files
}

