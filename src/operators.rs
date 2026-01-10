use crate::error::{RcaError, Result};
use crate::metadata::PipelineOp;
use polars::prelude::*;
use std::path::PathBuf;

pub struct RelationalEngine {
    data_dir: PathBuf,
}

impl RelationalEngine {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }
    
    /// Execute a single pipeline operation
    pub async fn execute_op(
        &self,
        op: &PipelineOp,
        input: Option<DataFrame>,
        table_path: Option<&str>,
    ) -> Result<DataFrame> {
        match op {
            PipelineOp::Scan { table } => {
                self.scan(table, table_path).await
            }
            PipelineOp::Join { table, on, join_type } => {
                let right = self.scan(table, table_path).await?;
                self.join(input.unwrap(), right, on, join_type).await
            }
            PipelineOp::Filter { expr } => {
                self.filter(input.unwrap(), expr).await
            }
            PipelineOp::Derive { expr, r#as } => {
                self.derive(input.unwrap(), expr, r#as).await
            }
            PipelineOp::Group { by, agg } => {
                self.group(input.unwrap(), by, agg).await
            }
            PipelineOp::Select { columns } => {
                self.select(input.unwrap(), columns).await
            }
        }
    }
    
    async fn scan(&self, table: &str, table_path: Option<&str>) -> Result<DataFrame> {
        let path = if let Some(p) = table_path {
            PathBuf::from(p)
        } else {
            self.data_dir.join(format!("{}.parquet", table))
        };
        
        if !path.exists() {
            // Return empty dataframe with schema if file doesn't exist
            // In production, would load schema from metadata
            return Ok(DataFrame::empty());
        }
        
        let df = LazyFrame::scan_parquet(&path, ScanArgsParquet::default())
            .map_err(|e| RcaError::Execution(format!("Failed to scan {}: {}", table, e)))?
            .collect()
            .map_err(|e| RcaError::Execution(format!("Failed to collect {}: {}", table, e)))?;
        
        Ok(df)
    }
    
    async fn join(
        &self,
        left: DataFrame,
        right: DataFrame,
        on: &[String],
        join_type: &str,
    ) -> Result<DataFrame> {
        let left_lazy = left.lazy();
        let right_lazy = right.lazy();
        
        let join_type_enum = match join_type.to_lowercase().as_str() {
            "left" => JoinType::Left,
            "inner" => JoinType::Inner,
            "outer" => JoinType::Outer,
            "right" => JoinType::Right,
            _ => JoinType::Inner,
        };
        
        let on_cols: Vec<Expr> = on.iter().map(|c| col(c)).collect();
        
        let result = left_lazy
            .join(right_lazy, on_cols.clone(), on_cols, join_type_enum)
            .collect()
            .map_err(|e| RcaError::Execution(format!("Join failed: {}", e)))?;
        
        // Check for join explosion
        let row_count_before = left.height();
        let row_count_after = result.height();
        if row_count_after > row_count_before * 10 {
            return Err(RcaError::Execution(format!(
                "Join explosion detected: {} rows -> {} rows",
                row_count_before, row_count_after
            )));
        }
        
        Ok(result)
    }
    
    async fn filter(&self, df: DataFrame, expr: &str) -> Result<DataFrame> {
        // Simple filter - in production would parse SQL-like expressions
        // For now, support basic column comparisons
        let lazy_df = df.lazy();
        
        // Parse simple expressions like "column = value" or "column > value"
        // This is simplified - real implementation would use a proper expression parser
        let filter_expr = self.parse_filter_expr(expr)?;
        
        let result = lazy_df
            .filter(filter_expr)
            .collect()
            .map_err(|e| RcaError::Execution(format!("Filter failed: {}", e)))?;
        
        Ok(result)
    }
    
    async fn derive(&self, df: DataFrame, expr: &str, alias: &str) -> Result<DataFrame> {
        // Parse expression like "emi_amount - COALESCE(transaction_amount, 0)"
        // Simplified - would need proper SQL expression parser
        let lazy_df = df.lazy();
        
        let derive_expr = self.parse_derive_expr(expr, alias)?;
        
        let result = lazy_df
            .with_columns([derive_expr])
            .collect()
            .map_err(|e| RcaError::Execution(format!("Derive failed: {}", e)))?;
        
        Ok(result)
    }
    
    async fn group(
        &self,
        df: DataFrame,
        by: &[String],
        agg: &std::collections::HashMap<String, String>,
    ) -> Result<DataFrame> {
        let lazy_df = df.lazy();
        
        let by_cols: Vec<Expr> = by.iter().map(|c| col(c)).collect();
        
        let mut agg_exprs = Vec::new();
        for (alias, func) in agg {
            let expr = match func.to_uppercase().as_str() {
                "SUM" | "SUM(OUTSTANDING)" => {
                    // Extract column name from expression like "SUM(outstanding)"
                    let col_name = if func.contains('(') {
                        func.strip_prefix("SUM(")
                            .and_then(|s| s.strip_suffix(")"))
                            .unwrap_or("outstanding")
                    } else {
                        "outstanding"
                    };
                    col(col_name).sum().alias(alias)
                }
                "COUNT" => count().alias(alias),
                "AVG" => {
                    let col_name = func.strip_prefix("AVG(")
                        .and_then(|s| s.strip_suffix(")"))
                        .unwrap_or(func);
                    col(col_name).mean().alias(alias)
                }
                _ => {
                    // Try to parse as column reference
                    col(func).alias(alias)
                }
            };
            agg_exprs.push(expr);
        }
        
        let result = lazy_df
            .group_by(by_cols)
            .agg(agg_exprs)
            .collect()
            .map_err(|e| RcaError::Execution(format!("Group failed: {}", e)))?;
        
        Ok(result)
    }
    
    async fn select(&self, df: DataFrame, columns: &[String]) -> Result<DataFrame> {
        let lazy_df = df.lazy();
        
        let select_exprs: Vec<Expr> = columns.iter()
            .map(|c| {
                // Handle aliases like "total_outstanding as tos"
                if let Some((src, alias)) = c.split_once(" as ") {
                    col(src.trim()).alias(alias.trim())
                } else {
                    col(c.trim())
                }
            })
            .collect();
        
        let result = lazy_df
            .select(select_exprs)
            .collect()
            .map_err(|e| RcaError::Execution(format!("Select failed: {}", e)))?;
        
        Ok(result)
    }
    
    fn parse_filter_expr(&self, _expr: &str) -> Result<Expr> {
        // Simplified - would need proper SQL parser
        // For now, return a placeholder
        Ok(lit(true))
    }
    
    fn parse_derive_expr(&self, expr: &str, alias: &str) -> Result<Expr> {
        // Simplified expression parsing
        // Handle simple arithmetic like "a - b" or "COALESCE(a, 0)"
        if expr.contains("COALESCE") {
            // Extract column name and default
            if let Ok(re) = regex::Regex::new(r"COALESCE\((\w+),\s*(\d+)\)") {
                if let Some(caps) = re.captures(expr) {
                    if let (Some(col_match), Some(val_match)) = (caps.get(1), caps.get(2)) {
                        let col_name = col_match.as_str();
                        if let Ok(default_val) = val_match.as_str().parse::<f64>() {
                            return Ok(col(col_name).fill_null(lit(default_val)).alias(alias));
                        }
                    }
                }
            }
        }
        
        // Handle subtraction like "a - b"
        if let Some((left, right)) = expr.split_once(" - ") {
            return Ok((col(left.trim()) - col(right.trim())).alias(alias));
        }
        
        // Default: treat as column reference
        Ok(col(expr).alias(alias))
    }
}

