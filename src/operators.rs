use crate::data_utils;
use crate::de_executor::DeExecutor;
use crate::error::{RcaError, Result};
use crate::metadata::PipelineOp;
use crate::tool_system::ToolExecutionContext;
use polars::prelude::*;
use std::collections::HashMap;
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
                // Note: This method doesn't have access to metadata
                // Joins should be handled in the executor which has metadata access
                // For now, use basic scan - but this should be refactored
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
    
    pub async fn scan(&self, table: &str, table_path: Option<&str>) -> Result<DataFrame> {
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
        
        // Convert any string columns containing scientific notation to numeric
        let df = data_utils::convert_scientific_notation_columns(df)?;
        
        Ok(df)
    }
    
    /// Scan with metadata - uses table path from metadata
    pub async fn scan_with_metadata(&self, table_name: &str, metadata: &crate::metadata::Metadata) -> Result<DataFrame> {
        let table = metadata.get_table(table_name)
            .ok_or_else(|| RcaError::Execution(format!("Table not found in metadata: {}", table_name)))?;
        
        let path = self.data_dir.join(&table.path);
        
        if !path.exists() {
            return Err(RcaError::Execution(format!("Table file not found: {}", path.display())));
        }
        
        // Load based on file extension
        let df = if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            // Load CSV file
            LazyCsvReader::new(&path)
                .with_try_parse_dates(true)
                .with_infer_schema_length(Some(1000))
                .finish()
                .map_err(|e| RcaError::Execution(format!("Failed to scan CSV {}: {}", table_name, e)))?
                .collect()
                .map_err(|e| RcaError::Execution(format!("Failed to collect CSV {}: {}", table_name, e)))?
        } else {
            // Load Parquet file (default)
            LazyFrame::scan_parquet(&path, ScanArgsParquet::default())
                .map_err(|e| RcaError::Execution(format!("Failed to scan {}: {}", table_name, e)))?
                .collect()
                .map_err(|e| RcaError::Execution(format!("Failed to collect {}: {}", table_name, e)))?
        };
        
        // Convert any string columns containing scientific notation to numeric
        let df = data_utils::convert_scientific_notation_columns(df)?;
        
        Ok(df)
    }
    
    /// Join with optional DE tool execution
    pub async fn join_with_de(
        &self,
        left: DataFrame,
        right: DataFrame,
        on: &[String],
        join_type: &str,
        de_context: Option<&ToolExecutionContext>,
    ) -> Result<DataFrame> {
        // Execute DE tools before join if requested
        if let Some(ctx) = de_context {
            // Validate join keys if requested
            for validation in &ctx.join_validations {
                if validation.join_type == join_type {
                    let join_keys: HashMap<String, String> = on.iter()
                        .map(|key| (key.clone(), key.clone()))
                        .collect();
                    
                    if let Err(e) = DeExecutor::execute_validate_join_keys(
                        &left,
                        &right,
                        &validation.left_table,
                        &validation.right_table,
                        &join_keys,
                        join_type,
                    ) {
                        println!("      ️  Join validation warning: {}", e);
                    }
                }
            }
            
            // Validate schema if requested
            for validation in &ctx.schema_validations {
                let join_keys: HashMap<String, String> = on.iter()
                    .map(|key| (key.clone(), key.clone()))
                    .collect();
                
                if let Err(e) = DeExecutor::execute_validate_schema(
                    &left,
                    &right,
                    &validation.left_table,
                    &validation.right_table,
                    &join_keys,
                ) {
                    println!("      ️  Schema validation warning: {}", e);
                }
            }
        }
        
        let row_count_before = left.height();
        let left_lazy = left.lazy();
        let right_lazy = right.lazy();
        
        let join_type_enum = match join_type.to_lowercase().as_str() {
            "left" => JoinArgs::new(JoinType::Left),
            "inner" => JoinArgs::new(JoinType::Inner),
            "outer" => JoinArgs::new(JoinType::Outer),
            _ => JoinArgs::new(JoinType::Inner),
        };
        
        let on_cols: Vec<Expr> = on.iter().map(|c| col(c)).collect();
        
        let result = left_lazy
            .join(right_lazy, on_cols.clone(), on_cols, join_type_enum)
            .collect()
            .map_err(|e| RcaError::Execution(format!("Join failed: {}", e)))?;
        
        // Check for join explosion - increased threshold for multi-grain scenarios
        let row_count_after = result.height();
        if row_count_after > row_count_before * 50 {
            return Err(RcaError::Execution(format!(
                "Join explosion detected: {} rows -> {} rows",
                row_count_before, row_count_after
            )));
        }
        
        Ok(result)
    }
    
    /// Join without DE tools (backward compatibility)
    pub async fn join(
        &self,
        left: DataFrame,
        right: DataFrame,
        on: &[String],
        join_type: &str,
    ) -> Result<DataFrame> {
        self.join_with_de(left, right, on, join_type, None).await
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
            let expr = if func.to_uppercase().starts_with("SUM(") {
                // Extract column name from "SUM(column_name)"
                let col_name = func
                    .strip_prefix("SUM(")
                    .or_else(|| func.strip_prefix("sum("))
                    .and_then(|s| s.strip_suffix(")"))
                    .unwrap_or("computed_value")
                    .trim();
                col(col_name).sum().alias(alias)
            } else if func.to_uppercase().starts_with("AVG(") {
                // Extract column name from "AVG(column_name)"
                let col_name = func
                    .strip_prefix("AVG(")
                    .or_else(|| func.strip_prefix("avg("))
                    .and_then(|s| s.strip_suffix(")"))
                    .unwrap_or(func)
                    .trim();
                col(col_name).mean().alias(alias)
            } else if func.to_uppercase().starts_with("COUNT(") {
                // Extract column name from "COUNT(column_name)"
                let col_name = func
                    .strip_prefix("COUNT(")
                    .or_else(|| func.strip_prefix("count("))
                    .and_then(|s| s.strip_suffix(")"))
                    .unwrap_or("*")
                    .trim();
                if col_name == "*" {
                    len().alias(alias)
                } else {
                    col(col_name).count().alias(alias)
                }
            } else if func.to_uppercase() == "COUNT" {
                len().alias(alias)
            } else {
                // Direct column reference (no aggregation function)
                col(func.trim()).alias(alias)
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
        // Parse arithmetic expressions with +, -, *, / operators
        // Handle expressions like "_agg_account_balance + _agg_transaction_amount - _agg_writeoff_amount"
        // or "emi_amount - COALESCE(transaction_amount, 0)"
        
        // First, try to parse as a complex arithmetic expression
        if expr.contains(" + ") || expr.contains(" - ") {
            return self.parse_arithmetic_expr(expr, alias);
        }
        
        // Handle standalone COALESCE
        if expr.contains("COALESCE") {
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
        
        // Default: treat as column reference
        Ok(col(expr.trim()).alias(alias))
    }
    
    /// Parse arithmetic expressions with multiple +, -, operators
    fn parse_arithmetic_expr(&self, expr: &str, alias: &str) -> Result<Expr> {
        // Tokenize the expression: split by + and - while keeping the operators
        let mut result_expr: Option<Expr> = None;
        let mut current_op: Option<char> = None;
        let mut current_token = String::new();
        let chars: Vec<char> = expr.chars().collect();
        let mut i = 0;
        
        while i < chars.len() {
            let c = chars[i];
            
            // Check for operators (with spaces around them)
            if (c == '+' || c == '-') && 
               (i > 0 && chars[i-1] == ' ') && 
               (i + 1 < chars.len() && chars[i+1] == ' ') {
                // Process the current token
                let token = current_token.trim().to_string();
                if !token.is_empty() {
                    let token_expr = self.parse_token(&token)?;
                    
                    result_expr = Some(match (result_expr, current_op) {
                        (None, _) => token_expr,
                        (Some(left), Some('+')) => left + token_expr,
                        (Some(left), Some('-')) => left - token_expr,
                        (Some(left), _) => left + token_expr, // Default to addition
                    });
                }
                
                current_op = Some(c);
                current_token.clear();
                i += 1; // Skip the space after operator
            } else {
                current_token.push(c);
            }
            i += 1;
        }
        
        // Process the last token
        let token = current_token.trim().to_string();
        if !token.is_empty() {
            let token_expr = self.parse_token(&token)?;
            
            result_expr = Some(match (result_expr, current_op) {
                (None, _) => token_expr,
                (Some(left), Some('+')) => left + token_expr,
                (Some(left), Some('-')) => left - token_expr,
                (Some(left), _) => left + token_expr,
            });
        }
        
        result_expr
            .map(|e| e.alias(alias))
            .ok_or_else(|| RcaError::Execution(format!("Failed to parse expression: {}", expr)))
    }
    
    /// Parse a single token in an arithmetic expression
    fn parse_token(&self, token: &str) -> Result<Expr> {
        let trimmed = token.trim();
        
        // Check if it's a COALESCE expression
        if trimmed.contains("COALESCE") {
            if let Ok(re) = regex::Regex::new(r"COALESCE\((\w+),\s*(\d+)\)") {
                if let Some(caps) = re.captures(trimmed) {
                    if let (Some(col_match), Some(val_match)) = (caps.get(1), caps.get(2)) {
                        let col_name = col_match.as_str();
                        if let Ok(default_val) = val_match.as_str().parse::<f64>() {
                            return Ok(col(col_name).fill_null(lit(default_val)));
                        }
                    }
                }
            }
        }
        
        // Check if it's a number
        if let Ok(num) = trimmed.parse::<f64>() {
            return Ok(lit(num));
        }
        
        // Otherwise, treat as column reference
        Ok(col(trimmed))
    }
}


