//! Pushdown Predicates
//! 
//! Optimizes data loading by pushing filters down to the data source.
//! Reduces data transfer and memory usage.

use crate::error::{RcaError, Result};
use polars::prelude::*;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

/// Pushdown predicate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushdownPredicate {
    /// Column name
    pub column: String,
    
    /// Operator: "=", "!=", ">", "<", ">=", "<=", "in", "not_in", "between"
    pub operator: String,
    
    /// Value(s) for the predicate
    pub value: serde_json::Value,
}

/// Pushdown optimizer
pub struct PushdownOptimizer;

impl PushdownOptimizer {
    /// Create a new pushdown optimizer
    pub fn new() -> Self {
        Self
    }
    
    /// Load parquet file with pushdown predicates
    /// 
    /// Applies filters at the parquet scan level, reducing data transfer.
    pub fn load_with_predicates(
        &self,
        file_path: &PathBuf,
        predicates: &[PushdownPredicate],
    ) -> Result<DataFrame> {
        // Build filter expression from predicates
        let filter_expr = self.build_filter_expression(predicates)?;
        
        // Scan with filter pushed down
        let df = LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())
            .map_err(|e| RcaError::Execution(format!("Failed to scan parquet: {}", e)))?
            .filter(filter_expr)
            .collect()
            .map_err(|e| RcaError::Execution(format!("Failed to load with predicates: {}", e)))?;
        
        Ok(df)
    }
    
    /// Build filter expression from predicates
    fn build_filter_expression(&self, predicates: &[PushdownPredicate]) -> Result<Expr> {
        if predicates.is_empty() {
            return Ok(lit(true)); // No filter
        }
        
        let mut conditions = Vec::new();
        
        for predicate in predicates {
            let condition = match predicate.operator.as_str() {
                "=" => {
                    let val = self.value_to_lit(&predicate.value)?;
                    col(&predicate.column).eq(val)
                }
                "!=" => {
                    let val = self.value_to_lit(&predicate.value)?;
                    col(&predicate.column).neq(val)
                }
                ">" => {
                    let val = self.value_to_lit(&predicate.value)?;
                    col(&predicate.column).gt(val)
                }
                "<" => {
                    let val = self.value_to_lit(&predicate.value)?;
                    col(&predicate.column).lt(val)
                }
                ">=" => {
                    let val = self.value_to_lit(&predicate.value)?;
                    col(&predicate.column).gt_eq(val)
                }
                "<=" => {
                    let val = self.value_to_lit(&predicate.value)?;
                    col(&predicate.column).lt_eq(val)
                }
                "in" => {
                    if let Some(arr) = predicate.value.as_array() {
                        let values: Vec<Expr> = arr.iter()
                            .filter_map(|v| self.value_to_lit(v).ok())
                            .collect();
                        if values.is_empty() {
                            lit(false)
                        } else {
                            // Use fold to combine with OR
                            values.into_iter()
                                .map(|v| col(&predicate.column).eq(v))
                                .reduce(|acc, cond| acc.or(cond))
                                .unwrap_or(lit(false))
                        }
                    } else {
                        return Err(RcaError::Execution("'in' operator requires array value".to_string()));
                    }
                }
                "not_in" => {
                    if let Some(arr) = predicate.value.as_array() {
                        let values: Vec<Expr> = arr.iter()
                            .filter_map(|v| self.value_to_lit(v).ok())
                            .collect();
                        if values.is_empty() {
                            lit(true)
                        } else {
                            // Use fold to combine with AND NOT
                            values.into_iter()
                                .map(|v| col(&predicate.column).neq(v))
                                .reduce(|acc, cond| acc.and(cond))
                                .unwrap_or(lit(true))
                        }
                    } else {
                        return Err(RcaError::Execution("'not_in' operator requires array value".to_string()));
                    }
                }
                "between" => {
                    if let Some(arr) = predicate.value.as_array() {
                        if arr.len() != 2 {
                            return Err(RcaError::Execution("'between' operator requires array of 2 values".to_string()));
                        }
                        let lower = self.value_to_lit(&arr[0])?;
                        let upper = self.value_to_lit(&arr[1])?;
                        col(&predicate.column).gt_eq(lower).and(col(&predicate.column).lt_eq(upper))
                    } else {
                        return Err(RcaError::Execution("'between' operator requires array value".to_string()));
                    }
                }
                _ => {
                    return Err(RcaError::Execution(format!("Unsupported operator: {}", predicate.operator)));
                }
            };
            
            conditions.push(condition);
        }
        
        // Combine all conditions with AND
        let combined = conditions
            .into_iter()
            .reduce(|acc, cond| acc.and(cond))
            .unwrap_or(lit(true));
        
        Ok(combined)
    }
    
    /// Convert JSON value to Polars literal expression
    fn value_to_lit(&self, value: &serde_json::Value) -> Result<Expr> {
        match value {
            serde_json::Value::String(s) => Ok(lit(s.as_str())),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(lit(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(lit(f))
                } else {
                    Err(RcaError::Execution("Invalid number value".to_string()))
                }
            }
            serde_json::Value::Bool(b) => Ok(lit(*b)),
            _ => Err(RcaError::Execution(format!("Unsupported value type: {:?}", value))),
        }
    }
}

impl Default for PushdownOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_build_filter_expression() {
        let optimizer = PushdownOptimizer::new();
        
        let predicates = vec![
            PushdownPredicate {
                column: "id".to_string(),
                operator: ">".to_string(),
                value: serde_json::json!(100),
            },
            PushdownPredicate {
                column: "status".to_string(),
                operator: "=".to_string(),
                value: serde_json::json!("active"),
            },
        ];
        
        let expr = optimizer.build_filter_expression(&predicates).unwrap();
        // Expression should combine both predicates with AND
        assert!(matches!(expr, Expr::BinaryExpr { .. }));
    }
}

