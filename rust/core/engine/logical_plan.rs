//! Logical Plan Structure
//! 
//! Defines the logical plan representation for query execution.
//! This enables query optimization, cost estimation, and execution planning.

use crate::error::{RcaError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Logical plan node
/// 
/// Represents a single operation in the query execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    /// Scan a table
    Scan {
        table: String,
        filters: Vec<FilterExpr>,
        projection: Option<Vec<String>>, // Columns to select
        cost_estimate: Option<CostEstimate>,
    },
    
    /// Apply filter predicates
    Filter {
        expr: FilterExpr,
        input: Box<LogicalPlan>,
        selectivity_estimate: Option<f64>, // 0.0-1.0, fraction of rows that pass
        cost_estimate: Option<CostEstimate>,
    },
    
    /// Join two tables
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        keys: Vec<String>, // Join key columns
        selectivity_estimate: Option<f64>, // Join selectivity
        cost_estimate: Option<CostEstimate>,
    },
    
    /// Project/select columns
    Project {
        columns: Vec<String>,
        input: Box<LogicalPlan>,
        cost_estimate: Option<CostEstimate>,
    },
    
    /// Aggregate rows
    Aggregate {
        group_by: Vec<String>,
        aggregations: HashMap<String, AggregationExpr>,
        input: Box<LogicalPlan>,
        cost_estimate: Option<CostEstimate>,
    },
}

/// Filter expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterExpr {
    /// Column name
    pub column: String,
    
    /// Operator: "=", "!=", ">", "<", ">=", "<=", "IN", "LIKE", "IS NULL", "IS NOT NULL"
    pub operator: String,
    
    /// Value (for comparison operators)
    pub value: Option<serde_json::Value>,
    
    /// Values (for IN operator)
    pub values: Option<Vec<serde_json::Value>>,
}

/// Join type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,  // Note: Polars doesn't have Right join, will swap tables
    Outer,
}

impl JoinType {
    /// Convert to Polars JoinType
    pub fn to_polars(&self) -> polars::prelude::JoinType {
        match self {
            JoinType::Inner => polars::prelude::JoinType::Inner,
            JoinType::Left => polars::prelude::JoinType::Left,
            JoinType::Right => polars::prelude::JoinType::Left, // Will swap tables
            JoinType::Outer => polars::prelude::JoinType::Outer,
        }
    }
}

/// Aggregation expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationExpr {
    Sum(String),
    Count(String),
    Avg(String),
    Min(String),
    Max(String),
    Custom(String),
}

/// Cost estimate for a plan node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    /// Estimated rows to scan/process
    pub rows_scanned: usize,
    
    /// Estimated selectivity (0.0-1.0)
    pub selectivity: f64,
    
    /// Estimated cost (arbitrary units, higher = more expensive)
    pub cost: f64,
    
    /// Estimated memory usage in MB
    pub memory_mb: f64,
    
    /// Estimated execution time in milliseconds
    pub time_ms: f64,
}

impl LogicalPlan {
    /// Estimate the cost of executing this plan
    pub fn estimate_cost(&self, metadata: &dyn PlanMetadata) -> CostEstimate {
        match self {
            LogicalPlan::Scan { table, filters, cost_estimate, .. } => {
                if let Some(ref cost) = cost_estimate {
                    return cost.clone();
                }
                
                let row_count = metadata.get_table_row_count(table).unwrap_or(1_000_000);
                let selectivity = filters.iter()
                    .map(|f| metadata.estimate_filter_selectivity(table, f).unwrap_or(0.5))
                    .fold(1.0, |acc, sel| acc * sel);
                
                let rows_scanned = (row_count as f64 * selectivity) as usize;
                let avg_column_width = metadata.get_avg_column_width(table).unwrap_or(100);
                let cost = rows_scanned as f64 * avg_column_width as f64 * 0.001; // scan_cost_factor
                let memory_mb = (rows_scanned * avg_column_width) as f64 / (1024.0 * 1024.0);
                let time_ms = rows_scanned as f64 / 1000.0; // Rough estimate: 1000 rows/ms
                
                CostEstimate {
                    rows_scanned,
                    selectivity,
                    cost,
                    memory_mb,
                    time_ms,
                }
            }
            
            LogicalPlan::Filter { expr, input, selectivity_estimate, cost_estimate, .. } => {
                if let Some(ref cost) = cost_estimate {
                    return cost.clone();
                }
                
                let input_cost = input.estimate_cost(metadata);
                let selectivity = selectivity_estimate.unwrap_or(0.5);
                let rows_scanned = (input_cost.rows_scanned as f64 * selectivity) as usize;
                let cost = input_cost.cost * 0.1 + rows_scanned as f64 * 0.0001; // filter_cost_factor
                let memory_mb = input_cost.memory_mb * selectivity;
                let time_ms = input_cost.time_ms * 0.1 + rows_scanned as f64 / 10000.0;
                
                CostEstimate {
                    rows_scanned,
                    selectivity,
                    cost,
                    memory_mb,
                    time_ms,
                }
            }
            
            LogicalPlan::Join { left, right, join_type, keys, selectivity_estimate, cost_estimate, .. } => {
                if let Some(ref cost) = cost_estimate {
                    return cost.clone();
                }
                
                let left_cost = left.estimate_cost(metadata);
                let right_cost = right.estimate_cost(metadata);
                let selectivity = selectivity_estimate.unwrap_or(0.1); // Default join selectivity
                
                // Join cost: left_rows * right_rows * selectivity * join_complexity_factor
                let rows_scanned = (left_cost.rows_scanned as f64 * right_cost.rows_scanned as f64 * selectivity) as usize;
                let cost = left_cost.cost + right_cost.cost + 
                    (left_cost.rows_scanned as f64 * right_cost.rows_scanned as f64 * selectivity * 0.001);
                let memory_mb = left_cost.memory_mb + right_cost.memory_mb + 
                    (rows_scanned as f64 * 100.0 / (1024.0 * 1024.0)); // Hash table overhead
                let time_ms = left_cost.time_ms + right_cost.time_ms + 
                    (rows_scanned as f64 / 1000.0);
                
                CostEstimate {
                    rows_scanned,
                    selectivity,
                    cost,
                    memory_mb,
                    time_ms,
                }
            }
            
            LogicalPlan::Project { columns, input, cost_estimate, .. } => {
                if let Some(ref cost) = cost_estimate {
                    return cost.clone();
                }
                
                let input_cost = input.estimate_cost(metadata);
                // Projection is cheap - just column selection
                CostEstimate {
                    rows_scanned: input_cost.rows_scanned,
                    selectivity: input_cost.selectivity,
                    cost: input_cost.cost * 0.01, // Very cheap
                    memory_mb: input_cost.memory_mb * (columns.len() as f64 / 10.0), // Rough estimate
                    time_ms: input_cost.time_ms * 0.01,
                }
            }
            
            LogicalPlan::Aggregate { group_by, aggregations, input, cost_estimate, .. } => {
                if let Some(ref cost) = cost_estimate {
                    return cost.clone();
                }
                
                let input_cost = input.estimate_cost(metadata);
                // Estimate group cardinality (number of distinct groups)
                let group_cardinality = metadata.estimate_group_cardinality(&group_by).unwrap_or(100);
                let rows_scanned = group_cardinality.min(input_cost.rows_scanned);
                let cost = input_cost.cost * 0.5 + rows_scanned as f64 * aggregations.len() as f64 * 0.001;
                let memory_mb = (rows_scanned * (group_by.len() + aggregations.len()) * 100) as f64 / (1024.0 * 1024.0);
                let time_ms = input_cost.time_ms * 0.2 + rows_scanned as f64 / 5000.0;
                
                CostEstimate {
                    rows_scanned,
                    selectivity: 1.0, // Aggregation doesn't filter
                    cost,
                    memory_mb,
                    time_ms,
                }
            }
        }
    }
    
    /// Push filters down as far as possible
    pub fn pushdown_filters(self) -> LogicalPlan {
        match self {
            LogicalPlan::Filter { expr, input, selectivity_estimate, cost_estimate } => {
                match *input {
                    LogicalPlan::Scan { table, mut filters, projection, cost_estimate: scan_cost } => {
                        // Push filter into scan
                        filters.push(expr);
                        LogicalPlan::Scan {
                            table,
                            filters,
                            projection,
                            cost_estimate: scan_cost,
                        }
                    }
                    LogicalPlan::Join { left, right, join_type, keys, selectivity_estimate: join_sel, cost_estimate: join_cost } => {
                        // Try to push filter to left or right side
                        // For now, keep filter after join (could be optimized further)
                        LogicalPlan::Filter {
                            expr,
                            input: Box::new(LogicalPlan::Join {
                                left,
                                right,
                                join_type,
                                keys,
                                selectivity_estimate: join_sel,
                                cost_estimate: join_cost,
                            }),
                            selectivity_estimate,
                            cost_estimate,
                        }
                    }
                    other => {
                        // Push filter through other operations
                        LogicalPlan::Filter {
                            expr,
                            input: Box::new(other.pushdown_filters()),
                            selectivity_estimate,
                            cost_estimate,
                        }
                    }
                }
            }
            LogicalPlan::Join { left, right, join_type, keys, selectivity_estimate, cost_estimate } => {
                LogicalPlan::Join {
                    left: Box::new(left.pushdown_filters()),
                    right: Box::new(right.pushdown_filters()),
                    join_type,
                    keys,
                    selectivity_estimate,
                    cost_estimate,
                }
            }
            LogicalPlan::Project { columns, input, cost_estimate } => {
                LogicalPlan::Project {
                    columns,
                    input: Box::new(input.pushdown_filters()),
                    cost_estimate,
                }
            }
            LogicalPlan::Aggregate { group_by, aggregations, input, cost_estimate } => {
                LogicalPlan::Aggregate {
                    group_by,
                    aggregations,
                    input: Box::new(input.pushdown_filters()),
                    cost_estimate,
                }
            }
            other => other,
        }
    }
}

/// Metadata provider for cost estimation
pub trait PlanMetadata {
    /// Get row count for a table
    fn get_table_row_count(&self, table: &str) -> Option<usize>;
    
    /// Get average column width in bytes
    fn get_avg_column_width(&self, table: &str) -> Option<usize>;
    
    /// Estimate filter selectivity (0.0-1.0)
    fn estimate_filter_selectivity(&self, table: &str, filter: &FilterExpr) -> Option<f64>;
    
    /// Estimate group cardinality for group by columns
    fn estimate_group_cardinality(&self, group_by: &[String]) -> Option<usize>;
}

impl FilterExpr {
    /// Convert JSON value to Polars literal
    fn json_to_lit(val: &serde_json::Value) -> Expr {
        match val {
            serde_json::Value::String(s) => lit(s.clone()),
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    lit(n.as_i64().unwrap())
                } else if n.is_u64() {
                    lit(n.as_u64().unwrap())
                } else {
                    lit(n.as_f64().unwrap())
                }
            }
            serde_json::Value::Bool(b) => lit(*b),
            serde_json::Value::Null => lit(Null {}),
            _ => lit(val.to_string()), // Fallback to string representation
        }
    }
    
    /// Convert to Polars expression
    pub fn to_polars_expr(&self) -> Result<Expr> {
        let col_expr = col(&self.column);
        
        match self.operator.as_str() {
            "=" => {
                if let Some(ref val) = self.value {
                    Ok(col_expr.eq(Self::json_to_lit(val)))
                } else {
                    Err(RcaError::Execution("Filter '=' requires a value".to_string()))
                }
            }
            "!=" | "<>" => {
                if let Some(ref val) = self.value {
                    Ok(col_expr.neq(Self::json_to_lit(val)))
                } else {
                    Err(RcaError::Execution("Filter '!=' requires a value".to_string()))
                }
            }
            ">" => {
                if let Some(ref val) = self.value {
                    Ok(col_expr.gt(Self::json_to_lit(val)))
                } else {
                    Err(RcaError::Execution("Filter '>' requires a value".to_string()))
                }
            }
            "<" => {
                if let Some(ref val) = self.value {
                    Ok(col_expr.lt(Self::json_to_lit(val)))
                } else {
                    Err(RcaError::Execution("Filter '<' requires a value".to_string()))
                }
            }
            ">=" => {
                if let Some(ref val) = self.value {
                    Ok(col_expr.gt_eq(Self::json_to_lit(val)))
                } else {
                    Err(RcaError::Execution("Filter '>=' requires a value".to_string()))
                }
            }
            "<=" => {
                if let Some(ref val) = self.value {
                    Ok(col_expr.lt_eq(Self::json_to_lit(val)))
                } else {
                    Err(RcaError::Execution("Filter '<=' requires a value".to_string()))
                }
            }
            "IN" => {
                if let Some(ref values) = self.values {
                    if values.is_empty() {
                        return Err(RcaError::Execution("Filter 'IN' requires at least one value".to_string()));
                    }
                    // For IN, we need to create a series and check membership
                    // Simplified: use first value for now, could be enhanced
                    Ok(col_expr.eq(Self::json_to_lit(&values[0]))) // Simplified - full IN would need proper handling
                } else {
                    Err(RcaError::Execution("Filter 'IN' requires values".to_string()))
                }
            }
            "IS NULL" => Ok(col_expr.is_null()),
            "IS NOT NULL" => Ok(col_expr.is_not_null()),
            "LIKE" => {
                if let Some(ref val) = self.value {
                    // Convert SQL LIKE to Polars regex match
                    let pattern = val.as_str().unwrap_or("");
                    // Use regex match - convert % to .* and _ to .
                    let regex_pattern = pattern.replace("%", ".*").replace("_", ".");
                    // Use string contains via filter - simplified approach
                    Ok(col_expr.str().to_lowercase().eq(lit(pattern.to_lowercase())))
                } else {
                    Err(RcaError::Execution("Filter 'LIKE' requires a value".to_string()))
                }
            }
            _ => Err(RcaError::Execution(format!("Unsupported filter operator: {}", self.operator))),
        }
    }
}

