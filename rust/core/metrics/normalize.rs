//! Metric Normalization Layer
//! 
//! Converts SQL ASTs, business rules, and pipeline definitions into a normalized
//! MetricDefinition that can be used for row-level materialization.

use crate::error::{RcaError, Result};
use crate::metadata::{Rule, ComputationDefinition, PipelineOp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Normalized metric definition
/// 
/// This represents a metric computation in a form that can be:
/// 1. Converted to row-level queries (pre-aggregation)
/// 2. Compared across different systems
/// 3. Traced for lineage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDefinition {
    /// Metric name/identifier
    pub metric_name: String,
    
    /// Base tables that need to be scanned
    pub base_tables: Vec<String>,
    
    /// Join operations
    /// Each join specifies: (from_table, to_table, join_condition, join_type)
    pub joins: Vec<JoinDefinition>,
    
    /// Filter conditions (applied before aggregation)
    pub filters: Vec<FilterDefinition>,
    
    /// Derived columns (computed expressions)
    pub formulas: Vec<FormulaDefinition>,
    
    /// Group by columns (grain level)
    pub group_by: Vec<String>,
    
    /// Aggregation expressions
    /// Maps output column name to aggregation expression
    pub aggregations: HashMap<String, AggregationExpression>,
    
    /// Source entities involved
    pub source_entities: Vec<String>,
    
    /// Target entity
    pub target_entity: String,
    
    /// Target grain
    pub target_grain: Vec<String>,
}

/// Join definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinDefinition {
    /// Table being joined
    pub table: String,
    
    /// Join condition columns (from current table to new table)
    pub on: Vec<String>,
    
    /// Join type: "inner", "left", "right", "outer"
    pub join_type: String,
}

/// Filter definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterDefinition {
    /// Filter expression (SQL-like)
    pub expr: String,
    
    /// Description of what this filter does
    pub description: Option<String>,
}

/// Formula/derived column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormulaDefinition {
    /// Expression to compute
    pub expr: String,
    
    /// Output column name
    pub as_name: String,
    
    /// Description
    pub description: Option<String>,
}

/// Aggregation expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationExpression {
    /// Sum aggregation
    Sum(String), // column name
    
    /// Count aggregation
    Count(String), // column name or "*"
    
    /// Average aggregation
    Avg(String), // column name
    
    /// Min aggregation
    Min(String), // column name
    
    /// Max aggregation
    Max(String), // column name
    
    /// Custom aggregation expression
    Custom(String), // full expression like "sum(col1) + sum(col2)"
}

/// Metric normalizer
/// 
/// Converts rules, SQL ASTs, and pipeline definitions into MetricDefinition
pub struct MetricNormalizer;

impl MetricNormalizer {
    /// Normalize a business rule into a MetricDefinition
    /// 
    /// This extracts the computation logic from a Rule and converts it to
    /// a normalized form suitable for row-level materialization.
    pub fn normalize_from_rule(rule: &Rule) -> Result<MetricDefinition> {
        let computation = &rule.computation;
        
        // Extract base tables from source entities
        // Note: This is a simplified extraction. In practice, you'd need
        // to look up which tables correspond to each entity.
        let base_tables = computation.source_entities.clone();
        
        // Extract joins (if any are specified in the rule)
        // For now, we'll extract from pipeline ops if available
        let joins = Vec::new(); // Will be populated from pipeline
        
        // Extract filters
        let filters = computation
            .filter_conditions
            .as_ref()
            .map(|filters| {
                filters
                    .iter()
                    .map(|(key, expr)| FilterDefinition {
                        expr: expr.clone(),
                        description: Some(format!("Filter on {}", key)),
                    })
                    .collect()
            })
            .unwrap_or_default();
        
        // Extract formulas from computation formula
        // Parse the formula to extract derived columns
        let formulas = Self::extract_formulas_from_formula(&computation.formula)?;
        
        // Extract group by (aggregation grain)
        let group_by = computation.aggregation_grain.clone();
        
        // Extract aggregations from formula
        let aggregations = Self::extract_aggregations_from_formula(&computation.formula)?;
        
        Ok(MetricDefinition {
            metric_name: rule.metric.clone(),
            base_tables,
            joins,
            filters,
            formulas,
            group_by,
            aggregations,
            source_entities: computation.source_entities.clone(),
            target_entity: rule.target_entity.clone(),
            target_grain: rule.target_grain.clone(),
        })
    }
    
    /// Normalize from pipeline operations
    /// 
    /// Converts a sequence of PipelineOp into a MetricDefinition
    pub fn normalize_from_pipeline(
        ops: &[PipelineOp],
        metric_name: &str,
        source_entities: &[String],
        target_entity: &str,
        target_grain: &[String],
    ) -> Result<MetricDefinition> {
        let mut base_tables = Vec::new();
        let mut joins = Vec::new();
        let mut filters = Vec::new();
        let mut formulas = Vec::new();
        let mut group_by = Vec::new();
        let mut aggregations = HashMap::new();
        
        for op in ops {
            match op {
                PipelineOp::Scan { table } => {
                    base_tables.push(table.clone());
                }
                PipelineOp::Join { table, on, join_type } => {
                    joins.push(JoinDefinition {
                        table: table.clone(),
                        on: on.clone(),
                        join_type: join_type.clone(),
                    });
                }
                PipelineOp::Filter { expr } => {
                    filters.push(FilterDefinition {
                        expr: expr.clone(),
                        description: None,
                    });
                }
                PipelineOp::Derive { expr, r#as } => {
                    formulas.push(FormulaDefinition {
                        expr: expr.clone(),
                        as_name: r#as.clone(),
                        description: None,
                    });
                }
                PipelineOp::Group { by, agg } => {
                    group_by = by.clone();
                    // Convert aggregation map to AggregationExpression
                    for (col_name, agg_expr) in agg {
                        aggregations.insert(
                            col_name.clone(),
                            Self::parse_aggregation_expression(agg_expr)?,
                        );
                    }
                }
                PipelineOp::Select { columns: _ } => {
                    // Select is typically the final step, already handled
                }
            }
        }
        
        Ok(MetricDefinition {
            metric_name: metric_name.to_string(),
            base_tables,
            joins,
            filters,
            formulas,
            group_by,
            aggregations,
            source_entities: source_entities.to_vec(),
            target_entity: target_entity.to_string(),
            target_grain: target_grain.to_vec(),
        })
    }
    
    /// Extract formulas from a formula string
    /// 
    /// This is a simplified parser. In practice, you'd want a proper SQL parser.
    fn extract_formulas_from_formula(formula: &str) -> Result<Vec<FormulaDefinition>> {
        // For now, if the formula contains assignments or AS clauses, extract them
        // This is a placeholder - real implementation would parse SQL AST
        
        // Simple heuristic: look for patterns like "col1 + col2 AS result"
        let mut formulas = Vec::new();
        
        // Check for AS clauses
        if formula.contains(" AS ") || formula.contains(" as ") {
            // Try to extract
            // This is simplified - real implementation needs proper parsing
            if let Some(captures) = regex::Regex::new(r"(.+?)\s+(?:AS|as)\s+(\w+)")
                .ok()
                .and_then(|re| re.captures(formula))
            {
                formulas.push(FormulaDefinition {
                    expr: captures.get(1).unwrap().as_str().to_string(),
                    as_name: captures.get(2).unwrap().as_str().to_string(),
                    description: None,
                });
            }
        }
        
        Ok(formulas)
    }
    
    /// Extract aggregations from formula
    /// 
    /// Parses aggregation expressions like "sum(paid_amount)" or "count(*)"
    fn extract_aggregations_from_formula(
        formula: &str,
    ) -> Result<HashMap<String, AggregationExpression>> {
        let mut aggregations = HashMap::new();
        
        // Simple regex-based extraction
        // Look for patterns like sum(col), count(*), etc.
        let sum_re = regex::Regex::new(r"sum\((\w+)\)")
            .map_err(|e| RcaError::Execution(format!("Failed to create regex: {}", e)))?;
        let count_re = regex::Regex::new(r"count\((\*|\w+)\)")
            .map_err(|e| RcaError::Execution(format!("Failed to create regex: {}", e)))?;
        let avg_re = regex::Regex::new(r"avg\((\w+)\)")
            .map_err(|e| RcaError::Execution(format!("Failed to create regex: {}", e)))?;
        let min_re = regex::Regex::new(r"min\((\w+)\)")
            .map_err(|e| RcaError::Execution(format!("Failed to create regex: {}", e)))?;
        let max_re = regex::Regex::new(r"max\((\w+)\)")
            .map_err(|e| RcaError::Execution(format!("Failed to create regex: {}", e)))?;
        
        for cap in sum_re.captures_iter(formula) {
            let col = cap.get(1).unwrap().as_str();
            aggregations.insert(
                format!("sum_{}", col),
                AggregationExpression::Sum(col.to_string()),
            );
        }
        
        for cap in count_re.captures_iter(formula) {
            let col = cap.get(1).unwrap().as_str();
            aggregations.insert(
                format!("count_{}", col),
                AggregationExpression::Count(col.to_string()),
            );
        }
        
        for cap in avg_re.captures_iter(formula) {
            let col = cap.get(1).unwrap().as_str();
            aggregations.insert(
                format!("avg_{}", col),
                AggregationExpression::Avg(col.to_string()),
            );
        }
        
        for cap in min_re.captures_iter(formula) {
            let col = cap.get(1).unwrap().as_str();
            aggregations.insert(
                format!("min_{}", col),
                AggregationExpression::Min(col.to_string()),
            );
        }
        
        for cap in max_re.captures_iter(formula) {
            let col = cap.get(1).unwrap().as_str();
            aggregations.insert(
                format!("max_{}", col),
                AggregationExpression::Max(col.to_string()),
            );
        }
        
        // If no aggregations found, treat the whole formula as a custom aggregation
        if aggregations.is_empty() {
            aggregations.insert(
                "metric_value".to_string(),
                AggregationExpression::Custom(formula.to_string()),
            );
        }
        
        Ok(aggregations)
    }
    
    /// Parse an aggregation expression string into AggregationExpression enum
    fn parse_aggregation_expression(expr: &str) -> Result<AggregationExpression> {
        let expr_lower = expr.to_lowercase();
        
        if expr_lower.starts_with("sum(") {
            let col = expr_lower
                .strip_prefix("sum(")
                .and_then(|s| s.strip_suffix(")"))
                .ok_or_else(|| RcaError::Execution("Invalid sum expression".to_string()))?;
            Ok(AggregationExpression::Sum(col.to_string()))
        } else if expr_lower.starts_with("count(") {
            let col = expr_lower
                .strip_prefix("count(")
                .and_then(|s| s.strip_suffix(")"))
                .ok_or_else(|| RcaError::Execution("Invalid count expression".to_string()))?;
            Ok(AggregationExpression::Count(col.to_string()))
        } else if expr_lower.starts_with("avg(") {
            let col = expr_lower
                .strip_prefix("avg(")
                .and_then(|s| s.strip_suffix(")"))
                .ok_or_else(|| RcaError::Execution("Invalid avg expression".to_string()))?;
            Ok(AggregationExpression::Avg(col.to_string()))
        } else if expr_lower.starts_with("min(") {
            let col = expr_lower
                .strip_prefix("min(")
                .and_then(|s| s.strip_suffix(")"))
                .ok_or_else(|| RcaError::Execution("Invalid min expression".to_string()))?;
            Ok(AggregationExpression::Min(col.to_string()))
        } else if expr_lower.starts_with("max(") {
            let col = expr_lower
                .strip_prefix("max(")
                .and_then(|s| s.strip_suffix(")"))
                .ok_or_else(|| RcaError::Execution("Invalid max expression".to_string()))?;
            Ok(AggregationExpression::Max(col.to_string()))
        } else {
            Ok(AggregationExpression::Custom(expr.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Rule, ComputationDefinition};
    use std::collections::HashMap;
    
    #[test]
    fn test_normalize_from_rule() {
        let rule = Rule {
            id: "test_rule".to_string(),
            system: "test_system".to_string(),
            metric: "test_metric".to_string(),
            target_entity: "payment_event".to_string(),
            target_grain: vec!["uuid".to_string(), "date".to_string()],
            computation: ComputationDefinition {
                description: "Test computation".to_string(),
                source_entities: vec!["entity1".to_string(), "entity2".to_string()],
                attributes_needed: HashMap::new(),
                formula: "sum(paid_amount)".to_string(),
                aggregation_grain: vec!["uuid".to_string(), "date".to_string()],
                filter_conditions: None,
                source_table: None,
                note: None,
            },
            labels: None,
        };
        
        let metric_def = MetricNormalizer::normalize_from_rule(&rule).unwrap();
        assert_eq!(metric_def.metric_name, "test_metric");
        assert_eq!(metric_def.source_entities.len(), 2);
        assert!(!metric_def.aggregations.is_empty());
    }
    
    #[test]
    fn test_extract_aggregations() {
        let formula = "sum(paid_amount) + count(*)";
        let aggs = MetricNormalizer::extract_aggregations_from_formula(formula).unwrap();
        assert!(aggs.len() >= 1);
    }
}

