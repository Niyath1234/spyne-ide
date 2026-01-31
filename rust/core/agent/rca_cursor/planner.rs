//! Execution Planner for RcaCursor
//! 
//! Agentic layer inside Cursor that chooses:
//! - Join order (reorder by selectivity)
//! - Pushdown level (how early to apply filters)
//! - Sampling vs full scan (based on mode)
//! - Early stopping conditions (confidence threshold)

use crate::core::agent::rca_cursor::{ValidatedTask, ExecutionMode};
use crate::core::engine::logical_plan::LogicalPlan;
use crate::metadata::Metadata;
use crate::error::{RcaError, Result};
use std::time::Duration;

/// Execution plan with optimized node order and execution strategy
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Optimized logical plan nodes
    pub nodes: Vec<ExecutionNode>,
    /// Early stopping conditions
    pub stop_conditions: StopConditions,
    /// Cost budget for this execution
    pub cost_budget: f64,
}

/// Execution node (simplified representation of logical plan node)
#[derive(Debug, Clone)]
pub enum ExecutionNode {
    /// Scan a table
    Scan {
        table: String,
        filters: Vec<String>,
        sampling: Option<SamplingStrategy>,
    },
    /// Join two tables
    Join {
        left_table: String,
        right_table: String,
        keys: Vec<String>,
        join_type: String,
        strategy: JoinStrategy,
    },
    /// Apply filter
    Filter {
        expr: String,
        pushdown: bool, // Whether to push down to scan
    },
    /// Aggregate
    Aggregate {
        group_by: Vec<String>,
        aggregations: Vec<String>,
    },
}

/// Join execution strategy
#[derive(Debug, Clone)]
pub enum JoinStrategy {
    /// Hash join (default for large tables)
    HashJoin,
    /// Broadcast join (for small tables)
    BroadcastJoin,
    /// Nested loop join (for very small tables)
    NestedLoopJoin,
}

/// Sampling strategy
#[derive(Debug, Clone)]
pub struct SamplingStrategy {
    /// Sampling method
    pub method: SamplingMethod,
    /// Sample size or ratio
    pub size: SamplingSize,
}

/// Sampling method
#[derive(Debug, Clone)]
pub enum SamplingMethod {
    /// Random sampling
    Random,
    /// Stratified sampling (by grain key)
    Stratified,
    /// Top-N sampling (highest value rows)
    TopN,
}

/// Sampling size
#[derive(Debug, Clone)]
pub enum SamplingSize {
    /// Fixed number of rows
    Rows(usize),
    /// Percentage of total rows (0.0-1.0)
    Ratio(f64),
}

/// Early stopping conditions
#[derive(Debug, Clone)]
pub struct StopConditions {
    /// Maximum rows to process
    pub max_rows: Option<usize>,
    /// Maximum execution time
    pub max_time: Option<Duration>,
    /// Confidence threshold (stop if confidence reaches this)
    pub confidence_threshold: Option<f64>,
    /// Cost budget threshold
    pub cost_threshold: Option<f64>,
}

/// Execution planner
pub struct ExecutionPlanner {
    metadata: Metadata,
}

impl ExecutionPlanner {
    /// Create a new execution planner
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }

    /// Plan execution for a validated task
    /// 
    /// Takes logical plans for both systems and creates optimized execution plans
    pub fn plan_execution(
        &self,
        validated_task: &ValidatedTask,
        logical_plan_a: &LogicalPlan,
        logical_plan_b: &LogicalPlan,
    ) -> Result<(ExecutionPlan, ExecutionPlan)> {
        let plan_a = self.optimize_plan(logical_plan_a, &validated_task.task.mode)?;
        let plan_b = self.optimize_plan(logical_plan_b, &validated_task.task.mode)?;

        let stop_conditions = self.determine_stop_conditions(&validated_task.task.mode);

        let execution_plan_a = ExecutionPlan {
            nodes: plan_a,
            stop_conditions: stop_conditions.clone(),
            cost_budget: self.estimate_cost_budget(&validated_task.task.mode),
        };

        let execution_plan_b = ExecutionPlan {
            nodes: plan_b,
            stop_conditions,
            cost_budget: self.estimate_cost_budget(&validated_task.task.mode),
        };

        Ok((execution_plan_a, execution_plan_b))
    }

    /// Optimize a logical plan based on execution mode
    fn optimize_plan(
        &self,
        logical_plan: &LogicalPlan,
        mode: &ExecutionMode,
    ) -> Result<Vec<ExecutionNode>> {
        // Convert logical plan to execution nodes
        let mut nodes = self.logical_plan_to_nodes(logical_plan)?;

        // Apply optimizations based on mode
        match mode {
            ExecutionMode::Fast => {
                // Fast mode: aggressive sampling, early stopping
                nodes = self.apply_sampling(nodes, SamplingStrategy {
                    method: SamplingMethod::Random,
                    size: SamplingSize::Ratio(0.1), // 10% sample
                })?;
            }
            ExecutionMode::Deep => {
                // Deep mode: full scan, no sampling
                // Already have full scan nodes
            }
            ExecutionMode::Forensic => {
                // Forensic mode: full scan, no sampling, full lineage
                // Already have full scan nodes
            }
        }

        // Reorder joins by selectivity (smaller tables first)
        nodes = self.reorder_joins(nodes)?;

        // Push down filters where possible
        nodes = self.pushdown_filters(nodes)?;

        Ok(nodes)
    }

    /// Convert logical plan to execution nodes
    fn logical_plan_to_nodes(&self, plan: &LogicalPlan) -> Result<Vec<ExecutionNode>> {
        // This is a simplified conversion
        // In practice, we'd traverse the logical plan tree and convert each node
        match plan {
            LogicalPlan::Scan { table, filters, .. } => {
                let filter_exprs: Vec<String> = filters.iter()
                    .map(|f| {
                        // Format filter value properly - extract from Option and serialize to JSON string
                        let value_str = if let Some(ref val) = f.value {
                            serde_json::to_string(val).unwrap_or_else(|_| format!("{:?}", val))
                        } else {
                            "NULL".to_string()
                        };
                        format!("{} {} {}", f.column, f.operator, value_str)
                    })
                    .collect();
                
                Ok(vec![ExecutionNode::Scan {
                    table: table.clone(),
                    filters: filter_exprs,
                    sampling: None,
                }])
            }
            LogicalPlan::Join { left, right, keys, join_type, .. } => {
                // Recursively convert left and right
                let mut left_nodes = self.logical_plan_to_nodes(left)?;
                let mut right_nodes = self.logical_plan_to_nodes(right)?;
                
                // Extract table names from scan nodes (simplified)
                let left_table = self.extract_table_name(left)?;
                let right_table = self.extract_table_name(right)?;
                
                // Determine join strategy based on table sizes
                let strategy = self.choose_join_strategy(&left_table, &right_table)?;
                
                // Combine nodes
                left_nodes.append(&mut right_nodes);
                left_nodes.push(ExecutionNode::Join {
                    left_table,
                    right_table,
                    keys: keys.clone(),
                    join_type: format!("{:?}", join_type),
                    strategy,
                });
                
                Ok(left_nodes)
            }
            LogicalPlan::Filter { expr, input, .. } => {
                let mut nodes = self.logical_plan_to_nodes(input)?;
                
                // Format filter value properly - extract from Option and serialize to JSON string
                let value_str = if let Some(ref val) = expr.value {
                    serde_json::to_string(val).unwrap_or_else(|_| format!("{:?}", val))
                } else {
                    "NULL".to_string()
                };
                
                // Try to push down filter to scan node if possible
                if let Some(ExecutionNode::Scan { filters, .. }) = nodes.first_mut() {
                    filters.push(format!("{} {} {}", expr.column, expr.operator, value_str));
                } else {
                    nodes.push(ExecutionNode::Filter {
                        expr: format!("{} {} {}", expr.column, expr.operator, value_str),
                        pushdown: false,
                    });
                }
                
                Ok(nodes)
            }
            LogicalPlan::Aggregate { group_by, aggregations, input, .. } => {
                let mut nodes = self.logical_plan_to_nodes(input)?;
                
                // Format aggregations properly for executor parsing
                // Executor expects format: "name: AggType(column)"
                let agg_exprs: Vec<String> = aggregations.iter()
                    .map(|(name, expr)| {
                        match expr {
                            crate::core::engine::logical_plan::AggregationExpr::Sum(col) => {
                                format!("{}: Sum({})", name, col)
                            }
                            crate::core::engine::logical_plan::AggregationExpr::Count(col) => {
                                format!("{}: Count({})", name, col)
                            }
                            crate::core::engine::logical_plan::AggregationExpr::Avg(col) => {
                                format!("{}: Avg({})", name, col)
                            }
                            crate::core::engine::logical_plan::AggregationExpr::Min(col) => {
                                format!("{}: Min({})", name, col)
                            }
                            crate::core::engine::logical_plan::AggregationExpr::Max(col) => {
                                format!("{}: Max({})", name, col)
                            }
                            crate::core::engine::logical_plan::AggregationExpr::Custom(expr_str) => {
                                // For custom expressions, try to infer aggregation type
                                // Default to Sum if it's just a column name
                                format!("{}: Sum({})", name, expr_str)
                            }
                        }
                    })
                    .collect();
                
                nodes.push(ExecutionNode::Aggregate {
                    group_by: group_by.clone(),
                    aggregations: agg_exprs,
                });
                
                Ok(nodes)
            }
            LogicalPlan::Project { columns, input, .. } => {
                // Projection is typically handled during execution
                // For now, just pass through
                self.logical_plan_to_nodes(input)
            }
        }
    }

    /// Extract table name from a logical plan (simplified)
    fn extract_table_name(&self, plan: &LogicalPlan) -> Result<String> {
        match plan {
            LogicalPlan::Scan { table, .. } => Ok(table.clone()),
            LogicalPlan::Join { left, .. } => self.extract_table_name(left),
            LogicalPlan::Filter { input, .. } => self.extract_table_name(input),
            LogicalPlan::Project { input, .. } => self.extract_table_name(input),
            LogicalPlan::Aggregate { input, .. } => self.extract_table_name(input),
        }
    }

    /// Choose join strategy based on table sizes
    fn choose_join_strategy(&self, left_table: &str, right_table: &str) -> Result<JoinStrategy> {
        // Estimate table sizes from metadata
        let left_size = self.estimate_table_size(left_table)?;
        let right_size = self.estimate_table_size(right_table)?;
        
        // Small table threshold: 10MB
        let small_table_threshold = 10 * 1024 * 1024;
        
        if left_size < small_table_threshold || right_size < small_table_threshold {
            Ok(JoinStrategy::BroadcastJoin)
        } else if left_size < 100_000 || right_size < 100_000 {
            Ok(JoinStrategy::NestedLoopJoin)
        } else {
            Ok(JoinStrategy::HashJoin)
        }
    }

    /// Estimate table size in bytes
    fn estimate_table_size(&self, table_name: &str) -> Result<usize> {
        // In practice, we'd use metadata statistics
        // For now, return a default estimate
        Ok(100_000_000) // 100MB default
    }

    /// Apply sampling to execution nodes
    fn apply_sampling(
        &self,
        mut nodes: Vec<ExecutionNode>,
        sampling: SamplingStrategy,
    ) -> Result<Vec<ExecutionNode>> {
        // Apply sampling to scan nodes
        for node in &mut nodes {
            if let ExecutionNode::Scan { sampling: ref mut s, .. } = node {
                *s = Some(sampling.clone());
            }
        }
        Ok(nodes)
    }

    /// Reorder joins by selectivity (smaller tables first)
    /// 
    /// This optimization reorders joins to process smaller tables first,
    /// which reduces memory usage and improves join performance.
    fn reorder_joins(&self, mut nodes: Vec<ExecutionNode>) -> Result<Vec<ExecutionNode>> {
        // Separate joins from other nodes
        let mut joins: Vec<(usize, ExecutionNode)> = Vec::new();
        let mut other_nodes: Vec<(usize, ExecutionNode)> = Vec::new();
        
        for (idx, node) in nodes.into_iter().enumerate() {
            match &node {
                ExecutionNode::Join { .. } => {
                    joins.push((idx, node));
                }
                _ => {
                    other_nodes.push((idx, node));
                }
            }
        }
        
        // If no joins, return original order
        if joins.is_empty() {
            return Ok(other_nodes.into_iter().map(|(_, n)| n).collect());
        }
        
        // Sort joins by estimated table size (smaller first)
        // Extract table names and estimate sizes
        joins.sort_by(|a, b| {
            let size_a = self.estimate_join_size(&a.1).unwrap_or(usize::MAX);
            let size_b = self.estimate_join_size(&b.1).unwrap_or(usize::MAX);
            size_a.cmp(&size_b)
        });
        
        // Reconstruct node list maintaining relative order
        let mut result: Vec<(usize, ExecutionNode)> = Vec::new();
        result.extend(other_nodes);
        result.extend(joins);
        result.sort_by_key(|(idx, _)| *idx);
        
        Ok(result.into_iter().map(|(_, n)| n).collect())
    }
    
    /// Estimate size of a join node for reordering
    fn estimate_join_size(&self, node: &ExecutionNode) -> Option<usize> {
        match node {
            ExecutionNode::Join { left_table, right_table, .. } => {
                let left_size = self.estimate_table_size(left_table).ok()?;
                let right_size = self.estimate_table_size(right_table).ok()?;
                // Estimate join result size as min of the two (conservative)
                Some(left_size.min(right_size))
            }
            _ => None,
        }
    }

    /// Push down filters to scan nodes where possible
    fn pushdown_filters(&self, mut nodes: Vec<ExecutionNode>) -> Result<Vec<ExecutionNode>> {
        // Collect filters and merge with scan nodes
        let mut filters_to_push = Vec::new();
        let mut filtered_nodes = Vec::new();
        
        for node in nodes {
            match node {
                ExecutionNode::Filter { expr, pushdown } if pushdown => {
                    filters_to_push.push(expr);
                }
                ExecutionNode::Scan { mut filters, table, sampling } => {
                    filters.extend(filters_to_push.drain(..));
                    filtered_nodes.push(ExecutionNode::Scan {
                        table,
                        filters,
                        sampling,
                    });
                }
                other => {
                    filtered_nodes.push(other);
                }
            }
        }
        
        Ok(filtered_nodes)
    }

    /// Determine stop conditions based on execution mode
    fn determine_stop_conditions(&self, mode: &ExecutionMode) -> StopConditions {
        match mode {
            ExecutionMode::Fast => StopConditions {
                max_rows: Some(1_000_000), // 1M rows max
                max_time: Some(Duration::from_secs(60)), // 1 minute
                confidence_threshold: Some(0.8), // Stop at 80% confidence
                cost_threshold: Some(100.0), // Low cost budget
            },
            ExecutionMode::Deep => StopConditions {
                max_rows: Some(10_000_000), // 10M rows max
                max_time: Some(Duration::from_secs(300)), // 5 minutes
                confidence_threshold: Some(0.95), // Stop at 95% confidence
                cost_threshold: Some(1000.0), // Higher cost budget
            },
            ExecutionMode::Forensic => StopConditions {
                max_rows: None, // No limit
                max_time: Some(Duration::from_secs(1800)), // 30 minutes
                confidence_threshold: None, // No early stopping
                cost_threshold: None, // No cost limit
            },
        }
    }

    /// Estimate cost budget based on execution mode
    fn estimate_cost_budget(&self, mode: &ExecutionMode) -> f64 {
        match mode {
            ExecutionMode::Fast => 100.0,
            ExecutionMode::Deep => 1000.0,
            ExecutionMode::Forensic => 10000.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_planning() {
        // Test would require mock metadata and logical plans
    }
}

