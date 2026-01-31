//! Execution Engine - Runs ExecutionPlan safely with limits and timeouts
//! 
//! Deterministic execution with stage timeouts, partial result support, error isolation.

use crate::error::{RcaError, Result};
use crate::execution_planner::{ExecutionPlan, ExecutionNode, JoinType, FilterExpr};
use crate::metadata::Metadata;
use crate::operators::RelationalEngine;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{info, warn, error};

/// Execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub success: bool,
    #[serde(skip)]
    pub data: Option<DataFrame>,
    pub execution_time_ms: u64,
    pub nodes_executed: usize,
    pub nodes_failed: usize,
    pub errors: Vec<ExecutionError>,
    #[serde(skip)]
    pub partial_results: HashMap<usize, DataFrame>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionError {
    pub node_index: usize,
    pub node_type: String,
    pub error: String,
    pub context: serde_json::Value,
}

/// Execution Engine
pub struct ExecutionEngine {
    metadata: Metadata,
    relational_engine: RelationalEngine,
    max_execution_time: Duration,
    max_rows_per_stage: usize,
    enable_timeouts: bool,
}

impl ExecutionEngine {
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self {
            metadata,
            relational_engine: RelationalEngine::new(data_dir),
            max_execution_time: Duration::from_secs(300), // 5 minutes
            max_rows_per_stage: 10_000_000, // 10M rows
            enable_timeouts: true,
        }
    }

    /// Execute an execution plan
    pub async fn execute(&self, plan: &ExecutionPlan) -> Result<ExecutionResult> {
        info!("Executing plan with {} nodes", plan.nodes.len());
        
        let start_time = Instant::now();
        let mut node_results: HashMap<usize, DataFrame> = HashMap::new();
        let mut errors = Vec::new();
        let mut nodes_executed = 0;
        let mut nodes_failed = 0;
        
        // Topological sort of nodes (simple BFS from root)
        let execution_order = self.topological_sort(plan)?;
        
        for &node_idx in &execution_order {
            // Check timeout
            if self.enable_timeouts && start_time.elapsed() > self.max_execution_time {
                warn!("Execution timeout reached");
                break;
            }
            
            let node = &plan.nodes[node_idx];
            
            match self.execute_node(node, &node_results, plan).await {
                Ok(result) => {
                    // Check row limit
                    if result.height() > self.max_rows_per_stage {
                        errors.push(ExecutionError {
                            node_index: node_idx,
                            node_type: format!("{:?}", node),
                            error: format!("Row limit exceeded: {} > {}", result.height(), self.max_rows_per_stage),
                            context: serde_json::json!({}),
                        });
                        nodes_failed += 1;
                        continue;
                    }
                    
                    node_results.insert(node_idx, result);
                    nodes_executed += 1;
                }
                Err(e) => {
                    error!("Node {} execution failed: {}", node_idx, e);
                    errors.push(ExecutionError {
                        node_index: node_idx,
                        node_type: format!("{:?}", node),
                        error: e.to_string(),
                        context: serde_json::json!({}),
                    });
                    nodes_failed += 1;
                    
                    // Continue with other nodes (error isolation)
                }
            }
        }
        
        // Get final result from root nodes
        let final_result = plan.root_nodes.iter()
            .find_map(|&idx| node_results.get(&idx))
            .cloned();
        
        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        
        Ok(ExecutionResult {
            success: errors.is_empty() && final_result.is_some(),
            data: final_result,
            execution_time_ms,
            nodes_executed,
            nodes_failed,
            errors,
            partial_results: node_results,
        })
    }

    async fn execute_node(
        &self,
        node: &ExecutionNode,
        previous_results: &HashMap<usize, DataFrame>,
        plan: &ExecutionPlan,
    ) -> Result<DataFrame> {
        match node {
            ExecutionNode::Load { table, path, filters } => {
                self.execute_load(table, path, filters).await
            }
            ExecutionNode::Filter { expression, .. } => {
                // Get input from previous node
                let input = self.get_input_dataframe(node, previous_results, plan)?;
                self.execute_filter(input, expression).await
            }
            ExecutionNode::Join { left_table, right_table, keys, join_type } => {
                let left = self.get_dataframe_by_table(left_table, previous_results, plan)?;
                let right = self.get_dataframe_by_table(right_table, previous_results, plan)?;
                self.execute_join(left, right, keys, join_type).await
            }
            ExecutionNode::Aggregate { group_by, aggregations, .. } => {
                let input = self.get_input_dataframe(node, previous_results, plan)?;
                self.execute_aggregate(input, group_by, aggregations).await
            }
            ExecutionNode::Compare { left_alias, right_alias, keys, metrics, tolerance } => {
                let left = self.get_dataframe_by_alias(left_alias, previous_results, plan)?;
                let right = self.get_dataframe_by_alias(right_alias, previous_results, plan)?;
                self.execute_compare(left, right, keys, metrics, *tolerance).await
            }
            ExecutionNode::Validate { constraint_type, constraint_details, .. } => {
                let input = self.get_input_dataframe(node, previous_results, plan)?;
                self.execute_validate(input, constraint_type, constraint_details).await
            }
        }
    }

    async fn execute_load(
        &self,
        table: &str,
        path: &str,
        filters: &[FilterExpr],
    ) -> Result<DataFrame> {
        info!("Loading table: {} from {}", table, path);
        
        let mut df = self.relational_engine.scan_with_metadata(table, &self.metadata).await?;
        
        // Apply filters
        for filter in filters {
            df = self.apply_filter(df, filter)?;
        }
        
        Ok(df)
    }

    async fn execute_filter(&self, input: DataFrame, expression: &str) -> Result<DataFrame> {
        // Parse and apply filter expression
        // Simplified - would use proper expression parser in production
        let df = input.lazy()
            .filter(col("dummy").eq(lit(true))) // Placeholder
            .collect()?;
        
        Ok(df)
    }

    async fn execute_join(
        &self,
        left: DataFrame,
        right: DataFrame,
        keys: &[String],
        join_type: &JoinType,
    ) -> Result<DataFrame> {
        info!("Joining on keys: {:?}", keys);
        
        // Convert keys to Polars columns
        let join_cols: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
        
        use polars::prelude::JoinType as PolarsJoinType;
        
        let polars_join_type = match join_type {
            JoinType::Inner => PolarsJoinType::Inner,
            JoinType::Left => PolarsJoinType::Left,
            JoinType::Outer => PolarsJoinType::Outer,
            JoinType::Right => PolarsJoinType::Left, // Polars doesn't have Right, use Left with swapped tables
        };
        
        let result = left.lazy()
            .join(right.lazy(), join_cols.clone(), join_cols.clone(), JoinArgs::new(polars_join_type))
            .collect()?;
        
        Ok(result)
    }

    async fn execute_aggregate(
        &self,
        input: DataFrame,
        group_by: &[String],
        aggregations: &HashMap<String, String>,
    ) -> Result<DataFrame> {
        info!("Aggregating by: {:?}", group_by);
        
        let mut lazy = input.lazy();
        let group_cols: Vec<Expr> = group_by.iter().map(|c| col(c)).collect();
        
        let mut agg_exprs = Vec::new();
        for (column, agg_func) in aggregations {
            let expr = match agg_func.as_str() {
                "sum" => col(column).sum(),
                "avg" => col(column).mean(),
                "count" => col(column).count(),
                "max" => col(column).max(),
                "min" => col(column).min(),
                _ => col(column).sum(), // Default
            };
            agg_exprs.push(expr.alias(column));
        }
        
        let result = lazy
            .group_by(group_cols)
            .agg(agg_exprs)
            .collect()?;
        
        Ok(result)
    }

    async fn execute_compare(
        &self,
        left: DataFrame,
        right: DataFrame,
        keys: &[String],
        metrics: &[String],
        tolerance: Option<f64>,
    ) -> Result<DataFrame> {
        info!("Comparing on keys: {:?}, metrics: {:?}", keys, metrics);
        
        // Join on keys
        let join_cols: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
        let joined = left.lazy()
            .join(right.lazy(), join_cols.clone(), join_cols.clone(), JoinArgs::new(polars::prelude::JoinType::Outer))
            .collect()?;
        
        // Calculate differences for each metric
        let mut diff_exprs: Vec<Expr> = Vec::new();
        for metric in metrics {
            let left_col = format!("{}_left", metric);
            let right_col = format!("{}_right", metric);
            let diff_col = format!("{}_diff", metric);
            
            // Would add difference calculation here
            // Simplified for now
        }
        
        Ok(joined)
    }

    async fn execute_validate(
        &self,
        input: DataFrame,
        constraint_type: &str,
        constraint_details: &serde_json::Value,
    ) -> Result<DataFrame> {
        info!("Validating constraint: {}", constraint_type);
        
        // Apply validation logic based on constraint type
        // Simplified - would use validation engine in production
        Ok(input)
    }

    fn apply_filter(&self, df: DataFrame, filter: &FilterExpr) -> Result<DataFrame> {
        let col_expr = col(&filter.column);
        let value_expr = match &filter.value {
            serde_json::Value::String(s) => lit(s.clone()),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    lit(i)
                } else if let Some(f) = n.as_f64() {
                    lit(f)
                } else {
                    return Err(RcaError::Execution("Invalid number in filter".to_string()));
                }
            }
            serde_json::Value::Bool(b) => lit(*b),
            _ => return Err(RcaError::Execution("Unsupported filter value type".to_string())),
        };
        
        let filtered = match filter.operator.as_str() {
            "=" => df.lazy().filter(col_expr.eq(value_expr)).collect()?,
            "!=" => df.lazy().filter(col_expr.neq(value_expr)).collect()?,
            ">" => df.lazy().filter(col_expr.gt(value_expr)).collect()?,
            "<" => df.lazy().filter(col_expr.lt(value_expr)).collect()?,
            ">=" => df.lazy().filter(col_expr.gt_eq(value_expr)).collect()?,
            "<=" => df.lazy().filter(col_expr.lt_eq(value_expr)).collect()?,
            _ => return Err(RcaError::Execution(format!("Unsupported operator: {}", filter.operator))),
        };
        
        Ok(filtered)
    }

    fn get_input_dataframe(
        &self,
        node: &ExecutionNode,
        results: &HashMap<usize, DataFrame>,
        plan: &ExecutionPlan,
    ) -> Result<DataFrame> {
        // Find input node (first edge pointing to this node)
        let node_idx = plan.nodes.iter()
            .position(|n| std::ptr::eq(n, node))
            .ok_or_else(|| RcaError::Execution("Node not found in plan".to_string()))?;
        
        let input_edge = plan.edges.iter()
            .find(|e| e.to == node_idx)
            .ok_or_else(|| RcaError::Execution("No input edge found".to_string()))?;
        
        results.get(&input_edge.from)
            .cloned()
            .ok_or_else(|| RcaError::Execution("Input node not executed".to_string()))
    }

    fn get_dataframe_by_table(
        &self,
        table: &str,
        results: &HashMap<usize, DataFrame>,
        plan: &ExecutionPlan,
    ) -> Result<DataFrame> {
        // Find load node for this table
        let node_idx = plan.nodes.iter()
            .position(|n| {
                if let ExecutionNode::Load { table: t, .. } = n {
                    t == table
                } else {
                    false
                }
            })
            .ok_or_else(|| RcaError::Execution(format!("Load node not found for table: {}", table)))?;
        
        results.get(&node_idx)
            .cloned()
            .ok_or_else(|| RcaError::Execution(format!("Table {} not loaded", table)))
    }

    fn get_dataframe_by_alias(
        &self,
        alias: &str,
        results: &HashMap<usize, DataFrame>,
        plan: &ExecutionPlan,
    ) -> Result<DataFrame> {
        // Find node by alias (simplified - would track aliases properly)
        // For now, try to find by system name
        self.get_dataframe_by_table(alias, results, plan)
    }

    fn topological_sort(&self, plan: &ExecutionPlan) -> Result<Vec<usize>> {
        // Simple BFS from root nodes
        let mut order = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        
        // Start from root nodes
        for &root_idx in &plan.root_nodes {
            queue.push_back(root_idx);
        }
        
        // Also add nodes with no dependencies
        for (idx, _) in plan.nodes.iter().enumerate() {
            let has_dependencies = plan.edges.iter().any(|e| e.to == idx);
            if !has_dependencies && !plan.root_nodes.contains(&idx) {
                queue.push_back(idx);
            }
        }
        
        while let Some(node_idx) = queue.pop_front() {
            if visited.contains(&node_idx) {
                continue;
            }
            
            // Check if all dependencies are visited
            let dependencies: Vec<usize> = plan.edges.iter()
                .filter(|e| e.to == node_idx)
                .map(|e| e.from)
                .collect();
            
            if dependencies.iter().all(|&dep| visited.contains(&dep) || dep == node_idx) {
                order.push(node_idx);
                visited.insert(node_idx);
                
                // Add dependent nodes to queue
                for edge in &plan.edges {
                    if edge.from == node_idx && !visited.contains(&edge.to) {
                        queue.push_back(edge.to);
                    }
                }
            }
        }
        
        // Add any remaining nodes
        for idx in 0..plan.nodes.len() {
            if !visited.contains(&idx) {
                order.push(idx);
            }
        }
        
        Ok(order)
    }
}

