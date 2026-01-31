//! Execution Planner - Builds ExecutionPlan DAG
//! 
//! Creates a unified execution plan DAG for both RCA and DV tasks.
//! Nodes: Load, Filter, Join, Aggregate, Compare, Validate

use crate::error::{RcaError, Result};
use crate::task_grounder::{GroundedTask, TableCandidate};
use crate::graph::Hypergraph;
use crate::metadata::Metadata;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{info, debug, warn};

/// Execution Plan DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub nodes: Vec<ExecutionNode>,
    pub edges: Vec<PlanEdge>,
    pub root_nodes: Vec<usize>, // Indices of root nodes
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanEdge {
    pub from: usize,
    pub to: usize,
}

/// Execution Node Types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ExecutionNode {
    /// Load data from table
    Load {
        table: String,
        path: String,
        filters: Vec<FilterExpr>,
    },
    
    /// Filter rows
    Filter {
        expression: String,
        description: String,
    },
    
    /// Join two dataframes
    Join {
        left_table: String,
        right_table: String,
        keys: Vec<String>,
        join_type: JoinType,
    },
    
    /// Aggregate data
    Aggregate {
        group_by: Vec<String>,
        aggregations: HashMap<String, String>, // column -> aggregation function
        description: String,
    },
    
    /// Compare two dataframes (for RCA)
    Compare {
        left_alias: String,
        right_alias: String,
        keys: Vec<String>,
        metrics: Vec<String>,
        tolerance: Option<f64>,
    },
    
    /// Validate data (for DV)
    Validate {
        constraint_type: String,
        constraint_details: serde_json::Value,
        description: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterExpr {
    pub column: String,
    pub operator: String,
    pub value: serde_json::Value,
}

/// Execution Planner - Builds execution plan DAG
pub struct ExecutionPlanner {
    metadata: Metadata,
    graph: Hypergraph,
}

impl ExecutionPlanner {
    pub fn new(metadata: Metadata, graph: Hypergraph) -> Self {
        Self { metadata, graph }
    }

    /// Build execution plan from grounded task
    pub fn build_plan(&self, grounded_task: &GroundedTask) -> Result<ExecutionPlan> {
        info!("Building execution plan for {:?} task", grounded_task.task_type);
        
        match grounded_task.task_type {
            crate::intent_compiler::TaskType::QUERY => {
                // Direct query - handled by QueryEngine, not ExecutionPlanner
                Err(crate::error::RcaError::Execution(
                    "QUERY tasks should be handled by QueryEngine, not ExecutionPlanner".to_string()
                ))
            }
            crate::intent_compiler::TaskType::RCA => {
                self.build_rca_plan(grounded_task)
            }
            crate::intent_compiler::TaskType::DV => {
                self.build_dv_plan(grounded_task)
            }
        }
    }

    fn build_rca_plan(&self, task: &GroundedTask) -> Result<ExecutionPlan> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut root_nodes = Vec::new();
        let mut node_map: HashMap<String, usize> = HashMap::new();
        
        // Group tables by system
        let mut tables_by_system: HashMap<String, Vec<&TableCandidate>> = HashMap::new();
        for table in &task.candidate_tables {
            tables_by_system
                .entry(table.system.clone())
                .or_insert_with(Vec::new)
                .push(table);
        }
        
        // Build load nodes for each system
        let mut system_nodes: HashMap<String, Vec<usize>> = HashMap::new();
        for (system, tables) in &tables_by_system {
            let mut system_node_indices = Vec::new();
            
            for table in tables {
                // Create load node
                let table_metadata = self.metadata.get_table(&table.table_name)
                    .ok_or_else(|| RcaError::Metadata(format!("Table not found: {}", table.table_name)))?;
                
                // Apply constraints as filters
                let filters: Vec<FilterExpr> = task.constraint_specs.iter()
                    .filter(|c| c.table.as_ref().map(|t| t == &table.table_name).unwrap_or(false))
                    .map(|c| FilterExpr {
                        column: c.column.clone(),
                        operator: c.operator.clone(),
                        value: c.value.clone(),
                    })
                    .collect();
                
                let load_node = ExecutionNode::Load {
                    table: table.table_name.clone(),
                    path: table_metadata.path.clone(),
                    filters,
                };
                
                let node_idx = nodes.len();
                nodes.push(load_node);
                system_node_indices.push(node_idx);
                node_map.insert(table.table_name.clone(), node_idx);
            }
            
            // If multiple tables per system, join them
            if system_node_indices.len() > 1 {
                // Find join paths between tables
                let joined_node_idx = self.join_tables_in_system(
                    &mut nodes,
                    &mut edges,
                    &system_node_indices,
                    &tables,
                )?;
                system_nodes.insert(system.clone(), vec![joined_node_idx]);
            } else {
                system_nodes.insert(system.clone(), system_node_indices);
            }
        }
        
        // Aggregate each system to required grain
        let mut aggregated_nodes: HashMap<String, usize> = HashMap::new();
        for (system, node_indices) in &system_nodes {
            if node_indices.is_empty() {
                continue;
            }
            
            // Get aggregation requirements from rules
            let aggregations = self.get_aggregations_for_system(system, &task.metrics)?;
            
            let agg_node = ExecutionNode::Aggregate {
                group_by: task.required_grain.clone(),
                aggregations,
                description: format!("Aggregate {} to grain {:?}", system, task.required_grain),
            };
            
            let agg_idx = nodes.len();
            nodes.push(agg_node);
            
            // Connect to system nodes
            for &node_idx in node_indices {
                edges.push(PlanEdge {
                    from: node_idx,
                    to: agg_idx,
                });
            }
            
            aggregated_nodes.insert(system.clone(), agg_idx);
        }
        
        // Compare systems (for RCA)
        if aggregated_nodes.len() >= 2 {
            let system_vec: Vec<_> = aggregated_nodes.keys().collect();
            let left_system = system_vec[0];
            let right_system = system_vec[1];
            
            let compare_node = ExecutionNode::Compare {
                left_alias: left_system.clone(),
                right_alias: right_system.clone(),
                keys: task.required_grain.clone(),
                metrics: task.metrics.clone(),
                tolerance: Some(0.01), // Default tolerance
            };
            
            let compare_idx = nodes.len();
            nodes.push(compare_node);
            
            // Connect aggregated nodes to compare
            if let Some(&left_idx) = aggregated_nodes.get(left_system) {
                edges.push(PlanEdge { from: left_idx, to: compare_idx });
            }
            if let Some(&right_idx) = aggregated_nodes.get(right_system) {
                edges.push(PlanEdge { from: right_idx, to: compare_idx });
            }
        }
        
        // Root nodes are the final compare node or aggregated nodes if only one system
        if let Some(compare_idx) = nodes.iter().enumerate()
            .find(|(_, n)| matches!(n, ExecutionNode::Compare { .. }))
            .map(|(idx, _)| idx) {
            root_nodes.push(compare_idx);
        } else {
            root_nodes.extend(aggregated_nodes.values());
        }
        
        Ok(ExecutionPlan {
            nodes,
            edges,
            root_nodes,
        })
    }

    fn build_dv_plan(&self, task: &GroundedTask) -> Result<ExecutionPlan> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut root_nodes = Vec::new();
        
        // Get validation constraint
        let constraint = task.candidate_tables.first()
            .ok_or_else(|| RcaError::Metadata("No candidate tables for DV".to_string()))?;
        
        let table_metadata = self.metadata.get_table(&constraint.table_name)
            .ok_or_else(|| RcaError::Metadata(format!("Table not found: {}", constraint.table_name)))?;
        
        // Load node
        let filters: Vec<FilterExpr> = task.constraint_specs.iter()
            .map(|c| FilterExpr {
                column: c.column.clone(),
                operator: c.operator.clone(),
                value: c.value.clone(),
            })
            .collect();
        
        let load_node = ExecutionNode::Load {
            table: constraint.table_name.clone(),
            path: table_metadata.path.clone(),
            filters,
        };
        
        let load_idx = nodes.len();
        nodes.push(load_node);
        
        // Validate node
        // Extract constraint details from intent (would need to pass intent or store in task)
        let validate_node = ExecutionNode::Validate {
            constraint_type: "value".to_string(), // Would come from intent
            constraint_details: serde_json::json!({}),
            description: "Validate constraint".to_string(),
        };
        
        let validate_idx = nodes.len();
        nodes.push(validate_node);
        
        edges.push(PlanEdge {
            from: load_idx,
            to: validate_idx,
        });
        
        root_nodes.push(validate_idx);
        
        Ok(ExecutionPlan {
            nodes,
            edges,
            root_nodes,
        })
    }

    fn join_tables_in_system(
        &self,
        nodes: &mut Vec<ExecutionNode>,
        edges: &mut Vec<PlanEdge>,
        node_indices: &[usize],
        tables: &[&TableCandidate],
    ) -> Result<usize> {
        // Simple strategy: join tables sequentially
        // In production, would use graph to find optimal join order
        
        if node_indices.len() < 2 {
            return Ok(node_indices[0]);
        }
        
        let mut current_idx = node_indices[0];
        
        for i in 1..node_indices.len() {
            let left_table = &tables[i - 1].table_name;
            let right_table = &tables[i].table_name;
            
            // Find common keys (simplified - would use graph in production)
            let common_keys = self.find_common_keys(left_table, right_table)?;
            
            if common_keys.is_empty() {
                warn!("No common keys found between {} and {}, skipping join", left_table, right_table);
                continue;
            }
            
            let join_node = ExecutionNode::Join {
                left_table: left_table.clone(),
                right_table: right_table.clone(),
                keys: common_keys,
                join_type: JoinType::Inner,
            };
            
            let join_idx = nodes.len();
            nodes.push(join_node);
            
            edges.push(PlanEdge {
                from: current_idx,
                to: join_idx,
            });
            
            edges.push(PlanEdge {
                from: node_indices[i],
                to: join_idx,
            });
            
            current_idx = join_idx;
        }
        
        Ok(current_idx)
    }

    fn find_common_keys(&self, left: &str, right: &str) -> Result<Vec<String>> {
        let left_table = self.metadata.get_table(left)
            .ok_or_else(|| RcaError::Metadata(format!("Table not found: {}", left)))?;
        let right_table = self.metadata.get_table(right)
            .ok_or_else(|| RcaError::Metadata(format!("Table not found: {}", right)))?;
        
        // Find common primary keys or columns
        let left_keys: HashSet<String> = left_table.primary_key.iter().cloned().collect();
        let right_keys: HashSet<String> = right_table.primary_key.iter().cloned().collect();
        
        let common: Vec<String> = left_keys.intersection(&right_keys).cloned().collect();
        
        if !common.is_empty() {
            return Ok(common);
        }
        
        // Try to find via lineage
        for edge in &self.metadata.lineage.edges {
            if (edge.from == left && edge.to == right) ||
               (edge.from == right && edge.to == left) {
                return Ok(edge.keys.keys().cloned().collect());
            }
        }
        
        Ok(vec![])
    }

    fn get_aggregations_for_system(
        &self,
        system: &str,
        metrics: &[String],
    ) -> Result<HashMap<String, String>> {
        let mut aggregations = HashMap::new();
        
        for metric in metrics {
            let rules = self.metadata.get_rules_for_system_metric(system, metric);
            for rule in rules {
                // Extract aggregation from formula (simplified)
                // In production, would parse formula properly
                if rule.computation.formula.contains("SUM") {
                    aggregations.insert(metric.clone(), "sum".to_string());
                } else if rule.computation.formula.contains("AVG") || rule.computation.formula.contains("AVERAGE") {
                    aggregations.insert(metric.clone(), "avg".to_string());
                } else if rule.computation.formula.contains("COUNT") {
                    aggregations.insert(metric.clone(), "count".to_string());
                } else {
                    aggregations.insert(metric.clone(), "sum".to_string()); // Default
                }
            }
        }
        
        Ok(aggregations)
    }
}

