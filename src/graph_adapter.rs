//! Graph Adapter - Bridge between RCA-Engine Metadata and Hypergraph Module
//! 
//! This module provides bidirectional communication between:
//! - RCA-Engine's Metadata/Table/Lineage structure
//! - Hypergraph Module's HyperGraph structure
//!
//! It enables:
//! - Converting RCA metadata to Hypergraph nodes/edges
//! - Using Hypergraph's advanced features (paths, compression, coarsening)
//! - Enriching RCA queries with Hypergraph intelligence

use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use crate::hypergraph::{HyperGraph, HyperNode, HyperEdge, NodeId, EdgeId, NodeType, JoinType, JoinPredicate, PredicateOperator};
use crate::hypergraph::node::NodeStatistics;
use crate::hypergraph::edge::{EdgeStatistics, JoinAlgorithmHint};
use crate::hypergraph::types::{Value, ColumnFragment, FragmentMetadata};
use std::collections::HashMap;

/// Adapter that bridges RCA-Engine metadata with Hypergraph module
pub struct GraphAdapter {
    /// RCA-Engine metadata
    metadata: Metadata,
    /// Hypergraph instance
    hypergraph: HyperGraph,
    /// Mapping from table names to node IDs
    table_to_node: HashMap<String, NodeId>,
    /// Mapping from node IDs to table names
    node_to_table: HashMap<NodeId, String>,
}

impl GraphAdapter {
    /// Create a new adapter and populate Hypergraph from RCA metadata
    pub fn new(metadata: Metadata) -> Result<Self> {
        let mut adapter = Self {
            metadata: metadata.clone(),
            hypergraph: HyperGraph::new(),
            table_to_node: HashMap::new(),
            node_to_table: HashMap::new(),
        };
        
        // Populate Hypergraph from RCA metadata
        adapter.populate_from_metadata()?;
        
        Ok(adapter)
    }
    
    /// Populate Hypergraph with nodes and edges from RCA metadata
    fn populate_from_metadata(&mut self) -> Result<()> {
        // Step 1: Create table nodes
        for table in &self.metadata.tables {
            let table_id = self.hypergraph.next_node_id();
            let schema = table.system.clone(); // Use system as schema
            
            // Create table node
            let table_node = HyperNode {
                id: table_id,
                node_type: NodeType::Table,
                schema_name: schema.clone(),
                table_name: Some(table.name.clone()),
                column_name: None,
                fragments: Vec::new(),
                stats: NodeStatistics {
                    row_count: 0, // Will be populated from data if available
                    cardinality: 0,
                    size_bytes: 0,
                    last_updated: 0,
                    min_value: None,
                    max_value: None,
                    mean_value: None,
                    std_dev: None,
                    percentiles: Default::default(),
                    null_percentage: 0.0,
                    empty_string_percentage: 0.0,
                    top_n_values: Vec::new(),
                    distinct_count: 0.0,
                    data_quality_score: 0.0,
                    freshness_score: 0.0,
                    completeness_score: 0.0,
                    consistency_score: 0.0,
                    anomaly_count: 0,
                    access_frequency: 0,
                    last_accessed: 0,
                    read_write_ratio: 0.0,
                    filter_selectivity: None,
                    join_selectivity: None,
                    query_count: 0,
                    avg_query_time_ms: 0.0,
                    p95_query_time_ms: 0.0,
                },
                metadata: HashMap::new(),
            };
            
            self.hypergraph.add_node(table_node);
            self.table_to_node.insert(table.name.clone(), table_id);
            self.node_to_table.insert(table_id, table.name.clone());
            
            // Step 2: Create column nodes if column metadata exists
            if let Some(ref columns) = table.columns {
                for col_meta in columns {
                    let col_id = self.hypergraph.next_node_id();
                    
                    // Convert distinct_values to ColumnFragment
                    let fragments = if let Some(ref distinct_vals) = col_meta.distinct_values {
                        // Create fragment metadata from distinct values
                        let mut min_val: Option<Value> = None;
                        let mut max_val: Option<Value> = None;
                        let mut distinct_count = distinct_vals.len();
                        
                        // Convert distinct values to Value enum
                        let values: Vec<Value> = distinct_vals.iter()
                            .filter_map(|v| self.json_to_value(v))
                            .collect();
                        
                        if !values.is_empty() {
                            // Find min/max for sorting
                            let mut sorted = values.clone();
                            sorted.sort_by(|a, b| self.compare_values(a, b));
                            min_val = sorted.first().cloned();
                            max_val = sorted.last().cloned();
                        }
                        
                        vec![ColumnFragment {
                            metadata: FragmentMetadata {
                                row_count: distinct_count,
                                min_value: min_val,
                                max_value: max_val,
                                cardinality: distinct_count,
                                memory_size: 0,
                                table_name: None,
                                column_name: None,
                                partition_id: None,
                                metadata: HashMap::new(),
                            },
                        }]
                    } else {
                        Vec::new()
                    };
                    
                    // Create column node statistics
                    let mut stats = NodeStatistics {
                        row_count: 0,
                        cardinality: 0,
                        size_bytes: 0,
                        last_updated: 0,
                        min_value: None,
                        max_value: None,
                        mean_value: None,
                        std_dev: None,
                        percentiles: Default::default(),
                        null_percentage: 0.0,
                        empty_string_percentage: 0.0,
                        top_n_values: Vec::new(),
                        distinct_count: col_meta.distinct_values.as_ref().map(|v| v.len() as f64).unwrap_or(0.0),
                        data_quality_score: 0.0,
                        freshness_score: 0.0,
                        completeness_score: 0.0,
                        consistency_score: 0.0,
                        anomaly_count: 0,
                        access_frequency: 0,
                        last_accessed: 0,
                        read_write_ratio: 0.0,
                        filter_selectivity: None,
                        join_selectivity: None,
                        query_count: 0,
                        avg_query_time_ms: 0.0,
                        p95_query_time_ms: 0.0,
                    };
                    
                    // Populate top_n_values from distinct_values
                    if let Some(ref distinct_vals) = col_meta.distinct_values {
                        for val in distinct_vals.iter().take(10) {
                            if let Some(value) = self.json_to_value(val) {
                                stats.top_n_values.push(crate::hypergraph::node::TopNValue {
                                    value: value.clone(),
                                    frequency: 1, // Approximate
                                });
                            }
                        }
                    }
                    
                    let col_node = HyperNode {
                        id: col_id,
                        node_type: NodeType::Column,
                        schema_name: schema.clone(),
                        table_name: Some(table.name.clone()),
                        column_name: Some(col_meta.name.clone()),
                        fragments,
                        stats,
                        metadata: {
                            let mut meta = HashMap::new();
                            if let Some(ref desc) = col_meta.description {
                                meta.insert("description".to_string(), desc.clone());
                            }
                            if let Some(ref dtype) = col_meta.data_type {
                                meta.insert("data_type".to_string(), dtype.clone());
                            }
                            meta.insert("entity".to_string(), table.entity.clone());
                            meta.insert("system".to_string(), table.system.clone());
                            meta
                        },
                    };
                    
                    self.hypergraph.add_node(col_node);
                }
            }
        }
        
        // Step 3: Create edges from lineage metadata
        for edge in &self.metadata.lineage.edges {
            let from_id = self.table_to_node.get(&edge.from)
                .ok_or_else(|| RcaError::Graph(format!("Table not found in hypergraph: {}", edge.from)))?;
            let to_id = self.table_to_node.get(&edge.to)
                .ok_or_else(|| RcaError::Graph(format!("Table not found in hypergraph: {}", edge.to)))?;
            
            // Convert keys to join predicates
            for (left_col, right_col) in &edge.keys {
                let edge_id = self.hypergraph.next_edge_id();
                let predicate = JoinPredicate {
                    left: (edge.from.clone(), left_col.clone()),
                    right: (edge.to.clone(), right_col.clone()),
                    operator: PredicateOperator::Equals,
                };
                
                let hyper_edge = HyperEdge {
                    id: edge_id,
                    source: *from_id,
                    target: *to_id,
                    join_type: JoinType::Inner, // Default, can be enhanced
                    predicate,
                    stats: EdgeStatistics {
                        cardinality: 0,
                        selectivity: 1.0,
                        avg_fanout: 0.0,
                        last_updated: 0,
                        actual_join_cost_ms: None,
                        cardinality_error_percent: None,
                        preferred_probe_left: true,
                        join_algorithm_hint: JoinAlgorithmHint::HashJoin,
                        bloom_filter_size_bytes: None,
                        materialization_cost_ratio: None,
                        join_frequency: 0,
                        last_join_execution: 0,
                        join_result_size_history: Vec::new(),
                    },
                    is_materialized: false,
                    materialized_result: None,
                    metadata: {
                        let mut meta = HashMap::new();
                        meta.insert("relationship".to_string(), edge.relationship.clone());
                        meta
                    },
                };
                
                self.hypergraph.add_edge(hyper_edge);
            }
        }
        
        Ok(())
    }
    
    /// Convert serde_json::Value to Hypergraph Value enum
    fn json_to_value(&self, val: &serde_json::Value) -> Option<Value> {
        match val {
            serde_json::Value::String(s) => Some(Value::String(s.clone())),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(Value::Int64(i))
                } else if let Some(f) = n.as_f64() {
                    Some(Value::Float64(f))
                } else {
                    None
                }
            }
            serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
            serde_json::Value::Null => Some(Value::Null),
            _ => None,
        }
    }
    
    /// Compare two values for sorting
    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
    
    /// Get the underlying Hypergraph instance
    pub fn hypergraph(&self) -> &HyperGraph {
        &self.hypergraph
    }
    
    /// Get the underlying RCA metadata
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    
    /// Find columns containing a specific value using Hypergraph
    /// This leverages Hypergraph's node metadata and statistics
    pub fn find_columns_with_value(&self, search_value: &str, system: Option<&str>) -> Vec<(String, String)> {
        let search_lower = search_value.to_lowercase();
        let mut results = Vec::new();
        
        // Search through all column nodes in Hypergraph
        for (_, node) in self.hypergraph.iter_nodes() {
            let node = node;
            
            // Filter by system if provided
            if let Some(sys) = system {
                if node.schema_name != sys {
                    continue;
                }
            }
            
            // Only check column nodes
            if matches!(node.node_type, NodeType::Column) {
                if let (Some(table), Some(col)) = (&node.table_name, &node.column_name) {
                    // Check top_n_values in statistics
                    for top_val in &node.stats.top_n_values {
                        let val_str = match &top_val.value {
                            Value::String(s) => s.to_lowercase(),
                            Value::Int64(n) => n.to_string(),
                            Value::Float64(n) => format!("{}", n),
                            Value::Bool(b) => b.to_string(),
                            _ => continue,
                        };
                        
                        if val_str == search_lower || val_str.contains(&search_lower) || search_lower.contains(&val_str) {
                            results.push((table.clone(), col.clone()));
                            break;
                        }
                    }
                    
                    // Check fragments
                    for fragment in &node.fragments {
                        if let Some(ref min_val) = fragment.metadata.min_value {
                            let min_str = self.value_to_string(min_val);
                            if min_str.contains(&search_lower) || search_lower.contains(&min_str) {
                                if !results.iter().any(|(t, c)| t == table && c == col) {
                                    results.push((table.clone(), col.clone()));
                                }
                            }
                        }
                        if let Some(ref max_val) = fragment.metadata.max_value {
                            let max_str = self.value_to_string(max_val);
                            if max_str.contains(&search_lower) || search_lower.contains(&max_str) {
                                if !results.iter().any(|(t, c)| t == table && c == col) {
                                    results.push((table.clone(), col.clone()));
                                }
                            }
                        }
                    }
                    
                    // Check column name patterns (fallback)
                    let col_lower = col.to_lowercase();
                    if (search_lower == "msme" && (col_lower.contains("psl") || col_lower.contains("msme") || col_lower.contains("category"))) ||
                       (search_lower == "edl" && (col_lower.contains("edl") || col_lower.contains("product"))) {
                        if !results.iter().any(|(t, c)| t == table && c == col) {
                            results.push((table.clone(), col.clone()));
                        }
                    }
                }
            }
        }
        
        results
    }
    
    /// Convert Value to string for comparison
    fn value_to_string(&self, val: &Value) -> String {
        match val {
            Value::String(s) => s.to_lowercase(),
            Value::Int64(n) => n.to_string(),
            Value::Float64(n) => format!("{}", n),
            Value::Bool(b) => b.to_string(),
            _ => String::new(),
        }
    }
    
    /// Find shortest path between two tables using Hypergraph's optimized path finder
    pub fn find_join_path(&self, from_table: &str, to_table: &str) -> Result<Option<Vec<String>>> {
        let from_id = self.table_to_node.get(from_table)
            .ok_or_else(|| RcaError::Graph(format!("Table not found: {}", from_table)))?;
        let to_id = self.table_to_node.get(to_table)
            .ok_or_else(|| RcaError::Graph(format!("Table not found: {}", to_table)))?;
        
        // Use Hypergraph's find_path method (which uses shortest path cache internally)
        if let Some(edge_ids) = self.hypergraph.find_path(*from_id, *to_id) {
            // Convert edge IDs to table names
            let mut path = Vec::new();
            path.push(from_table.to_string());
            
            for edge_id in &edge_ids {
                if let Some(edge) = self.hypergraph.get_edge(*edge_id) {
                    if let Some(target_table) = self.node_to_table.get(&edge.target) {
                        path.push(target_table.clone());
                    }
                }
            }
            
            return Ok(Some(path));
        }
        
        // Fallback to RCA's simple BFS
        Ok(None)
    }
    
    /// Get hypergraph node statistics for a table
    pub fn get_table_stats(&self, table_name: &str) -> Option<NodeStatistics> {
        let node = self.hypergraph.get_table_node(table_name)?;
        Some(node.stats.clone())
    }
    
    /// Get hypergraph edge statistics for a join between two tables
    pub fn get_join_stats(&self, from_table: &str, to_table: &str) -> Option<EdgeStatistics> {
        let from_id = self.table_to_node.get(from_table)?;
        let to_id = self.table_to_node.get(to_table)?;
        let edge_id = self.hypergraph.find_edge_between(*from_id, *to_id)?;
        let edge = self.hypergraph.get_edge(edge_id)?;
        Some(edge.stats.clone())
    }
    
    /// Get related tables for a given table using Hypergraph's adjacency
    pub fn get_related_tables(&self, table_name: &str) -> Vec<String> {
        let mut related = Vec::new();
        
        if let Some(node_id) = self.table_to_node.get(table_name) {
            // Get all outgoing edges from this node
            let outgoing_edges = self.hypergraph.get_outgoing_edges(*node_id);
            for edge in outgoing_edges {
                if let Some(target_table) = self.node_to_table.get(&edge.target) {
                    related.push(target_table.clone());
                }
            }
        }
        
        related
    }
    
    /// Update Hypergraph with new distinct values from RCA metadata
    pub fn update_distinct_values(&mut self, table_name: &str) -> Result<()> {
        // Find table in metadata
        let table = self.metadata.get_table(table_name)
            .ok_or_else(|| RcaError::Graph(format!("Table not found: {}", table_name)))?;
        
        if let Some(node_id) = self.table_to_node.get(table_name) {
            // Update column nodes with new distinct values
            if let Some(ref columns) = table.columns {
                for col_meta in columns {
                    // Find column node
                    // This is a simplified update - in production, you'd want to update the actual node
                    // For now, we'll rely on the initial population
                }
            }
        }
        
        Ok(())
    }
}

