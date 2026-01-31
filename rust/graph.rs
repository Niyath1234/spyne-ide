use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use crate::graph_adapter::GraphAdapter;
use std::collections::{HashMap, HashSet};

pub struct Hypergraph {
    metadata: Metadata,
    adapter: Option<GraphAdapter>,
}

impl Hypergraph {
    pub fn new(metadata: Metadata) -> Self {
        // Try to create adapter, but don't fail if it doesn't work
        let adapter = GraphAdapter::new(metadata.clone()).ok();
        Self { 
            metadata,
            adapter,
        }
    }
    
    /// Get the graph adapter (creates if not exists)
    pub fn adapter(&mut self) -> Result<&GraphAdapter> {
        if self.adapter.is_none() {
            self.adapter = Some(GraphAdapter::new(self.metadata.clone())?);
        }
        Ok(self.adapter.as_ref().unwrap())
    }
    
    /// Get all tables referenced by a rule (derived from rule's computation definition)
    pub fn get_rule_tables(&self, rule_id: &str) -> Result<Vec<String>> {
        let rule = self.metadata
            .get_rule(rule_id)
            .ok_or_else(|| RcaError::Graph(format!("Rule not found: {}", rule_id)))?;
        
        // Derive tables from rule's source entities
        let mut tables = HashSet::new();
        for entity in &rule.computation.source_entities {
            // Find all tables for this entity in the rule's system
            let entity_tables: Vec<&crate::metadata::Table> = self.metadata.tables
                .iter()
                .filter(|t| t.entity == *entity && t.system == rule.system)
                .collect();
            
            for table in entity_tables {
                tables.insert(table.name.clone());
            }
        }
        
        Ok(tables.into_iter().collect())
    }
    
    /// Get subgraph of tables needed for reconciliation
    /// Supports both same-metric and cross-metric reconciliation
    pub fn get_reconciliation_subgraph(
        &self,
        system_a: &str,
        system_b: &str,
        metric: &str,
    ) -> Result<ReconciliationSubgraph> {
        // Try to find rules - support cross-metric reconciliation
        // If metric not found in system_a, try to find any metric in that system
        let rules_a = self.metadata.get_rules_for_system_metric(system_a, metric);
        let rules_b = self.metadata.get_rules_for_system_metric(system_b, metric);
        
        // If no rules found, try auto-inference (for simple column access)
        let rules_a = if rules_a.is_empty() {
            // Try auto-inference - check if metric exists as a column in system_a tables
            let system_tables: Vec<&crate::metadata::Table> = self.metadata.tables
                .iter()
                .filter(|t| t.system.to_lowercase() == system_a.to_lowercase())
                .collect();
            
            for table in system_tables {
                if let Some(ref cols) = table.columns {
                    if cols.iter().any(|c| c.name.to_lowercase() == metric.to_lowercase()) {
                        // Auto-infer rule will be created by get_rules_for_system_metric
                        // Try again after potential auto-inference
                        return self.get_reconciliation_subgraph(system_a, system_b, metric);
                    }
                }
            }
            rules_a
        } else {
            rules_a
        };
        
        let rules_b = if rules_b.is_empty() {
            // Try auto-inference for system_b
            let system_tables: Vec<&crate::metadata::Table> = self.metadata.tables
                .iter()
                .filter(|t| t.system.to_lowercase() == system_b.to_lowercase())
                .collect();
            
            for table in system_tables {
                if let Some(ref cols) = table.columns {
                    if cols.iter().any(|c| c.name.to_lowercase() == metric.to_lowercase()) {
                        // Auto-infer rule will be created by get_rules_for_system_metric
                        // Try again after potential auto-inference
                        return self.get_reconciliation_subgraph(system_a, system_b, metric);
                    }
                }
            }
            rules_b
        } else {
            rules_b
        };
        
        if rules_a.is_empty() {
            return Err(RcaError::Graph(format!("No rules found for {} {}. Try: 1) Define explicit rule, 2) Ensure metric column exists in tables, 3) Check auto-inference is working", system_a, metric)));
        }
        if rules_b.is_empty() {
            return Err(RcaError::Graph(format!("No rules found for {} {}. Try: 1) Define explicit rule, 2) Ensure metric column exists in tables, 3) Check auto-inference is working", system_b, metric)));
        }
        
        // Get all tables for each side
        let mut tables_a = HashSet::new();
        let mut tables_b = HashSet::new();
        
        for rule in &rules_a {
            let tables = self.get_rule_tables(&rule.id)?;
            tables_a.extend(tables);
        }
        
        for rule in &rules_b {
            let tables = self.get_rule_tables(&rule.id)?;
            tables_b.extend(tables);
        }
        
        Ok(ReconciliationSubgraph {
            system_a: system_a.to_string(),
            system_b: system_b.to_string(),
            metric: metric.to_string(),
            rules_a: rules_a.iter().map(|r| r.id.clone()).collect(),
            rules_b: rules_b.iter().map(|r| r.id.clone()).collect(),
            tables_a: tables_a.into_iter().collect(),
            tables_b: tables_b.into_iter().collect(),
        })
    }
    
    /// Get subgraph for cross-metric reconciliation (different metrics in different systems)
    pub fn get_reconciliation_subgraph_cross_metric(
        &self,
        system_a: &str,
        system_b: &str,
        metric_a: &str,
        metric_b: &str,
    ) -> Result<ReconciliationSubgraph> {
        // Find rules for metric_a in system_a and metric_b in system_b
        let rules_a = self.metadata.get_rules_for_system_metric(system_a, metric_a);
        let rules_b = self.metadata.get_rules_for_system_metric(system_b, metric_b);
        
        // Try auto-inference if rules not found
        let rules_a = if rules_a.is_empty() {
            let system_tables: Vec<&crate::metadata::Table> = self.metadata.tables
                .iter()
                .filter(|t| t.system.to_lowercase() == system_a.to_lowercase())
                .collect();
            
            for table in system_tables {
                if let Some(ref cols) = table.columns {
                    if cols.iter().any(|c| c.name.to_lowercase() == metric_a.to_lowercase()) {
                        // Auto-infer rule will be created by get_rules_for_system_metric
                        return self.get_reconciliation_subgraph_cross_metric(system_a, system_b, metric_a, metric_b);
                    }
                }
            }
            rules_a
        } else {
            rules_a
        };
        
        let rules_b = if rules_b.is_empty() {
            let system_tables: Vec<&crate::metadata::Table> = self.metadata.tables
                .iter()
                .filter(|t| t.system.to_lowercase() == system_b.to_lowercase())
                .collect();
            
            for table in system_tables {
                if let Some(ref cols) = table.columns {
                    if cols.iter().any(|c| c.name.to_lowercase() == metric_b.to_lowercase()) {
                        // Auto-infer rule will be created by get_rules_for_system_metric
                        return self.get_reconciliation_subgraph_cross_metric(system_a, system_b, metric_a, metric_b);
                    }
                }
            }
            rules_b
        } else {
            rules_b
        };
        
        if rules_a.is_empty() {
            return Err(RcaError::Graph(format!("No rules found for {} {}. Try: 1) Define explicit rule, 2) Ensure metric column exists in tables, 3) Check auto-inference is working", system_a, metric_a)));
        }
        if rules_b.is_empty() {
            return Err(RcaError::Graph(format!("No rules found for {} {}. Try: 1) Define explicit rule, 2) Ensure metric column exists in tables, 3) Check auto-inference is working", system_b, metric_b)));
        }
        
        // Get all tables for each side
        let mut tables_a = HashSet::new();
        let mut tables_b = HashSet::new();
        
        for rule in &rules_a {
            let tables = self.get_rule_tables(&rule.id)?;
            tables_a.extend(tables);
        }
        
        for rule in &rules_b {
            let tables = self.get_rule_tables(&rule.id)?;
            tables_b.extend(tables);
        }
        
        // For cross-metric, use a combined metric name for the subgraph
        let combined_metric = format!("{}_vs_{}", metric_a, metric_b);
        
        Ok(ReconciliationSubgraph {
            system_a: system_a.to_string(),
            system_b: system_b.to_string(),
            metric: combined_metric,
            rules_a: rules_a.iter().map(|r| r.id.clone()).collect(),
            rules_b: rules_b.iter().map(|r| r.id.clone()).collect(),
            tables_a: tables_a.into_iter().collect(),
            tables_b: tables_b.into_iter().collect(),
        })
    }
    
    /// Find join paths between two tables
    pub fn find_join_path(&self, from: &str, to: &str) -> Result<Option<Vec<JoinStep>>> {
        // Simple BFS to find shortest path
        let mut queue = vec![(from.to_string(), vec![])];
        let mut visited = HashSet::new();
        visited.insert(from.to_string());
        
        while let Some((current, path)) = queue.pop() {
            if current == to {
                return Ok(Some(path));
            }
            
            // Find all edges from current table
            for edge in &self.metadata.lineage.edges {
                if edge.from == current && !visited.contains(&edge.to) {
                    let mut new_path = path.clone();
                    new_path.push(JoinStep {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                        keys: edge.keys.clone(),
                    });
                    visited.insert(edge.to.clone());
                    queue.push((edge.to.clone(), new_path));
                }
            }
        }
        
        Ok(None)
    }
    
    /// Get final grain of a rule
    pub fn get_rule_grain(&self, rule_id: &str) -> Result<Vec<String>> {
        let rule = self.metadata
            .get_rule(rule_id)
            .ok_or_else(|| RcaError::Graph(format!("Rule not found: {}", rule_id)))?;
        
        Ok(rule.target_grain.clone())
    }
    
    /// Find columns containing a specific value using metadata distinct_values and Hypergraph
    /// Returns a list of (table_name, column_name) tuples where the value might be found
    /// Uses Hypergraph's advanced node statistics and fragments for better matching
    pub fn find_columns_with_value(&self, search_value: &str, system: Option<&str>) -> Vec<(String, String)> {
        // First try using Hypergraph adapter if available
        if let Some(ref adapter) = self.adapter {
            let hypergraph_results = adapter.find_columns_with_value(search_value, system);
            if !hypergraph_results.is_empty() {
                return hypergraph_results;
            }
        }
        
        // Fallback to metadata-based search
        let search_lower = search_value.to_lowercase();
        let mut results = Vec::new();
        
        for table in &self.metadata.tables {
            // Filter by system if provided
            if let Some(sys) = system {
                if table.system != sys {
                    continue;
                }
            }
            
            // Check columns metadata
            if let Some(ref columns) = table.columns {
                for col_meta in columns {
                    // Check if column has distinct values
                    if let Some(ref distinct_vals) = col_meta.distinct_values {
                        // Check if any distinct value matches the search value
                        for val in distinct_vals {
                            let val_str = match val {
                                serde_json::Value::String(s) => s.to_lowercase(),
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                _ => continue,
                            };
                            
                            // Check for exact match or substring match
                            if val_str == search_lower || val_str.contains(&search_lower) || search_lower.contains(&val_str) {
                                results.push((table.name.clone(), col_meta.name.clone()));
                                break; // Found a match, move to next column
                            }
                        }
                    }
                    
                    // Also check column name patterns (fallback)
                    let col_lower = col_meta.name.to_lowercase();
                    if (search_lower == "msme" && (col_lower.contains("psl") || col_lower.contains("msme") || col_lower.contains("category"))) ||
                       (search_lower == "edl" && (col_lower.contains("edl") || col_lower.contains("product"))) {
                        // Only add if not already added
                        if !results.iter().any(|(t, c)| t == &table.name && c == &col_meta.name) {
                            results.push((table.name.clone(), col_meta.name.clone()));
                        }
                    }
                }
            }
        }
        
        results
    }
    
    /// Find join path using Hypergraph's optimized path finder
    pub fn find_join_path_optimized(&mut self, from: &str, to: &str) -> Result<Option<Vec<String>>> {
        if let Ok(adapter) = self.adapter() {
            adapter.find_join_path(from, to)
        } else {
            // Fallback to simple BFS - convert JoinStep to Vec<String>
            self.find_join_path(from, to).map(|opt| {
                opt.map(|steps| {
                    let mut tables = vec![steps[0].from.clone()];
                    for step in steps {
                        tables.push(step.to.clone());
                    }
                    tables
                })
            })
        }
    }
    
    /// Get related tables using Hypergraph's adjacency
    pub fn get_related_tables(&mut self, table_name: &str) -> Result<Vec<String>> {
        if let Ok(adapter) = self.adapter() {
            Ok(adapter.get_related_tables(table_name))
        } else {
            // Fallback: use lineage edges
            let mut related = Vec::new();
            for edge in &self.metadata.lineage.edges {
                if edge.from == table_name {
                    related.push(edge.to.clone());
                } else if edge.to == table_name {
                    related.push(edge.from.clone());
                }
            }
            Ok(related)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReconciliationSubgraph {
    pub system_a: String,
    pub system_b: String,
    pub metric: String,
    pub rules_a: Vec<String>,
    pub rules_b: Vec<String>,
    pub tables_a: Vec<String>,
    pub tables_b: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct JoinStep {
    pub from: String,
    pub to: String,
    pub keys: HashMap<String, String>,
}

