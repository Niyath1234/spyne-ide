use crate::error::{RcaError, Result};
use crate::metadata::{Lineage, Metadata, Rule};
use std::collections::{HashMap, HashSet};

pub struct Hypergraph {
    metadata: Metadata,
}

impl Hypergraph {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
    
    /// Get all tables referenced by a rule
    pub fn get_rule_tables(&self, rule_id: &str) -> Result<Vec<String>> {
        let rule = self.metadata
            .get_rule(rule_id)
            .ok_or_else(|| RcaError::Graph(format!("Rule not found: {}", rule_id)))?;
        
        let mut tables = HashSet::new();
        for op in &rule.pipeline {
            match op {
                crate::metadata::PipelineOp::Scan { table } => {
                    tables.insert(table.clone());
                }
                crate::metadata::PipelineOp::Join { table, .. } => {
                    tables.insert(table.clone());
                }
                _ => {}
            }
        }
        
        Ok(tables.into_iter().collect())
    }
    
    /// Get subgraph of tables needed for reconciliation
    pub fn get_reconciliation_subgraph(
        &self,
        system_a: &str,
        system_b: &str,
        metric: &str,
    ) -> Result<ReconciliationSubgraph> {
        // Find rules for both systems
        let rules_a = self.metadata.get_rules_for_system_metric(system_a, metric);
        let rules_b = self.metadata.get_rules_for_system_metric(system_b, metric);
        
        if rules_a.is_empty() {
            return Err(RcaError::Graph(format!("No rules found for {} {}", system_a, metric)));
        }
        if rules_b.is_empty() {
            return Err(RcaError::Graph(format!("No rules found for {} {}", system_b, metric)));
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
        
        Ok(rule.grain.clone())
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

