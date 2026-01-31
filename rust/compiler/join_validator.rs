//! Join Validator
//! 
//! Validates join paths and detects issues.

use crate::error::{RcaError, Result};
use crate::semantic::join_graph::JoinEdge;
use std::collections::{HashMap, HashSet};

/// Join validator
pub struct JoinValidator;

impl JoinValidator {
    pub fn new() -> Self {
        Self
    }

    /// Validate join graph connectivity
    pub fn validate_joins(
        &self,
        base_table: &str,
        joins: &[JoinEdge],
    ) -> Result<()> {
        // Build adjacency list
        let mut graph: HashMap<String, Vec<String>> = HashMap::new();
        let mut all_tables: HashSet<String> = HashSet::new();
        all_tables.insert(base_table.to_string());

        for join in joins {
            all_tables.insert(join.from_table.clone());
            all_tables.insert(join.to_table.clone());

            graph
                .entry(join.from_table.clone())
                .or_insert_with(Vec::new)
                .push(join.to_table.clone());
        }

        // Check reachability
        let mut visited: HashSet<String> = HashSet::new();
        self.dfs_reachable(base_table, &graph, &mut visited);

        let unreachable: Vec<String> = all_tables
            .iter()
            .filter(|t| !visited.contains(*t))
            .cloned()
            .collect();

        if !unreachable.is_empty() {
            return Err(RcaError::Execution(format!(
                "Tables unreachable from base table '{}': {:?}",
                base_table, unreachable
            )));
        }

        // Check for cycles
        for table in &all_tables {
            if self.has_cycle(table, &graph, &mut HashSet::new(), &mut HashSet::new()) {
                return Err(RcaError::Execution(format!(
                    "Cycle detected in join graph involving table '{}'",
                    table
                )));
            }
        }

        Ok(())
    }

    fn dfs_reachable(
        &self,
        node: &str,
        graph: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
    ) {
        if visited.contains(node) {
            return;
        }
        visited.insert(node.to_string());
        if let Some(neighbors) = graph.get(node) {
            for neighbor in neighbors {
                self.dfs_reachable(neighbor, graph, visited);
            }
        }
    }

    fn has_cycle(
        &self,
        node: &str,
        graph: &HashMap<String, Vec<String>>,
        visiting: &mut HashSet<String>,
        visited: &mut HashSet<String>,
    ) -> bool {
        if visiting.contains(node) {
            return true;
        }
        if visited.contains(node) {
            return false;
        }

        visiting.insert(node.to_string());
        if let Some(neighbors) = graph.get(node) {
            for neighbor in neighbors {
                if self.has_cycle(neighbor, graph, visiting, visited) {
                    return true;
                }
            }
        }
        visiting.remove(node);
        visited.insert(node.to_string());
        false
    }
}

impl Default for JoinValidator {
    fn default() -> Self {
        Self::new()
    }
}





