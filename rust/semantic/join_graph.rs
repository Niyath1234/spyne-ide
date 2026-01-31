//! Join Graph Resolution
//! 
//! Defines join edges and provides utilities for resolving join paths.
//! 
//! ## Key Design Principle
//! 
//! **Join mechanics are compiler-deterministic, not LLM-decided.**
//! 
//! Join edges encode authoritative metadata:
//! - Cardinality (1:1, 1:N, N:1, N:N)
//! - Optionality (can right side be missing?)
//! - Fan-out safety (can this join duplicate rows?)
//! 
//! The compiler uses this metadata + dimension usage to deterministically choose join types.

use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Join type - determined by compiler, not LLM
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    pub fn as_sql(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Full => "FULL OUTER JOIN",
        }
    }
}

/// Cardinality of a join relationship
/// 
/// This is authoritative metadata - not inferred, but declared in schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Cardinality {
    /// One-to-one: each row in left matches at most one row in right
    OneToOne,
    /// Many-to-one: multiple rows in left match one row in right (FK relationship)
    ManyToOne,
    /// One-to-many: one row in left matches multiple rows in right (fan-out risk)
    OneToMany,
    /// Many-to-many: multiple rows in left match multiple rows in right (highest fan-out risk)
    ManyToMany,
}

impl Cardinality {
    /// Check if this cardinality can cause fan-out (row duplication)
    pub fn is_to_many(&self) -> bool {
        matches!(self, Cardinality::OneToMany | Cardinality::ManyToMany)
    }
    
    /// Check if this cardinality is safe for aggregation (no fan-out)
    pub fn is_fan_out_safe(&self) -> bool {
        matches!(self, Cardinality::OneToOne | Cardinality::ManyToOne)
    }
}

/// Join edge representing a connection between two tables
/// 
/// This structure encodes authoritative join metadata:
/// - Cardinality: relationship cardinality (1:1, 1:N, etc.)
/// - Optional: can the right side be missing?
/// - Fan-out safe: can this join duplicate rows?
/// 
/// The `join_type` field is determined by the compiler based on:
/// 1. Dimension usage (Filter vs Select)
/// 2. Optionality metadata
/// 3. Fan-out safety
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinEdge {
    pub from_table: String,
    pub to_table: String,
    pub on: String, // SQL join condition, e.g., "users.id = orders.user_id"
    
    /// Join type - determined by compiler based on intent + metadata
    /// This should NOT be set by LLM, only by compiler logic
    pub join_type: JoinType,
    
    /// Cardinality of the relationship (authoritative metadata)
    /// Defaults to ManyToOne (most common FK relationship)
    #[serde(default = "default_cardinality")]
    pub cardinality: Cardinality,
    
    /// Can the right side (to_table) be missing?
    /// If true, LEFT JOIN is appropriate for augmentation.
    /// If false, INNER JOIN is appropriate.
    /// Defaults to true (most relationships are optional)
    #[serde(default = "default_optional")]
    pub optional: bool,
    
    /// Is this join safe from fan-out (row duplication)?
    /// If false, compiler must apply fan-out protection.
    /// Defaults based on cardinality
    #[serde(default)]
    pub fan_out_safe: Option<bool>,
}

fn default_cardinality() -> Cardinality {
    Cardinality::ManyToOne // Most common: FK relationship
}

fn default_optional() -> bool {
    true // Most relationships are optional
}

impl JoinEdge {
    /// Create a new join edge with default metadata
    pub fn new(from_table: String, to_table: String, on: String, join_type: JoinType) -> Self {
        Self {
            from_table,
            to_table,
            on,
            join_type,
            cardinality: Cardinality::ManyToOne,
            optional: true,
            fan_out_safe: None, // Will be computed from cardinality
        }
    }
    
    /// Create with explicit metadata
    pub fn with_metadata(
        from_table: String,
        to_table: String,
        on: String,
        cardinality: Cardinality,
        optional: bool,
    ) -> Self {
        let fan_out_safe = Some(cardinality.is_fan_out_safe());
        Self {
            from_table,
            to_table,
            on,
            join_type: JoinType::Left, // Will be determined by compiler
            cardinality,
            optional,
            fan_out_safe,
        }
    }
    
    /// Get fan-out safety (computed from cardinality if not explicitly set)
    pub fn is_fan_out_safe(&self) -> bool {
        self.fan_out_safe.unwrap_or_else(|| self.cardinality.is_fan_out_safe())
    }
    
    /// Check if this join can cause fan-out
    pub fn can_fan_out(&self) -> bool {
        !self.is_fan_out_safe()
    }
}

/// Resolve joins from a metric base table to dimensions
pub fn resolve_joins(
    base_table: &str,
    dimension_join_paths: &[&[JoinEdge]],
) -> Result<Vec<JoinEdge>> {
    let mut all_joins: Vec<JoinEdge> = Vec::new();
    let mut seen_joins: HashSet<(String, String)> = HashSet::new();

    // Collect all unique joins from dimension paths
    for path in dimension_join_paths {
        for edge in *path {
            let key = (edge.from_table.clone(), edge.to_table.clone());
            if !seen_joins.contains(&key) {
                seen_joins.insert(key.clone());
                all_joins.push(edge.clone());
            }
        }
    }

    // Validate connectivity from base_table
    validate_join_graph(base_table, &all_joins)?;

    // Topologically sort joins (simple approach: ensure base_table comes first)
    let sorted_joins = topological_sort(base_table, all_joins)?;

    Ok(sorted_joins)
}

/// Validate that all tables in the join graph are reachable from the base table
fn validate_join_graph(base_table: &str, joins: &[JoinEdge]) -> Result<()> {
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

    // Check reachability using DFS
    let mut visited: HashSet<String> = HashSet::new();
    dfs_reachable(base_table, &graph, &mut visited);

    // All tables should be reachable
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

    // Check for cycles (simple check: if we can reach a node from itself)
    for table in &all_tables {
        if has_cycle(table, &graph, &mut HashSet::new(), &mut HashSet::new()) {
            return Err(RcaError::Execution(format!(
                "Cycle detected in join graph involving table '{}'",
                table
            )));
        }
    }

    Ok(())
}

fn dfs_reachable(node: &str, graph: &HashMap<String, Vec<String>>, visited: &mut HashSet<String>) {
    if visited.contains(node) {
        return;
    }
    visited.insert(node.to_string());
    if let Some(neighbors) = graph.get(node) {
        for neighbor in neighbors {
            dfs_reachable(neighbor, graph, visited);
        }
    }
}

fn has_cycle(
    node: &str,
    graph: &HashMap<String, Vec<String>>,
    visiting: &mut HashSet<String>,
    visited: &mut HashSet<String>,
) -> bool {
    if visiting.contains(node) {
        return true; // Cycle detected
    }
    if visited.contains(node) {
        return false; // Already processed
    }

    visiting.insert(node.to_string());
    if let Some(neighbors) = graph.get(node) {
        for neighbor in neighbors {
            if has_cycle(neighbor, graph, visiting, visited) {
                return true;
            }
        }
    }
    visiting.remove(node);
    visited.insert(node.to_string());
    false
}

/// Topologically sort joins ensuring base_table dependencies come first
fn topological_sort(base_table: &str, mut joins: Vec<JoinEdge>) -> Result<Vec<JoinEdge>> {
    // Simple approach: put joins starting from base_table first
    let mut sorted: Vec<JoinEdge> = Vec::new();
    let mut remaining: Vec<JoinEdge> = joins;
    let mut added_tables: HashSet<String> = HashSet::new();
    added_tables.insert(base_table.to_string());

    // Keep adding joins where the from_table is already in added_tables
    let mut changed = true;
    while changed && !remaining.is_empty() {
        changed = false;
        let mut to_remove: Vec<usize> = Vec::new();

        for (idx, join) in remaining.iter().enumerate() {
            if added_tables.contains(&join.from_table) {
                sorted.push(join.clone());
                added_tables.insert(join.to_table.clone());
                to_remove.push(idx);
                changed = true;
            }
        }

        // Remove in reverse order to maintain indices
        for &idx in to_remove.iter().rev() {
            remaining.remove(idx);
        }
    }

    if !remaining.is_empty() {
        return Err(RcaError::Execution(format!(
            "Could not resolve join order. Remaining joins: {:?}",
            remaining
        )));
    }

    Ok(sorted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_simple_joins() {
        let base_table = "users";
        let join1 = JoinEdge::new(
            "users".to_string(),
            "orders".to_string(),
            "users.id = orders.user_id".to_string(),
            JoinType::Inner,
        );
        let join2 = JoinEdge::new(
            "orders".to_string(),
            "products".to_string(),
            "orders.product_id = products.id".to_string(),
            JoinType::Left,
        );

        let paths = vec![&[join1.clone(), join2.clone()][..]];
        let result = resolve_joins(base_table, &paths);
        assert!(result.is_ok());
        let joins = result.unwrap();
        assert_eq!(joins.len(), 2);
    }

    #[test]
    fn test_validate_unreachable_table() {
        let base_table = "users";
        let join1 = JoinEdge::new(
            "users".to_string(),
            "orders".to_string(),
            "users.id = orders.user_id".to_string(),
            JoinType::Inner,
        );
        let join2 = JoinEdge::new(
            "orphan".to_string(),
            "other".to_string(),
            "orphan.id = other.id".to_string(),
            JoinType::Inner,
        );

        let joins = vec![join1, join2];
        let result = validate_join_graph(base_table, &joins);
        assert!(result.is_err());
    }
}

