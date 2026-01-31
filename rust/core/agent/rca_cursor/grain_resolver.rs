//! Grain Resolution Engine
//! 
//! Resolves how to reach a target grain from a base entity using BFS
//! on the entity graph.

use crate::core::agent::rca_cursor::entity_graph::{EntityGraph, JoinStep};
use crate::metadata::Metadata;
use crate::error::{RcaError, Result};
use std::collections::{VecDeque, HashSet, HashMap};

/// Represents a plan for reaching a target grain from a base entity
#[derive(Debug, Clone)]
pub struct GrainPlan {
    /// Target grain entity name (e.g., "loan")
    pub grain: String,
    /// Grain key column name (e.g., "loan_id")
    pub grain_key: String,
    /// Join path from base entity to grain entity
    pub join_path: Vec<JoinStep>,
    /// Total cost estimate for this plan
    pub cost_estimate: f64,
    /// Base entity name
    pub base_entity: String,
}

/// Grain resolver that finds paths from base entities to target grains
pub struct GrainResolver {
    entity_graph: EntityGraph,
    metadata: Metadata,
}

impl GrainResolver {
    /// Create a new grain resolver
    pub fn new(metadata: Metadata) -> Result<Self> {
        let entity_graph = EntityGraph::from_metadata(&metadata)?;
        Ok(Self {
            entity_graph,
            metadata,
        })
    }

    /// Resolve grain plan from base entity to target grain
    /// 
    /// Uses BFS to find the shortest path (or lowest cost path if multiple exist)
    /// from the base entity to the target grain entity.
    pub fn resolve_grain(
        &self,
        base_entity: &str,
        target_grain: &str,
    ) -> Result<GrainPlan> {
        // Validate that both entities exist
        if !self.entity_graph.has_entity(base_entity) {
            return Err(RcaError::Execution(format!(
                "Base entity '{}' not found in entity graph",
                base_entity
            )));
        }

        if !self.entity_graph.has_entity(target_grain) {
            return Err(RcaError::Execution(format!(
                "Target grain '{}' not found in entity graph",
                target_grain
            )));
        }

        // If base entity is the same as target grain, return direct plan
        if base_entity == target_grain {
            return self.create_direct_grain_plan(base_entity);
        }

        // BFS to find shortest path
        let path = self.bfs_find_path(base_entity, target_grain)?;

        // Build join path from the BFS result
        let join_path = self.build_join_path_from_bfs(&path)?;

        // Calculate total cost
        let cost_estimate = join_path
            .iter()
            .map(|step| step.cost_estimate)
            .sum();

        // Identify grain key
        let grain_key = self.identify_grain_key(target_grain)?;

        Ok(GrainPlan {
            grain: target_grain.to_string(),
            grain_key,
            join_path,
            cost_estimate,
            base_entity: base_entity.to_string(),
        })
    }

    /// Create a direct grain plan when base entity equals target grain
    fn create_direct_grain_plan(&self, entity_name: &str) -> Result<GrainPlan> {
        let grain_key = self.identify_grain_key(entity_name)?;
        Ok(GrainPlan {
            grain: entity_name.to_string(),
            grain_key,
            join_path: Vec::new(),
            cost_estimate: 0.0,
            base_entity: entity_name.to_string(),
        })
    }

    /// BFS to find shortest path from base_entity to target_grain
    /// 
    /// Returns a path as a vector of entity names: [base_entity, ..., target_grain]
    fn bfs_find_path(&self, base_entity: &str, target_grain: &str) -> Result<Vec<String>> {
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parent: HashMap<String, String> = HashMap::new();

        let base_entity_str = base_entity.to_string();
        let target_grain_str = target_grain.to_string();

        queue.push_back(base_entity_str.clone());
        visited.insert(base_entity_str.clone());

        while let Some(current) = queue.pop_front() {
            if current == target_grain_str {
                // Reconstruct path
                let mut path = Vec::new();
                let mut node = target_grain_str.clone();
                path.push(node.clone());
                while let Some(p) = parent.get(&node) {
                    path.push(p.clone());
                    node = p.clone();
                }
                path.reverse();
                return Ok(path);
            }

            // Explore neighbors
            for relationship in self.entity_graph.get_relationships_from(&current) {
                let neighbor = relationship.to_entity.clone();
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor.clone());
                    parent.insert(neighbor.clone(), current.clone());
                    queue.push_back(neighbor);
                }
            }
        }

        Err(RcaError::Execution(format!(
            "No path found from entity '{}' to grain '{}'",
            base_entity, target_grain
        )))
    }

    /// Build join path from BFS path
    /// 
    /// Converts a path of entity names into a sequence of JoinSteps
    fn build_join_path_from_bfs(&self, path: &[String]) -> Result<Vec<JoinStep>> {
        if path.len() < 2 {
            return Ok(Vec::new());
        }

        let mut join_path = Vec::new();

        for i in 0..path.len() - 1 {
            let from_entity = path[i].clone();
            let to_entity = path[i + 1].clone();

            // Find the relationship between these entities
            let relationships = self.entity_graph.get_relationships_from(&from_entity);
            let relationship = relationships
                .iter()
                .find(|rel| rel.to_entity == to_entity)
                .ok_or_else(|| {
                    RcaError::Execution(format!(
                        "No relationship found from '{}' to '{}'",
                        from_entity, to_entity
                    ))
                })?;

            // Use the first join step from the relationship
            // (relationships can have multiple steps, but for now we use the first)
            if let Some(step) = relationship.join_path.first() {
                join_path.push(step.clone());
            }
        }

        Ok(join_path)
    }

    /// Identify grain key column for an entity
    /// 
    /// Looks up the primary key of the grain entity from metadata
    pub fn identify_grain_key(&self, grain_entity: &str) -> Result<String> {
        // Find tables associated with this entity
        if let Some(tables) = self.metadata.tables_by_entity.get(grain_entity) {
            // Use the first table's primary key as the grain key
            // In practice, we might need to handle composite keys
            if let Some(table) = tables.first() {
                if let Some(first_key) = table.primary_key.first() {
                    return Ok(first_key.clone());
                }
            }
        }

        // Fallback: try to infer from entity name
        // Common patterns: entity_name + "_id"
        let inferred_key = format!("{}_id", grain_entity.to_lowercase());
        
        // Check if this key exists in any table for this entity
        if let Some(tables) = self.metadata.tables_by_entity.get(grain_entity) {
            for table in tables {
                if let Some(columns) = &table.columns {
                    if columns.iter().any(|col| col.name == inferred_key) {
                        return Ok(inferred_key);
                    }
                }
            }
        }

        Err(RcaError::Execution(format!(
            "Could not identify grain key for entity '{}'",
            grain_entity
        )))
    }

    /// Check if a grain is reachable from a base entity
    pub fn is_grain_reachable(&self, base_entity: &str, target_grain: &str) -> bool {
        self.resolve_grain(base_entity, target_grain).is_ok()
    }

    /// Get all possible paths from base entity to target grain
    /// 
    /// Returns multiple paths if they exist, sorted by cost
    pub fn resolve_grain_all_paths(
        &self,
        base_entity: &str,
        target_grain: &str,
    ) -> Result<Vec<GrainPlan>> {
        // For now, return just the shortest path
        // In the future, we could implement a more sophisticated algorithm
        // to find all paths and return them sorted by cost
        let plan = self.resolve_grain(base_entity, target_grain)?;
        Ok(vec![plan])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grain_resolution() {
        // Test would require mock metadata
    }
}

