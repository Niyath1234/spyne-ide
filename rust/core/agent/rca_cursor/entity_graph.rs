//! Entity Graph for Grain Resolution
//! 
//! Represents relationships between entities and enables BFS traversal
//! to find paths from base entities to target grains.

use crate::metadata::{Metadata, Entity};
use crate::error::{RcaError, Result};
use std::collections::HashMap;

/// Represents a single step in a join path between entities
#[derive(Debug, Clone)]
pub struct JoinStep {
    /// Source entity name
    pub from_entity: String,
    /// Target entity name
    pub to_entity: String,
    /// Join keys mapping: from_key -> to_key
    pub join_keys: HashMap<String, String>,
    /// Join type (inner, left, etc.)
    pub join_type: String,
    /// Cost estimate for this join step
    pub cost_estimate: f64,
}

/// Represents a relationship between two entities
#[derive(Debug, Clone)]
pub struct EntityRelationship {
    /// Source entity name
    pub from_entity: String,
    /// Target entity name
    pub to_entity: String,
    /// Join path steps
    pub join_path: Vec<JoinStep>,
    /// Total cost estimate for this relationship
    pub cost_estimate: f64,
}

/// Entity graph that represents relationships between entities
#[derive(Debug, Clone)]
pub struct EntityGraph {
    /// Map of entity name to Entity
    entities: HashMap<String, Entity>,
    /// Map of entity name to list of relationships from that entity
    relationships: HashMap<String, Vec<EntityRelationship>>,
    /// Reverse relationships (to_entity -> from_entity) for bidirectional traversal
    reverse_relationships: HashMap<String, Vec<EntityRelationship>>,
}

impl EntityGraph {
    /// Build entity graph from metadata
    pub fn from_metadata(metadata: &Metadata) -> Result<Self> {
        let mut entities = HashMap::new();
        let mut relationships: HashMap<String, Vec<EntityRelationship>> = HashMap::new();
        let mut reverse_relationships: HashMap<String, Vec<EntityRelationship>> = HashMap::new();

        // Index entities by name
        for entity in &metadata.entities {
            entities.insert(entity.name.clone(), entity.clone());
        }

        // Build relationships from lineage edges
        for edge in &metadata.lineage.edges {
            // Try to infer entity names from table names
            // In practice, we might need to map tables to entities
            let from_entity = Self::infer_entity_from_table(&edge.from, metadata)?;
            let to_entity = Self::infer_entity_from_table(&edge.to, metadata)?;

            let join_step = JoinStep {
                from_entity: from_entity.clone(),
                to_entity: to_entity.clone(),
                join_keys: edge.keys.clone(),
                join_type: edge.relationship.clone(),
                cost_estimate: Self::estimate_join_cost(metadata, &edge.from, &edge.to),
            };

            let relationship = EntityRelationship {
                from_entity: from_entity.clone(),
                to_entity: to_entity.clone(),
                join_path: vec![join_step],
                cost_estimate: Self::estimate_join_cost(metadata, &edge.from, &edge.to),
            };

            // Add forward relationship
            relationships
                .entry(from_entity.clone())
                .or_insert_with(Vec::new)
                .push(relationship.clone());

            // Add reverse relationship
            reverse_relationships
                .entry(to_entity)
                .or_insert_with(Vec::new)
                .push(relationship);
        }

        // Also build relationships from possible joins
        for possible_join in &metadata.lineage.possible_joins {
            if possible_join.tables.len() >= 2 {
                // Create relationships between consecutive tables
                for i in 0..possible_join.tables.len() - 1 {
                    let from_table = &possible_join.tables[i];
                    let to_table = &possible_join.tables[i + 1];

                    let from_entity = Self::infer_entity_from_table(from_table, metadata)?;
                    let to_entity = Self::infer_entity_from_table(to_table, metadata)?;

                    let join_step = JoinStep {
                        from_entity: from_entity.clone(),
                        to_entity: to_entity.clone(),
                        join_keys: possible_join.keys.clone(),
                        join_type: "inner".to_string(), // Default for possible joins
                        cost_estimate: Self::estimate_join_cost(metadata, from_table, to_table),
                    };

                    let relationship = EntityRelationship {
                        from_entity: from_entity.clone(),
                        to_entity: to_entity.clone(),
                        join_path: vec![join_step],
                        cost_estimate: Self::estimate_join_cost(metadata, from_table, to_table),
                    };

                    relationships
                        .entry(from_entity.clone())
                        .or_insert_with(Vec::new)
                        .push(relationship.clone());

                    reverse_relationships
                        .entry(to_entity)
                        .or_insert_with(Vec::new)
                        .push(relationship);
                }
            }
        }

        Ok(Self {
            entities,
            relationships,
            reverse_relationships,
        })
    }

    /// Infer entity name from table name
    /// 
    /// Looks up the table in metadata and returns its associated entity name
    fn infer_entity_from_table(table_name: &str, metadata: &Metadata) -> Result<String> {
        if let Some(table) = metadata.tables_by_name.get(table_name) {
            Ok(table.entity.clone())
        } else {
            // Fallback: try to use table name as entity name
            // This is a heuristic and may not always work
            Ok(table_name.to_string())
        }
    }

    /// Estimate join cost between two tables
    /// 
    /// Uses table sizes from metadata if available, otherwise uses heuristics
    fn estimate_join_cost(metadata: &Metadata, from_table: &str, to_table: &str) -> f64 {
        // Get table sizes (row counts) if available
        let from_size = Self::estimate_table_size(metadata, from_table);
        let to_size = Self::estimate_table_size(metadata, to_table);

        // Simple cost model: product of table sizes
        // In practice, this could be more sophisticated
        from_size * to_size * 0.0001 // Scale factor
    }

    /// Estimate table size (row count)
    /// 
    /// Uses metadata if available, otherwise returns a default estimate
    fn estimate_table_size(_metadata: &Metadata, _table_name: &str) -> f64 {
        // Check if we have table metadata with size information
        // For now, return a default estimate
        // In practice, this could read from hypergraph or table statistics
        10000.0 // Default estimate
    }

    /// Get entity by name
    pub fn get_entity(&self, entity_name: &str) -> Option<&Entity> {
        self.entities.get(entity_name)
    }

    /// Get all relationships from an entity
    pub fn get_relationships_from(&self, entity_name: &str) -> Vec<&EntityRelationship> {
        self.relationships
            .get(entity_name)
            .map(|rels| rels.iter().collect())
            .unwrap_or_default()
    }

    /// Get all relationships to an entity
    pub fn get_relationships_to(&self, entity_name: &str) -> Vec<&EntityRelationship> {
        self.reverse_relationships
            .get(entity_name)
            .map(|rels| rels.iter().collect())
            .unwrap_or_default()
    }

    /// Check if an entity exists in the graph
    pub fn has_entity(&self, entity_name: &str) -> bool {
        self.entities.contains_key(entity_name)
    }

    /// Get all entity names
    pub fn entity_names(&self) -> Vec<String> {
        self.entities.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_graph_creation() {
        // Test would require mock metadata
    }
}

