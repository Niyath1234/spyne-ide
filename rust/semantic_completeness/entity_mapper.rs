//! Entity-to-Table Mapping
//! 
//! Maps extracted entities to canonical table names using metadata.

use crate::error::Result;
use crate::metadata::Metadata;
use crate::semantic::registry::SemanticRegistry;
use std::collections::HashSet;
use std::sync::Arc;

pub struct EntityMapper {
    semantic_registry: Option<Arc<dyn SemanticRegistry>>,
    metadata: Metadata,
}

impl EntityMapper {
    pub fn new(
        semantic_registry: Option<Arc<dyn SemanticRegistry>>,
        metadata: Metadata,
    ) -> Self {
        Self {
            semantic_registry,
            metadata,
        }
    }

    pub fn map_entities_to_tables(
        &self,
        entities: &super::entity_extractor::RequiredEntitySet,
    ) -> Result<Vec<String>> {
        let mut required_tables = Vec::new();
        let mut found_tables = HashSet::new();

        // Map anchor entities
        for entity in &entities.anchor_entities {
            if let Some(tables) = self.find_tables_for_entity(entity) {
                for table in tables {
                    if !found_tables.contains(&table) {
                        required_tables.push(table.clone());
                        found_tables.insert(table);
                    }
                }
            }
        }

        // Map attribute entities
        for entity in &entities.attribute_entities {
            if let Some(tables) = self.find_tables_for_entity(entity) {
                for table in tables {
                    if !found_tables.contains(&table) {
                        required_tables.push(table.clone());
                        found_tables.insert(table);
                    }
                }
            }
        }

        // Map relationship entities
        for entity in &entities.relationship_entities {
            if let Some(tables) = self.find_tables_for_entity(entity) {
                for table in tables {
                    if !found_tables.contains(&table) {
                        required_tables.push(table.clone());
                        found_tables.insert(table);
                    }
                }
            }
        }

        Ok(required_tables)
    }

    fn find_tables_for_entity(&self, entity: &str) -> Option<Vec<String>> {
        let entity_lower = entity.to_lowercase();
        let mut matching_tables = Vec::new();

        // Strategy 1: Check metadata tables by entity field
        for table in &self.metadata.tables {
            if table.entity.to_lowercase() == entity_lower {
                matching_tables.push(table.name.clone());
            }
        }

        // Strategy 2: Match entity name to table names (e.g., "loan" → "loan_master_b")
        if matching_tables.is_empty() {
            for table in &self.metadata.tables {
                let table_name_lower = table.name.to_lowercase();
                let entity_in_table = table_name_lower.contains(&entity_lower);
                let table_in_entity = entity_lower.contains(&table_name_lower.split('_').next().unwrap_or(""));
                
                if entity_in_table || table_in_entity {
                    matching_tables.push(table.name.clone());
                }
            }
        }

        // Strategy 3: Check semantic registry for entity definitions
        if let Some(registry) = &self.semantic_registry {
            // Check if entity matches any metric or dimension names
            for metric_name in registry.list_metrics() {
                if metric_name.to_lowercase().contains(&entity_lower) {
                    // Try to find tables associated with this metric
                    // This would require additional registry methods
                }
            }
        }

        // Strategy 4: Fuzzy matching on table names
        if matching_tables.is_empty() {
            for table in &self.metadata.tables {
                // Check if entity words appear in table name
                let entity_words: Vec<&str> = entity_lower.split_whitespace().collect();
                let table_name_lower = table.name.to_lowercase();
                
                let matches = entity_words.iter().any(|word| {
                    table_name_lower.contains(word) || 
                    word.len() > 3 && table_name_lower.contains(&word[..3])
                });
                
                if matches {
                    matching_tables.push(table.name.clone());
                }
            }
        }

        if matching_tables.is_empty() {
            None
        } else {
            Some(matching_tables)
        }
    }
}

