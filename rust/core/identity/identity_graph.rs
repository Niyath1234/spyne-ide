//! Identity Graph
//! 
//! Maps system-specific entity identifiers to canonical column names.
//! This enables reconciliation across systems that use different column names
//! for the same logical entity (e.g., user_uuid vs uuid vs customer_id).

use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Identity mapping for a single entity/system combination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityMapping {
    /// Entity name (e.g., "user", "loan", "customer", "payment")
    pub entity: String,
    
    /// System name (e.g., "system_a", "system_b", "collections_mis")
    pub system: String,
    
    /// System-specific column name (e.g., "user_uuid", "uuid", "customer_id")
    pub column: String,
    
    /// Canonical column name (e.g., "uuid", "id", "entity_id")
    pub canonical_column: String,
    
    /// Optional: Data type of the column
    pub data_type: Option<String>,
    
    /// Optional: Confidence level of the mapping
    pub confidence: Option<String>,
}

/// Identity graph that manages all identity mappings
pub struct IdentityGraph {
    /// Maps (entity, system) -> IdentityMapping
    mappings: HashMap<(String, String), IdentityMapping>,
    
    /// Maps entity -> canonical column name
    entity_canonical: HashMap<String, String>,
    
    /// Maps (entity, system, column) -> canonical column
    lookup_cache: HashMap<(String, String, String), String>,
}

impl IdentityGraph {
    /// Create a new empty identity graph
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
            entity_canonical: HashMap::new(),
            lookup_cache: HashMap::new(),
        }
    }
    
    /// Add an identity mapping
    pub fn add_mapping(&mut self, mapping: IdentityMapping) {
        let key = (mapping.entity.clone(), mapping.system.clone());
        let canonical = mapping.canonical_column.clone();
        
        // Store mapping
        self.mappings.insert(key.clone(), mapping.clone());
        
        // Update entity canonical mapping (use first one found, or prefer explicit canonical)
        if !self.entity_canonical.contains_key(&mapping.entity) {
            self.entity_canonical.insert(mapping.entity.clone(), canonical.clone());
        }
        
        // Update lookup cache
        let lookup_key = (
            mapping.entity.clone(),
            mapping.system.clone(),
            mapping.column.clone(),
        );
        self.lookup_cache.insert(lookup_key, canonical);
    }
    
    /// Get canonical column name for an entity/system/column combination
    pub fn get_canonical(&self, entity: &str, system: &str, column: &str) -> Option<String> {
        // Try direct lookup first
        let lookup_key = (entity.to_string(), system.to_string(), column.to_string());
        if let Some(canonical) = self.lookup_cache.get(&lookup_key) {
            return Some(canonical.clone());
        }
        
        // Try entity-level mapping
        if let Some(mapping) = self.mappings.get(&(entity.to_string(), system.to_string())) {
            if mapping.column == column {
                return Some(mapping.canonical_column.clone());
            }
        }
        
        // If column already matches canonical, return as-is
        if let Some(canonical) = self.entity_canonical.get(entity) {
            if column == canonical {
                return Some(column.to_string());
            }
        }
        
        None
    }
    
    /// Get all mappings for an entity
    pub fn get_entity_mappings(&self, entity: &str) -> Vec<&IdentityMapping> {
        self.mappings
            .values()
            .filter(|m| m.entity == entity)
            .collect()
    }
    
    /// Get canonical column name for an entity (default)
    pub fn get_entity_canonical(&self, entity: &str) -> Option<&String> {
        self.entity_canonical.get(entity)
    }
    
    /// Detect UUID columns automatically
    /// 
    /// Looks for columns that match common UUID patterns:
    /// - Contains "uuid" in name (case-insensitive)
    /// - Contains "id" in name and matches UUID format
    /// - Metadata hints
    pub fn detect_uuid_columns(
        &self,
        table_name: &str,
        system: &str,
        columns: &[String],
        metadata: &Metadata,
    ) -> Vec<(String, String)> {
        let mut detected = Vec::new();
        
        // Get table metadata to check for entity type
        let table = metadata.tables.iter().find(|t| t.name == table_name);
        let entity = table.and_then(|t| Some(t.entity.clone())).unwrap_or_default();
        
        for column in columns {
            let col_lower = column.to_lowercase();
            
            // Check if column name suggests UUID
            if col_lower.contains("uuid") || 
               (col_lower.contains("id") && self.looks_like_uuid_column(column)) {
                
                // Determine canonical name
                let canonical = if col_lower.contains("uuid") {
                    "uuid".to_string()
                } else if col_lower.contains("user") {
                    "user_id".to_string()
                } else if col_lower.contains("customer") {
                    "customer_id".to_string()
                } else if col_lower.contains("loan") {
                    "loan_id".to_string()
                } else {
                    format!("{}_id", entity) // Use entity name + _id
                };
                
                detected.push((column.clone(), canonical));
            }
        }
        
        detected
    }
    
    /// Check if a column name looks like a UUID column
    fn looks_like_uuid_column(&self, column: &str) -> bool {
        let col_lower = column.to_lowercase();
        // Common UUID column patterns
        col_lower.contains("uuid") ||
        col_lower.ends_with("_id") ||
        col_lower == "id"
    }
    
    /// Normalize UUID format (uppercase/lowercase, with/without dashes)
    pub fn normalize_uuid_format(&self, value: &str) -> String {
        // Remove dashes and convert to lowercase for comparison
        value.replace("-", "").to_lowercase()
    }
    
    /// Apply identity mapping to a column name
    /// 
    /// Returns the canonical column name if mapping exists, otherwise returns original
    pub fn apply_mapping(&self, entity: &str, system: &str, column: &str) -> String {
        self.get_canonical(entity, system, column)
            .unwrap_or_else(|| column.to_string())
    }
    
    /// Get all systems that have mappings for an entity
    pub fn get_systems_for_entity(&self, entity: &str) -> Vec<String> {
        self.mappings
            .iter()
            .filter(|((e, _), _)| e == entity)
            .map(|((_, s), _)| s.clone())
            .collect()
    }
}

impl Default for IdentityGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Load identity graph from metadata
/// 
/// Reads identity.json if it exists and builds the identity graph
pub fn load_identity_graph_from_metadata(
    metadata_dir: &PathBuf,
) -> Result<IdentityGraph> {
    let identity_file = metadata_dir.join("identity.json");
    
    let mut graph = IdentityGraph::new();
    
    // Try to load identity.json if it exists
    if identity_file.exists() {
        let content = std::fs::read_to_string(&identity_file)
            .map_err(|e| RcaError::Metadata(format!("Failed to read identity.json: {}", e)))?;
        
        let identity_data: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| RcaError::Metadata(format!("Failed to parse identity.json: {}", e)))?;
        
        // Parse identity.json structure
        if let Some(entries) = identity_data.as_array() {
            for entry in entries {
                if let Some(entry_type) = entry.get("type").and_then(|v| v.as_str()) {
                    match entry_type {
                        "canonical_key" => {
                            // Format: { "type": "canonical_key", "entity": "loan", "canonical": "uuid", "alternates": [...] }
                            if let (Some(entity), Some(canonical)) = (
                                entry.get("entity").and_then(|v| v.as_str()),
                                entry.get("canonical").and_then(|v| v.as_str()),
                            ) {
                                // Store canonical for entity
                                graph.entity_canonical.insert(entity.to_string(), canonical.to_string());
                                
                                // Process alternates
                                if let Some(alternates) = entry.get("alternates").and_then(|v| v.as_array()) {
                                    for alt in alternates {
                                        if let (Some(system), Some(key)) = (
                                            alt.get("system").and_then(|v| v.as_str()),
                                            alt.get("key").and_then(|v| v.as_str()),
                                        ) {
                                            let mapping = IdentityMapping {
                                                entity: entity.to_string(),
                                                system: system.to_string(),
                                                column: key.to_string(),
                                                canonical_column: canonical.to_string(),
                                                data_type: None,
                                                confidence: Some("high".to_string()),
                                            };
                                            graph.add_mapping(mapping);
                                        }
                                    }
                                }
                            }
                        }
                        "key_mapping" => {
                            // Format: { "type": "key_mapping", "from_system": "...", "to_system": "...", "from_key": "...", "to_key": "..." }
                            // This represents a direct mapping between systems
                            // We can infer entity from context or use a default
                            if let (Some(from_system), Some(to_system), Some(from_key), Some(to_key)) = (
                                entry.get("from_system").and_then(|v| v.as_str()),
                                entry.get("to_system").and_then(|v| v.as_str()),
                                entry.get("from_key").and_then(|v| v.as_str()),
                                entry.get("to_key").and_then(|v| v.as_str()),
                            ) {
                                // Infer entity from key name or use "entity" as default
                                let entity = entry.get("entity")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("entity");
                                
                                // Determine canonical (use to_key as canonical, or infer)
                                let canonical = entry.get("canonical")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or(to_key);
                                
                                // Add mapping for from_system
                                let mapping_from = IdentityMapping {
                                    entity: entity.to_string(),
                                    system: from_system.to_string(),
                                    column: from_key.to_string(),
                                    canonical_column: canonical.to_string(),
                                    data_type: None,
                                    confidence: entry.get("confidence").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                };
                                graph.add_mapping(mapping_from);
                                
                                // Add mapping for to_system
                                let mapping_to = IdentityMapping {
                                    entity: entity.to_string(),
                                    system: to_system.to_string(),
                                    column: to_key.to_string(),
                                    canonical_column: canonical.to_string(),
                                    data_type: None,
                                    confidence: entry.get("confidence").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                };
                                graph.add_mapping(mapping_to);
                            }
                        }
                        _ => {
                            // Unknown type, skip
                        }
                    }
                }
            }
        }
    }
    
    Ok(graph)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_identity_mapping() {
        let mut graph = IdentityGraph::new();
        
        let mapping = IdentityMapping {
            entity: "user".to_string(),
            system: "system_a".to_string(),
            column: "user_uuid".to_string(),
            canonical_column: "uuid".to_string(),
            data_type: None,
            confidence: None,
        };
        
        graph.add_mapping(mapping);
        
        assert_eq!(
            graph.get_canonical("user", "system_a", "user_uuid"),
            Some("uuid".to_string())
        );
    }
    
    #[test]
    fn test_multiple_systems() {
        let mut graph = IdentityGraph::new();
        
        graph.add_mapping(IdentityMapping {
            entity: "loan".to_string(),
            system: "system_a".to_string(),
            column: "loan_id".to_string(),
            canonical_column: "id".to_string(),
            data_type: None,
            confidence: None,
        });
        
        graph.add_mapping(IdentityMapping {
            entity: "loan".to_string(),
            system: "system_b".to_string(),
            column: "uuid".to_string(),
            canonical_column: "id".to_string(),
            data_type: None,
            confidence: None,
        });
        
        assert_eq!(
            graph.get_canonical("loan", "system_a", "loan_id"),
            Some("id".to_string())
        );
        assert_eq!(
            graph.get_canonical("loan", "system_b", "uuid"),
            Some("id".to_string())
        );
    }
}





