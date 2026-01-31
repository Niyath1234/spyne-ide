//! Canonical Entity Schema
//! 
//! Defines the canonical representation of entities for row-level reconciliation.
//! This provides a normalized view that both pipelines can map to for comparison.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Canonical representation of an entity
/// 
/// This defines the standard structure for comparing data across different systems.
/// All pipelines map their data to this canonical form before comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalEntity {
    /// Entity name (e.g., "payment_event", "recovery_ftd")
    pub name: String,
    
    /// Key columns that uniquely identify a row at this grain
    /// e.g., ["uuid_or_order_id", "paid_date"]
    pub keys: Vec<String>,
    
    /// Value columns that contain the metric values to compare
    /// e.g., ["paid_amount"]
    pub value_columns: Vec<String>,
    
    /// Attribute columns that provide context but aren't part of the key
    /// e.g., ["loan_type", "bucket", "asset_class"]
    pub attributes: Vec<String>,
}

impl CanonicalEntity {
    /// Create a new canonical entity
    pub fn new(
        name: impl Into<String>,
        keys: Vec<String>,
        value_columns: Vec<String>,
        attributes: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            keys,
            value_columns,
            attributes,
        }
    }
    
    /// Get all columns (keys + value_columns + attributes)
    pub fn all_columns(&self) -> Vec<String> {
        let mut cols = self.keys.clone();
        cols.extend(self.value_columns.clone());
        cols.extend(self.attributes.clone());
        cols
    }
    
    /// Validate that required columns are present
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("Entity name cannot be empty".to_string());
        }
        if self.keys.is_empty() {
            return Err("Keys cannot be empty".to_string());
        }
        if self.value_columns.is_empty() {
            return Err("Value columns cannot be empty".to_string());
        }
        
        // Check for duplicate column names
        let all_cols = self.all_columns();
        let unique_cols: std::collections::HashSet<_> = all_cols.iter().collect();
        if unique_cols.len() != all_cols.len() {
            return Err("Duplicate column names found".to_string());
        }
        
        Ok(())
    }
}

/// Registry of canonical entities
/// 
/// Maps entity identifiers to their canonical definitions.
/// This allows lookup by name or by system/metric combination.
pub struct CanonicalEntityRegistry {
    /// Direct mapping by entity name
    entities_by_name: HashMap<String, CanonicalEntity>,
    
    /// Mapping by system and metric to entity name
    /// Key format: "{system}:{metric}"
    system_metric_to_entity: HashMap<String, String>,
}

impl CanonicalEntityRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self {
            entities_by_name: HashMap::new(),
            system_metric_to_entity: HashMap::new(),
        }
    }
    
    /// Register a canonical entity
    pub fn register(&mut self, entity: CanonicalEntity) -> Result<(), String> {
        entity.validate()?;
        self.entities_by_name.insert(entity.name.clone(), entity);
        Ok(())
    }
    
    /// Register a system-metric mapping to an entity
    pub fn register_system_metric(
        &mut self,
        system: &str,
        metric: &str,
        entity_name: &str,
    ) -> Result<(), String> {
        if !self.entities_by_name.contains_key(entity_name) {
            return Err(format!("Entity '{}' not found in registry", entity_name));
        }
        let key = format!("{}:{}", system, metric);
        self.system_metric_to_entity.insert(key, entity_name.to_string());
        Ok(())
    }
    
    /// Get entity by name
    pub fn get_by_name(&self, name: &str) -> Option<&CanonicalEntity> {
        self.entities_by_name.get(name)
    }
    
    /// Get entity by system and metric
    pub fn get_by_system_metric(&self, system: &str, metric: &str) -> Option<&CanonicalEntity> {
        let key = format!("{}:{}", system, metric);
        self.system_metric_to_entity
            .get(&key)
            .and_then(|entity_name| self.entities_by_name.get(entity_name))
    }
    
    /// List all registered entities
    pub fn list_entities(&self) -> Vec<&CanonicalEntity> {
        self.entities_by_name.values().collect()
    }
}

impl Default for CanonicalEntityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Predefined canonical entities
/// 
/// Common entities used in reconciliation scenarios.
/// These can be extended with domain-specific entities.
pub fn create_default_registry() -> CanonicalEntityRegistry {
    let mut registry = CanonicalEntityRegistry::new();
    
    // Recovery FTD (First Time Default) - Payment Event
    registry.register(CanonicalEntity::new(
        "payment_event",
        vec!["uuid_or_order_id".to_string(), "paid_date".to_string()],
        vec!["paid_amount".to_string()],
        vec!["loan_type".to_string(), "bucket".to_string(), "asset_class".to_string()],
    )).expect("Failed to register payment_event");
    
    // Example: Register system-metric mappings
    // registry.register_system_metric("recovery", "ftd", "payment_event")
    //     .expect("Failed to register system-metric mapping");
    
    registry
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_canonical_entity_creation() {
        let entity = CanonicalEntity::new(
            "payment_event",
            vec!["uuid".to_string(), "date".to_string()],
            vec!["amount".to_string()],
            vec!["type".to_string()],
        );
        
        assert_eq!(entity.name, "payment_event");
        assert_eq!(entity.keys.len(), 2);
        assert_eq!(entity.value_columns.len(), 1);
        assert_eq!(entity.attributes.len(), 1);
    }
    
    #[test]
    fn test_canonical_entity_validation() {
        let mut entity = CanonicalEntity::new(
            "test",
            vec![],
            vec!["amount".to_string()],
            vec![],
        );
        
        assert!(entity.validate().is_err());
        
        entity.keys = vec!["uuid".to_string()];
        assert!(entity.validate().is_ok());
    }
    
    #[test]
    fn test_registry() {
        let mut registry = CanonicalEntityRegistry::new();
        let entity = CanonicalEntity::new(
            "payment_event",
            vec!["uuid".to_string()],
            vec!["amount".to_string()],
            vec![],
        );
        
        registry.register(entity.clone()).unwrap();
        assert!(registry.get_by_name("payment_event").is_some());
        
        registry.register_system_metric("recovery", "ftd", "payment_event").unwrap();
        assert!(registry.get_by_system_metric("recovery", "ftd").is_some());
    }
}





