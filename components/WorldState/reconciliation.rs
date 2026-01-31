//! Reconciliation Rule Registry
//! 
//! Stores reconciliation rules that define metrics to compare.
//! 
//! NOTE: This is a simplified version. For full reconciliation functionality
//! including expression trees and root cause analysis, use the reconciliation module.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Simplified Reconciliation Rule
/// For full functionality, use the reconciliation module's ReconciliationRule
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReconciliationRule {
    /// Rule ID
    pub id: String,
    
    /// Rule name
    pub name: String,
    
    /// Description
    pub description: Option<String>,
    
    /// Is enabled
    pub enabled: bool,
    
    /// Created timestamp
    pub created_at: u64,
    
    /// Updated timestamp
    pub updated_at: u64,
}

impl ReconciliationRule {
    pub fn new(id: String, name: String) -> Self {
        let now = Self::now_timestamp();
        Self {
            id,
            name,
            description: None,
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

impl Hash for ReconciliationRule {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.name.hash(state);
        self.enabled.hash(state);
    }
}

/// Reconciliation Rule Registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReconciliationRuleRegistry {
    /// Rule ID → ReconciliationRule
    rules: HashMap<String, ReconciliationRule>,
}

impl ReconciliationRuleRegistry {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
        }
    }
    
    /// Register a reconciliation rule
    pub fn register_rule(&mut self, rule: ReconciliationRule) {
        let id = rule.id.clone();
        self.rules.insert(id, rule);
    }
    
    /// Get a rule by ID
    pub fn get_rule(&self, id: &str) -> Option<&ReconciliationRule> {
        self.rules.get(id)
    }
    
    /// Get a mutable reference to a rule
    pub fn get_rule_mut(&mut self, id: &str) -> Option<&mut ReconciliationRule> {
        self.rules.get_mut(id)
    }
    
    /// List all rules
    pub fn list_rules(&self) -> Vec<&ReconciliationRule> {
        self.rules.values().collect()
    }
    
    /// List enabled rules
    pub fn list_enabled_rules(&self) -> Vec<&ReconciliationRule> {
        self.rules.values()
            .filter(|r| r.enabled)
            .collect()
    }
    
    /// Delete a rule
    pub fn delete_rule(&mut self, id: &str) -> bool {
        self.rules.remove(id).is_some()
    }
    
    /// Check if a rule exists
    pub fn has_rule(&self, id: &str) -> bool {
        self.rules.contains_key(id)
    }
}

impl Default for ReconciliationRuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Hash for ReconciliationRuleRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut keys: Vec<_> = self.rules.keys().collect();
        keys.sort();
        for key in keys {
            key.hash(state);
            if let Some(rule) = self.rules.get(key) {
                rule.hash(state);
            }
        }
    }
}
