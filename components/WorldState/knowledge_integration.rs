//! Knowledge Base Integration
//! 
//! This module provides integration between WorldState and KnowledgeBase modules.
//! It's only available when the "knowledge-base" feature is enabled.

#[cfg(feature = "knowledge-base")]
use knowledge_base::{KnowledgeBase, BusinessRulesRegistry};

#[cfg(feature = "knowledge-base")]
use crate::types::WorldState;

/// Helper methods for WorldState with KnowledgeBase integration
#[cfg(feature = "knowledge-base")]
impl WorldState {
    /// Get the knowledge base (mutable)
    pub fn knowledge_base_mut(&mut self) -> &mut KnowledgeBase {
        &mut self.knowledge_base
    }
    
    /// Get the knowledge base (immutable)
    pub fn knowledge_base(&self) -> &KnowledgeBase {
        &self.knowledge_base
    }
    
    /// Get or create business rules registry
    pub fn get_or_create_business_rules_registry(&mut self) -> &mut BusinessRulesRegistry {
        if self.business_rules_registry.is_none() {
            self.business_rules_registry = Some(BusinessRulesRegistry::new());
        }
        self.business_rules_registry.as_mut().unwrap()
    }
    
    /// Get business rules registry (immutable)
    pub fn business_rules_registry(&self) -> Option<&BusinessRulesRegistry> {
        self.business_rules_registry.as_ref()
    }
}

