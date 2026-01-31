//! Business Rules Storage - Registry for parsed business rules
//! 
//! Stores business rules parsed from natural language, with versioning and approval workflow

use super::concepts::BusinessConcept;
use super::types::ConceptType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Simplified parsed business rule structure for standalone module
/// In a full integration, this would use ParsedBusinessRule from LLM module
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParsedBusinessRule {
    pub concept_name: String,
    pub concept_type: ConceptType,
    pub definition: String,
    pub aliases: Vec<String>,
    pub related_tables: Vec<String>,
    pub related_columns: Vec<String>,
    pub components: Vec<String>,
    pub sql_expression: Option<String>,
    pub tags: Vec<String>,
}

impl ParsedBusinessRule {
    /// Convert to BusinessConcept
    pub fn to_business_concept(&self, concept_id: Option<String>) -> BusinessConcept {
        use super::concepts::BusinessConcept;
        let id = concept_id.unwrap_or_else(|| {
            // Generate a simple ID if uuid is not available
            format!("concept_{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos())
        });
        let mut concept = BusinessConcept::new(
            id,
            self.concept_name.clone(),
            self.concept_type.clone(),
            self.definition.clone(),
        );
        concept.related_tables = self.related_tables.clone();
        concept.related_columns = self.related_columns.clone();
        concept.components = self.components.clone();
        concept.sql_expression = self.sql_expression.clone();
        concept.tags = self.tags.clone();
        concept
    }
}

/// State of a business rule
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuleState {
    /// Draft - newly created, not yet reviewed
    Draft,
    /// Pending - submitted for approval
    Pending,
    /// Approved - active and used in queries
    Approved,
    /// Rejected - not approved
    Rejected,
    /// Deprecated - replaced by newer version
    Deprecated,
}

/// A business rule entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BusinessRule {
    /// Unique rule ID
    pub rule_id: String,
    
    /// Original natural language description
    pub original_description: String,
    
    /// Parsed business rule
    pub parsed_rule: ParsedBusinessRule,
    
    /// Associated business concept (if created)
    pub concept_id: Option<String>,
    
    /// Current state
    pub state: RuleState,
    
    /// Version number (increments on updates)
    pub version: u32,
    
    /// Created by (username)
    pub created_by: Option<String>,
    
    /// Created timestamp
    pub created_at: u64,
    
    /// Updated timestamp
    pub updated_at: u64,
    
    /// Approved by (username)
    pub approved_by: Option<String>,
    
    /// Approval timestamp
    pub approved_at: Option<u64>,
    
    /// Rejection reason (if rejected)
    pub rejection_reason: Option<String>,
    
    /// Metadata (additional key-value pairs)
    pub metadata: HashMap<String, String>,
    
    /// Labels for categorization and fast retrieval (Jira-style)
    pub labels: Vec<String>,
}

impl BusinessRule {
    pub fn new(
        rule_id: String,
        original_description: String,
        parsed_rule: ParsedBusinessRule,
        created_by: Option<String>,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            rule_id,
            original_description,
            parsed_rule,
            concept_id: None,
            state: RuleState::Draft,
            version: 1,
            created_by,
            created_at: now,
            updated_at: now,
            approved_by: None,
            approved_at: None,
            rejection_reason: None,
            metadata: HashMap::new(),
            labels: Vec::new(),
        }
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
    
    /// Approve the rule
    pub fn approve(&mut self, approved_by: String) {
        self.state = RuleState::Approved;
        self.approved_by = Some(approved_by);
        self.approved_at = Some(Self::now_timestamp());
        self.updated_at = Self::now_timestamp();
    }
    
    /// Reject the rule
    pub fn reject(&mut self, reason: String) {
        self.state = RuleState::Rejected;
        self.rejection_reason = Some(reason);
        self.updated_at = Self::now_timestamp();
    }
    
    /// Update the rule (creates new version)
    pub fn update(&mut self, new_description: String, new_parsed_rule: ParsedBusinessRule) {
        self.original_description = new_description;
        self.parsed_rule = new_parsed_rule;
        self.version += 1;
        self.updated_at = Self::now_timestamp();
        // Reset approval state on update
        if self.state == RuleState::Approved {
            self.state = RuleState::Pending;
            self.approved_by = None;
            self.approved_at = None;
        }
    }
    
    /// Link to a business concept
    pub fn link_concept(&mut self, concept_id: String) {
        self.concept_id = Some(concept_id);
        self.updated_at = Self::now_timestamp();
    }
    
    /// Convert to BusinessConcept
    pub fn to_business_concept(&self) -> BusinessConcept {
        self.parsed_rule.to_business_concept(self.concept_id.clone())
    }
}

/// Business Rules Registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BusinessRulesRegistry {
    /// Rule ID → BusinessRule
    rules: HashMap<String, BusinessRule>,
    
    /// Concept ID → Rule IDs (for reverse lookup)
    concept_index: HashMap<String, Vec<String>>,
    
    /// Concept name → Rule IDs (for name-based lookup)
    name_index: HashMap<String, Vec<String>>,
    
    /// Label → Rule IDs (for fast label-based retrieval)
    label_index: HashMap<String, Vec<String>>,
}

impl BusinessRulesRegistry {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
            concept_index: HashMap::new(),
            name_index: HashMap::new(),
            label_index: HashMap::new(),
        }
    }
    
    /// Add or update a business rule
    pub fn add_rule(&mut self, rule: BusinessRule) {
        let rule_id = rule.rule_id.clone();
        let concept_name = rule.parsed_rule.concept_name.clone();
        
        // Remove old rule from indexes if updating
        if let Some(old_rule) = self.rules.get(&rule_id) {
            // Remove from concept index
            if let Some(ref concept_id) = old_rule.concept_id {
                if let Some(ids) = self.concept_index.get_mut(concept_id) {
                    ids.retain(|id| id != &rule_id);
                }
            }
            // Remove from name index
            if let Some(ids) = self.name_index.get_mut(&old_rule.parsed_rule.concept_name) {
                ids.retain(|id| id != &rule_id);
            }
            // Remove from label index
            for label in &old_rule.labels {
                if let Some(ids) = self.label_index.get_mut(label) {
                    ids.retain(|id| id != &rule_id);
                }
            }
        }
        
        // Update indexes
        if let Some(ref concept_id) = rule.concept_id {
            self.concept_index
                .entry(concept_id.clone())
                .or_insert_with(Vec::new)
                .push(rule_id.clone());
        }
        
        self.name_index
            .entry(concept_name)
            .or_insert_with(Vec::new)
            .push(rule_id.clone());
        
        // Update label index
        for label in &rule.labels {
            self.label_index
                .entry(label.clone())
                .or_insert_with(Vec::new)
                .push(rule_id.clone());
        }
        
        // Store rule
        self.rules.insert(rule_id, rule);
    }
    
    /// Get rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<&BusinessRule> {
        self.rules.get(rule_id)
    }
    
    /// Get rule by ID (mutable)
    pub fn get_rule_mut(&mut self, rule_id: &str) -> Option<&mut BusinessRule> {
        self.rules.get_mut(rule_id)
    }
    
    /// List all rules
    pub fn list_all(&self) -> Vec<&BusinessRule> {
        self.rules.values().collect()
    }
    
    /// List rules by state
    pub fn list_by_state(&self, state: &RuleState) -> Vec<&BusinessRule> {
        self.rules.values()
            .filter(|r| &r.state == state)
            .collect()
    }
    
    /// List approved rules
    pub fn list_approved(&self) -> Vec<&BusinessRule> {
        self.list_by_state(&RuleState::Approved)
    }
    
    /// Get rules by concept ID
    pub fn get_by_concept(&self, concept_id: &str) -> Vec<&BusinessRule> {
        self.concept_index
            .get(concept_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.rules.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Get rules by concept name
    pub fn get_by_name(&self, name: &str) -> Vec<&BusinessRule> {
        self.name_index
            .get(name)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.rules.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Delete a rule
    pub fn delete_rule(&mut self, rule_id: &str) -> bool {
        if let Some(rule) = self.rules.remove(rule_id) {
            // Remove from indexes
            if let Some(ref concept_id) = rule.concept_id {
                if let Some(ids) = self.concept_index.get_mut(concept_id) {
                    ids.retain(|id| id != rule_id);
                    if ids.is_empty() {
                        self.concept_index.remove(concept_id);
                    }
                }
            }
            
            let concept_name = rule.parsed_rule.concept_name;
            if let Some(ids) = self.name_index.get_mut(&concept_name) {
                ids.retain(|id| id != rule_id);
                if ids.is_empty() {
                    self.name_index.remove(&concept_name);
                }
            }
            
            // Remove from label index
            for label in &rule.labels {
                if let Some(ids) = self.label_index.get_mut(label) {
                    ids.retain(|id| id != rule_id);
                    if ids.is_empty() {
                        self.label_index.remove(label);
                    }
                }
            }
            
            true
        } else {
            false
        }
    }
    
    /// Get rules by label (O(1) lookup)
    pub fn get_by_label(&self, label: &str) -> Vec<&BusinessRule> {
        self.label_index
            .get(label)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.rules.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Get all unique labels
    pub fn get_all_labels(&self) -> Vec<String> {
        self.label_index.keys().cloned().collect()
    }
    
    /// Search rules by label (fuzzy match)
    pub fn search_by_label(&self, query: &str) -> Vec<&BusinessRule> {
        let query_lower = query.to_lowercase();
        let mut results = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();
        
        for (label, rule_ids) in &self.label_index {
            if label.to_lowercase().contains(&query_lower) {
                for rule_id in rule_ids {
                    if !seen_ids.contains(rule_id) {
                        if let Some(rule) = self.rules.get(rule_id) {
                            seen_ids.insert(rule_id.clone());
                            results.push(rule);
                        }
                    }
                }
            }
        }
        
        results
    }
    
    /// Search rules by text (simple text matching)
    pub fn search(&self, query: &str) -> Vec<&BusinessRule> {
        let query_lower = query.to_lowercase();
        self.rules.values()
            .filter(|rule| {
                rule.original_description.to_lowercase().contains(&query_lower) ||
                rule.parsed_rule.concept_name.to_lowercase().contains(&query_lower) ||
                rule.parsed_rule.definition.to_lowercase().contains(&query_lower)
            })
            .collect()
    }
}

impl Default for BusinessRulesRegistry {
    fn default() -> Self {
        Self::new()
    }
}

