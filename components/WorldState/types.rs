//! Types module for WorldState
//! Contains the main WorldState struct and thread-safe wrappers

use super::schema::SchemaRegistry;
use super::keys::KeyRegistry;
use super::rules::{JoinRuleRegistry, FilterRuleRegistry, CalculatedMetricRuleRegistry};
use super::stats::StatsRegistry;
use super::lineage::LineageRegistry;
use super::policies::PolicyRegistry;
use super::quality::{DataQualityRuleRegistry, DataQualityReportRegistry};
use super::aliases::TableAliasRegistry;
use super::contract::ContractRegistry;
use super::source_registry::ApiSourceRegistry;
use super::reconciliation::ReconciliationRuleRegistry;

// Optional KnowledgeBase integration
#[cfg(feature = "knowledge-base")]
use knowledge_base::{KnowledgeBase, BusinessRulesRegistry};

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::RwLock;

/// The authoritative WorldState - the "spine" of the system
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorldState {
    /// Schema registry: table → schema version → columns/types
    pub schema_registry: SchemaRegistry,
    
    /// Key registry: primary keys, natural keys, event time columns
    pub key_registry: KeyRegistry,
    
    /// Join rules: authoritative join relationships
    pub rule_registry: JoinRuleRegistry,
    
    /// Filter rules: business rules for default filters
    pub filter_rule_registry: FilterRuleRegistry,
    
    /// Calculated metric rules: business rules for computed metrics
    pub calculated_metric_rule_registry: CalculatedMetricRuleRegistry,
    
    /// Statistics: row counts, NDV, null rates, distributions
    pub stats_registry: StatsRegistry,
    
    /// Lineage: source → ingestion → table → schema version
    pub lineage_registry: LineageRegistry,
    
    /// Policies: RBAC, SQL allowed verbs, query limits
    pub policy_registry: PolicyRegistry,

    /// Data quality rules (ingestion-time validation)
    #[serde(default)]
    pub quality_rule_registry: DataQualityRuleRegistry,

    /// Data quality reports (history of validations)
    #[serde(default)]
    pub quality_report_registry: DataQualityReportRegistry,

    /// Table alias registry (human-friendly names → table IDs)
    #[serde(default)]
    pub table_alias_registry: TableAliasRegistry,
    
    /// Contract registry (API endpoint → table mappings)
    #[serde(default)]
    pub contract_registry: ContractRegistry,
    
    /// API source registry (data pool of available API columns)
    #[serde(default)]
    pub source_registry: ApiSourceRegistry,
    
    /// Reconciliation rule registry (metric comparison rules)
    #[serde(default)]
    pub reconciliation_rule_registry: ReconciliationRuleRegistry,

    /// Knowledge base (business concepts and definitions)
    /// Optional - only available if knowledge-base feature is enabled
    #[cfg(feature = "knowledge-base")]
    #[serde(default)]
    pub knowledge_base: KnowledgeBase,

    /// Business rules registry (parsed business rules from natural language)
    /// Optional - only available if knowledge-base feature is enabled
    #[cfg(feature = "knowledge-base")]
    #[serde(default)]
    pub business_rules_registry: Option<BusinessRulesRegistry>,

    /// Global version counter (increments on any change)
    pub version: u64,
    
    /// Timestamp of last update
    pub last_updated: u64,
}

impl WorldState {
    /// Create a new empty WorldState
    pub fn new() -> Self {
        Self {
            schema_registry: SchemaRegistry::new(),
            key_registry: KeyRegistry::new(),
            rule_registry: JoinRuleRegistry::new(),
            filter_rule_registry: FilterRuleRegistry::new(),
            calculated_metric_rule_registry: CalculatedMetricRuleRegistry::new(),
            stats_registry: StatsRegistry::new(),
            lineage_registry: LineageRegistry::new(),
            policy_registry: PolicyRegistry::new(),
            quality_rule_registry: DataQualityRuleRegistry::new(),
            quality_report_registry: DataQualityReportRegistry::new(),
            table_alias_registry: TableAliasRegistry::new(),
            contract_registry: ContractRegistry::new(),
            source_registry: ApiSourceRegistry::new(),
            reconciliation_rule_registry: ReconciliationRuleRegistry::new(),
            #[cfg(feature = "knowledge-base")]
            knowledge_base: KnowledgeBase::new(),
            #[cfg(feature = "knowledge-base")]
            business_rules_registry: None,
            version: 1,
            last_updated: Self::now_timestamp(),
        }
    }
    
    /// Compute global world hash (changes when ANY part of world changes)
    pub fn world_hash_global(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.version.hash(&mut hasher);
        // Note: SchemaRegistry, KeyRegistry, JoinRuleRegistry don't implement Hash
        // Using their Debug representation for hashing instead
        format!("{:?}", &self.schema_registry).hash(&mut hasher);
        format!("{:?}", &self.key_registry).hash(&mut hasher);
        format!("{:?}", &self.rule_registry).hash(&mut hasher);
        // Note: stats may change frequently, so we may want to exclude them from global hash
        // or use a separate "schema hash" vs "data hash"
        hasher.finish()
    }
    
    /// Compute relevant world hash for specific tables/edges
    /// Used for cache keying: only invalidate when relevant parts change
    pub fn world_hash_relevant(&self, tables: &[String], edges: &[String]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        
        // Hash relevant table schemas
        for table in tables {
            if let Some(schema) = self.schema_registry.get_table(table) {
                use std::hash::Hash;
                schema.hash(&mut hasher);
            }
        }
        
        // Hash relevant join rules/edges
        for edge_id in edges {
            if let Some(rule) = self.rule_registry.get_rule(edge_id) {
                use std::hash::Hash;
                rule.hash(&mut hasher);
            }
        }
        
        hasher.finish()
    }
    
    /// Increment version (call after any mutation)
    pub fn bump_version(&mut self) {
        self.version += 1;
        self.last_updated = Self::now_timestamp();
    }
    
    /// Get current timestamp (Unix epoch seconds)
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

impl Default for WorldState {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe wrapper for WorldState
pub type WorldStateRef = Arc<RwLock<WorldState>>;

/// Helper to create a WorldStateRef
pub fn new_world_state_ref() -> WorldStateRef {
    Arc::new(RwLock::new(WorldState::new()))
}

