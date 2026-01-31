//! # WorldState Module
//! 
//! A standalone WorldState module that serves as the authoritative "spine" of the system.
//! This module can be used as a standalone block or integrated into larger systems.
//! 
//! ## Features
//! 
//! - **Schema Registry**: Table schemas with versioning and dual-name architecture
//! - **Key Registry**: Primary keys, natural keys, event time columns
//! - **Rules**: Join rules, filter rules, calculated metric rules
//! - **Statistics**: Row counts, NDV, null rates, distributions
//! - **Lineage**: Source → table → schema version tracking
//! - **Policies**: RBAC, SQL allowed verbs, query limits
//! - **Knowledge Base Integration**: Business concepts and rules
//! 
//! ## Usage
//! 
//! To use this module in your project, add it to your `Cargo.toml`:
//! 
//! ```toml
//! [dependencies]
//! world_state = { path = "./WorldState" }
//! knowledge_base = { path = "./KnowledgeBase" }  # Optional integration
//! ```
//! 
//! ## Example
//! 
//! ```rust,no_run
//! use world_state::{WorldState, SchemaRegistry, TableSchema, ColumnInfo};
//! 
//! // Create a new world state
//! let mut world_state = WorldState::new();
//! 
//! // Create a table schema
//! let mut schema = TableSchema::new("employees".to_string());
//! let column = ColumnInfo::with_single_name("id".to_string());
//! schema.add_column(column);
//! world_state.schema_registry.register_table(schema);
//! ```

pub mod types;
pub mod schema;
pub mod keys;
pub mod rules;
pub mod stats;
pub mod lineage;
pub mod policies;
pub mod quality;
pub mod aliases;
pub mod contract;
pub mod source_registry;
pub mod reconciliation;

// Optional integration with KnowledgeBase
#[cfg(feature = "knowledge-base")]
pub mod knowledge_integration;

#[cfg(feature = "knowledge-base")]
pub use knowledge_integration::*;

// Re-export main types for convenience
pub use types::WorldState;
pub use schema::{SchemaRegistry, TableSchema, ColumnInfo, SchemaVersion};
pub use keys::{KeyRegistry, PrimaryKey, NaturalKey, EventTime, DedupeStrategy};
pub use rules::{JoinRule, JoinRuleRegistry, FilterRule, FilterRuleRegistry, CalculatedMetricRule, CalculatedMetricRuleRegistry, RuleState};
pub use stats::{StatsRegistry, ColumnStats, TableStats};
pub use lineage::{LineageRegistry, SourceInfo, IngestionRun, TableLineage};
pub use policies::{PolicyRegistry, RBACPolicy, SQLPolicy, QueryPolicy};
pub use quality::{DataQualityRule, DataQualityRuleRegistry, DataQualitySeverity, DataQualityRuleKind, DataQualityReport, DataQualityReportRegistry};
pub use aliases::{TableAlias, TableAliasRegistry, AliasScope};
pub use contract::{ContractRegistry, TableContract, ColumnMapping, PoolColumnReference};
pub use source_registry::{ApiSourceRegistry, ApiSource, PoolColumn};
pub use reconciliation::ReconciliationRuleRegistry;

// Re-export thread-safe wrapper
pub use types::{WorldStateRef, new_world_state_ref};

