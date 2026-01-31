//! # Knowledge Base Module
//! 
//! A standalone knowledge base module for storing business concepts, rules, and semantic metadata.
//! This module can be used as a standalone block or integrated into larger systems.
//! 
//! ## Features
//! 
//! - **Business Concepts**: Store semantic definitions of tables, metrics, entities, and domain concepts
//! - **Business Rules**: Parse and store business rules from natural language with versioning and approval workflow
//! - **Vector Store**: Embedding-based similarity search for semantic concepts
//! - **Semantic Resolution**: Resolve business terms to tables and columns using semantic matching
//! - **Indexes**: Fast O(1) lookups via name, type, table, tag, and component indexes
//! 
//! ## Usage
//! 
//! To use this module in your project, add it to your `Cargo.toml`:
//! 
//! ```toml
//! [dependencies]
//! knowledge_base = { path = "./KnowledgeBase" }
//! ```
//! 
//! Or if using as a module within the same crate:
//! 
//! ```rust
//! mod knowledge_base;
//! use knowledge_base::{KnowledgeBase, BusinessConcept, ConceptType, BusinessRulesRegistry};
//! ```
//! 
//! ## Example
//! 
//! ```rust,no_run
//! use knowledge_base::{KnowledgeBase, BusinessConcept, ConceptType};
//! 
//! // Create a new knowledge base
//! let mut kb = KnowledgeBase::new();
//! 
//! // Create a business concept
//! let concept = BusinessConcept::new(
//!     "concept_1".to_string(),
//!     "TOS".to_string(),
//!     ConceptType::Metric,
//!     "Total Outstanding - sum of principal and interest".to_string(),
//! );
//! kb.add_concept(concept);
//! 
//! // Search for concepts
//! let results = kb.search_by_name("TOS");
//! println!("Found {} concepts", results.len());
//! ```

pub mod types;
pub mod concepts;
pub mod rules;
pub mod vector_store;
#[cfg(feature = "api-server")]
pub mod api_server;

// Optional modules that require external dependencies
#[cfg(feature = "semantic-resolution")]
pub mod semantic_resolver;
#[cfg(feature = "semantic-resolution")]
pub mod business_term_resolver;
#[cfg(feature = "semantic-resolution")]
pub mod contract_extractor;
#[cfg(feature = "semantic-resolution")]
pub mod semantic_sync;

// Re-export main types for convenience
pub use types::{ConceptType, ConceptUsage};
pub use concepts::{KnowledgeBase, BusinessConcept};
pub use rules::{BusinessRulesRegistry, BusinessRule, RuleState, ParsedBusinessRule};
pub use vector_store::{VectorStore, ConceptSearchResult};
#[cfg(feature = "api-server")]
pub use api_server::{ApiState, start_server, create_router};

// Conditional exports for optional modules
#[cfg(feature = "semantic-resolution")]
pub use semantic_resolver::{SemanticColumnResolver, ColumnMatch};
#[cfg(feature = "semantic-resolution")]
pub use business_term_resolver::{BusinessTermResolver, ResolvedComponent};
#[cfg(feature = "semantic-resolution")]
pub use contract_extractor::ContractTableExtractor;
#[cfg(feature = "semantic-resolution")]
pub use semantic_sync::{SemanticSyncService, SyncReport, ValidationResult};

