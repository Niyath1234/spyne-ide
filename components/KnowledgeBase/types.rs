//! Types module for Knowledge Base
//! Contains core types and enums used throughout the knowledge base module

use serde::{Deserialize, Serialize};

/// Type of business concept
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ConceptType {
    /// Table definition (what a table represents)
    Table,
    /// Metric definition (e.g., TOS, Revenue)
    Metric,
    /// Business entity (e.g., Khatabook, Trial Balance)
    Entity,
    /// Domain concept (e.g., Principal, Interest, Writeoff)
    DomainConcept,
    /// Column semantic meaning
    ColumnSemantics,
}

/// Track which concepts were used in a query/response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConceptUsage {
    /// Concept ID that was used
    pub concept_id: String,
    
    /// Concept name (for display)
    pub concept_name: String,
    
    /// How the concept was used
    pub usage_type: String, // "definition", "calculation", "reference", etc.
    
    /// Relevance score (0.0-1.0)
    pub relevance: f32,
    
    /// Extracted information from the concept
    pub extracted_info: Option<String>,
}

