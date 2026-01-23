//! Semantic Dimension Definition
//! 
//! Defines the structure and behavior of semantic dimensions in the registry.

use crate::error::Result;
use crate::semantic::join_graph::JoinEdge;
use serde::{Deserialize, Serialize};

/// Dimension data type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DimensionType {
    String,
    Integer,
    Decimal,
    Date,
    Boolean,
    Enum,
}

impl DimensionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DimensionType::String => "string",
            DimensionType::Integer => "integer",
            DimensionType::Decimal => "decimal",
            DimensionType::Date => "date",
            DimensionType::Boolean => "boolean",
            DimensionType::Enum => "enum",
        }
    }
}

/// Trait for dimension definitions
pub trait DimensionDefinition: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn base_table(&self) -> &str;
    fn column(&self) -> &str;
    fn join_path(&self) -> &[JoinEdge];
    fn data_type(&self) -> DimensionType;
    /// Optional SQL expression to use instead of simple column reference
    /// If Some, this expression will be used in SELECT clause (e.g., CASE statements, literals)
    fn sql_expression(&self) -> Option<&str>;
}

/// Semantic dimension implementation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticDimension {
    pub name: String,
    pub description: String,
    pub base_table: String,
    pub column: String,
    pub join_path: Vec<JoinEdge>,
    pub data_type: DimensionType,
    /// Optional SQL expression to use instead of simple column reference
    /// If Some, this expression will be used in SELECT clause (e.g., CASE statements, literals)
    #[serde(default)]
    pub sql_expression: Option<String>,
}

impl DimensionDefinition for SemanticDimension {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn base_table(&self) -> &str {
        &self.base_table
    }

    fn column(&self) -> &str {
        &self.column
    }

    fn join_path(&self) -> &[JoinEdge] {
        &self.join_path
    }

    fn data_type(&self) -> DimensionType {
        self.data_type
    }

    fn sql_expression(&self) -> Option<&str> {
        self.sql_expression.as_deref()
    }
}

impl SemanticDimension {
    pub fn new(
        name: String,
        description: String,
        base_table: String,
        column: String,
        data_type: DimensionType,
    ) -> Self {
        Self {
            name,
            description,
            base_table,
            column,
            join_path: Vec::new(),
            data_type,
            sql_expression: None,
        }
    }

    pub fn with_join_path(mut self, join_path: Vec<JoinEdge>) -> Self {
        self.join_path = join_path;
        self
    }

    pub fn with_sql_expression(mut self, sql_expression: Option<String>) -> Self {
        self.sql_expression = sql_expression;
        self
    }
}

