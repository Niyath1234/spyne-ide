//! Semantic Metric Definition
//! 
//! Defines the structure and behavior of semantic metrics in the registry.

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Time grain for metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeGrain {
    /// Real-time or no time grain
    None,
    /// Daily aggregation
    Day,
    /// Weekly aggregation
    Week,
    /// Monthly aggregation
    Month,
    /// Quarterly aggregation
    Quarter,
    /// Yearly aggregation
    Year,
}

impl TimeGrain {
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeGrain::None => "none",
            TimeGrain::Day => "day",
            TimeGrain::Week => "week",
            TimeGrain::Month => "month",
            TimeGrain::Quarter => "quarter",
            TimeGrain::Year => "year",
        }
    }
}

/// Aggregation function type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Aggregation {
    Sum,
    Avg,
    Count,
    CountDistinct,
    Min,
    Max,
}

impl Aggregation {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Aggregation::Sum => "SUM",
            Aggregation::Avg => "AVG",
            Aggregation::Count => "COUNT",
            Aggregation::CountDistinct => "COUNT(DISTINCT ...)",
            Aggregation::Min => "MIN",
            Aggregation::Max => "MAX",
        }
    }
}

/// Filter constraint for metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConstraint {
    pub column: String,
    pub operator: String,
    pub value: serde_json::Value,
    pub required: bool,
}

/// Metric access policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPolicy {
    pub allowed_roles: Vec<String>,
    pub max_time_range_days: Option<u32>,
    pub row_limit: Option<u32>,
    pub max_dimensions: Option<u32>,
}

impl Default for MetricPolicy {
    fn default() -> Self {
        Self {
            allowed_roles: vec!["public".to_string()],
            max_time_range_days: None,
            row_limit: None,
            max_dimensions: None,
        }
    }
}

/// Trait for metric definitions
pub trait MetricDefinition: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn base_table(&self) -> &str;
    fn grain(&self) -> TimeGrain;
    fn sql_expression(&self) -> &str;
    fn allowed_dimensions(&self) -> &[String];
    fn required_filters(&self) -> &[FilterConstraint];
    fn policy(&self) -> &MetricPolicy;
    fn aggregation(&self) -> Aggregation;
}

/// Semantic metric implementation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticMetric {
    pub name: String,
    pub description: String,
    pub aggregation: Aggregation,
    pub base_table: String,
    pub grain: TimeGrain,
    pub sql_expression: String,
    pub allowed_dimensions: Vec<String>,
    pub required_filters: Vec<FilterConstraint>,
    pub policy: MetricPolicy,
}

impl MetricDefinition for SemanticMetric {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn base_table(&self) -> &str {
        &self.base_table
    }

    fn grain(&self) -> TimeGrain {
        self.grain
    }

    fn sql_expression(&self) -> &str {
        &self.sql_expression
    }

    fn allowed_dimensions(&self) -> &[String] {
        &self.allowed_dimensions
    }

    fn required_filters(&self) -> &[FilterConstraint] {
        &self.required_filters
    }

    fn policy(&self) -> &MetricPolicy {
        &self.policy
    }

    fn aggregation(&self) -> Aggregation {
        self.aggregation
    }
}

impl SemanticMetric {
    pub fn new(
        name: String,
        description: String,
        aggregation: Aggregation,
        base_table: String,
        grain: TimeGrain,
        sql_expression: String,
    ) -> Self {
        Self {
            name,
            description,
            aggregation,
            base_table,
            grain,
            sql_expression,
            allowed_dimensions: Vec::new(),
            required_filters: Vec::new(),
            policy: MetricPolicy::default(),
        }
    }

    pub fn with_allowed_dimensions(mut self, dimensions: Vec<String>) -> Self {
        self.allowed_dimensions = dimensions;
        self
    }

    pub fn with_required_filters(mut self, filters: Vec<FilterConstraint>) -> Self {
        self.required_filters = filters;
        self
    }

    pub fn with_policy(mut self, policy: MetricPolicy) -> Self {
        self.policy = policy;
        self
    }
}





