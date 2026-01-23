//! Semantic Registry Loader
//! 
//! Loads semantic registry from JSON configuration files.

use crate::error::{RcaError, Result};
use crate::semantic::dimension::{DimensionType, SemanticDimension};
use crate::semantic::join_graph::JoinType;
use crate::semantic::metric::{Aggregation, SemanticMetric, TimeGrain};
use crate::semantic::registry::{InMemorySemanticRegistry, SemanticRegistry};
use crate::semantic::join_graph::JoinEdge;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// JSON representation of a semantic metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricJson {
    pub name: String,
    pub description: String,
    pub aggregation: String, // "sum", "avg", "count", "count_distinct", "min", "max"
    pub base_table: String,
    pub grain: String, // "none", "day", "week", "month", "quarter", "year"
    pub sql_expression: String,
    pub allowed_dimensions: Vec<String>,
    pub required_filters: Option<Vec<FilterJson>>,
    pub policy: Option<PolicyJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterJson {
    pub column: String,
    pub operator: String,
    pub value: serde_json::Value,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyJson {
    pub allowed_roles: Vec<String>,
    pub max_time_range_days: Option<u32>,
    pub row_limit: Option<u32>,
    pub max_dimensions: Option<u32>,
}

/// JSON representation of a semantic dimension
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionJson {
    pub name: String,
    pub description: String,
    pub base_table: String,
    pub column: String,
    pub data_type: String, // "string", "integer", "decimal", "date", "boolean", "enum"
    pub join_path: Vec<JoinEdgeJson>,
    /// Optional SQL expression to use instead of simple column reference
    /// If provided, this expression will be used in SELECT clause (e.g., CASE statements, literals)
    #[serde(default)]
    pub sql_expression: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinEdgeJson {
    pub from_table: String,
    pub to_table: String,
    pub on: String,
    /// Join type - DEPRECATED: Should not be set by LLM/JSON.
    /// The compiler will determine join type based on dimension usage + metadata.
    /// Kept for backward compatibility only.
    #[serde(default)]
    pub join_type: Option<String>, // Optional: "inner", "left", "right", "full". If not specified, defaults to LEFT
    
    /// Cardinality of the relationship (authoritative metadata)
    /// Options: "one_to_one", "many_to_one", "one_to_many", "many_to_many"
    /// Defaults to "many_to_one" (most common FK relationship)
    #[serde(default)]
    pub cardinality: Option<String>,
    
    /// Can the right side (to_table) be missing?
    /// If true, LEFT JOIN is appropriate for augmentation.
    /// If false, INNER JOIN is appropriate.
    /// Defaults to true (most relationships are optional)
    #[serde(default)]
    pub optional: Option<bool>,
    
    /// Is this join safe from fan-out (row duplication)?
    /// If false, compiler must apply fan-out protection.
    /// If not specified, computed from cardinality
    #[serde(default)]
    pub fan_out_safe: Option<bool>,
}

/// Semantic registry JSON structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticRegistryJson {
    pub metrics: Vec<MetricJson>,
    pub dimensions: Vec<DimensionJson>,
}

/// Load semantic registry from JSON
pub fn load_from_json(json_str: &str) -> Result<Arc<dyn SemanticRegistry>> {
    let registry_json: SemanticRegistryJson = serde_json::from_str(json_str)
        .map_err(|e| RcaError::Metadata(format!("Failed to parse semantic registry JSON: {}", e)))?;

    let mut registry = InMemorySemanticRegistry::new();

    // Load metrics
    for metric_json in registry_json.metrics {
        let aggregation = match metric_json.aggregation.as_str() {
            "sum" => Aggregation::Sum,
            "avg" => Aggregation::Avg,
            "count" => Aggregation::Count,
            "count_distinct" => Aggregation::CountDistinct,
            "min" => Aggregation::Min,
            "max" => Aggregation::Max,
            _ => return Err(RcaError::Metadata(format!(
                "Unknown aggregation type: {}",
                metric_json.aggregation
            ))),
        };

        let grain = match metric_json.grain.as_str() {
            "none" => TimeGrain::None,
            "day" => TimeGrain::Day,
            "week" => TimeGrain::Week,
            "month" => TimeGrain::Month,
            "quarter" => TimeGrain::Quarter,
            "year" => TimeGrain::Year,
            _ => return Err(RcaError::Metadata(format!(
                "Unknown time grain: {}",
                metric_json.grain
            ))),
        };

        let mut metric = SemanticMetric::new(
            metric_json.name.clone(),
            metric_json.description,
            aggregation,
            metric_json.base_table,
            grain,
            metric_json.sql_expression,
        )
        .with_allowed_dimensions(metric_json.allowed_dimensions);

        if let Some(filters) = metric_json.required_filters {
            let filter_constraints: Vec<_> = filters
                .into_iter()
                .map(|f| crate::semantic::metric::FilterConstraint {
                    column: f.column,
                    operator: f.operator,
                    value: f.value,
                    required: f.required,
                })
                .collect();
            metric = metric.with_required_filters(filter_constraints);
        }

        if let Some(policy_json) = metric_json.policy {
            let policy = crate::semantic::metric::MetricPolicy {
                allowed_roles: policy_json.allowed_roles,
                max_time_range_days: policy_json.max_time_range_days,
                row_limit: policy_json.row_limit,
                max_dimensions: policy_json.max_dimensions,
            };
            metric = metric.with_policy(policy);
        }

        registry.register_metric(Arc::new(metric));
    }

    // Load dimensions
    for dimension_json in registry_json.dimensions {
        let data_type = match dimension_json.data_type.as_str() {
            "string" => DimensionType::String,
            "integer" => DimensionType::Integer,
            "decimal" => DimensionType::Decimal,
            "date" => DimensionType::Date,
            "boolean" => DimensionType::Boolean,
            "enum" => DimensionType::Enum,
            _ => return Err(RcaError::Metadata(format!(
                "Unknown data type: {}",
                dimension_json.data_type
            ))),
        };

        let join_path: Vec<JoinEdge> = dimension_json
            .join_path
            .into_iter()
            .map(|j| {
                // Parse cardinality
                let cardinality = j.cardinality.as_deref()
                    .and_then(|c| match c.to_lowercase().as_str() {
                        "one_to_one" | "1:1" => Some(crate::semantic::join_graph::Cardinality::OneToOne),
                        "many_to_one" | "n:1" | "many-to-one" => Some(crate::semantic::join_graph::Cardinality::ManyToOne),
                        "one_to_many" | "1:n" | "one-to-many" => Some(crate::semantic::join_graph::Cardinality::OneToMany),
                        "many_to_many" | "n:n" | "many-to-many" => Some(crate::semantic::join_graph::Cardinality::ManyToMany),
                        _ => None,
                    })
                    .unwrap_or(crate::semantic::join_graph::Cardinality::ManyToOne); // Default
                
                // Parse optionality (defaults to true)
                let optional = j.optional.unwrap_or(true);
                
                // Parse fan_out_safe (if not specified, computed from cardinality)
                let fan_out_safe = j.fan_out_safe.or_else(|| {
                    Some(matches!(cardinality, 
                        crate::semantic::join_graph::Cardinality::OneToOne | 
                        crate::semantic::join_graph::Cardinality::ManyToOne))
                });
                
                // Create join edge with metadata
                // Note: join_type will be determined by compiler, not from JSON
                // We still parse it for backward compatibility, but it should be ignored
                let mut join = JoinEdge::with_metadata(
                    j.from_table,
                    j.to_table,
                    j.on,
                    cardinality,
                    optional,
                );
                
                // Set fan_out_safe if explicitly provided
                if let Some(safe) = fan_out_safe {
                    join.fan_out_safe = Some(safe);
                }
                
                // Legacy: Parse join_type for backward compatibility (but compiler will override)
                if let Some(ref jt) = j.join_type {
                    join.join_type = match jt.as_str() {
                        "inner" => JoinType::Inner,
                        "left" => JoinType::Left,
                        "right" => JoinType::Right,
                        "full" => JoinType::Full,
                        _ => JoinType::Left, // Default
                    };
                }
                
                Ok(join)
            })
            .collect::<Result<Vec<_>>>()?;

        let dimension = SemanticDimension::new(
            dimension_json.name.clone(),
            dimension_json.description,
            dimension_json.base_table,
            dimension_json.column,
            data_type,
        )
        .with_join_path(join_path)
        .with_sql_expression(dimension_json.sql_expression);

        registry.register_dimension(Arc::new(dimension));
    }

    Ok(Arc::new(registry))
}

/// Load semantic registry from file
pub fn load_from_file(path: &str) -> Result<Arc<dyn SemanticRegistry>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| RcaError::Metadata(format!("Failed to read semantic registry file: {}", e)))?;
    load_from_json(&contents)
}

