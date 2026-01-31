//! Error Recovery
//! 
//! Builds recovery prompts from error classifications.

use crate::error::Result;
use crate::execution_loop::error_classifier::SqlErrorClass;
use crate::intent::SemanticSqlIntent;
use crate::semantic::registry::SemanticRegistry;
use std::sync::Arc;

/// Error recovery prompt builder
pub struct ErrorRecovery {
    semantic_registry: Arc<dyn SemanticRegistry>,
}

impl ErrorRecovery {
    pub fn new(semantic_registry: Arc<dyn SemanticRegistry>) -> Self {
        Self { semantic_registry }
    }

    /// Build a recovery prompt from an error classification
    pub fn build_recovery_prompt(
        &self,
        error_class: &SqlErrorClass,
        previous_intent: &SemanticSqlIntent,
        attempt: u8,
    ) -> String {
        let mut prompt_parts = Vec::new();

        prompt_parts.push(format!(
            "⚠️ RETRY ATTEMPT {}: Your previous intent failed with the following error:",
            attempt
        ));

        match error_class {
            SqlErrorClass::MetricNotFound => {
                prompt_parts.push("The metric you referenced does not exist in the semantic registry.".to_string());
                prompt_parts.push(format!(
                    "Available metrics: {}",
                    self.semantic_registry.list_metrics().join(", ")
                ));
                prompt_parts.push("Please regenerate the intent using only valid metric names.".to_string());
            }
            SqlErrorClass::DimensionNotAllowed => {
                prompt_parts.push("One or more dimensions you specified are not allowed for the selected metric(s).".to_string());
                if let Some(first_metric) = previous_intent.metrics.first() {
                    if let Some(metric) = self.semantic_registry.metric(first_metric) {
                        prompt_parts.push(format!(
                            "For metric '{}', allowed dimensions are: {}",
                            first_metric,
                            metric.allowed_dimensions().join(", ")
                        ));
                    }
                }
                prompt_parts.push("Please adjust the dimensions in your intent.".to_string());
            }
            SqlErrorClass::ColumnNotFound => {
                prompt_parts.push("A column referenced in your intent does not exist.".to_string());
                prompt_parts.push("Please review the schema and use only valid column names.".to_string());
            }
            SqlErrorClass::TableNotFound => {
                prompt_parts.push("A table referenced in your intent does not exist.".to_string());
                prompt_parts.push("Please review the available tables and regenerate the intent.".to_string());
            }
            SqlErrorClass::AmbiguousColumn => {
                prompt_parts.push("A column reference is ambiguous (exists in multiple tables).".to_string());
                prompt_parts.push("Please specify the table name along with the column name.".to_string());
            }
            SqlErrorClass::InvalidAggregation => {
                prompt_parts.push("The aggregation function you specified is invalid for this metric.".to_string());
                prompt_parts.push("Please use the metric's defined aggregation type.".to_string());
            }
            SqlErrorClass::TimeGrainMismatch => {
                prompt_parts.push("The time grain you specified does not match the metric's required grain.".to_string());
                if let Some(first_metric) = previous_intent.metrics.first() {
                    if let Some(metric) = self.semantic_registry.metric(first_metric) {
                        prompt_parts.push(format!(
                            "Metric '{}' requires time grain: {}",
                            first_metric,
                            metric.grain().as_str()
                        ));
                    }
                }
            }
            SqlErrorClass::JoinPathFailure => {
                prompt_parts.push("The join path from the metric's base table to the requested dimensions cannot be resolved.".to_string());
                prompt_parts.push("Please use only dimensions that have valid join paths from the metric's base table.".to_string());
            }
            SqlErrorClass::ExecutionError(msg) | SqlErrorClass::CompilerError(msg) => {
                prompt_parts.push(format!("Error details: {}", msg));
                prompt_parts.push("Please review your intent and fix any issues.".to_string());
            }
        }

        prompt_parts.push("\nPrevious intent summary:".to_string());
        prompt_parts.push(format!("Metrics: {:?}", previous_intent.metrics));
        prompt_parts.push(format!("Dimensions: {:?}", previous_intent.dimensions));
        if !previous_intent.filters.is_empty() {
            prompt_parts.push(format!("Filters: {:?}", previous_intent.filters));
        }

        prompt_parts.push("\nPlease regenerate the intent with the corrections above.".to_string());

        prompt_parts.join("\n")
    }
}





