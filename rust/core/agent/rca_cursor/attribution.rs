//! Grain-Level Attribution Engine
//! 
//! Traces backward from grain_key to contributing base rows, joined tables,
//! and filters with impact scoring.

use crate::core::agent::rca_cursor::{executor::ExecutionResult, diff::GrainDiffResult};
use crate::core::rca::result_v2::{Attribution, RowRef};
use crate::error::{RcaError, Result};
use polars::prelude::*;
use std::collections::HashMap;

/// Grain-level attribution engine
pub struct GrainAttributionEngine {
    /// Maximum number of contributors to track per grain unit
    max_contributors: usize,
}

impl GrainAttributionEngine {
    /// Create a new grain attribution engine
    pub fn new(max_contributors: usize) -> Self {
        Self { max_contributors }
    }

    /// Compute attributions for top differences
    /// 
    /// For each top grain_key difference, traces backward to find:
    /// - Contributing base rows
    /// - Joined tables that contributed
    /// - Filters that were applied
    /// - Impact scoring and percentage contribution
    pub fn compute_attributions(
        &self,
        diff_result: &GrainDiffResult,
        result_a: &ExecutionResult,
        result_b: &ExecutionResult,
        metric_column: &str,
    ) -> Result<Vec<Attribution>> {
        let mut attributions = Vec::new();

        for difference in &diff_result.differences {
            // Get grain value
            let grain_value = &difference.grain_value;

            // Find the row in both systems for this grain_key
            let attribution_a = self.attribute_grain_unit(
                result_a,
                grain_value,
                &diff_result.grain_key,
                metric_column,
                difference.value_a,
            )?;

            let attribution_b = self.attribute_grain_unit(
                result_b,
                grain_value,
                &diff_result.grain_key,
                metric_column,
                difference.value_b,
            )?;

            // Combine attributions from both systems
            // For now, we'll focus on the system with the larger absolute value
            let attribution = if difference.value_a.abs() > difference.value_b.abs() {
                attribution_a
            } else {
                attribution_b
            };

            // Calculate contribution percentage
            let total_impact: f64 = diff_result.differences.iter()
                .map(|d| d.impact)
                .sum();

            let contribution_percentage = if total_impact > 0.0 {
                (difference.impact / total_impact) * 100.0
            } else {
                0.0
            };

            attributions.push(Attribution {
                grain_value: grain_value.clone(),
                impact: difference.impact,
                contribution_percentage,
                contributors: attribution.contributors,
                explanation_graph: attribution.explanation_graph,
            });
        }

        Ok(attributions)
    }

    /// Attribute a single grain unit to its contributing rows
    fn attribute_grain_unit(
        &self,
        result: &ExecutionResult,
        grain_value: &[String],
        grain_key: &str,
        metric_column: &str,
        metric_value: f64,
    ) -> Result<Attribution> {
        // Find the row for this grain_key
        let df = &result.dataframe;
        
        // Filter dataframe to find the row with this grain_key
        let grain_key_value = grain_value.first()
            .ok_or_else(|| RcaError::Execution("Empty grain value".to_string()))?;

        // Convert grain_key_value to appropriate type for filtering
        let filtered_df = df
            .clone()
            .lazy()
            .filter(col(grain_key).eq(lit(grain_key_value.clone())))
            .collect()?;

        if filtered_df.height() == 0 {
            return Ok(Attribution {
                grain_value: grain_value.to_vec(),
                impact: 0.0,
                contribution_percentage: 0.0,
                contributors: Vec::new(),
                explanation_graph: HashMap::new(),
            });
        }

        // Extract contributing rows
        // For now, we'll create a simplified attribution
        // In a full implementation, we'd trace back through joins and filters
        let mut contributors = Vec::new();

        // Create a row reference for the aggregated row
        contributors.push(RowRef {
            table: "aggregated".to_string(),
            row_id: grain_value.to_vec(),
            contribution: metric_value,
        });

        // Limit contributors
        contributors.truncate(self.max_contributors);

        // Build explanation graph (simplified)
        let mut explanation_graph = HashMap::new();
        explanation_graph.insert("grain_key".to_string(), serde_json::Value::String(grain_key.to_string()));
        explanation_graph.insert("metric_value".to_string(), serde_json::Value::Number(
            serde_json::Number::from_f64(metric_value).unwrap_or(serde_json::Number::from(0))
        ));
        explanation_graph.insert("row_count".to_string(), serde_json::Value::Number(
            serde_json::Number::from(filtered_df.height())
        ));

        Ok(Attribution {
            grain_value: grain_value.to_vec(),
            impact: metric_value.abs(),
            contribution_percentage: 0.0, // Will be calculated by caller
            contributors,
            explanation_graph,
        })
    }

    /// Trace backward through joins to find contributing base rows
    /// 
    /// This is a placeholder for future implementation that would:
    /// 1. Track join lineage during execution
    /// 2. Map aggregated rows back to contributing base rows
    /// 3. Calculate contribution of each base row
    fn trace_join_lineage(
        &self,
        _result: &ExecutionResult,
        _grain_value: &[String],
    ) -> Result<Vec<RowRef>> {
        // TODO: Implement join lineage tracing
        // This would require storing join metadata during execution
        Ok(Vec::new())
    }

    /// Calculate impact score for a grain unit
    /// 
    /// Impact is the absolute value of the difference, weighted by:
    /// - The magnitude of the difference
    /// - The relative importance of the grain unit
    fn calculate_impact(
        &self,
        value_a: f64,
        value_b: f64,
        total_a: f64,
        total_b: f64,
    ) -> f64 {
        let delta = value_b - value_a;
        let abs_delta = delta.abs();

        // Weight by relative contribution if totals are available
        let weight = if total_a.abs() > 1e-10 {
            (value_a.abs() / total_a.abs()).min(1.0)
        } else {
            1.0
        };

        abs_delta * weight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribution_engine_creation() {
        // Test would require mock execution results and diff results
    }
}





