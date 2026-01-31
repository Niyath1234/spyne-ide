//! Root Cause Attribution
//! 
//! Explains row differences by combining join traces, filter traces, and rule traces.
//! Produces structured explanations that can be converted to human-readable narratives.

use crate::core::engine::row_diff::{RowDiffResult, DiffSummary};
use crate::core::lineage::{
    JoinTrace, JoinTraceCollection,
    FilterDecision, FilterTraceCollection,
    RuleExecution, RuleTraceCollection,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Root cause explanation for a row difference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowExplanation {
    /// Row identifier
    pub row_id: Vec<String>,
    
    /// Type of difference
    pub difference_type: DifferenceType,
    
    /// Explanations from different sources
    pub explanations: Vec<ExplanationItem>,
    
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
}

/// Type of difference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DifferenceType {
    /// Row only exists in left
    MissingInRight,
    
    /// Row only exists in right
    MissingInLeft,
    
    /// Row exists in both but values differ
    ValueMismatch,
    
    /// Row matches exactly
    Match,
}

/// Individual explanation item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplanationItem {
    /// Source of explanation (join, filter, rule)
    pub source: ExplanationSource,
    
    /// Explanation text
    pub explanation: String,
    
    /// Evidence (specific values, conditions, etc.)
    pub evidence: HashMap<String, String>,
}

/// Source of explanation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExplanationSource {
    Join,
    Filter,
    Rule,
    DataQuality,
}

/// Root cause attribution engine
pub struct AttributionEngine;

impl AttributionEngine {
    /// Explain a row difference
    /// 
    /// Combines traces from joins, filters, and rules to explain why a row
    /// differs between left and right.
    pub fn explain_row(
        &self,
        row_id: &[String],
        diff_type: &DifferenceType,
        join_traces: &JoinTraceCollection,
        filter_traces: &FilterTraceCollection,
        rule_traces: &RuleTraceCollection,
    ) -> RowExplanation {
        let mut explanations = Vec::new();
        
        // Check join traces
        let join_traces_for_row = join_traces.get_traces(row_id);
        for trace in join_traces_for_row {
            if !trace.succeeded {
                explanations.push(ExplanationItem {
                    source: ExplanationSource::Join,
                    explanation: format!(
                        "Row dropped due to failed join: {} -> {} on {}",
                        trace.table_from,
                        trace.table_to,
                        trace.join_condition.join(", ")
                    ),
                    evidence: {
                        let mut ev = HashMap::new();
                        ev.insert("table_from".to_string(), trace.table_from.clone());
                        ev.insert("table_to".to_string(), trace.table_to.clone());
                        ev.insert("join_type".to_string(), trace.join_type.clone());
                        if let Some(ref reason) = trace.failure_reason {
                            ev.insert("failure_reason".to_string(), reason.clone());
                        }
                        ev
                    },
                });
            }
        }
        
        // Check filter traces
        let filter_decisions = filter_traces.get_decisions(row_id);
        for decision in filter_decisions {
            if !decision.passed {
                explanations.push(ExplanationItem {
                    source: ExplanationSource::Filter,
                    explanation: format!(
                        "Row dropped due to filter: {}",
                        decision.filter_expr
                    ),
                    evidence: {
                        let mut ev = HashMap::new();
                        ev.insert("filter_expr".to_string(), decision.filter_expr.clone());
                        if let Some(ref desc) = decision.description {
                            ev.insert("description".to_string(), desc.clone());
                        }
                        if let Some(ref values) = decision.failure_values {
                            for (k, v) in values {
                                ev.insert(format!("value_{}", k), v.clone());
                            }
                        }
                        ev
                    },
                });
            }
        }
        
        // Check rule traces
        let rule_executions = rule_traces.get_executions(row_id);
        for execution in rule_executions {
            if execution.fired {
                explanations.push(ExplanationItem {
                    source: ExplanationSource::Rule,
                    explanation: format!(
                        "Row transformed by rule {}: {}",
                        execution.rule_id,
                        execution.change_description.as_ref()
                            .unwrap_or(&"values changed".to_string())
                    ),
                    evidence: {
                        let mut ev = HashMap::new();
                        ev.insert("rule_id".to_string(), execution.rule_id.clone());
                        for (k, v) in &execution.input_values {
                            ev.insert(format!("input_{}", k), v.clone());
                        }
                        for (k, v) in &execution.output_values {
                            ev.insert(format!("output_{}", k), v.clone());
                        }
                        ev
                    },
                });
            }
        }
        
        // Calculate confidence based on number and quality of explanations
        let confidence = self.calculate_confidence(&explanations, diff_type);
        
        RowExplanation {
            row_id: row_id.to_vec(),
            difference_type: diff_type.clone(),
            explanations,
            confidence,
        }
    }
    
    /// Explain all differences from a diff result
    /// 
    /// Produces explanations for all rows in missing_left, missing_right, and value_mismatch
    pub fn explain_all_differences(
        &self,
        diff_result: &RowDiffResult,
        join_traces_left: &JoinTraceCollection,
        filter_traces_left: &FilterTraceCollection,
        rule_traces_left: &RuleTraceCollection,
        join_traces_right: &JoinTraceCollection,
        filter_traces_right: &FilterTraceCollection,
        rule_traces_right: &RuleTraceCollection,
    ) -> Vec<RowExplanation> {
        let mut explanations = Vec::new();
        
        // Explain missing_left rows
        for row_idx in 0..diff_result.missing_left.height() {
            let row_id = self.extract_row_id(&diff_result.missing_left, row_idx);
            let explanation = self.explain_row(
                &row_id,
                &DifferenceType::MissingInLeft,
                join_traces_left,
                filter_traces_left,
                rule_traces_left,
            );
            explanations.push(explanation);
        }
        
        // Explain missing_right rows
        for row_idx in 0..diff_result.missing_right.height() {
            let row_id = self.extract_row_id(&diff_result.missing_right, row_idx);
            let explanation = self.explain_row(
                &row_id,
                &DifferenceType::MissingInRight,
                join_traces_right,
                filter_traces_right,
                rule_traces_right,
            );
            explanations.push(explanation);
        }
        
        // Explain value mismatches
        for row_idx in 0..diff_result.value_mismatch.height() {
            let row_id = self.extract_row_id(&diff_result.value_mismatch, row_idx);
            // Combine traces from both sides for value mismatches
            let mut combined_explanations = Vec::new();
            
            // Left side explanations
            let left_explanation = self.explain_row(
                &row_id,
                &DifferenceType::ValueMismatch,
                join_traces_left,
                filter_traces_left,
                rule_traces_left,
            );
            combined_explanations.extend(left_explanation.explanations);
            
            // Right side explanations
            let right_explanation = self.explain_row(
                &row_id,
                &DifferenceType::ValueMismatch,
                join_traces_right,
                filter_traces_right,
                rule_traces_right,
            );
            combined_explanations.extend(right_explanation.explanations);
            
            explanations.push(RowExplanation {
                row_id,
                difference_type: DifferenceType::ValueMismatch,
                explanations: combined_explanations,
                confidence: (left_explanation.confidence + right_explanation.confidence) / 2.0,
            });
        }
        
        explanations
    }
    
    /// Extract row ID from dataframe
    fn extract_row_id(&self, df: &polars::prelude::DataFrame, row_idx: usize) -> Vec<String> {
        // This is simplified - in practice would use actual key columns
        // For now, try to extract from first few columns
        let mut row_id = Vec::new();
        let col_names = df.get_column_names();
        
        for col_name in col_names.iter().take(3) {
            if let Ok(col_series) = df.column(col_name) {
                let val_str = match col_series.dtype() {
                    polars::prelude::DataType::String => {
                        col_series.str().ok()
                            .and_then(|s| s.get(row_idx))
                            .unwrap_or("")
                            .to_string()
                    }
                    polars::prelude::DataType::Int64 => {
                        col_series.i64().ok()
                            .and_then(|s| s.get(row_idx))
                            .map(|v| v.to_string())
                            .unwrap_or_default()
                    }
                    _ => format!("{:?}", col_series.get(row_idx)),
                };
                row_id.push(val_str);
            }
        }
        
        row_id
    }
    
    /// Calculate confidence score
    fn calculate_confidence(&self, explanations: &[ExplanationItem], diff_type: &DifferenceType) -> f64 {
        if explanations.is_empty() {
            return 0.3; // Low confidence if no explanations
        }
        
        let mut score = 0.5; // Base score
        
        // Boost confidence if we have multiple sources
        let source_count = explanations.len();
        score += (source_count as f64 * 0.1).min(0.3);
        
        // Boost confidence if explanations have evidence
        let has_evidence: usize = explanations.iter()
            .map(|e| if e.evidence.is_empty() { 0 } else { 1 })
            .sum();
        score += (has_evidence as f64 / explanations.len() as f64) * 0.2;
        
        score.min(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_explain_row() {
        let engine = AttributionEngine;
        let join_traces = JoinTraceCollection::new();
        let filter_traces = FilterTraceCollection::new();
        let rule_traces = RuleTraceCollection::new();
        
        let explanation = engine.explain_row(
            &["1".to_string()],
            &DifferenceType::MissingInRight,
            &join_traces,
            &filter_traces,
            &rule_traces,
        );
        
        assert_eq!(explanation.row_id, vec!["1".to_string()]);
    }
}

