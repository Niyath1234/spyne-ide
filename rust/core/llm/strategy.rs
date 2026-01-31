//! LLM Strategy Layer
//! 
//! Provides LLM-guided metric and pipeline selection for RCA tasks.
//! Uses LLM reasoning to:
//! - Select optimal metrics for analysis
//! - Choose best pipeline/rule combinations
//! - Prioritize investigation paths
//! - Suggest drilldown strategies

use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use crate::metadata::{Metadata, Rule};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, warn};

/// Strategy for metric/pipeline selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricStrategy {
    /// Selected metric
    pub metric: String,
    
    /// Selected rules (left and right)
    pub left_rule_id: String,
    pub right_rule_id: String,
    
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    
    /// Reasoning for selection
    pub reasoning: String,
    
    /// Alternative strategies considered
    pub alternatives: Vec<AlternativeStrategy>,
}

/// Alternative strategy that was considered
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlternativeStrategy {
    pub metric: String,
    pub left_rule_id: String,
    pub right_rule_id: String,
    pub confidence: f64,
    pub reasoning: String,
}

/// Drilldown strategy for narrowing investigation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrilldownStrategy {
    /// Suggested drilldown dimensions
    pub dimensions: Vec<DrilldownDimension>,
    
    /// Priority order (higher = more important)
    pub priority_order: Vec<String>,
    
    /// Reasoning for drilldown approach
    pub reasoning: String,
    
    /// Expected insights from drilldown
    pub expected_insights: Vec<String>,
}

/// Dimension for drilldown analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrilldownDimension {
    /// Dimension name (e.g., "date", "product_type", "region")
    pub name: String,
    
    /// Column name in the data
    pub column: String,
    
    /// Why this dimension is important
    pub importance: String,
    
    /// Suggested granularity (e.g., "daily", "monthly", "by_category")
    pub granularity: Option<String>,
}

/// Investigation path suggestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvestigationPath {
    /// Path name/description
    pub name: String,
    
    /// Steps in this path
    pub steps: Vec<InvestigationStep>,
    
    /// Expected outcome
    pub expected_outcome: String,
    
    /// Priority (higher = more important)
    pub priority: f64,
}

/// Step in investigation path
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvestigationStep {
    /// Step description
    pub description: String,
    
    /// Action to take
    pub action: String,
    
    /// What to look for
    pub look_for: String,
}

/// LLM Strategy Engine
/// 
/// Uses LLM to guide metric selection, pipeline selection, and drilldown strategies
pub struct LlmStrategyEngine {
    llm: LlmClient,
    metadata: Metadata,
}

impl LlmStrategyEngine {
    /// Create a new LLM strategy engine
    pub fn new(llm: LlmClient, metadata: Metadata) -> Self {
        Self { llm, metadata }
    }
    
    /// Select optimal metric and pipeline for RCA
    /// 
    /// Given a problem description and available rules, uses LLM to select
    /// the best metric and rule combination for investigation.
    pub async fn select_metric_strategy(
        &self,
        problem_description: &str,
        system_a: &str,
        system_b: &str,
        available_rules: &[&Rule],
    ) -> Result<MetricStrategy> {
        info!("🤖 LLM selecting metric strategy for: {}", problem_description);
        
        // Build context about available rules
        let rules_context: Vec<_> = available_rules.iter().map(|r| {
            serde_json::json!({
                "id": r.id,
                "metric": r.metric,
                "system": r.system,
                "description": r.computation.description,
                "formula": r.computation.formula,
                "source_entities": r.computation.source_entities,
                "target_entity": r.target_entity,
                "target_grain": r.target_grain,
            })
        }).collect();
        
        let prompt = format!(
            r#"You are a data analyst selecting the optimal metric and pipeline for root cause analysis.

PROBLEM: {}
SYSTEM A: {}
SYSTEM B: {}

AVAILABLE RULES:
{}

TASK:
1. Analyze the problem description and identify which metric is most relevant
2. Select the best rule from System A and System B for this metric
3. Consider:
   - Which metric best addresses the problem
   - Which rules are most appropriate for comparison
   - Rule complexity and data availability
   - Expected accuracy of comparison

Return JSON:
{{
  "metric": "metric_name",
  "left_rule_id": "rule_id_for_system_a",
  "right_rule_id": "rule_id_for_system_b",
  "confidence": 0.0-1.0,
  "reasoning": "Why this selection is optimal",
  "alternatives": [
    {{
      "metric": "alternative_metric",
      "left_rule_id": "alt_rule_id",
      "right_rule_id": "alt_rule_id",
      "confidence": 0.0-1.0,
      "reasoning": "Why this alternative was considered"
    }}
  ]
}}
"#,
            problem_description,
            system_a,
            system_b,
            serde_json::to_string_pretty(&rules_context)?
        );
        
        let response = self.llm.call_llm(&prompt).await?;
        let cleaned = self.extract_json_from_response(&response);
        
        let strategy: MetricStrategy = serde_json::from_str(&cleaned)
            .map_err(|e| RcaError::Llm(format!("Failed to parse metric strategy: {}. Response: {}", e, cleaned)))?;
        
        info!("✅ Selected metric strategy: {} (confidence: {:.2})", strategy.metric, strategy.confidence);
        
        Ok(strategy)
    }
    
    /// Generate drilldown strategy
    /// 
    /// Given RCA results, suggests how to drill down to find root causes.
    pub async fn generate_drilldown_strategy(
        &self,
        problem_description: &str,
        metric: &str,
        rca_summary: &RcaSummary,
        available_columns: &[String],
    ) -> Result<DrilldownStrategy> {
        info!("🤖 LLM generating drilldown strategy for metric: {}", metric);
        
        let prompt = format!(
            r#"You are a data analyst suggesting drilldown strategies for root cause analysis.

PROBLEM: {}
METRIC: {}

RCA SUMMARY:
- Total rows analyzed: {}
- Missing in left: {}
- Missing in right: {}
- Value mismatches: {}
- Aggregate mismatch: {}

AVAILABLE COLUMNS FOR DRILLDOWN:
{}

TASK:
Suggest dimensions to drill down on to identify root causes. Consider:
- Which dimensions are most likely to reveal patterns
- Temporal patterns (date, time)
- Categorical patterns (product type, region, customer segment)
- Hierarchical patterns (aggregate -> detail)

Return JSON:
{{
  "dimensions": [
    {{
      "name": "dimension_name",
      "column": "column_name",
      "importance": "Why this dimension matters",
      "granularity": "suggested_granularity" | null
    }}
  ],
  "priority_order": ["dimension1", "dimension2", ...],
  "reasoning": "Overall strategy reasoning",
  "expected_insights": [
    "What we expect to learn from this drilldown",
    ...
  ]
}}
"#,
            problem_description,
            metric,
            rca_summary.total_rows,
            rca_summary.missing_left_count,
            rca_summary.missing_right_count,
            rca_summary.mismatch_count,
            rca_summary.aggregate_mismatch,
            available_columns.join(", ")
        );
        
        let response = self.llm.call_llm(&prompt).await?;
        let cleaned = self.extract_json_from_response(&response);
        
        let strategy: DrilldownStrategy = serde_json::from_str(&cleaned)
            .map_err(|e| RcaError::Llm(format!("Failed to parse drilldown strategy: {}. Response: {}", e, cleaned)))?;
        
        info!("✅ Generated drilldown strategy with {} dimensions", strategy.dimensions.len());
        
        Ok(strategy)
    }
    
    /// Suggest investigation paths
    /// 
    /// Given RCA results, suggests multiple investigation paths to explore.
    pub async fn suggest_investigation_paths(
        &self,
        problem_description: &str,
        rca_summary: &RcaSummary,
        explanations: &[String],
    ) -> Result<Vec<InvestigationPath>> {
        info!("🤖 LLM suggesting investigation paths");
        
        let explanations_text = if explanations.is_empty() {
            "No specific explanations available yet".to_string()
        } else {
            explanations.iter().enumerate()
                .map(|(i, e)| format!("{}. {}", i + 1, e))
                .collect::<Vec<_>>()
                .join("\n")
        };
        
        let prompt = format!(
            r#"You are a data analyst suggesting investigation paths for root cause analysis.

PROBLEM: {}

RCA SUMMARY:
- Total rows analyzed: {}
- Missing in left: {}
- Missing in right: {}
- Value mismatches: {}
- Aggregate mismatch: {}

CURRENT EXPLANATIONS:
{}

TASK:
Suggest 2-4 investigation paths to explore. Each path should:
- Have a clear objective
- Include specific steps to take
- Indicate what to look for at each step
- Have a priority based on likelihood of finding root cause

Return JSON array:
[
  {{
    "name": "Path name",
    "steps": [
      {{
        "description": "Step description",
        "action": "What to do",
        "look_for": "What to look for"
      }}
    ],
    "expected_outcome": "What we expect to find",
    "priority": 0.0-1.0
  }}
]
"#,
            problem_description,
            rca_summary.total_rows,
            rca_summary.missing_left_count,
            rca_summary.missing_right_count,
            rca_summary.mismatch_count,
            rca_summary.aggregate_mismatch,
            explanations_text
        );
        
        let response = self.llm.call_llm(&prompt).await?;
        let cleaned = self.extract_json_from_response(&response);
        
        let paths: Vec<InvestigationPath> = serde_json::from_str(&cleaned)
            .map_err(|e| RcaError::Llm(format!("Failed to parse investigation paths: {}. Response: {}", e, cleaned)))?;
        
        info!("✅ Suggested {} investigation paths", paths.len());
        
        Ok(paths)
    }
    
    /// Extract JSON from LLM response (handles markdown code blocks)
    fn extract_json_from_response(&self, response: &str) -> String {
        // Try to find JSON array or object
        let json_start = response.find('[').or_else(|| response.find('{'));
        let json_end = response.rfind(']').or_else(|| response.rfind('}'));
        
        if let (Some(start), Some(end)) = (json_start, json_end) {
            response[start..=end].to_string()
        } else {
            // Try to extract from markdown code blocks
            if let Some(start) = response.find("```json") {
                let after_start = &response[start + 7..];
                if let Some(end) = after_start.find("```") {
                    return after_start[..end].trim().to_string();
                }
            }
            if let Some(start) = response.find("```") {
                let after_start = &response[start + 3..];
                if let Some(end) = after_start.find("```") {
                    return after_start[..end].trim().to_string();
                }
            }
            response.to_string()
        }
    }
}

/// Summary of RCA results (for strategy generation)
#[derive(Debug, Clone)]
pub struct RcaSummary {
    pub total_rows: usize,
    pub missing_left_count: usize,
    pub missing_right_count: usize,
    pub mismatch_count: usize,
    pub aggregate_mismatch: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_extract_json() {
        // Use metadata from the metadata folder if available, otherwise skip
        let metadata = match Metadata::load("metadata") {
            Ok(m) => m,
            Err(_) => {
                eprintln!("⚠️  Skipping test: metadata folder not found");
                return;
            }
        };
        
        let engine = LlmStrategyEngine::new(
            LlmClient::new("test".to_string(), "test".to_string(), "test".to_string()),
            metadata,
        );
        
        let response = r#"Here's the JSON:
```json
{"metric": "tos", "confidence": 0.9}
```"#;
        
        let extracted = engine.extract_json_from_response(response);
        assert!(extracted.contains("tos"));
    }
}

