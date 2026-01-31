//! Narrative Builder
//! 
//! Converts structured explanations into human-readable narratives using LLM assistance.
//! Produces clear, actionable explanations of root causes.

use crate::core::rca::attribution::{RowExplanation, ExplanationItem, DifferenceType};
use crate::llm::LlmClient;
use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};

/// Narrative for a single row explanation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowNarrative {
    /// Row identifier
    pub row_id: Vec<String>,
    
    /// Human-readable summary
    pub summary: String,
    
    /// Detailed explanation
    pub details: String,
    
    /// Recommended actions
    pub recommendations: Vec<String>,
}

/// Narrative builder
/// 
/// Converts structured explanations to human-readable narratives
pub struct NarrativeBuilder {
    llm_client: Option<LlmClient>,
}

impl NarrativeBuilder {
    /// Create a new narrative builder
    pub fn new() -> Self {
        Self {
            llm_client: None,
        }
    }
    
    /// Create with LLM client for enhanced narratives
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm_client = Some(llm);
        self
    }
    
    /// Build narrative from structured explanation
    /// 
    /// If LLM is available, uses it to generate natural language.
    /// Otherwise, uses template-based generation.
    pub async fn build_narrative(&self, explanation: &RowExplanation) -> Result<RowNarrative> {
        if let Some(ref llm) = self.llm_client {
            self.build_narrative_with_llm(llm, explanation).await
        } else {
            Ok(self.build_narrative_template(explanation))
        }
    }
    
    /// Build narrative using LLM
    async fn build_narrative_with_llm(
        &self,
        llm: &LlmClient,
        explanation: &RowExplanation,
    ) -> Result<RowNarrative> {
        let prompt = self.create_llm_prompt(explanation);
        
        let response = llm.call_llm(&prompt).await?;
        
        // Parse LLM response (simplified - in practice would use structured output)
        let summary = self.extract_summary_from_response(&response);
        let details = self.extract_details_from_response(&response);
        let recommendations = self.extract_recommendations_from_response(&response);
        
        Ok(RowNarrative {
            row_id: explanation.row_id.clone(),
            summary,
            details,
            recommendations,
        })
    }
    
    /// Build narrative using templates
    fn build_narrative_template(&self, explanation: &RowExplanation) -> RowNarrative {
        let summary = match explanation.difference_type {
            DifferenceType::MissingInRight => {
                format!("Row {:?} exists in left system but not in right", explanation.row_id)
            }
            DifferenceType::MissingInLeft => {
                format!("Row {:?} exists in right system but not in left", explanation.row_id)
            }
            DifferenceType::ValueMismatch => {
                format!("Row {:?} exists in both systems but with different values", explanation.row_id)
            }
            DifferenceType::Match => {
                format!("Row {:?} matches exactly between systems", explanation.row_id)
            }
        };
        
        let mut details_parts = Vec::new();
        for (idx, item) in explanation.explanations.iter().enumerate() {
            details_parts.push(format!(
                "{}. {}",
                idx + 1,
                item.explanation
            ));
        }
        
        let details = if details_parts.is_empty() {
            "No specific root cause identified. May be due to data quality issues or missing lineage information.".to_string()
        } else {
            details_parts.join("\n")
        };
        
        let recommendations = self.generate_recommendations(explanation);
        
        RowNarrative {
            row_id: explanation.row_id.clone(),
            summary,
            details,
            recommendations,
        }
    }
    
    /// Create LLM prompt for narrative generation
    fn create_llm_prompt(&self, explanation: &RowExplanation) -> String {
        format!(
            r#"You are a data quality analyst explaining root causes of data discrepancies.

ROW ID: {:?}
DIFFERENCE TYPE: {:?}
CONFIDENCE: {:.2}%

EXPLANATIONS:
{}

Please provide:
1. A concise summary (1-2 sentences)
2. Detailed explanation of root causes
3. Recommended actions to fix or investigate

Format your response as:
SUMMARY: <summary>
DETAILS: <details>
RECOMMENDATIONS:
- <recommendation 1>
- <recommendation 2>
"#,
            explanation.row_id,
            explanation.difference_type,
            explanation.confidence * 100.0,
            explanation.explanations.iter()
                .enumerate()
                .map(|(i, e)| format!("{}. {} (Source: {:?}, Evidence: {:?})", i+1, e.explanation, e.source, e.evidence))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
    
    /// Extract summary from LLM response
    fn extract_summary_from_response(&self, response: &str) -> String {
        if let Some(start) = response.find("SUMMARY:") {
            let after_summary = &response[start + 8..];
            if let Some(end) = after_summary.find("\nDETAILS:") {
                return after_summary[..end].trim().to_string();
            }
            return after_summary.trim().to_string();
        }
        "Summary not found in LLM response".to_string()
    }
    
    /// Extract details from LLM response
    fn extract_details_from_response(&self, response: &str) -> String {
        if let Some(start) = response.find("DETAILS:") {
            let after_details = &response[start + 8..];
            if let Some(end) = after_details.find("\nRECOMMENDATIONS:") {
                return after_details[..end].trim().to_string();
            }
            return after_details.trim().to_string();
        }
        "Details not found in LLM response".to_string()
    }
    
    /// Extract recommendations from LLM response
    fn extract_recommendations_from_response(&self, response: &str) -> Vec<String> {
        if let Some(start) = response.find("RECOMMENDATIONS:") {
            let after_recs = &response[start + 16..];
            return after_recs
                .lines()
                .filter_map(|line| {
                    let trimmed = line.trim();
                    if trimmed.starts_with("-") || trimmed.starts_with("*") {
                        Some(trimmed[1..].trim().to_string())
                    } else if !trimmed.is_empty() {
                        Some(trimmed.to_string())
                    } else {
                        None
                    }
                })
                .collect();
        }
        Vec::new()
    }
    
    /// Generate recommendations based on explanation
    fn generate_recommendations(&self, explanation: &RowExplanation) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        match explanation.difference_type {
            DifferenceType::MissingInRight => {
                recommendations.push("Check if right system's data pipeline is missing this row".to_string());
                recommendations.push("Verify join conditions in right system's pipeline".to_string());
            }
            DifferenceType::MissingInLeft => {
                recommendations.push("Check if left system's data pipeline is missing this row".to_string());
                recommendations.push("Verify join conditions in left system's pipeline".to_string());
            }
            DifferenceType::ValueMismatch => {
                recommendations.push("Compare rule execution between systems".to_string());
                recommendations.push("Verify filter conditions are consistent".to_string());
            }
            DifferenceType::Match => {
                recommendations.push("No action needed - rows match".to_string());
            }
        }
        
        // Add recommendations based on explanation sources
        for item in &explanation.explanations {
            match item.source {
                crate::core::rca::attribution::ExplanationSource::Join => {
                    recommendations.push("Review join logic and key matching".to_string());
                }
                crate::core::rca::attribution::ExplanationSource::Filter => {
                    recommendations.push("Review filter conditions and thresholds".to_string());
                }
                crate::core::rca::attribution::ExplanationSource::Rule => {
                    recommendations.push("Verify rule logic and parameters".to_string());
                }
                crate::core::rca::attribution::ExplanationSource::DataQuality => {
                    recommendations.push("Investigate data quality issues".to_string());
                }
            }
        }
        
        // Remove duplicates
        recommendations.sort();
        recommendations.dedup();
        
        recommendations
    }
}

impl Default for NarrativeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rca::attribution::{AttributionEngine, ExplanationItem, ExplanationSource};
    
    #[test]
    fn test_template_narrative() {
        let builder = NarrativeBuilder::new();
        
        let explanation = RowExplanation {
            row_id: vec!["1".to_string()],
            difference_type: DifferenceType::MissingInRight,
            explanations: vec![
                ExplanationItem {
                    source: ExplanationSource::Filter,
                    explanation: "Row dropped due to filter: amount > 100".to_string(),
                    evidence: {
                        let mut m = std::collections::HashMap::new();
                        m.insert("filter_expr".to_string(), "amount > 100".to_string());
                        m
                    },
                }
            ],
            confidence: 0.8,
        };
        
        let narrative = builder.build_narrative_template(&explanation);
        assert!(!narrative.summary.is_empty());
        assert!(!narrative.details.is_empty());
    }
}





