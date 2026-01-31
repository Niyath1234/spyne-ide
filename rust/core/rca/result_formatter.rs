//! Result Formatter
//! 
//! Uses LLM to intelligently decide what to display in the final results.
//! Analyzes the RCA results and determines the most important information to show.

use crate::core::agent::RcaCursorResult;
use crate::core::rca::attribution::RowExplanation;
use crate::llm::LlmClient;
use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Formatted display result with LLM-decided content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormattedDisplayResult {
    /// Main display content (what LLM decided to show)
    pub display_content: String,
    
    /// Key identifiers extracted (UUIDs, row IDs, etc.)
    pub key_identifiers: Vec<String>,
    
    /// Summary statistics
    pub summary_stats: DisplaySummaryStats,
    
    /// Display format type (what the LLM decided to emphasize)
    pub display_format: DisplayFormat,
    
    /// Metadata about what was displayed and why
    pub display_metadata: DisplayMetadata,
}

/// Summary statistics for display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplaySummaryStats {
    pub total_explanations: usize,
    pub missing_left_count: usize,
    pub missing_right_count: usize,
    pub mismatch_count: usize,
    pub aggregate_mismatch: f64,
}

/// Display format type - what the LLM decided to emphasize
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DisplayFormat {
    /// Emphasize UUIDs/row identifiers
    IdentifierFocused {
        identifiers: Vec<String>,
        identifier_type: String, // "uuid", "loan_id", "order_id", etc.
    },
    /// Emphasize summary statistics
    SummaryFocused {
        summary_text: String,
    },
    /// Emphasize root cause explanations
    ExplanationFocused {
        top_explanations: Vec<String>,
    },
    /// Balanced display
    Balanced {
        summary: String,
        key_identifiers: Vec<String>,
        top_explanations: Vec<String>,
    },
}

/// Metadata about display decisions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayMetadata {
    /// Why this format was chosen
    pub reasoning: String,
    
    /// What fields were available in the results
    pub available_fields: Vec<String>,
    
    /// What the LLM decided was most important
    pub priority_fields: Vec<String>,
}

/// Result formatter that uses LLM to decide what to display
pub struct ResultFormatter {
    llm_client: Option<LlmClient>,
}

impl ResultFormatter {
    /// Create a new result formatter
    pub fn new() -> Self {
        Self {
            llm_client: None,
        }
    }
    
    /// Create with LLM client for intelligent display decisions
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm_client = Some(llm);
        self
    }
    
    /// Format RCA results using LLM to decide what to display
    pub async fn format_result(
        &self,
        result: &RcaCursorResult,
        original_query: Option<&str>,
    ) -> Result<FormattedDisplayResult> {
        if let Some(ref llm) = self.llm_client {
            self.format_with_llm(llm, result, original_query).await
        } else {
            self.format_template(result)
        }
    }
    
    /// Format using LLM to intelligently decide what to display
    async fn format_with_llm(
        &self,
        llm: &LlmClient,
        result: &RcaCursorResult,
        original_query: Option<&str>,
    ) -> Result<FormattedDisplayResult> {
        // Extract all available information
        let available_data = self.extract_available_data(result);
        
        // Build prompt for LLM to decide what to display
        let prompt = self.build_display_decision_prompt(&available_data, original_query);
        
        // Call LLM
        let response = llm.call_llm(&prompt).await?;
        
        // Parse LLM response
        let display_decision = self.parse_display_decision(&response, &available_data)?;
        
        // Build formatted display
        self.build_formatted_display(result, display_decision)
    }
    
    /// Extract all available data from results
    fn extract_available_data(&self, result: &RcaCursorResult) -> HashMap<String, serde_json::Value> {
        let mut data = HashMap::new();
        
        // Extract identifiers from explanations
        let mut identifiers = Vec::new();
        let mut identifier_types = std::collections::HashSet::new();
        
        for explanation in &result.explanations {
            // Extract row_id (could be UUID, loan_id, etc.)
            for id in &explanation.row_id {
                identifiers.push(id.clone());
                // Try to infer identifier type from column names
                if id.contains("uuid") || id.contains("UUID") {
                    identifier_types.insert("uuid".to_string());
                } else if id.contains("loan") {
                    identifier_types.insert("loan_id".to_string());
                } else if id.contains("order") {
                    identifier_types.insert("order_id".to_string());
                } else if id.contains("user") {
                    identifier_types.insert("user_id".to_string());
                }
            }
            
            // Also check evidence for identifiers
            for item in &explanation.explanations {
                for (key, value) in &item.evidence {
                    if key.contains("uuid") || key.contains("id") {
                        identifiers.push(value.clone());
                    }
                }
            }
        }
        
        // Extract from row_diff if available (DataFrame is not Option, check if empty)
        if result.row_diff.missing_left.height() > 0 {
            // Try to extract identifiers from missing_left dataframe
            // This is a simplified version - in practice would need to access dataframe columns
        }
        
        data.insert("identifiers".to_string(), serde_json::json!(identifiers));
        data.insert("identifier_types".to_string(), serde_json::json!(identifier_types.into_iter().collect::<Vec<_>>()));
        data.insert("total_explanations".to_string(), serde_json::json!(result.explanations.len()));
        data.insert("missing_left_count".to_string(), serde_json::json!(result.summary.missing_left_count));
        data.insert("missing_right_count".to_string(), serde_json::json!(result.summary.missing_right_count));
        data.insert("mismatch_count".to_string(), serde_json::json!(result.summary.mismatch_count));
        data.insert("aggregate_mismatch".to_string(), serde_json::json!(result.summary.aggregate_mismatch));
        data.insert("reconciliation_passes".to_string(), serde_json::json!(result.reconciliation.passes));
        
        // Extract explanation summaries
        let explanation_summaries: Vec<String> = result.explanations.iter()
            .take(10)
            .map(|e| {
                format!("Row ID: {:?}, Type: {:?}, Confidence: {:.2}%", 
                    e.row_id, e.difference_type, e.confidence * 100.0)
            })
            .collect();
        data.insert("explanation_summaries".to_string(), serde_json::json!(explanation_summaries));
        
        data
    }
    
    /// Build prompt for LLM to decide what to display
    fn build_display_decision_prompt(
        &self,
        available_data: &HashMap<String, serde_json::Value>,
        original_query: Option<&str>,
    ) -> String {
        let query_context = if let Some(query) = original_query {
            format!("ORIGINAL USER QUERY: {}\n\n", query)
        } else {
            String::new()
        };
        
        format!(
            r#"You are a data analyst deciding what information to display in RCA (Root Cause Analysis) results.

{query_context}AVAILABLE DATA:
{}

YOUR TASK:
Analyze the available data and decide what is most important to display to the user. Consider:
1. What did the user ask for? (e.g., if they mentioned UUIDs, show UUIDs)
2. What are the key identifiers in the data? (UUIDs, loan_ids, order_ids, etc.)
3. What are the most important findings? (missing rows, value mismatches, root causes)
4. What format would be most useful? (identifier-focused, summary-focused, explanation-focused, or balanced)

Return your decision as JSON:
{{
  "display_format": "identifier_focused" | "summary_focused" | "explanation_focused" | "balanced",
  "identifier_type": "uuid" | "loan_id" | "order_id" | "user_id" | "row_id" | null,
  "priority_fields": ["field1", "field2", ...],
  "reasoning": "Why you chose this format and what to emphasize",
  "display_content": "The actual formatted content to show (main text)",
  "key_identifiers": ["id1", "id2", ...] (if identifier-focused)
}}

Guidelines:
- If user query mentions specific identifiers (UUID, loan_id, etc.), prioritize showing those
- If there are many identifiers, show top 20-50 most relevant ones
- If aggregate mismatch is significant, include summary statistics
- If there are clear root cause explanations, include top explanations
- Balance between detail and readability
"#,
            serde_json::to_string_pretty(available_data).unwrap_or_default()
        )
    }
    
    /// Parse LLM display decision
    fn parse_display_decision(
        &self,
        response: &str,
        available_data: &HashMap<String, serde_json::Value>,
    ) -> Result<DisplayDecision> {
        // Extract JSON from response
        let json_str = self.extract_json_from_response(response);
        
        let decision: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| RcaError::Llm(format!("Failed to parse display decision: {}. Response: {}", e, json_str)))?;
        
        let display_format_str = decision["display_format"]
            .as_str()
            .unwrap_or("balanced")
            .to_string();
        
        let display_format = match display_format_str.as_str() {
            "identifier_focused" => {
                let identifiers: Vec<String> = decision["key_identifiers"]
                    .as_array()
                    .map(|arr| arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect())
                    .unwrap_or_else(|| {
                        // Fallback to available identifiers
                        available_data.get("identifiers")
                            .and_then(|v| v.as_array())
                            .map(|arr| arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect())
                            .unwrap_or_default()
                    });
                
                let identifier_type = decision["identifier_type"]
                    .as_str()
                    .unwrap_or("row_id")
                    .to_string();
                
                DisplayFormat::IdentifierFocused {
                    identifiers,
                    identifier_type,
                }
            }
            "summary_focused" => {
                let summary_text = decision["display_content"]
                    .as_str()
                    .unwrap_or("Summary not available")
                    .to_string();
                
                DisplayFormat::SummaryFocused {
                    summary_text,
                }
            }
            "explanation_focused" => {
                let top_explanations: Vec<String> = decision["top_explanations"]
                    .as_array()
                    .map(|arr| arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect())
                    .unwrap_or_default();
                
                DisplayFormat::ExplanationFocused {
                    top_explanations,
                }
            }
            _ => {
                // Balanced format
                let summary = decision["summary"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                
                let key_identifiers: Vec<String> = decision["key_identifiers"]
                    .as_array()
                    .map(|arr| arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect())
                    .unwrap_or_default();
                
                let top_explanations: Vec<String> = decision["top_explanations"]
                    .as_array()
                    .map(|arr| arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect())
                    .unwrap_or_default();
                
                DisplayFormat::Balanced {
                    summary,
                    key_identifiers,
                    top_explanations,
                }
            }
        };
        
        let priority_fields: Vec<String> = decision["priority_fields"]
            .as_array()
            .map(|arr| arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect())
            .unwrap_or_default();
        
        let reasoning = decision["reasoning"]
            .as_str()
            .unwrap_or("No reasoning provided")
            .to_string();
        
        let display_content = decision["display_content"]
            .as_str()
            .unwrap_or("")
            .to_string();
        
        let key_identifiers: Vec<String> = decision["key_identifiers"]
            .as_array()
            .map(|arr| arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect())
            .unwrap_or_else(|| {
                // Extract from available data if not in decision
                available_data.get("identifiers")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .take(50) // Limit to 50
                        .collect())
                    .unwrap_or_default()
            });
        
        Ok(DisplayDecision {
            display_format,
            priority_fields,
            reasoning,
            display_content,
            key_identifiers,
        })
    }
    
    /// Build formatted display from decision
    fn build_formatted_display(
        &self,
        result: &RcaCursorResult,
        decision: DisplayDecision,
    ) -> Result<FormattedDisplayResult> {
        let summary_stats = DisplaySummaryStats {
            total_explanations: result.explanations.len(),
            missing_left_count: result.summary.missing_left_count,
            missing_right_count: result.summary.missing_right_count,
            mismatch_count: result.summary.mismatch_count,
            aggregate_mismatch: result.summary.aggregate_mismatch,
        };
        
        // Build display content based on format
        let display_content = if !decision.display_content.is_empty() {
            decision.display_content.clone()
        } else {
            self.build_display_content_from_format(&decision.display_format, result)
        };
        
        // Extract available fields
        let available_fields = vec![
            "explanations".to_string(),
            "identifiers".to_string(),
            "summary_stats".to_string(),
            "narratives".to_string(),
            "reconciliation".to_string(),
        ];
        
        let display_metadata = DisplayMetadata {
            reasoning: decision.reasoning,
            available_fields,
            priority_fields: decision.priority_fields,
        };
        
        Ok(FormattedDisplayResult {
            display_content,
            key_identifiers: decision.key_identifiers.clone(),
            summary_stats,
            display_format: decision.display_format.clone(),
            display_metadata,
        })
    }
    
    /// Build display content from format type
    fn build_display_content_from_format(
        &self,
        format: &DisplayFormat,
        result: &RcaCursorResult,
    ) -> String {
        match format {
            DisplayFormat::IdentifierFocused { identifiers, identifier_type } => {
                let mut content = format!("RCA Analysis Complete - {} Focused Display\n\n", identifier_type);
                content.push_str(&format!("Found {} root cause explanations\n\n", result.explanations.len()));
                content.push_str(&format!("{}s causing mismatch:\n", identifier_type));
                for (idx, id) in identifiers.iter().take(50).enumerate() {
                    content.push_str(&format!("  {}. {}\n", idx + 1, id));
                }
                if identifiers.len() > 50 {
                    content.push_str(&format!("\n... and {} more {}s\n", identifiers.len() - 50, identifier_type));
                }
                content
            }
            DisplayFormat::SummaryFocused { summary_text } => {
                format!("RCA Analysis Summary:\n\n{}\n\nTotal Explanations: {}", 
                    summary_text, result.explanations.len())
            }
            DisplayFormat::ExplanationFocused { top_explanations } => {
                let mut content = format!("RCA Analysis - Root Cause Explanations\n\n");
                content.push_str(&format!("Found {} root cause explanations\n\n", result.explanations.len()));
                for (idx, exp) in top_explanations.iter().take(10).enumerate() {
                    content.push_str(&format!("{}. {}\n\n", idx + 1, exp));
                }
                content
            }
            DisplayFormat::Balanced { summary, key_identifiers, top_explanations } => {
                let mut content = format!("RCA Analysis Complete\n\n");
                if !summary.is_empty() {
                    content.push_str(&format!("Summary: {}\n\n", summary));
                }
                content.push_str(&format!("Found {} root cause explanations\n", result.explanations.len()));
                if !key_identifiers.is_empty() {
                    content.push_str(&format!("\nKey Identifiers:\n"));
                    for (idx, id) in key_identifiers.iter().take(20).enumerate() {
                        content.push_str(&format!("  {}. {}\n", idx + 1, id));
                    }
                    if key_identifiers.len() > 20 {
                        content.push_str(&format!("  ... and {} more\n", key_identifiers.len() - 20));
                    }
                }
                if !top_explanations.is_empty() {
                    content.push_str(&format!("\nTop Explanations:\n"));
                    for (idx, exp) in top_explanations.iter().take(5).enumerate() {
                        content.push_str(&format!("{}. {}\n\n", idx + 1, exp));
                    }
                }
                content
            }
        }
    }
    
    /// Extract JSON from LLM response
    fn extract_json_from_response(&self, response: &str) -> String {
        // Try to find JSON object
        let json_start = response.find('{');
        let json_end = response.rfind('}');
        
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
    
    /// Format using template (fallback when LLM not available)
    fn format_template(&self, result: &RcaCursorResult) -> Result<FormattedDisplayResult> {
        // Extract identifiers
        let mut identifiers = Vec::new();
        for explanation in &result.explanations {
            identifiers.extend(explanation.row_id.clone());
        }
        
        let summary_stats = DisplaySummaryStats {
            total_explanations: result.explanations.len(),
            missing_left_count: result.summary.missing_left_count,
            missing_right_count: result.summary.missing_right_count,
            mismatch_count: result.summary.mismatch_count,
            aggregate_mismatch: result.summary.aggregate_mismatch,
        };
        
        let display_content = format!(
            "RCA Analysis Complete\n\nFound {} root cause explanations\n\nSummary:\n- Missing in right: {}\n- Missing in left: {}\n- Value mismatches: {}\n- Aggregate mismatch: {:.2}",
            result.explanations.len(),
            result.summary.missing_left_count,
            result.summary.missing_right_count,
            result.summary.mismatch_count,
            result.summary.aggregate_mismatch
        );
        
        let identifiers_clone = identifiers.clone();
        let display_content_clone = display_content.clone();
        
        Ok(FormattedDisplayResult {
            display_content,
            key_identifiers: identifiers_clone.clone(),
            summary_stats,
            display_format: DisplayFormat::Balanced {
                summary: display_content_clone,
                key_identifiers: identifiers_clone,
                top_explanations: Vec::new(),
            },
            display_metadata: DisplayMetadata {
                reasoning: "Template-based formatting (LLM not available)".to_string(),
                available_fields: vec!["explanations".to_string(), "identifiers".to_string()],
                priority_fields: vec!["explanations".to_string(), "identifiers".to_string()],
            },
        })
    }
}

/// Internal structure for LLM display decision
#[derive(Debug, Clone)]
struct DisplayDecision {
    display_format: DisplayFormat,
    priority_fields: Vec<String>,
    reasoning: String,
    display_content: String,
    key_identifiers: Vec<String>,
}

impl Default for ResultFormatter {
    fn default() -> Self {
        Self::new()
    }
}

