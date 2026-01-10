use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInterpretation {
    pub system_a: String,
    pub system_b: String,
    pub metric: String,
    pub as_of_date: Option<String>,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmbiguityQuestion {
    pub question: String,
    pub options: Vec<AmbiguityOption>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmbiguityOption {
    pub id: String,
    pub label: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmbiguityResolution {
    pub questions: Vec<AmbiguityQuestion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Explanation {
    pub summary: String,
    pub details: Vec<String>,
}

pub struct LlmClient {
    api_key: String,
    base_url: String,
}

impl LlmClient {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            base_url: "https://api.openai.com/v1".to_string(),
        }
    }
    
    pub async fn interpret_query(
        &self,
        query: &str,
        business_labels: &crate::metadata::BusinessLabel,
        metrics: &[crate::metadata::Metric],
    ) -> Result<QueryInterpretation> {
        // Build context for LLM
        let systems: Vec<String> = business_labels.systems.iter()
            .map(|s| format!("- {} (aliases: {})", s.label, s.aliases.join(", ")))
            .collect();
        
        let metric_labels: Vec<String> = business_labels.metrics.iter()
            .map(|m| format!("- {} (aliases: {})", m.label, m.aliases.join(", ")))
            .collect();
        
        let prompt = format!(
            r#"You are a query interpreter for a data reconciliation system. 
Extract the following from the user query and return ONLY valid JSON:

1. system_a: First system name (must match one of: {})
2. system_b: Second system name (must match one of: {})
3. metric: Metric name (must match one of: {})
4. as_of_date: Date in YYYY-MM-DD format if mentioned, null otherwise
5. confidence: Your confidence (0.0 to 1.0)

User query: "{}"

Return JSON in this exact format:
{{
  "system_a": "khatabook",
  "system_b": "tb",
  "metric": "tos",
  "as_of_date": "2025-12-31",
  "confidence": 0.95
}}

Only return the JSON, no other text."#,
            systems.join(", "),
            systems.join(", "),
            metric_labels.join(", "),
            query
        );
        
        let response = self.call_llm(&prompt).await?;
        
        // Parse JSON response
        let interpretation: QueryInterpretation = serde_json::from_str(&response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM response: {}", e)))?;
        
        Ok(interpretation)
    }
    
    pub async fn resolve_ambiguity(
        &self,
        ambiguity_type: &str,
        options: Vec<AmbiguityOption>,
    ) -> Result<AmbiguityResolution> {
        let options_json = serde_json::to_string(&options)
            .map_err(|e| RcaError::Llm(format!("Failed to serialize options: {}", e)))?;
        
        let prompt = format!(
            r#"Generate at most 3 multiple-choice questions to resolve ambiguity: "{}"

Available options:
{}

Return JSON in this format:
{{
  "questions": [
    {{
      "question": "Which time column should be used?",
      "options": [
        {{"id": "option1", "label": "disbursement_date", "description": "Loan disbursement date"}},
        {{"id": "option2", "label": "as_of_date", "description": "Snapshot date"}}
      ]
    }}
  ]
}}

Only return the JSON, no other text."#,
            ambiguity_type,
            options_json
        );
        
        let response = self.call_llm(&prompt).await?;
        let resolution: AmbiguityResolution = serde_json::from_str(&response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse ambiguity resolution: {}", e)))?;
        
        Ok(resolution)
    }
    
    pub async fn explain_rca(
        &self,
        rca_result: &crate::rca::RcaResult,
    ) -> Result<Explanation> {
        let result_json = serde_json::to_string(rca_result)
            .map_err(|e| RcaError::Llm(format!("Failed to serialize RCA result: {}", e)))?;
        
        let prompt = format!(
            r#"Explain this Root Cause Analysis result in business-friendly language:

{}

Provide:
1. A clear summary (2-3 sentences)
2. Key findings (bullet points)

Return JSON:
{{
  "summary": "Brief summary",
  "details": ["Finding 1", "Finding 2"]
}}

Only return the JSON, no other text."#,
            result_json
        );
        
        let response = self.call_llm(&prompt).await?;
        let explanation: Explanation = serde_json::from_str(&response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse explanation: {}", e)))?;
        
        Ok(explanation)
    }
    
    async fn call_llm(&self, prompt: &str) -> Result<String> {
        // For now, return dummy response if API key is dummy
        if self.api_key == "dummy-api-key" {
            return Ok(r#"{"system_a": "khatabook", "system_b": "tb", "metric": "tos", "as_of_date": "2025-12-31", "confidence": 0.95}"#.to_string());
        }
        
        let client = reqwest::Client::new();
        let body = serde_json::json!({
            "model": "gpt-4",
            "messages": [
                {"role": "system", "content": "You are a precise JSON-only responder. Always return valid JSON, no other text."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 1000
        });
        
        let response = client
            .post(&format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| RcaError::Llm(format!("LLM API call failed: {}", e)))?;
        
        let response_json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM response: {}", e)))?;
        
        let content = response_json["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| RcaError::Llm("No content in LLM response".to_string()))?;
        
        Ok(content.to_string())
    }
}

