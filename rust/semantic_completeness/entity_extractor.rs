//! Entity Extraction
//! 
//! Extracts required entities (anchor, attribute, relationship) from natural language questions.

use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequiredEntitySet {
    pub anchor_entities: Vec<String>,
    pub attribute_entities: Vec<String>,
    pub relationship_entities: Vec<String>,
}

pub struct EntityExtractor {
    llm: LlmClient,
}

impl EntityExtractor {
    pub fn new(llm: LlmClient) -> Self {
        Self { llm }
    }

    pub async fn extract_entities(&self, question: &str) -> Result<RequiredEntitySet> {
        let prompt = format!(
            r#"Extract required entities from this question. Identify anchor entities (primary subject), 
            attribute entities (properties/attributes), and relationship entities (related entities).

QUESTION: "{}"

Return JSON only (no markdown, no explanations):
{{
  "anchor_entities": ["entity1", "entity2"],
  "attribute_entities": ["entity3"],
  "relationship_entities": ["entity4"]
}}

Examples:
- "Show me all loans" → {{"anchor_entities": ["loan"], "attribute_entities": [], "relationship_entities": []}}
- "Show me loans with disbursement dates" → {{"anchor_entities": ["loan"], "attribute_entities": ["disbursement"], "relationship_entities": []}}
- "Transactions for customer accounts" → {{"anchor_entities": ["transaction"], "attribute_entities": [], "relationship_entities": ["customer_account"]}}
- "Show me all loans with their disbursement dates and amounts" → {{"anchor_entities": ["loan"], "attribute_entities": ["disbursement", "amount"], "relationship_entities": []}}

JSON:"#,
            question
        );

        let json_str = self.llm.call_llm(&prompt).await?;
        let cleaned_json = json_str
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim()
            .trim_start_matches("JSON:")
            .trim();

        let entities: RequiredEntitySet = serde_json::from_str(cleaned_json)
            .map_err(|e| RcaError::Llm(format!("Failed to parse entities: {}. Response: {}", e, cleaned_json)))?;

        Ok(entities)
    }
}





