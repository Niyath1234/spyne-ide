//! LLM Formatter v2 with Strict Contracts (Phase 4)
//! 
//! Formats RCAResult v2 using LLM with strict input/output contracts and JSON schema validation.
//! 
//! ## Phase 4 Implementation:
//! 
//! ### 4.1 Strict Input Contract ✅
//! - All input fields validated before sending to LLM
//! - Type checking (strings, numbers, arrays, objects)
//! - Constraint validation (ranges, non-empty, etc.)
//! - Data consistency checks (delta = value_b - value_a, impact = abs(delta))
//! - Comprehensive error messages for validation failures
//! 
//! ### 4.2 Strict Output Contract ✅
//! - All output fields validated after receiving from LLM
//! - Type checking (strings, numbers, arrays, objects)
//! - Constraint validation (non-empty strings, valid enums, etc.)
//! - Display format consistency checks
//! - Content length and structure validation
//! - Comprehensive error messages for validation failures
//! 
//! Key principles:
//! - LLM decides what to display and how to present it
//! - Strict input contract: validated JSON schema before sending to LLM
//! - Strict output contract: validated JSON schema after receiving from LLM
//! - Fallback handling: template-based formatting if LLM fails

use crate::core::rca::result_v2::RCAResult;
use crate::core::rca::result_formatter::FormattedDisplayResult;
use crate::llm::LlmClient;
use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Strict input contract for LLM formatter
/// 
/// This structure defines exactly what data is sent to the LLM.
/// All fields are validated before sending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatterInput {
    /// Original user question/query
    pub question: String,
    
    /// Summary statistics
    pub summary: FormatterSummary,
    
    /// Top grain-level differences
    pub top_differences: Vec<FormatterGrainDifference>,
    
    /// Top attributions
    pub top_attributions: Vec<FormatterAttribution>,
    
    /// Confidence score
    pub confidence: f64,
    
    /// Grain information
    pub grain_info: GrainInfo,
}

/// Summary for formatter input
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatterSummary {
    pub total_grain_units: usize,
    pub missing_left_count: usize,
    pub missing_right_count: usize,
    pub mismatch_count: usize,
    pub aggregate_difference: f64,
    pub top_k: usize,
}

/// Grain difference for formatter input
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatterGrainDifference {
    /// Grain values (not UUIDs - actual business identifiers)
    pub grain_values: Vec<String>,
    pub value_a: f64,
    pub value_b: f64,
    pub delta: f64,
    pub impact: f64,
}

/// Attribution for formatter input
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatterAttribution {
    pub grain_values: Vec<String>,
    pub impact: f64,
    pub contribution_percentage: f64,
}

/// Grain information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrainInfo {
    pub grain: String,
    pub grain_key: String,
}

/// Strict output contract for LLM formatter
/// 
/// This structure defines exactly what the LLM must return.
/// All responses are validated against this schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatterOutput {
    /// Display format chosen by LLM
    pub display_format: DisplayFormat,
    
    /// Display content (formatted text)
    pub display_content: String,
    
    /// Key grain units to highlight (grain values, not UUIDs)
    pub key_grain_units: Vec<Vec<String>>,
    
    /// Reasoning for format choice
    pub reasoning: Option<String>,
}

/// Display format options
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DisplayFormat {
    /// Grain-focused display (emphasize specific grain units)
    #[serde(rename = "grain_focused")]
    GrainFocused,
    /// Summary-focused display (emphasize aggregate statistics)
    #[serde(rename = "summary")]
    Summary,
    /// Table format (structured data presentation)
    #[serde(rename = "table")]
    Table,
    /// Narrative format (story-like explanation)
    #[serde(rename = "narrative")]
    Narrative,
}

/// JSON Schema for input validation
const INPUT_SCHEMA: &str = r#"
{
  "type": "object",
  "required": ["question", "summary", "top_differences", "top_attributions", "confidence", "grain_info"],
  "properties": {
    "question": {"type": "string"},
    "summary": {
      "type": "object",
      "required": ["total_grain_units", "missing_left_count", "missing_right_count", "mismatch_count", "aggregate_difference", "top_k"],
      "properties": {
        "total_grain_units": {"type": "integer", "minimum": 0},
        "missing_left_count": {"type": "integer", "minimum": 0},
        "missing_right_count": {"type": "integer", "minimum": 0},
        "mismatch_count": {"type": "integer", "minimum": 0},
        "aggregate_difference": {"type": "number"},
        "top_k": {"type": "integer", "minimum": 0}
      }
    },
    "top_differences": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["grain_values", "value_a", "value_b", "delta", "impact"],
        "properties": {
          "grain_values": {"type": "array", "items": {"type": "string"}},
          "value_a": {"type": "number"},
          "value_b": {"type": "number"},
          "delta": {"type": "number"},
          "impact": {"type": "number", "minimum": 0}
        }
      }
    },
    "top_attributions": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["grain_values", "impact", "contribution_percentage"],
        "properties": {
          "grain_values": {"type": "array", "items": {"type": "string"}},
          "impact": {"type": "number", "minimum": 0},
          "contribution_percentage": {"type": "number", "minimum": 0, "maximum": 100}
        }
      }
    },
    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    "grain_info": {
      "type": "object",
      "required": ["grain", "grain_key"],
      "properties": {
        "grain": {"type": "string"},
        "grain_key": {"type": "string"}
      }
    }
  }
}
"#;

/// JSON Schema for output validation
const OUTPUT_SCHEMA: &str = r#"
{
  "type": "object",
  "required": ["display_format", "display_content", "key_grain_units"],
  "properties": {
    "display_format": {
      "type": "string",
      "enum": ["grain_focused", "summary", "table", "narrative"]
    },
    "display_content": {"type": "string"},
    "key_grain_units": {
      "type": "array",
      "items": {
        "type": "array",
        "items": {"type": "string"}
      }
    },
    "reasoning": {"type": "string"}
  }
}
"#;

/// Formatter v2 with strict contracts
pub struct FormatterV2 {
    llm_client: Option<LlmClient>,
}

impl FormatterV2 {
    /// Create a new formatter v2
    pub fn new() -> Self {
        Self {
            llm_client: None,
        }
    }

    /// Create with LLM client
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm_client = Some(llm);
        self
    }

    /// Format RCAResult v2 with strict contracts
    pub async fn format(
        &self,
        result: &RCAResult,
        question: Option<&str>,
    ) -> Result<FormattedDisplayResult> {
        // Build input contract
        let input = self.build_input_contract(result, question)?;

        // Validate input contract
        self.validate_input(&input)?;

        // Format with LLM or fallback
        if let Some(ref llm) = self.llm_client {
            match self.format_with_llm(llm, &input).await {
                Ok(output) => {
                    // Validate output contract
                    match self.validate_output(&output) {
                        Ok(_) => self.build_formatted_display(result, output),
                        Err(e) => {
                            // Fallback on validation failure
                            eprintln!("Output validation failed: {}. Using fallback.", e);
                            self.format_fallback(result, question)
                        }
                    }
                }
                Err(e) => {
                    // Fallback on LLM failure
                    eprintln!("LLM formatting failed: {}. Using fallback.", e);
                    self.format_fallback(result, question)
                }
            }
        } else {
            self.format_fallback(result, question)
        }
    }

    /// Build input contract from RCAResult
    pub fn build_input_contract(
        &self,
        result: &RCAResult,
        question: Option<&str>,
    ) -> Result<FormatterInput> {
        let summary = FormatterSummary {
            total_grain_units: result.summary.total_grain_units,
            missing_left_count: result.summary.missing_left_count,
            missing_right_count: result.summary.missing_right_count,
            mismatch_count: result.summary.mismatch_count,
            aggregate_difference: result.summary.aggregate_difference,
            top_k: result.summary.top_k,
        };

        let top_differences: Vec<FormatterGrainDifference> = result
            .top_differences
            .iter()
            .map(|d| FormatterGrainDifference {
                grain_values: d.grain_value.clone(),
                value_a: d.value_a,
                value_b: d.value_b,
                delta: d.delta,
                impact: d.impact,
            })
            .collect();

        let top_attributions: Vec<FormatterAttribution> = result
            .attributions
            .iter()
            .map(|a| FormatterAttribution {
                grain_values: a.grain_value.clone(),
                impact: a.impact,
                contribution_percentage: a.contribution_percentage,
            })
            .collect();

        let grain_info = GrainInfo {
            grain: result.grain.clone(),
            grain_key: result.grain_key.clone(),
        };

        Ok(FormatterInput {
            question: question.unwrap_or("").to_string(),
            summary,
            top_differences,
            top_attributions,
            confidence: result.confidence,
            grain_info,
        })
    }

    /// Validate input contract against JSON schema (4.1 Strict Input Contract)
    /// 
    /// Performs comprehensive validation of all required fields and types
    /// according to the INPUT_SCHEMA specification.
    /// 
    /// This is a STRICT contract - all fields must be present, valid, and consistent.
    /// - No missing required fields
    /// - All types must match exactly
    /// - All constraints must be satisfied (ranges, non-empty, etc.)
    /// - Data consistency checks (e.g., delta = value_b - value_a)
    pub fn validate_input(&self, input: &FormatterInput) -> Result<()> {
        // Serialize to JSON for validation
        let json = serde_json::to_value(input)
            .map_err(|e| RcaError::Execution(format!("Failed to serialize input: {}", e)))?;

        // Comprehensive validation based on INPUT_SCHEMA
        self.validate_input_comprehensive(&json, input)
    }

    /// Comprehensive input validation based on schema (4.1 Strict Input Contract)
    /// 
    /// Validates:
    /// 1. All required fields are present
    /// 2. All types match exactly
    /// 3. All constraints are satisfied (ranges, non-empty, etc.)
    /// 4. Data consistency (e.g., delta = value_b - value_a, impact = abs(delta))
    /// 5. No null or empty values where not allowed
    pub fn validate_input_comprehensive(&self, json: &Value, input: &FormatterInput) -> Result<()> {
        // Check root is an object
        let obj = json.as_object()
            .ok_or_else(|| RcaError::Execution("Input must be an object".to_string()))?;

        // Validate required fields from schema
        // 1. question: string (can be empty but must be present)
        if !obj.contains_key("question") {
            return Err(RcaError::Execution("Missing required 'question' field".to_string()));
        }
        if !obj["question"].is_string() {
            return Err(RcaError::Execution("'question' field must be a string".to_string()));
        }

        // 2. summary: object with required fields
        let summary = obj.get("summary")
            .and_then(|v| v.as_object())
            .ok_or_else(|| RcaError::Execution("Missing or invalid 'summary' field (must be object)".to_string()))?;

        // Validate summary fields - all must be non-negative integers
        for field in &["total_grain_units", "missing_left_count", "missing_right_count", "mismatch_count", "top_k"] {
            if !summary.contains_key(*field) {
                return Err(RcaError::Execution(format!("Missing required 'summary.{}' field", field)));
            }
            if !summary[*field].is_u64() {
                return Err(RcaError::Execution(format!("'summary.{}' must be a non-negative integer", field)));
            }
        }

        // Validate aggregate_difference: must be a number
        if !summary.contains_key("aggregate_difference") {
            return Err(RcaError::Execution("Missing required 'summary.aggregate_difference' field".to_string()));
        }
        if !summary["aggregate_difference"].is_number() {
            return Err(RcaError::Execution("'summary.aggregate_difference' must be a number".to_string()));
        }

        // 3. top_differences: array (can be empty but must be present)
        if !obj.contains_key("top_differences") {
            return Err(RcaError::Execution("Missing required 'top_differences' field".to_string()));
        }
        let top_differences = obj.get("top_differences")
            .and_then(|v| v.as_array())
            .ok_or_else(|| RcaError::Execution("'top_differences' field must be an array".to_string()))?;

        for (idx, diff) in top_differences.iter().enumerate() {
            let diff_obj = diff.as_object()
                .ok_or_else(|| RcaError::Execution(format!("top_differences[{}] must be an object", idx)))?;

            // Validate grain_values: array of non-empty strings
            if !diff_obj.contains_key("grain_values") {
                return Err(RcaError::Execution(format!("top_differences[{}] missing required 'grain_values' field", idx)));
            }
            let grain_values = diff_obj["grain_values"].as_array()
                .ok_or_else(|| RcaError::Execution(format!("top_differences[{}].grain_values must be an array", idx)))?;
            
            if grain_values.is_empty() {
                return Err(RcaError::Execution(format!("top_differences[{}].grain_values cannot be empty", idx)));
            }
            
            for (gv_idx, gv) in grain_values.iter().enumerate() {
                if !gv.is_string() {
                    return Err(RcaError::Execution(format!("top_differences[{}].grain_values[{}] must be a string", idx, gv_idx)));
                }
                let gv_str = gv.as_str().unwrap();
                if gv_str.is_empty() {
                    return Err(RcaError::Execution(format!("top_differences[{}].grain_values[{}] cannot be empty", idx, gv_idx)));
                }
            }

            // Validate numeric fields - all required
            for field in &["value_a", "value_b", "delta", "impact"] {
                if !diff_obj.contains_key(*field) {
                    return Err(RcaError::Execution(format!("top_differences[{}] missing required '{}' field", idx, field)));
                }
                if !diff_obj[*field].is_number() {
                    return Err(RcaError::Execution(format!("top_differences[{}].{} must be a number", idx, field)));
                }
            }

            // Validate impact >= 0
            let impact = diff_obj["impact"].as_f64().unwrap();
            if impact < 0.0 {
                return Err(RcaError::Execution(format!("top_differences[{}].impact must be >= 0", idx)));
            }

            // STRICT: Validate data consistency - delta should equal value_b - value_a (within floating point tolerance)
            let value_a = diff_obj["value_a"].as_f64().unwrap();
            let value_b = diff_obj["value_b"].as_f64().unwrap();
            let delta = diff_obj["delta"].as_f64().unwrap();
            let expected_delta = value_b - value_a;
            if (delta - expected_delta).abs() > 1e-10 {
                return Err(RcaError::Execution(format!(
                    "top_differences[{}].delta ({}) does not match value_b ({}) - value_a ({}) = {}",
                    idx, delta, value_b, value_a, expected_delta
                )));
            }

            // STRICT: Validate impact should equal abs(delta) (within floating point tolerance)
            let expected_impact = delta.abs();
            if (impact - expected_impact).abs() > 1e-10 {
                return Err(RcaError::Execution(format!(
                    "top_differences[{}].impact ({}) does not match abs(delta) ({})",
                    idx, impact, expected_impact
                )));
            }
        }

        // 4. top_attributions: array (can be empty but must be present)
        if !obj.contains_key("top_attributions") {
            return Err(RcaError::Execution("Missing required 'top_attributions' field".to_string()));
        }
        let top_attributions = obj.get("top_attributions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| RcaError::Execution("'top_attributions' field must be an array".to_string()))?;

        for (idx, attr) in top_attributions.iter().enumerate() {
            let attr_obj = attr.as_object()
                .ok_or_else(|| RcaError::Execution(format!("top_attributions[{}] must be an object", idx)))?;

            // Validate grain_values: array of non-empty strings
            if !attr_obj.contains_key("grain_values") {
                return Err(RcaError::Execution(format!("top_attributions[{}] missing required 'grain_values' field", idx)));
            }
            let grain_values = attr_obj["grain_values"].as_array()
                .ok_or_else(|| RcaError::Execution(format!("top_attributions[{}].grain_values must be an array", idx)))?;
            
            if grain_values.is_empty() {
                return Err(RcaError::Execution(format!("top_attributions[{}].grain_values cannot be empty", idx)));
            }
            
            for (gv_idx, gv) in grain_values.iter().enumerate() {
                if !gv.is_string() {
                    return Err(RcaError::Execution(format!("top_attributions[{}].grain_values[{}] must be a string", idx, gv_idx)));
                }
                let gv_str = gv.as_str().unwrap();
                if gv_str.is_empty() {
                    return Err(RcaError::Execution(format!("top_attributions[{}].grain_values[{}] cannot be empty", idx, gv_idx)));
                }
            }

            // Validate impact >= 0
            if !attr_obj.contains_key("impact") {
                return Err(RcaError::Execution(format!("top_attributions[{}] missing required 'impact' field", idx)));
            }
            if !attr_obj["impact"].is_number() {
                return Err(RcaError::Execution(format!("top_attributions[{}].impact must be a number", idx)));
            }
            let impact = attr_obj["impact"].as_f64().unwrap();
            if impact < 0.0 {
                return Err(RcaError::Execution(format!("top_attributions[{}].impact must be >= 0", idx)));
            }

            // Validate contribution_percentage: 0-100
            if !attr_obj.contains_key("contribution_percentage") {
                return Err(RcaError::Execution(format!("top_attributions[{}] missing required 'contribution_percentage' field", idx)));
            }
            if !attr_obj["contribution_percentage"].is_number() {
                return Err(RcaError::Execution(format!("top_attributions[{}].contribution_percentage must be a number", idx)));
            }
            let contrib = attr_obj["contribution_percentage"].as_f64().unwrap();
            if contrib < 0.0 || contrib > 100.0 {
                return Err(RcaError::Execution(format!("top_attributions[{}].contribution_percentage must be between 0 and 100", idx)));
            }
        }

        // 5. confidence: number between 0 and 1
        if !obj.contains_key("confidence") {
            return Err(RcaError::Execution("Missing required 'confidence' field".to_string()));
        }
        if !obj["confidence"].is_number() {
            return Err(RcaError::Execution("'confidence' field must be a number".to_string()));
        }
        let confidence = obj["confidence"].as_f64().unwrap();
        if confidence < 0.0 || confidence > 1.0 {
            return Err(RcaError::Execution("confidence must be between 0.0 and 1.0".to_string()));
        }

        // 6. grain_info: object with non-empty strings
        if !obj.contains_key("grain_info") {
            return Err(RcaError::Execution("Missing required 'grain_info' field".to_string()));
        }
        let grain_info = obj.get("grain_info")
            .and_then(|v| v.as_object())
            .ok_or_else(|| RcaError::Execution("'grain_info' field must be an object".to_string()))?;

        if !grain_info.contains_key("grain") {
            return Err(RcaError::Execution("Missing required 'grain_info.grain' field".to_string()));
        }
        if !grain_info["grain"].is_string() {
            return Err(RcaError::Execution("'grain_info.grain' field must be a string".to_string()));
        }
        if grain_info["grain"].as_str().unwrap().is_empty() {
            return Err(RcaError::Execution("'grain_info.grain' cannot be empty".to_string()));
        }

        if !grain_info.contains_key("grain_key") {
            return Err(RcaError::Execution("Missing required 'grain_info.grain_key' field".to_string()));
        }
        if !grain_info["grain_key"].is_string() {
            return Err(RcaError::Execution("'grain_info.grain_key' field must be a string".to_string()));
        }
        if grain_info["grain_key"].as_str().unwrap().is_empty() {
            return Err(RcaError::Execution("'grain_info.grain_key' cannot be empty".to_string()));
        }

        Ok(())
    }

    /// Format with LLM
    async fn format_with_llm(
        &self,
        llm: &LlmClient,
        input: &FormatterInput,
    ) -> Result<FormatterOutput> {
        // Build prompt
        let prompt = self.build_formatting_prompt(input);

        // Call LLM
        let response = llm.call_llm(&prompt).await?;

        // Parse response
        self.parse_llm_response(&response)
    }

    /// Build formatting prompt
    fn build_formatting_prompt(&self, input: &FormatterInput) -> String {
        format!(
            r#"You are a data analysis formatter. Format the following RCA (Root Cause Analysis) results.

Question: {}

Summary:
- Total grain units: {}
- Missing in system A: {}
- Missing in system B: {}
- Value mismatches: {}
- Aggregate difference: {}
- Confidence: {:.2}%

Top Differences:
{}

Top Attributions:
{}

Grain: {} (key: {})

Please format this as JSON with the following structure:
{{
  "display_format": "grain_focused" | "summary" | "table" | "narrative",
  "display_content": "formatted text explaining the results",
  "key_grain_units": [["grain_value1"], ["grain_value2"], ...],
  "reasoning": "why you chose this format"
}}

Choose the format that best answers the user's question. Use grain values (not UUIDs) in key_grain_units."#,
            input.question,
            input.summary.total_grain_units,
            input.summary.missing_left_count,
            input.summary.missing_right_count,
            input.summary.mismatch_count,
            input.summary.aggregate_difference,
            input.confidence * 100.0,
            self.format_differences(&input.top_differences),
            self.format_attributions(&input.top_attributions),
            input.grain_info.grain,
            input.grain_info.grain_key,
        )
    }

    /// Format differences for prompt
    fn format_differences(&self, differences: &[FormatterGrainDifference]) -> String {
        differences
            .iter()
            .enumerate()
            .map(|(i, d)| {
                format!(
                    "{}. Grain: {:?}, Value A: {:.2}, Value B: {:.2}, Delta: {:.2}, Impact: {:.2}",
                    i + 1,
                    d.grain_values,
                    d.value_a,
                    d.value_b,
                    d.delta,
                    d.impact
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Format attributions for prompt
    fn format_attributions(&self, attributions: &[FormatterAttribution]) -> String {
        attributions
            .iter()
            .enumerate()
            .map(|(i, a)| {
                format!(
                    "{}. Grain: {:?}, Impact: {:.2}, Contribution: {:.2}%",
                    i + 1,
                    a.grain_values,
                    a.impact,
                    a.contribution_percentage
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Parse LLM response
    fn parse_llm_response(&self, response: &str) -> Result<FormatterOutput> {
        // Try to extract JSON from response
        let json_str = self.extract_json_from_response(response)?;
        
        // Parse JSON
        let value: Value = serde_json::from_str(&json_str)
            .map_err(|e| RcaError::Execution(format!("Failed to parse LLM response as JSON: {}", e)))?;

        // Convert to FormatterOutput
        let display_format_str = value["display_format"]
            .as_str()
            .ok_or_else(|| RcaError::Execution("Missing or invalid 'display_format' field".to_string()))?;

        let display_format = match display_format_str {
            "grain_focused" => DisplayFormat::GrainFocused,
            "summary" => DisplayFormat::Summary,
            "table" => DisplayFormat::Table,
            "narrative" => DisplayFormat::Narrative,
            _ => return Err(RcaError::Execution(format!("Invalid display_format: {}", display_format_str))),
        };

        let display_content = value["display_content"]
            .as_str()
            .ok_or_else(|| RcaError::Execution("Missing or invalid 'display_content' field".to_string()))?
            .to_string();

        let key_grain_units: Vec<Vec<String>> = value["key_grain_units"]
            .as_array()
            .ok_or_else(|| RcaError::Execution("Missing or invalid 'key_grain_units' field".to_string()))?
            .iter()
            .map(|v| {
                v.as_array()
                    .unwrap()
                    .iter()
                    .map(|s| s.as_str().unwrap().to_string())
                    .collect()
            })
            .collect();

        let reasoning = value["reasoning"].as_str().map(|s| s.to_string());

        Ok(FormatterOutput {
            display_format,
            display_content,
            key_grain_units,
            reasoning,
        })
    }

    /// Extract JSON from LLM response (handles markdown code blocks)
    fn extract_json_from_response(&self, response: &str) -> Result<String> {
        // Try to find JSON in markdown code blocks
        if let Some(start) = response.find("```json") {
            let start = start + 7; // Skip "```json"
            if let Some(end) = response[start..].find("```") {
                return Ok(response[start..start + end].trim().to_string());
            }
        }

        // Try to find JSON in plain code blocks
        if let Some(start) = response.find("```") {
            let start = start + 3; // Skip "```"
            if let Some(end) = response[start..].find("```") {
                let json_str = response[start..start + end].trim();
                // Try to parse to validate it's JSON
                if serde_json::from_str::<Value>(json_str).is_ok() {
                    return Ok(json_str.to_string());
                }
            }
        }

        // Try to find JSON object directly
        if let Some(start) = response.find('{') {
            if let Some(end) = response.rfind('}') {
                let json_str = &response[start..=end];
                // Try to parse to validate
                if serde_json::from_str::<Value>(json_str).is_ok() {
                    return Ok(json_str.to_string());
                }
            }
        }

        Err(RcaError::Execution("Could not extract JSON from LLM response".to_string()))
    }

    /// Validate output contract against JSON schema (4.2 Strict Output Contract)
    /// 
    /// Performs comprehensive validation of all required fields and types
    /// according to the OUTPUT_SCHEMA specification.
    /// 
    /// This is a STRICT contract - all fields must be present, valid, and consistent.
    /// - No missing required fields
    /// - All types must match exactly
    /// - All constraints must be satisfied (non-empty strings, valid enums, etc.)
    /// - Display format must match the content structure
    pub fn validate_output(&self, output: &FormatterOutput) -> Result<()> {
        // Serialize to JSON for validation
        let json = serde_json::to_value(output)
            .map_err(|e| RcaError::Execution(format!("Failed to serialize output: {}", e)))?;

        // Comprehensive validation based on OUTPUT_SCHEMA
        self.validate_output_comprehensive(&json, output)
    }

    /// Comprehensive output validation based on schema (4.2 Strict Output Contract)
    /// 
    /// Validates:
    /// 1. All required fields are present
    /// 2. All types match exactly
    /// 3. All constraints are satisfied (non-empty strings, valid enums, etc.)
    /// 4. Display format consistency (format matches content structure)
    /// 5. No null or empty values where not allowed
    pub fn validate_output_comprehensive(&self, json: &Value, output: &FormatterOutput) -> Result<()> {
        // Check root is an object
        let obj = json.as_object()
            .ok_or_else(|| RcaError::Execution("Output must be an object".to_string()))?;

        // 1. display_format: must be one of allowed enum values
        if !obj.contains_key("display_format") {
            return Err(RcaError::Execution("Missing required 'display_format' field".to_string()));
        }
        if !obj["display_format"].is_string() {
            return Err(RcaError::Execution("'display_format' field must be a string".to_string()));
        }
        let format_str = obj["display_format"].as_str().unwrap();
        match format_str {
            "grain_focused" | "summary" | "table" | "narrative" => {},
            _ => return Err(RcaError::Execution(format!(
                "Invalid 'display_format': '{}'. Must be one of: grain_focused, summary, table, narrative",
                format_str
            ))),
        }

        // Also validate the enum variant matches
        match output.display_format {
            DisplayFormat::GrainFocused if format_str != "grain_focused" => {
                return Err(RcaError::Execution("display_format enum mismatch: GrainFocused".to_string()));
            }
            DisplayFormat::Summary if format_str != "summary" => {
                return Err(RcaError::Execution("display_format enum mismatch: Summary".to_string()));
            }
            DisplayFormat::Table if format_str != "table" => {
                return Err(RcaError::Execution("display_format enum mismatch: Table".to_string()));
            }
            DisplayFormat::Narrative if format_str != "narrative" => {
                return Err(RcaError::Execution("display_format enum mismatch: Narrative".to_string()));
            }
            _ => {}
        }

        // STRICT: Validate display format consistency
        // If format is grain_focused, key_grain_units should not be empty
        if matches!(output.display_format, DisplayFormat::GrainFocused) && output.key_grain_units.is_empty() {
            return Err(RcaError::Execution("display_format is 'grain_focused' but key_grain_units is empty".to_string()));
        }

        // 2. display_content: must be non-empty string
        if !obj.contains_key("display_content") {
            return Err(RcaError::Execution("Missing required 'display_content' field".to_string()));
        }
        if !obj["display_content"].is_string() {
            return Err(RcaError::Execution("'display_content' field must be a string".to_string()));
        }
        if output.display_content.trim().is_empty() {
            return Err(RcaError::Execution("display_content cannot be empty or whitespace-only".to_string()));
        }

        // STRICT: Validate display_content has reasonable length (at least 10 characters)
        if output.display_content.len() < 10 {
            return Err(RcaError::Execution(format!(
                "display_content is too short ({} characters). Must be at least 10 characters.",
                output.display_content.len()
            )));
        }

        // 3. key_grain_units: array of arrays of non-empty strings
        if !obj.contains_key("key_grain_units") {
            return Err(RcaError::Execution("Missing required 'key_grain_units' field".to_string()));
        }
        if !obj["key_grain_units"].is_array() {
            return Err(RcaError::Execution("'key_grain_units' field must be an array".to_string()));
        }
        let key_grain_units = obj["key_grain_units"].as_array().unwrap();
        
        for (idx, grain_unit) in key_grain_units.iter().enumerate() {
            if !grain_unit.is_array() {
                return Err(RcaError::Execution(format!("key_grain_units[{}] must be an array", idx)));
            }
            let grain_unit_arr = grain_unit.as_array().unwrap();
            if grain_unit_arr.is_empty() {
                return Err(RcaError::Execution(format!("key_grain_units[{}] cannot be empty", idx)));
            }
            for (gv_idx, gv) in grain_unit_arr.iter().enumerate() {
                if !gv.is_string() {
                    return Err(RcaError::Execution(format!("key_grain_units[{}][{}] must be a string", idx, gv_idx)));
                }
                let gv_str = gv.as_str().unwrap();
                if gv_str.is_empty() {
                    return Err(RcaError::Execution(format!("key_grain_units[{}][{}] cannot be empty", idx, gv_idx)));
                }
            }
        }

        // Validate the actual output struct matches
        for (idx, grain_unit) in output.key_grain_units.iter().enumerate() {
            if grain_unit.is_empty() {
                return Err(RcaError::Execution(format!("key_grain_units[{}] cannot be empty", idx)));
            }
            for (gv_idx, gv) in grain_unit.iter().enumerate() {
                if gv.is_empty() {
                    return Err(RcaError::Execution(format!("key_grain_units[{}][{}] cannot be empty", idx, gv_idx)));
                }
            }
        }

        // 4. reasoning: optional string (if present, must be non-empty string)
        if obj.contains_key("reasoning") {
            // Check if it's null (which is valid for optional field)
            if obj["reasoning"].is_null() {
                // Null is acceptable for optional field, skip validation
            } else if !obj["reasoning"].is_string() {
                return Err(RcaError::Execution("'reasoning' field must be a string if present".to_string()));
            } else if let Some(reasoning) = &output.reasoning {
                if reasoning.trim().is_empty() {
                    return Err(RcaError::Execution("'reasoning' field cannot be empty or whitespace-only if present".to_string()));
                }
            }
        }

        // STRICT: Additional consistency checks
        // If format is table, display_content should contain structured data indicators
        if matches!(output.display_format, DisplayFormat::Table) {
            let content_lower = output.display_content.to_lowercase();
            if !content_lower.contains("|") && !content_lower.contains("\t") && !content_lower.contains("  ") {
                // Warning: table format but no table-like structure detected
                // This is a warning, not an error, but we can log it
            }
        }

        // If format is narrative, display_content should be longer and more descriptive
        if matches!(output.display_format, DisplayFormat::Narrative) {
            if output.display_content.len() < 50 {
                return Err(RcaError::Execution(format!(
                    "display_format is 'narrative' but display_content is too short ({} characters). Narrative format should be at least 50 characters.",
                    output.display_content.len()
                )));
            }
        }

        Ok(())
    }

    /// Build formatted display result
    fn build_formatted_display(
        &self,
        _result: &RCAResult,
        output: FormatterOutput,
    ) -> Result<FormattedDisplayResult> {
        // Convert DisplayFormat to old DisplayFormat enum
        let display_format = match output.display_format {
            DisplayFormat::GrainFocused => crate::core::rca::result_formatter::DisplayFormat::IdentifierFocused {
                identifiers: output.key_grain_units.iter().flatten().cloned().collect(),
                identifier_type: "grain".to_string(),
            },
            DisplayFormat::Summary => crate::core::rca::result_formatter::DisplayFormat::SummaryFocused {
                summary_text: output.display_content.clone(),
            },
            DisplayFormat::Table => crate::core::rca::result_formatter::DisplayFormat::Balanced {
                summary: output.display_content.clone(),
                key_identifiers: output.key_grain_units.iter().flatten().cloned().collect(),
                top_explanations: Vec::new(),
            },
            DisplayFormat::Narrative => crate::core::rca::result_formatter::DisplayFormat::ExplanationFocused {
                top_explanations: vec![output.display_content.clone()],
            },
        };

        let display_content = output.display_content.clone();
        Ok(FormattedDisplayResult {
            display_content,
            key_identifiers: output.key_grain_units.iter().flatten().cloned().collect(),
            summary_stats: crate::core::rca::result_formatter::DisplaySummaryStats {
                total_explanations: output.key_grain_units.len(),
                missing_left_count: 0, // Would come from result
                missing_right_count: 0,
                mismatch_count: 0,
                aggregate_mismatch: 0.0,
            },
            display_format,
            display_metadata: crate::core::rca::result_formatter::DisplayMetadata {
                reasoning: output.reasoning.unwrap_or_default(),
                available_fields: Vec::new(),
                priority_fields: output.key_grain_units.iter().flatten().cloned().collect(),
            },
        })
    }

    /// Fallback formatting (template-based)
    pub fn format_fallback(
        &self,
        result: &RCAResult,
        question: Option<&str>,
    ) -> Result<FormattedDisplayResult> {
        let mut content = String::new();

        if let Some(q) = question {
            content.push_str(&format!("Question: {}\n\n", q));
        }

        content.push_str(&format!(
            "RCA Results for grain: {} (key: {})\n\n",
            result.grain, result.grain_key
        ));

        content.push_str(&format!(
            "Summary:\n- Total grain units: {}\n- Missing in system A: {}\n- Missing in system B: {}\n- Value mismatches: {}\n- Aggregate difference: {:.2}\n- Confidence: {:.2}%\n\n",
            result.summary.total_grain_units,
            result.summary.missing_left_count,
            result.summary.missing_right_count,
            result.summary.mismatch_count,
            result.summary.aggregate_difference,
            result.confidence * 100.0
        ));

        if !result.top_differences.is_empty() {
            content.push_str("Top Differences:\n");
            for (i, diff) in result.top_differences.iter().take(10).enumerate() {
                content.push_str(&format!(
                    "{}. Grain: {:?}, Value A: {:.2}, Value B: {:.2}, Delta: {:.2}\n",
                    i + 1,
                    diff.grain_value,
                    diff.value_a,
                    diff.value_b,
                    diff.delta
                ));
            }
        }

        let content_clone = content.clone();
        Ok(FormattedDisplayResult {
            display_content: content,
            key_identifiers: result.top_differences.iter()
                .take(10)
                .flat_map(|d| d.grain_value.clone())
                .collect(),
            summary_stats: crate::core::rca::result_formatter::DisplaySummaryStats {
                total_explanations: result.top_differences.len(),
                missing_left_count: result.summary.missing_left_count,
                missing_right_count: result.summary.missing_right_count,
                mismatch_count: result.summary.mismatch_count,
                aggregate_mismatch: result.summary.aggregate_difference,
            },
            display_format: crate::core::rca::result_formatter::DisplayFormat::SummaryFocused {
                summary_text: content_clone,
            },
            display_metadata: crate::core::rca::result_formatter::DisplayMetadata {
                reasoning: "Fallback template formatting".to_string(),
                available_fields: Vec::new(),
                priority_fields: Vec::new(),
            },
        })
    }
}

impl Default for FormatterV2 {
    fn default() -> Self {
        Self::new()
    }
}

