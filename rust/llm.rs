use crate::error::{RcaError, Result};
use crate::intent::function_schema::{ChatMessage, FunctionCall, FunctionDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInterpretation {
    pub system_a: String,
    pub system_b: String,
    pub metric: String,
    pub as_of_date: Option<String>,
    #[serde(default = "default_confidence")]
    pub confidence: f64,
    // Cross-metric comparison fields
    #[serde(default)]
    pub is_cross_metric: bool,
    #[serde(default)]
    pub metric_a: Option<String>,
    #[serde(default)]
    pub metric_b: Option<String>,
}

fn default_confidence() -> f64 {
    0.9
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvAnalysis {
    pub grain_column: String,
    pub metric_column: Option<String>,
    pub aggregation_type: String, // "count", "sum", "avg", "max", "min"
    pub filters: Vec<CsvFilter>,
    pub metric_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvFilter {
    pub column: String,
    pub operator: String, // "=", "!=", ">", "<", ">=", "<=", "in", "contains"
    pub value: serde_json::Value, // Can be string, number, array, etc.
}

/// Interpretation of a validation query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationQueryInterpretation {
    pub constraint_type: String,
    pub system: String,
    pub table: Option<String>,
    pub entity_filter: Option<EntityFilterInterpretation>,
    pub constraint_details: serde_json::Value,
}

/// Entity filter interpretation from LLM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityFilterInterpretation {
    pub column: String,
    pub operator: String,
    pub value: serde_json::Value,
}

#[derive(Clone)]
pub struct LlmClient {
    api_key: String,
    base_url: String,
    model: String,
}

impl LlmClient {
    pub fn new(api_key: String, model: String, base_url: String) -> Self {
        Self {
            api_key,
            base_url,
            model,
        }
    }
    
    pub async fn interpret_query(
        &self,
        query: &str,
        business_labels: &crate::metadata::BusinessLabelObject,
        _metrics: &[crate::metadata::Metric],
    ) -> Result<QueryInterpretation> {
        // Token-optimized: Build compact context lists (only IDs and primary aliases)
        let systems: Vec<String> = business_labels.systems.iter()
            .map(|s| {
                // Only include first alias if exists, otherwise just ID
                let alias_hint = s.aliases.first().map(|a| format!(" or {}", a)).unwrap_or_default();
                format!("{}{}", s.system_id, alias_hint)
            })
            .collect();
        
        let metrics: Vec<String> = business_labels.metrics.iter()
            .map(|m| {
                let alias_hint = m.aliases.first().map(|a| format!(" or {}", a)).unwrap_or_default();
                format!("{}{}", m.metric_id, alias_hint)
            })
            .collect();
        
        // Token-optimized prompt: concise, no repetition, minimal example
        let prompt = format!(
            r#"Extract from query and return JSON only:
Query: "{}"
Systems: {}
Metrics: {}
Format: {{"system_a":"id","system_b":"id","metric":"id","as_of_date":"YYYY-MM-DD"|null,"confidence":0.0-1.0}}"#,
            query,
            systems.join(","),
            metrics.join(",")
        );
        
        let response = self.call_llm(&prompt).await?;
        
        // Clean response - remove markdown code blocks if present and handle null confidence
        let cleaned_response = response
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        
        // Replace null confidence with default value
        let cleaned_response = cleaned_response.replace(r#""confidence": null"#, r#""confidence": 0.9"#);
        let cleaned_response = cleaned_response.replace(r#""confidence":null"#, r#""confidence":0.9"#);
        
        // Parse JSON response
        let mut interpretation: QueryInterpretation = serde_json::from_str(&cleaned_response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM response: {}. Response: {}", e, cleaned_response)))?;
        
        // Validate and fix system names - must be from the known systems list
        let valid_system_ids: std::collections::HashSet<String> = business_labels.systems.iter()
            .map(|s| s.system_id.clone())
            .collect();
        
        // Check system_a
        if !valid_system_ids.contains(&interpretation.system_a) {
            warn!("LLM returned invalid system_a '{}', available: {:?}", interpretation.system_a, valid_system_ids);
            // Try to match by alias or fallback
            let matched = business_labels.systems.iter().find(|s| {
                s.aliases.iter().any(|a| a.to_lowercase() == interpretation.system_a.to_lowercase()) ||
                s.system_id.to_lowercase().contains(&interpretation.system_a.to_lowercase()) ||
                interpretation.system_a.to_lowercase().contains(&s.system_id.to_lowercase())
            });
            interpretation.system_a = matched.map(|s| s.system_id.clone()).unwrap_or_else(|| "khatabook".to_string());
            info!("Fixed system_a to: {}", interpretation.system_a);
        }
        
        // Check system_b
        if !valid_system_ids.contains(&interpretation.system_b) {
            warn!("LLM returned invalid system_b '{}', available: {:?}", interpretation.system_b, valid_system_ids);
            // Try to match by alias or fallback
            let matched = business_labels.systems.iter().find(|s| {
                s.aliases.iter().any(|a| a.to_lowercase() == interpretation.system_b.to_lowercase()) ||
                s.system_id.to_lowercase().contains(&interpretation.system_b.to_lowercase()) ||
                interpretation.system_b.to_lowercase().contains(&s.system_id.to_lowercase())
            });
            interpretation.system_b = matched.map(|s| s.system_id.clone()).unwrap_or_else(|| "tb".to_string());
            info!("Fixed system_b to: {}", interpretation.system_b);
        }
        
        // Validate metric as well
        let valid_metric_ids: std::collections::HashSet<String> = business_labels.metrics.iter()
            .map(|m| m.metric_id.clone())
            .collect();
        
        if !valid_metric_ids.contains(&interpretation.metric) {
            warn!("LLM returned invalid metric '{}', available: {:?}", interpretation.metric, valid_metric_ids);
            // Try to match by alias
            let matched = business_labels.metrics.iter().find(|m| {
                m.aliases.iter().any(|a| a.to_lowercase() == interpretation.metric.to_lowercase()) ||
                m.metric_id.to_lowercase().contains(&interpretation.metric.to_lowercase()) ||
                interpretation.metric.to_lowercase().contains(&m.metric_id.to_lowercase())
            });
            interpretation.metric = matched.map(|m| m.metric_id.clone()).unwrap_or_else(|| "tos".to_string());
            info!("Fixed metric to: {}", interpretation.metric);
        }
        
        Ok(interpretation)
    }
    
    pub async fn resolve_ambiguity(
        &self,
        ambiguity_type: &str,
        options: Vec<AmbiguityOption>,
    ) -> Result<AmbiguityResolution> {
        // Token-optimized: Compact JSON serialization
        let options_json = serde_json::to_string(&options)
            .map_err(|e| RcaError::Llm(format!("Failed to serialize options: {}", e)))?;
        
        // Token-optimized prompt: concise, minimal example
        let prompt = format!(
            r#"Generate ≤3 questions for: "{}"
Options: {}
Return: {{"questions":[{{"question":"text","options":[{{"id":"id","label":"label","description":"desc"}}]}}]}}"#,
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
        // For now, skip LLM explanation since we're not testing LLM yet
        // Return a simple explanation based on the result structure
        let summary = format!(
            "RCA completed for {} vs {} - {} metric",
            rca_result.system_a, rca_result.system_b, rca_result.metric
        );
        let details: Vec<String> = rca_result.classifications.iter()
            .map(|c| format!("{}: {}", c.root_cause, c.description))
            .collect();
        
        Ok(Explanation { summary, details })
    }
    
    pub async fn analyze_csv_query(
        &self,
        query: &str,
        columns_a: &[String],
        columns_b: &[String],
        sample_data_a: Option<&str>,
        sample_data_b: Option<&str>,
    ) -> Result<CsvAnalysis> {
        // Build column information
        let common_cols: Vec<String> = columns_a.iter()
            .filter(|c| columns_b.contains(c))
            .cloned()
            .collect();
        
        let all_cols: Vec<String> = columns_a.iter()
            .chain(columns_b.iter())
            .cloned()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        
        // Create prompt for LLM to analyze the query and columns
        let prompt = format!(
            r#"Analyze this reconciliation query and CSV structure. Return JSON only.

Query: "{}"

Available Columns (both CSVs): {}
Common Columns: {}

{}Return JSON with:
{{
  "grain_column": "column_name_for_entity_key",
  "metric_column": "column_name_for_metric_value" | null,
  "aggregation_type": "count" | "sum" | "avg" | "max" | "min",
  "filters": [
    {{"column": "col_name", "operator": "=" | "!=" | ">" | "<" | ">=" | "<=" | "in" | "contains", "value": "value_or_array"}}
  ],
  "metric_name": "descriptive_name"
}}

Rules:
- grain_column: The column that uniquely identifies entities (e.g., loan_id, customer_id)
- metric_column: The numeric column to aggregate (null if counting rows)
- aggregation_type: 
  * "count" if query mentions "numbers", "count", "how many"
  * "sum" if query mentions "total", "sum", "amount"
  * "avg" if query mentions "average", "mean"
- filters: Extract any conditions from query. Match query terms to actual column names and values:
  * If query mentions "MSME", look for columns like msme_flag, psl_type, msme_category, etc.
  * Match the actual value format: could be "yes"/"no", "MSME"/"N/A", true/false, 1/0, etc.
  * Use the exact value format found in the data (check sample data if provided)
- metric_name: Short descriptive name for the metric

Examples:
- Query "MSME numbers not matching" with column psl_type having values ["MSME", "N/A"] 
  -> filter: [{{"column":"psl_type","operator":"=","value":"MSME"}}]
- Query "MSME numbers not matching" with column msme_flag having values ["yes", "no"]
  -> filter: [{{"column":"msme_flag","operator":"=","value":"yes"}}]
- Query "Total disbursement amount differences" -> grain: loan_id, metric: disbursement_amount, agg: sum, filters: []
- Query "Average loan amount for MSME" with column psl_type -> filter: [{{"column":"psl_type","operator":"=","value":"MSME"}}]"#,
            query,
            all_cols.join(", "),
            common_cols.join(", "),
            if let (Some(sa), Some(sb)) = (sample_data_a, sample_data_b) {
                format!("Sample Data A (first 3 rows): {}\nSample Data B (first 3 rows): {}\n\n", sa, sb)
            } else {
                String::new()
            }
        );
        
        let response = self.call_llm(&prompt).await?;
        
        // Clean response - remove markdown code blocks if present
        let cleaned_response = response
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        
        // Parse JSON response
        let analysis: CsvAnalysis = serde_json::from_str(&cleaned_response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse CSV analysis: {}. Response: {}", e, cleaned_response)))?;
        
        Ok(analysis)
    }
    
    /// Interpret a validation query and extract constraint details
    pub async fn interpret_validation_query(
        &self,
        query: &str,
        business_labels: &crate::metadata::BusinessLabelObject,
        tables: &[crate::metadata::Table],
    ) -> Result<ValidationQueryInterpretation> {
        // Build system and table context
        let systems: Vec<String> = business_labels.systems.iter()
            .map(|s| format!("{} ({})", s.system_id, s.label))
            .collect();
        
        let table_names: Vec<String> = tables.iter()
            .map(|t| format!("{} (system: {})", t.name, t.system))
            .collect();
        
        let prompt = format!(
            r#"Analyze this data validation query and return JSON only.

Query: "{}"

Available Systems: {}
Available Tables: {}

Determine the constraint type and extract details. Return JSON:
{{
  "constraint_type": "value" | "nullability" | "uniqueness" | "referential" | "aggregation" | "cross_column" | "format" | "drift",
  "system": "system_id",
  "table": "table_name" | null,
  "entity_filter": {{"column": "col", "operator": "=", "value": "val"}} | null,
  "constraint_details": {{
    // For value constraint:
    "column": "column_name",
    "operator": ">" | "<" | ">=" | "<=" | "=" | "!=" | "in" | "not_in",
    "threshold": number | string | array,
    
    // For nullability constraint:
    "column": "column_name",
    "must_be_null": true | false,
    "min_completeness": 0.0-1.0 | null,
    
    // For uniqueness constraint:
    "columns": ["col1", "col2"],
    
    // For referential constraint:
    "fk_column": "foreign_key_column",
    "ref_table": "reference_table_name",
    "ref_column": "reference_column_name",
    
    // For aggregation constraint:
    "group_by": ["col1", "col2"],
    "agg_type": "sum" | "avg" | "count" | "max" | "min",
    "column": "column_name",
    "operator": ">" | "<" | ">=" | "<=" | "=" | "!=",
    "threshold": number | string,
    
    // For cross-column constraint:
    "expression": "col1 <= col2",
    "condition": "col3 = 'value'" | null,
    
    // For format constraint:
    "column": "column_name",
    "pattern": "regex_pattern_or_format",
    "pattern_type": "regex" | "format" | "length" | "digits",
    
    // For drift constraint:
    "column": "column_name",
    "baseline_path": "path_or_reference" | null,
    "threshold": 0.1,
    "metric": "mean_change" | "distribution_shift" | "std_dev_change"
  }}
}}

Examples:
- "MSME can't have ledger >5000" -> constraint_type: "value", entity_filter: {{"column": "psl_type", "operator": "=", "value": "MSME"}}, constraint_details: {{"column": "ledger", "operator": "<=", "threshold": 5000}}
  Note: For entity filters, try to detect the actual column name. Common patterns:
  * "MSME" -> look for columns like "psl_type", "msme_flag", "msme_category", "psl_category"
  * "EDL" -> look for columns like "product_type", "edl_flag", "product_category"
  * Match the column name from available tables, or use a generic name if unsure (will be auto-detected from data)
- "customer_id cannot be null" -> constraint_type: "nullability", constraint_details: {{"column": "customer_id", "must_be_null": false}}
- "loan_id must be unique" -> constraint_type: "uniqueness", constraint_details: {{"columns": ["loan_id"]}}
- "loan.customer_id must exist in customer table" -> constraint_type: "referential", constraint_details: {{"fk_column": "customer_id", "ref_table": "customer", "ref_column": "customer_id"}}
- "Sum(disbursed) per day must equal control_total" -> constraint_type: "aggregation", constraint_details: {{"group_by": ["day"], "agg_type": "sum", "column": "disbursed", "operator": "=", "threshold": "control_total"}}
- "disbursement_date <= emi_start_date" -> constraint_type: "cross_column", constraint_details: {{"expression": "disbursement_date <= emi_start_date"}}
- "PAN must match regex" -> constraint_type: "format", constraint_details: {{"column": "PAN", "pattern": "^[A-Z]{{5}}[0-9]{{4}}[A-Z]$", "pattern_type": "regex"}}
- "Mean balance should not change >10% vs yesterday" -> constraint_type: "drift", constraint_details: {{"column": "balance", "threshold": 0.1, "metric": "mean_change"}}"#,
            query,
            systems.join(", "),
            table_names.join(", ")
        );
        
        let response = self.call_llm(&prompt).await?;
        
        // Clean response
        let cleaned_response = response
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        
        // Parse JSON response
        let interpretation: ValidationQueryInterpretation = serde_json::from_str(&cleaned_response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse validation query interpretation: {}. Response: {}", e, cleaned_response)))?;
        
        Ok(interpretation)
    }
    
    pub async fn call_llm(&self, prompt: &str) -> Result<String> {
        // For now, return dummy response if API key is dummy
        if self.api_key == "dummy-api-key" {
            // Check if this is an intent compilation prompt
            if prompt.contains("Intent Compiler") || prompt.contains("task_type") || prompt.contains("IntentSpec") {
                // Extract systems from prompt
                let system_a = if prompt.contains("khatabook") || prompt.contains("kb") {
                    "khatabook"
                } else if prompt.contains("system_a") || prompt.contains("System A") {
                    "system_a"
                } else {
                    "khatabook" // default
                };
                
                let system_b = if prompt.contains("tb") || prompt.contains("tally") {
                    "tb"
                } else if prompt.contains("system_b") || prompt.contains("System B") {
                    "system_b"
                } else {
                    "tb" // default
                };
                
                // Extract metric
                let metric = if prompt.contains("tos") || prompt.contains("TOS") {
                    "tos"
                } else if prompt.contains("interest") {
                    "interest"
                } else if prompt.contains("fees") {
                    "fees"
                } else {
                    "tos" // default
                };
                
                // Extract date if present
                let date_match = regex::Regex::new(r"\d{4}-\d{2}-\d{2}").ok();
                let as_of_date = date_match
                    .and_then(|re| re.find(prompt))
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_else(|| "null".to_string());
                
                // Return IntentSpec format
                return Ok(format!(
                    r#"{{
  "task_type": "RCA",
  "target_metrics": ["{}"],
  "entities": ["loan"],
  "constraints": [],
  "grain": ["loan_id"],
  "time_scope": {},
  "systems": ["{}", "{}"],
  "validation_constraint": null
}}"#,
                    metric,
                    if as_of_date != "null" {
                        format!(r#"{{"as_of_date": "{}", "start_date": null, "end_date": null, "time_grain": null}}"#, as_of_date)
                    } else {
                        "null".to_string()
                    },
                    system_a,
                    system_b
                ));
            }
            
            // Check if this is a tool selection prompt
            if prompt.contains("determine which tools to use") || prompt.contains("\"steps\"") || prompt.contains("tool_name") {
                // Return tool execution plan format
                return Ok(r#"{
  "reasoning": "Standard reconciliation query - using compare tool for final comparison",
  "steps": [
    {
      "tool_name": "compare",
      "parameters": {},
      "reasoning": "Compare System A and System B results to identify discrepancies",
      "confidence": 0.95
    }
  ]
}"#.to_string());
            }
            
            // Check if this is a validation query interpretation prompt
            if prompt.contains("validation query") || prompt.contains("constraint") || prompt.contains("violation") {
                // Return validation query interpretation format
                return Ok(r#"{
  "constraint_type": "value",
  "column": "unknown",
  "operator": ">",
  "threshold": 0,
  "entity_filter": null,
  "confidence": 0.9
}"#.to_string());
            }
            
            // Check if this is a CSV analysis prompt
            if prompt.contains("CSV") || prompt.contains("csv") || prompt.contains("grain_column") {
                // Return CSV analysis format
                return Ok(r#"{
  "grain_column": "id",
  "metric_column": null,
  "aggregation_type": "count",
  "metric_name": "count",
  "filters": []
}"#.to_string());
            }
            
            // Default: query interpretation format
            // Smart dummy response: extract system names from prompt
            // Check for System A first
            let system_a = if prompt.contains("system_a") || prompt.contains("System A") {
                "system_a"
            } else if prompt.contains("khatabook") || prompt.contains("kb") {
                "khatabook"
            } else {
                "system_a" // default fallback for tests
            };
            
            // Check for System B, C, D, E, F by looking for "vs System X" pattern
            let system_b = if prompt.contains("vs System F") || prompt.contains("vs system_f") {
                "system_f"
            } else if prompt.contains("vs System E") || prompt.contains("vs system_e") {
                "system_e"
            } else if prompt.contains("vs System D") || prompt.contains("vs system_d") {
                "system_d"
            } else if prompt.contains("vs System C") || prompt.contains("vs system_c") {
                "system_c"
            } else if prompt.contains("system_b") || prompt.contains("System B") {
                "system_b"
            } else if prompt.contains("tb") || prompt.contains("tally") {
                "tb"
            } else {
                "system_b" // default fallback
            };
            
            // Extract metric if present
            let metric = if prompt.contains("tos") || prompt.contains("TOS") {
                "tos"
            } else if prompt.contains("interest") {
                "interest"
            } else if prompt.contains("fees") {
                "fees"
            } else {
                "tos" // default
            };
            
            // Extract date if present
            let date_match = regex::Regex::new(r"\d{4}-\d{2}-\d{2}").ok();
            let as_of_date = date_match
                .and_then(|re| re.find(prompt))
                .map(|m| format!("\"{}\"", m.as_str()))
                .unwrap_or_else(|| "null".to_string());
            
            return Ok(format!(
                r#"{{"system_a": "{}", "system_b": "{}", "metric": "{}", "as_of_date": {}, "confidence": 0.95}}"#,
                system_a, system_b, metric, as_of_date
            ));
        }
        
        let client = reqwest::Client::new();
        // Token-optimized: concise system message, lower max_completion_tokens for JSON responses
        // Use max_completion_tokens for newer models (like gpt-5.2), fallback to max_tokens for older models
        let mut body = serde_json::json!({
            "model": self.model,
            "messages": [
                {"role": "system", "content": "Return JSON only, no text."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,
        });
        
        // Use max_completion_tokens for newer models, max_tokens for older ones
        // For reasoning models (gpt-5.2, o1), need more tokens as they use reasoning tokens
        if self.model.starts_with("gpt-5") || self.model.contains("o1") {
            // Reasoning models need more tokens - reasoning tokens + completion tokens
            body["max_completion_tokens"] = serde_json::json!(2000);
        } else if self.model.starts_with("gpt-4") {
            body["max_completion_tokens"] = serde_json::json!(500);
        } else {
            body["max_tokens"] = serde_json::json!(500);
        }
        
        let response = client
            .post(&format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| RcaError::Llm(format!("LLM API call failed: {}", e)))?;
        
        // Check HTTP status
        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(RcaError::Llm(format!("LLM API error ({}): {}", status, error_text)));
        }
        
        let response_json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM response: {}", e)))?;
        
        // Check for error in response
        if let Some(error) = response_json.get("error") {
            return Err(RcaError::Llm(format!("LLM API error: {}", serde_json::to_string(error).unwrap_or_else(|_| "Unknown error".to_string()))));
        }
        
        // Extract content with better error message
        let choices = response_json.get("choices")
            .and_then(|c| c.as_array())
            .ok_or_else(|| RcaError::Llm(format!("No choices array in LLM response. Response: {}", serde_json::to_string(&response_json).unwrap_or_else(|_| "Could not serialize".to_string()))))?;
        
        if choices.is_empty() {
            return Err(RcaError::Llm(format!("Empty choices array in LLM response. Response: {}", serde_json::to_string(&response_json).unwrap_or_else(|_| "Could not serialize".to_string()))));
        }
        
        // Check for finish_reason - if it's "length" or "content_filter", content might be truncated
        if let Some(finish_reason) = choices[0].get("finish_reason").and_then(|r| r.as_str()) {
            if finish_reason == "length" {
                eprintln!("⚠️  Warning: LLM response was truncated due to length limit");
            } else if finish_reason == "content_filter" {
                return Err(RcaError::Llm("LLM response was filtered by content policy".to_string()));
            }
        }
        
        let content = choices[0]["message"]["content"]
            .as_str()
            .ok_or_else(|| {
                let response_str = serde_json::to_string(&response_json).unwrap_or_else(|_| "Could not serialize".to_string());
                eprintln!("Debug: Full response structure: {}", response_str);
                RcaError::Llm(format!("No content in LLM response. Response structure: {}", response_str))
            })?;
        
        if content.is_empty() {
            return Err(RcaError::Llm(format!("Empty content in LLM response. Full response: {}", serde_json::to_string(&response_json).unwrap_or_else(|_| "Could not serialize".to_string()))));
        }
        
        Ok(content.to_string())
    }
    
    /// Compile natural language intent to JSON specification
    pub async fn compile_intent(&self, system_prompt: &str, user_prompt: &str) -> Result<String> {
        // Combine prompts
        let combined_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
        
        // Use existing call_llm which handles mock mode
        self.call_llm(&combined_prompt).await
    }
    
    /// Call LLM with function calling support
    pub async fn call_llm_with_functions(
        &self,
        messages: &[ChatMessage],
        functions: &[FunctionDefinition],
    ) -> Result<FunctionCall> {
        // Handle dummy mode
        if self.api_key == "dummy-api-key" {
            // Return a dummy function call for testing
            return Ok(FunctionCall {
                name: "generate_sql_intent".to_string(),
                arguments: r#"{"metrics": ["tos"], "dimensions": [], "filters": [], "limit": null}"#.to_string(),
            });
        }
        
        let client = reqwest::Client::new();
        
        // Build messages for API
        let api_messages: Vec<serde_json::Value> = messages
            .iter()
            .map(|m| {
                let mut msg = serde_json::json!({
                    "role": m.role,
                });
                
                if let Some(ref content) = m.content {
                    msg["content"] = serde_json::json!(content);
                }
                
                if let Some(ref function_call) = m.function_call {
                    msg["function_call"] = serde_json::json!({
                        "name": function_call.name,
                        "arguments": function_call.arguments,
                    });
                }
                
                if let Some(ref name) = m.name {
                    msg["name"] = serde_json::json!(name);
                }
                
                msg
            })
            .collect();
        
        // Build functions array
        let api_functions: Vec<serde_json::Value> = functions
            .iter()
            .map(|f| {
                serde_json::json!({
                    "name": f.name,
                    "description": f.description,
                    "parameters": f.parameters,
                })
            })
            .collect();
        
        let mut body = serde_json::json!({
            "model": self.model,
            "messages": api_messages,
            "functions": api_functions,
            "function_call": "auto", // Let model decide when to call functions
            "temperature": 0.1,
        });
        
        // Set token limits
        if self.model.starts_with("gpt-5") || self.model.contains("o1") {
            body["max_completion_tokens"] = serde_json::json!(2000);
        } else if self.model.starts_with("gpt-4") {
            body["max_completion_tokens"] = serde_json::json!(500);
        } else {
            body["max_tokens"] = serde_json::json!(500);
        }
        
        let response = client
            .post(&format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| RcaError::Llm(format!("LLM API call failed: {}", e)))?;
        
        // Check HTTP status
        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(RcaError::Llm(format!("LLM API error ({}): {}", status, error_text)));
        }
        
        let response_json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM response: {}", e)))?;
        
        // Check for error
        if let Some(error) = response_json.get("error") {
            return Err(RcaError::Llm(format!("LLM API error: {}", serde_json::to_string(error).unwrap_or_else(|_| "Unknown error".to_string()))));
        }
        
        // Extract function call from response
        let choices = response_json.get("choices")
            .and_then(|c| c.as_array())
            .ok_or_else(|| RcaError::Llm("No choices array in LLM response".to_string()))?;
        
        if choices.is_empty() {
            return Err(RcaError::Llm("Empty choices array in LLM response".to_string()));
        }
        
        let message = &choices[0]["message"];
        
        // Check if there's a function call
        if let Some(function_call_json) = message.get("function_call") {
            let name = function_call_json["name"]
                .as_str()
                .ok_or_else(|| RcaError::Llm("No function name in function_call".to_string()))?
                .to_string();
            
            let arguments = function_call_json["arguments"]
                .as_str()
                .ok_or_else(|| RcaError::Llm("No arguments in function_call".to_string()))?
                .to_string();
            
            Ok(FunctionCall { name, arguments })
        } else {
            // If no function call, check if there's content (model chose not to call function)
            let content = message.get("content")
                .and_then(|c| c.as_str())
                .unwrap_or("");
            
            Err(RcaError::Llm(format!(
                "LLM did not call a function. Response: {}",
                content
            )))
        }
    }
}

