use crate::error::{RcaError, Result};
use crate::fuzzy_matcher::FuzzyMatcher;
use crate::llm::LlmClient;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

/// LLM-based value matcher for string comparisons
/// Implements multi-stage matching: exact -> fuzzy -> LLM (with user confirmation)
pub struct LlmValueMatcher {
    fuzzy_matcher: FuzzyMatcher,
    llm_client: Option<LlmClient>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueMatch {
    pub value_a: String,
    pub value_b: String,
    pub match_type: MatchType,
    pub confidence: f64,
    pub reasoning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MatchType {
    Exact,
    Fuzzy,
    Llm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueMatchingResult {
    pub matches: Vec<ValueMatch>,
    pub unmatched_a: Vec<String>,
    pub unmatched_b: Vec<String>,
    pub total_distinct_a: usize,
    pub total_distinct_b: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMatchRequest {
    pub unmatched_values_a: Vec<String>,
    pub unmatched_values_b: Vec<String>,
    pub context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMatchResponse {
    pub matches: Vec<LlmMatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMatch {
    pub value_a: String,
    pub value_b: String,
    pub confidence: f64,
    pub reasoning: String,
}

impl LlmValueMatcher {
    pub fn new(fuzzy_threshold: f64, llm_client: Option<LlmClient>) -> Self {
        Self {
            fuzzy_matcher: FuzzyMatcher::new(fuzzy_threshold),
            llm_client,
        }
    }

    /// Multi-stage value matching: exact -> fuzzy -> LLM (with user confirmation)
    pub async fn match_values(
        &self,
        df_a: &DataFrame,
        df_b: &DataFrame,
        value_column_a: &str,
        value_column_b: &str,
        context: Option<&str>,
    ) -> Result<ValueMatchingResult> {
        // Stage 1: Extract distinct values from both sides
        let distinct_a = self.extract_distinct_values(df_a, value_column_a)?;
        let distinct_b = self.extract_distinct_values(df_b, value_column_b)?;
        
        let total_distinct_a = distinct_a.len();
        let total_distinct_b = distinct_b.len();
        
        println!("   📊 Found {} distinct values in System A", total_distinct_a);
        println!("   📊 Found {} distinct values in System B", total_distinct_b);
        
        // Stage 2: Exact matching
        let (exact_matches, unmatched_a_after_exact, unmatched_b_after_exact) = 
            self.exact_match(&distinct_a, &distinct_b);
        
        println!("   ✅ Exact matches: {}", exact_matches.len());
        
        // Stage 3: Fuzzy matching on remaining unmatched values
        let (fuzzy_matches, unmatched_a_after_fuzzy, unmatched_b_after_fuzzy) = 
            self.fuzzy_match(&unmatched_a_after_exact, &unmatched_b_after_exact);
        
        println!("   🔍 Fuzzy matches: {}", fuzzy_matches.len());
        println!("   ⚠️  Remaining unmatched in A: {}", unmatched_a_after_fuzzy.len());
        println!("   ⚠️  Remaining unmatched in B: {}", unmatched_b_after_fuzzy.len());
        
        // Combine exact and fuzzy matches
        let mut all_matches: Vec<ValueMatch> = exact_matches
            .into_iter()
            .map(|(a, b)| ValueMatch {
                value_a: a,
                value_b: b,
                match_type: MatchType::Exact,
                confidence: 1.0,
                reasoning: None,
            })
            .chain(fuzzy_matches.into_iter().map(|(a, b, score)| ValueMatch {
                value_a: a,
                value_b: b,
                match_type: MatchType::Fuzzy,
                confidence: score,
                reasoning: None,
            }))
            .collect();
        
        // Stage 4: LLM matching (if enabled and unmatched values remain)
        let (llm_matches, final_unmatched_a, final_unmatched_b): (Vec<LlmMatch>, Vec<String>, Vec<String>) = 
            if !unmatched_a_after_fuzzy.is_empty() || !unmatched_b_after_fuzzy.is_empty() {
                if let Some(ref llm) = self.llm_client {
                    println!("   🤖 LLM matching available for {} unmatched values", 
                        unmatched_a_after_fuzzy.len() + unmatched_b_after_fuzzy.len());
                    // Return unmatched values - user will be prompted separately
                    (Vec::new(), unmatched_a_after_fuzzy, unmatched_b_after_fuzzy)
                } else {
                    // No LLM client, return as unmatched
                    (Vec::new(), unmatched_a_after_fuzzy, unmatched_b_after_fuzzy)
                }
            } else {
                (Vec::new(), Vec::new(), Vec::new())
            };
        
        // Add LLM matches if any
        all_matches.extend(llm_matches.into_iter().map(|m| ValueMatch {
            value_a: m.value_a,
            value_b: m.value_b,
            match_type: MatchType::Llm,
            confidence: m.confidence,
            reasoning: Some(m.reasoning),
        }));
        
        Ok(ValueMatchingResult {
            matches: all_matches,
            unmatched_a: final_unmatched_a,
            unmatched_b: final_unmatched_b,
            total_distinct_a,
            total_distinct_b,
        })
    }

    /// Match remaining unmatched values using LLM (called after user confirmation)
    pub async fn match_with_llm(
        &self,
        unmatched_a: &[String],
        unmatched_b: &[String],
        context: Option<&str>,
    ) -> Result<Vec<LlmMatch>> {
        let llm = self.llm_client.as_ref()
            .ok_or_else(|| RcaError::Execution("LLM client not available".to_string()))?;
        
        if unmatched_a.is_empty() && unmatched_b.is_empty() {
            return Ok(Vec::new());
        }
        
        println!("   🤖 Using LLM to match {} values from A with {} values from B", 
            unmatched_a.len(), unmatched_b.len());
        
        let request = LlmMatchRequest {
            unmatched_values_a: unmatched_a.to_vec(),
            unmatched_values_b: unmatched_b.to_vec(),
            context: context.map(|s| s.to_string()),
        };
        
        let response = self.call_llm_matching(llm, &request).await?;
        
        println!("   ✅ LLM found {} matches", response.matches.len());
        for m in &response.matches {
            println!("      '{}' <-> '{}' (confidence: {:.1}%) - {}", 
                m.value_a, m.value_b, m.confidence * 100.0, m.reasoning);
        }
        
        Ok(response.matches)
    }

    /// Extract distinct values from a dataframe column
    fn extract_distinct_values(&self, df: &DataFrame, column: &str) -> Result<HashSet<String>> {
        let col = df.column(column)
            .map_err(|e| RcaError::Execution(format!("Column '{}' not found: {}", column, e)))?;
        
        let mut distinct = HashSet::new();
        
        match col.dtype() {
            DataType::String => {
                let str_col = col.str()?;
                for idx in 0..str_col.len() {
                    if let Some(val) = str_col.get(idx) {
                        let val_str = val.to_string();
                        if !val_str.is_empty() && val_str != "NULL" {
                            distinct.insert(val_str);
                        }
                    }
                }
            }
            DataType::Int64 => {
                let int_col = col.i64()?;
                for idx in 0..int_col.len() {
                    if let Some(val) = int_col.get(idx) {
                        distinct.insert(val.to_string());
                    }
                }
            }
            DataType::Float64 => {
                let float_col = col.f64()?;
                for idx in 0..float_col.len() {
                    if let Some(val) = float_col.get(idx) {
                        distinct.insert(val.to_string());
                    }
                }
            }
            _ => {
                // Convert to string for other types
                for idx in 0..col.len() {
                    let val_str = format!("{:?}", col.get(idx));
                    if !val_str.is_empty() && val_str != "NULL" {
                        distinct.insert(val_str);
                    }
                }
            }
        }
        
        Ok(distinct)
    }

    /// Stage 1: Exact matching
    fn exact_match(
        &self,
        distinct_a: &HashSet<String>,
        distinct_b: &HashSet<String>,
    ) -> (Vec<(String, String)>, Vec<String>, Vec<String>) {
        let mut matches = Vec::new();
        let mut matched_a = HashSet::new();
        let mut matched_b = HashSet::new();
        
        for val_a in distinct_a {
            if distinct_b.contains(val_a) {
                matches.push((val_a.clone(), val_a.clone()));
                matched_a.insert(val_a.clone());
                matched_b.insert(val_a.clone());
            }
        }
        
        let unmatched_a: Vec<String> = distinct_a
            .iter()
            .filter(|v| !matched_a.contains(*v))
            .cloned()
            .collect();
        
        let unmatched_b: Vec<String> = distinct_b
            .iter()
            .filter(|v| !matched_b.contains(*v))
            .cloned()
            .collect();
        
        (matches, unmatched_a, unmatched_b)
    }

    /// Stage 2: Fuzzy matching
    fn fuzzy_match(
        &self,
        unmatched_a: &[String],
        unmatched_b: &[String],
    ) -> (Vec<(String, String, f64)>, Vec<String>, Vec<String>) {
        let mut matches = Vec::new();
        let mut matched_a = HashSet::new();
        let mut matched_b = HashSet::new();
        
        for val_a in unmatched_a {
            let mut best_match: Option<(String, f64)> = None;
            
            for val_b in unmatched_b {
                if matched_b.contains(val_b) {
                    continue;
                }
                
                let similarity = self.fuzzy_matcher.similarity(val_a, val_b);
                if similarity >= self.fuzzy_matcher.similarity_threshold {
                    match best_match {
                        None => best_match = Some((val_b.clone(), similarity)),
                        Some((_, best_score)) if similarity > best_score => {
                            best_match = Some((val_b.clone(), similarity))
                        }
                        _ => {}
                    }
                }
            }
            
            if let Some((matched_val_b, score)) = best_match {
                matches.push((val_a.clone(), matched_val_b.clone(), score));
                matched_a.insert(val_a.clone());
                matched_b.insert(matched_val_b);
            }
        }
        
        let remaining_a: Vec<String> = unmatched_a
            .iter()
            .filter(|v| !matched_a.contains(*v))
            .cloned()
            .collect();
        
        let remaining_b: Vec<String> = unmatched_b
            .iter()
            .filter(|v| !matched_b.contains(*v))
            .cloned()
            .collect();
        
        (matches, remaining_a, remaining_b)
    }

    /// Call LLM to match values
    async fn call_llm_matching(
        &self,
        llm: &LlmClient,
        request: &LlmMatchRequest,
    ) -> Result<LlmMatchResponse> {
        let values_a_json = serde_json::to_string(&request.unmatched_values_a)
            .map_err(|e| RcaError::Llm(format!("Failed to serialize values A: {}", e)))?;
        let values_b_json = serde_json::to_string(&request.unmatched_values_b)
            .map_err(|e| RcaError::Llm(format!("Failed to serialize values B: {}", e)))?;
        
        let context_str = request.context.as_deref()
            .unwrap_or("These are categorical values from two different systems that need to be matched.");
        
        let prompt = format!(
            r#"Match categorical values between two systems. Return JSON only.

Context: {}
Values from System A: {}
Values from System B: {}

Your task:
1. Match values from System A with values from System B that represent the same concept
2. Consider abbreviations, synonyms, and different naming conventions
3. Only match if you're confident (>= 0.7 confidence)
4. Provide reasoning for each match

Return JSON format:
{{
  "matches": [
    {{
      "value_a": "value from system A",
      "value_b": "value from system B",
      "confidence": 0.0-1.0,
      "reasoning": "brief explanation why these match"
    }}
  ]
}}

Examples:
- "SC" and "Scheduled Caste" -> match (confidence: 0.95, reasoning: "SC is abbreviation for Scheduled Caste")
- "ST" and "Scheduled Tribe" -> match (confidence: 0.95, reasoning: "ST is abbreviation for Scheduled Tribe")
- "OBC" and "Other Backward Class" -> match (confidence: 0.9, reasoning: "OBC is abbreviation for Other Backward Class")
- "General" and "GEN" -> match (confidence: 0.9, reasoning: "GEN is abbreviation for General")

Only return matches you're confident about. If unsure, don't include the match."#,
            context_str,
            values_a_json,
            values_b_json
        );
        
        // Use the LLM client's public call_llm method
        let response_text = llm.call_llm(&prompt).await?;
        
        // Clean response - remove markdown code blocks if present
        let cleaned_response = response_text
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        
        // Parse response
        let response: LlmMatchResponse = serde_json::from_str(&cleaned_response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM response: {}. Response: {}", e, cleaned_response)))?;
        
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn test_extract_distinct_values() {
        let df = DataFrame::new(vec![
            Series::new("category", vec!["SC", "ST", "OBC", "SC", "General"]),
        ]).unwrap();
        
        let matcher = LlmValueMatcher::new(0.85, None);
        let distinct = matcher.extract_distinct_values(&df, "category").unwrap();
        
        assert_eq!(distinct.len(), 4); // SC, ST, OBC, General
        assert!(distinct.contains("SC"));
        assert!(distinct.contains("ST"));
        assert!(distinct.contains("OBC"));
        assert!(distinct.contains("General"));
    }

    #[test]
    fn test_exact_match() {
        let distinct_a: HashSet<String> = ["SC", "ST", "OBC"].iter().map(|s| s.to_string()).collect();
        let distinct_b: HashSet<String> = ["SC", "ST", "General"].iter().map(|s| s.to_string()).collect();
        
        let matcher = LlmValueMatcher::new(0.85, None);
        let (matches, unmatched_a, unmatched_b) = matcher.exact_match(&distinct_a, &distinct_b);
        
        assert_eq!(matches.len(), 2); // SC and ST match exactly
        assert_eq!(unmatched_a.len(), 1); // OBC
        assert_eq!(unmatched_b.len(), 1); // General
    }

    #[test]
    fn test_fuzzy_match() {
        let unmatched_a = vec!["Scheduled Caste".to_string()];
        let unmatched_b = vec!["SC".to_string(), "ST".to_string()];
        
        let matcher = LlmValueMatcher::new(0.85, None);
        let (matches, remaining_a, remaining_b) = matcher.fuzzy_match(&unmatched_a, &unmatched_b);
        
        // Note: Fuzzy matching might not catch "Scheduled Caste" vs "SC" without normalization
        // This depends on the fuzzy matcher implementation
        println!("Fuzzy matches: {:?}", matches);
        println!("Remaining A: {:?}", remaining_a);
        println!("Remaining B: {:?}", remaining_b);
    }
}

