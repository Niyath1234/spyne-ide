//! Rule Reasoner - Chain-of-Thought Reasoning for Rule Selection
//! 
//! Uses LLM with chain-of-thought reasoning to:
//! 1. Analyze available rules from knowledge base
//! 2. Match rules to query filters (e.g., product type)
//! 3. Reason about which rule applies
//! 4. Handle missing rules by asking for clarification

use crate::error::{RcaError, Result};
use crate::intent_compiler::IntentSpec;
use crate::metadata::{Metadata, Rule};
use crate::llm::LlmClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, debug, warn};

/// Selected rule with reasoning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedRule {
    pub rule: Rule,
    pub reasoning: String,
    pub confidence: f64,
    pub alternatives_considered: Vec<Rule>,
    pub chain_of_thought: Vec<ReasoningStep>,
}

/// Step in chain-of-thought reasoning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReasoningStep {
    pub step: String,
    pub reasoning: String,
    pub conclusion: String,
}

/// Rule Reasoner with Chain-of-Thought Reasoning
pub struct RuleReasoner {
    llm: LlmClient,
    metadata: Metadata,
}

impl RuleReasoner {
    pub fn new(llm: LlmClient, metadata: Metadata) -> Self {
        Self { llm, metadata }
    }

    /// Select the best rule using chain-of-thought reasoning
    pub async fn select_rule(
        &self,
        intent: &IntentSpec,
        system: &str,
        metric: &str,
    ) -> Result<SelectedRule> {
        info!("🔍 RuleReasoner: Selecting rule for {} {} with chain-of-thought reasoning", system, metric);
        
        // Step 1: Get all candidate rules
        let candidate_rules = self.metadata.get_rules_for_system_metric(system, metric);
        
        if candidate_rules.is_empty() {
            return Err(RcaError::Llm(format!(
                "No rules found for system '{}' and metric '{}'",
                system, metric
            )));
        }

        // Step 2: Extract filters from intent
        let filters = self.extract_filters_from_intent(intent);
        
        // Step 3: Filter rules by conditions
        let matching_rules = self.filter_rules_by_conditions(&candidate_rules, &filters);
        
        // Step 4: Use logical reasoning first, LLM only when needed
        if matching_rules.len() == 1 {
            // Single match - use deterministic logical reasoning (no LLM needed)
            let reasoning = self.generate_deterministic_reasoning(&matching_rules[0], &filters, &candidate_rules);
            Ok(SelectedRule {
                rule: matching_rules[0].clone(),
                reasoning: reasoning.reasoning,
                confidence: 0.95,
                alternatives_considered: candidate_rules.iter()
                    .filter(|r| r.id != matching_rules[0].id)
                    .cloned()
                    .collect(),
                chain_of_thought: reasoning.steps,
            })
        } else if matching_rules.is_empty() {
            // No match - ask for clarification (LLM needed for user interaction)
            self.handle_missing_rule(system, metric, &filters, &candidate_rules).await
        } else {
            // Multiple matches - try logical reasoning first, fallback to LLM if ambiguous
            match self.reason_about_multiple_rules_logical(&matching_rules, &filters, &candidate_rules) {
                Some(selected) => Ok(selected),
                None => {
                    // Ambiguous case - use LLM reasoning
                    info!("Multiple rules match but logic cannot determine best - using LLM reasoning");
                    self.reason_about_multiple_rules(&matching_rules, &filters, &candidate_rules).await
                }
            }
        }
    }

    /// Extract filters from intent constraints
    fn extract_filters_from_intent(&self, intent: &IntentSpec) -> HashMap<String, String> {
        let mut filters = HashMap::new();
        
        for constraint in &intent.constraints {
            if let (Some(column), Some(value)) = (&constraint.column, &constraint.value) {
                if let Some(value_str) = value.as_str() {
                    filters.insert(column.clone(), value_str.to_string());
                }
            }
        }
        
        // Also check time_scope for as_of_date
        if let Some(ref time_scope) = intent.time_scope {
            if let Some(ref as_of_date) = time_scope.as_of_date {
                filters.insert("as_of_date".to_string(), as_of_date.clone());
            }
        }
        
        filters
    }

    /// Filter rules by their filter_conditions
    fn filter_rules_by_conditions(
        &self,
        rules: &[Rule],
        filters: &HashMap<String, String>,
    ) -> Vec<Rule> {
        let mut matching = Vec::new();
        
        debug!("Filtering {} rules with filters: {:?}", rules.len(), filters);
        
        for rule in rules {
            // Check if rule has filter_conditions
            if let Some(ref filter_conditions) = rule.computation.filter_conditions {
                debug!("Rule {} has filter_conditions: {:?}", rule.id, filter_conditions);
                // Check if all filter conditions match
                let mut all_match = true;
                for (key, value) in filter_conditions {
                    // Try multiple key variations (loan_type, product_type, etc.)
                    let filter_value = filters.get(key)
                        .or_else(|| {
                            // Try case-insensitive match
                            filters.iter()
                                .find(|(k, _)| k.to_lowercase() == key.to_lowercase())
                                .map(|(_, v)| v)
                        });
                    
                    if let Some(filter_value) = filter_value {
                        // Check if values match (case-insensitive)
                        if value.to_lowercase() != filter_value.to_lowercase() {
                            debug!("Rule {} filter {}={} doesn't match query value {}", 
                                rule.id, key, value, filter_value);
                            all_match = false;
                            break;
                        } else {
                            debug!("Rule {} filter {}={} MATCHES query value {}", 
                                rule.id, key, value, filter_value);
                        }
                    } else {
                        // Filter key not in query - rule doesn't match
                        debug!("Rule {} requires filter {}={} but query doesn't have it", 
                            rule.id, key, value);
                        all_match = false;
                        break;
                    }
                }
                
                if all_match {
                    debug!("✅ Rule {} matches all filter conditions", rule.id);
                    matching.push(rule.clone());
                }
            } else {
                // Rule has no filter_conditions - it's a generic rule
                // Only include if no specific filters were requested
                if filters.is_empty() || !filters.iter().any(|(k, _)| k.to_lowercase().contains("loan_type") || k.to_lowercase().contains("product")) {
                    debug!("Rule {} is generic (no filters) - including", rule.id);
                    matching.push(rule.clone());
                } else {
                    debug!("Rule {} is generic but query has filters - excluding", rule.id);
                }
            }
        }
        
        debug!("Found {} matching rules after filtering", matching.len());
        matching
    }

    /// Generate deterministic logical reasoning (no LLM needed)
    fn generate_deterministic_reasoning(
        &self,
        rule: &Rule,
        filters: &HashMap<String, String>,
        all_rules: &[Rule],
    ) -> ChainOfThought {
        let mut steps = Vec::new();
        let mut reasoning_parts = Vec::new();
        
        // Step 1: Filter matching
        if let Some(ref filter_conditions) = rule.computation.filter_conditions {
            let mut filter_matches = Vec::new();
            for (key, value) in filter_conditions {
                if let Some(query_value) = filters.get(key) {
                    filter_matches.push(format!("{}={}", key, query_value));
                }
            }
            if !filter_matches.is_empty() {
                steps.push(ReasoningStep {
                    step: "Filter Matching".to_string(),
                    reasoning: format!("Query filters match rule's filter conditions: {}", filter_matches.join(", ")),
                    conclusion: "Rule is applicable for this query".to_string(),
                });
                reasoning_parts.push(format!("Rule {} matches query filters", rule.id));
            }
        } else {
            steps.push(ReasoningStep {
                step: "Generic Rule".to_string(),
                reasoning: "Rule has no specific filter conditions - applies to all cases".to_string(),
                conclusion: "Rule is applicable".to_string(),
            });
            reasoning_parts.push(format!("Rule {} is a generic rule", rule.id));
        }
        
        // Step 2: Entity analysis
        steps.push(ReasoningStep {
            step: "Entity Analysis".to_string(),
            reasoning: format!("Rule uses entities: {:?} to compute the metric", rule.computation.source_entities),
            conclusion: "These entities provide the necessary data for calculation".to_string(),
        });
        reasoning_parts.push(format!("Uses entities: {:?}", rule.computation.source_entities));
        
        // Step 3: Formula explanation
        steps.push(ReasoningStep {
            step: "Formula Application".to_string(),
            reasoning: format!("Formula: {} - {}", rule.computation.formula, rule.computation.description),
            conclusion: "This formula correctly computes the metric for the given context".to_string(),
        });
        reasoning_parts.push(format!("Formula: {}", rule.computation.formula));
        
        // Step 4: Alternative comparison
        let alternatives: Vec<String> = all_rules.iter()
            .filter(|r| r.id != rule.id && r.system == rule.system && r.metric == rule.metric)
            .map(|r| {
                if let Some(ref filters) = r.computation.filter_conditions {
                    format!("{} (filters: {:?})", r.id, filters)
                } else {
                    format!("{} (generic)", r.id)
                }
            })
            .collect();
        
        if !alternatives.is_empty() {
            steps.push(ReasoningStep {
                step: "Alternative Rules".to_string(),
                reasoning: format!("Other available rules: {}. These don't match because they have different filter conditions or are generic.", alternatives.join(", ")),
                conclusion: "Selected rule is the best match".to_string(),
            });
            reasoning_parts.push(format!("Better than alternatives: {}", alternatives.join(", ")));
        }
        
        let reasoning = format!(
            "Rule selected: {}. {}",
            rule.id,
            reasoning_parts.join(". ")
        );
        
        ChainOfThought {
            reasoning,
            steps,
        }
    }
    
    /// Try to reason about multiple rules using logical reasoning (no LLM)
    /// Returns Some(SelectedRule) if logic can determine the best rule, None if ambiguous
    fn reason_about_multiple_rules_logical(
        &self,
        matching_rules: &[Rule],
        filters: &HashMap<String, String>,
        candidate_rules: &[Rule],
    ) -> Option<SelectedRule> {
        // Strategy 1: Prefer rules with more specific filter conditions
        let mut rules_with_filters: Vec<&Rule> = matching_rules.iter()
            .filter(|r| r.computation.filter_conditions.is_some())
            .collect();
        
        if rules_with_filters.len() == 1 {
            // Only one rule with filters - it's the most specific match
            let rule = rules_with_filters[0].clone();
            let reasoning = self.generate_deterministic_reasoning(&rule, filters, candidate_rules);
            return Some(SelectedRule {
                rule,
                reasoning: reasoning.reasoning,
                confidence: 0.90,
                alternatives_considered: matching_rules.iter()
                    .filter(|r| r.id != rules_with_filters[0].id)
                    .cloned()
                    .collect(),
                chain_of_thought: reasoning.steps,
            });
        }
        
        // Strategy 2: Count matching filter conditions (more matches = better)
        let mut scored_rules: Vec<(&Rule, usize)> = matching_rules.iter()
            .map(|r| {
                let score = if let Some(ref rule_filters) = r.computation.filter_conditions {
                    rule_filters.iter()
                        .filter(|(k, v)| {
                            filters.get(k.as_str())
                                .map(|fv| v.to_lowercase() == fv.to_lowercase())
                                .unwrap_or(false)
                        })
                        .count()
                } else {
                    0
                };
                (r, score)
            })
            .collect();
        
        scored_rules.sort_by(|a, b| b.1.cmp(&a.1));
        
        if scored_rules[0].1 > scored_rules.get(1).map(|(_, s)| *s).unwrap_or(0) {
            // Clear winner by filter match count
            let rule = scored_rules[0].0.clone();
            let reasoning = self.generate_deterministic_reasoning(&rule, filters, candidate_rules);
            return Some(SelectedRule {
                rule,
                reasoning: format!("{} Selected because it matches {} filter conditions (more than alternatives)", 
                    reasoning.reasoning, scored_rules[0].1),
                confidence: 0.85,
                alternatives_considered: matching_rules.iter()
                    .filter(|r| r.id != scored_rules[0].0.id)
                    .cloned()
                    .collect(),
                chain_of_thought: reasoning.steps,
            });
        }
        
        // Strategy 3: Prefer rules that use more entities (more comprehensive)
        let mut entity_scored: Vec<(&Rule, usize)> = matching_rules.iter()
            .map(|r| (r, r.computation.source_entities.len()))
            .collect();
        
        entity_scored.sort_by(|a, b| b.1.cmp(&a.1));
        
        if entity_scored[0].1 > entity_scored.get(1).map(|(_, s)| *s).unwrap_or(0) {
            // Clear winner by entity count
            let rule = entity_scored[0].0.clone();
            let reasoning = self.generate_deterministic_reasoning(&rule, filters, candidate_rules);
            return Some(SelectedRule {
                rule,
                reasoning: format!("{} Selected because it uses {} entities (most comprehensive)", 
                    reasoning.reasoning, entity_scored[0].1),
                confidence: 0.80,
                alternatives_considered: matching_rules.iter()
                    .filter(|r| r.id != entity_scored[0].0.id)
                    .cloned()
                    .collect(),
                chain_of_thought: reasoning.steps,
            });
        }
        
        // Cannot determine logically - return None to use LLM
        None
    }
    
    /// Generate chain-of-thought reasoning for a rule using LLM (only when needed)
    async fn generate_reasoning_for_rule(
        &self,
        rule: &Rule,
        filters: &HashMap<String, String>,
        all_rules: &[Rule],
    ) -> Result<ChainOfThought> {
        let prompt = self.build_reasoning_prompt(rule, filters, all_rules);
        
        info!("🤔 Generating chain-of-thought reasoning for rule: {}", rule.id);
        
        let response = self.llm.call_llm(&prompt).await?;
        
        // Parse chain-of-thought from LLM response
        self.parse_chain_of_thought(&response, rule)
    }

    /// Build prompt for chain-of-thought reasoning
    fn build_reasoning_prompt(
        &self,
        rule: &Rule,
        filters: &HashMap<String, String>,
        all_rules: &[Rule],
    ) -> String {
        let mut prompt = String::from("You are a reasoning engine that selects the correct business rule for data reconciliation.\n\n");
        
        prompt.push_str("## Query Context\n");
        prompt.push_str(&format!("Filters requested: {:?}\n\n", filters));
        
        prompt.push_str("## Available Rules\n");
        for r in all_rules {
            prompt.push_str(&format!("\n### Rule: {}\n", r.id));
            prompt.push_str(&format!("- System: {}\n", r.system));
            prompt.push_str(&format!("- Metric: {}\n", r.metric));
            prompt.push_str(&format!("- Description: {}\n", r.computation.description));
            prompt.push_str(&format!("- Formula: {}\n", r.computation.formula));
            prompt.push_str(&format!("- Source Entities: {:?}\n", r.computation.source_entities));
            if let Some(ref filter_conditions) = r.computation.filter_conditions {
                prompt.push_str(&format!("- Filter Conditions: {:?}\n", filter_conditions));
            } else {
                prompt.push_str("- Filter Conditions: None (generic rule)\n");
            }
        }
        
        prompt.push_str("\n## Selected Rule\n");
        prompt.push_str(&format!("Rule ID: {}\n", rule.id));
        prompt.push_str(&format!("Description: {}\n", rule.computation.description));
        
        prompt.push_str("\n## Your Task\n");
        prompt.push_str("Provide chain-of-thought reasoning explaining:\n");
        prompt.push_str("1. Why this rule was selected\n");
        prompt.push_str("2. How the filters match the rule's conditions\n");
        prompt.push_str("3. Why this rule is better than alternatives\n");
        prompt.push_str("4. What tables/entities this rule uses and why\n\n");
        
        prompt.push_str("Format your response as JSON:\n");
        prompt.push_str(r#"{
  "reasoning": "Overall reasoning explanation",
  "steps": [
    {
      "step": "Step 1 description",
      "reasoning": "Why this step matters",
      "conclusion": "What we conclude from this step"
    }
  ]
}"#);
        
        prompt
    }

    /// Parse chain-of-thought from LLM response
    fn parse_chain_of_thought(
        &self,
        response: &str,
        rule: &Rule,
    ) -> Result<ChainOfThought> {
        // Extract JSON from response
        let json_str = self.extract_json(response);
        
        #[derive(Deserialize)]
        struct CoTResponse {
            reasoning: String,
            steps: Vec<ReasoningStep>,
        }
        
        match serde_json::from_str::<CoTResponse>(&json_str) {
            Ok(cot) => Ok(ChainOfThought {
                reasoning: cot.reasoning,
                steps: cot.steps,
            }),
            Err(e) => {
                warn!("Failed to parse chain-of-thought JSON: {}. Using fallback.", e);
                // Fallback: create simple reasoning
                Ok(ChainOfThought {
                    reasoning: format!(
                        "Selected rule {} because it matches the filter conditions and uses the appropriate entities for the calculation.",
                        rule.id
                    ),
                    steps: vec![
                        ReasoningStep {
                            step: "Rule Matching".to_string(),
                            reasoning: format!("Rule {} has filter conditions that match the query filters", rule.id),
                            conclusion: "This rule is applicable".to_string(),
                        },
                        ReasoningStep {
                            step: "Entity Analysis".to_string(),
                            reasoning: format!("Rule uses entities: {:?}", rule.computation.source_entities),
                            conclusion: "These entities provide the necessary data for calculation".to_string(),
                        },
                    ],
                })
            }
        }
    }

    /// Handle case where no rule matches filters
    async fn handle_missing_rule(
        &self,
        system: &str,
        metric: &str,
        filters: &HashMap<String, String>,
        available_rules: &[Rule],
    ) -> Result<SelectedRule> {
        let prompt = format!(
            r#"You are a business rule advisor. A user is trying to perform reconciliation for:

System: {}
Metric: {}
Filters: {:?}

However, NO RULE EXISTS that matches these filters.

Available rules are:
{}

Provide chain-of-thought reasoning and suggest:
1. Which existing rule should be used instead?
2. Or what clarification is needed from the business?

Format as JSON:
{{
  "reasoning": "Why no rule matches",
  "suggestion": "Which rule to use or what to ask",
  "clarification_needed": "What business decision is needed",
  "steps": [
    {{
      "step": "Step description",
      "reasoning": "Why",
      "conclusion": "Conclusion"
    }}
  ]
}}"#,
            system,
            metric,
            filters,
            self.format_rules_for_prompt(available_rules)
        );

        let response = self.llm.call_llm(&prompt).await?;
        
        // Parse suggestion
        #[derive(Deserialize)]
        struct SuggestionResponse {
            reasoning: String,
            suggestion: String,
            clarification_needed: String,
            steps: Vec<ReasoningStep>,
        }
        
        let json_str = self.extract_json(&response);
        let suggestion: SuggestionResponse = serde_json::from_str(&json_str)
            .map_err(|e| RcaError::Llm(format!("Failed to parse suggestion: {}", e)))?;
        
        // Find the suggested rule
        let suggested_rule = available_rules.iter()
            .find(|r| suggestion.suggestion.contains(&r.id))
            .or_else(|| available_rules.first())
            .ok_or_else(|| RcaError::Llm("No rules available".to_string()))?;
        
        let full_reasoning = format!(
            "{}\n\nClarification Needed: {}\n\nSuggested Rule: {}",
            suggestion.reasoning,
            suggestion.clarification_needed,
            suggestion.suggestion
        );
        
        Ok(SelectedRule {
            rule: suggested_rule.clone(),
            reasoning: full_reasoning,
            confidence: 0.6, // Lower confidence since rule doesn't perfectly match
            alternatives_considered: available_rules.iter()
                .filter(|r| r.id != suggested_rule.id)
                .cloned()
                .collect(),
            chain_of_thought: suggestion.steps,
        })
    }

    /// Reason about multiple matching rules
    async fn reason_about_multiple_rules(
        &self,
        matching_rules: &[Rule],
        filters: &HashMap<String, String>,
        all_rules: &[Rule],
    ) -> Result<SelectedRule> {
        let prompt = format!(
            r#"You are a reasoning engine. Multiple rules match the query filters.

Filters: {:?}

Matching Rules:
{}

All Available Rules:
{}

Use chain-of-thought reasoning to determine which rule is BEST. Consider:
1. Which rule's filter conditions most precisely match the query?
2. Which rule uses the most appropriate entities for the calculation?
3. Which rule's description best fits the business context?

Format as JSON:
{{
  "selected_rule_id": "rule_id",
  "reasoning": "Why this rule is best",
  "confidence": 0.95,
  "steps": [
    {{
      "step": "Step description",
      "reasoning": "Why",
      "conclusion": "Conclusion"
    }}
  ]
}}"#,
            filters,
            self.format_rules_for_prompt(matching_rules),
            self.format_rules_for_prompt(all_rules)
        );

        let response = self.llm.call_llm(&prompt).await?;
        let json_str = self.extract_json(&response);
        
        #[derive(Deserialize)]
        struct SelectionResponse {
            selected_rule_id: String,
            reasoning: String,
            confidence: f64,
            steps: Vec<ReasoningStep>,
        }
        
        let selection: SelectionResponse = serde_json::from_str(&json_str)
            .map_err(|e| RcaError::Llm(format!("Failed to parse selection: {}", e)))?;
        
        let selected_rule = matching_rules.iter()
            .find(|r| r.id == selection.selected_rule_id)
            .or_else(|| matching_rules.first())
            .ok_or_else(|| RcaError::Llm("Selected rule not found".to_string()))?;
        
        Ok(SelectedRule {
            rule: selected_rule.clone(),
            reasoning: selection.reasoning,
            confidence: selection.confidence,
            alternatives_considered: matching_rules.iter()
                .filter(|r| r.id != selected_rule.id)
                .cloned()
                .collect(),
            chain_of_thought: selection.steps,
        })
    }

    /// Format rules for prompt
    fn format_rules_for_prompt(&self, rules: &[Rule]) -> String {
        rules.iter()
            .map(|r| {
                format!(
                    "- {}: {} (filter: {:?})",
                    r.id,
                    r.computation.description,
                    r.computation.filter_conditions
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Extract JSON from LLM response
    fn extract_json(&self, text: &str) -> String {
        let text = text.trim();
        if let Some(start) = text.find('{') {
            if let Some(end) = text.rfind('}') {
                return text[start..=end].to_string();
            }
        }
        text.to_string()
    }
}

#[derive(Debug, Clone)]
struct ChainOfThought {
    reasoning: String,
    steps: Vec<ReasoningStep>,
}

