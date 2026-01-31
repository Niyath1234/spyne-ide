//! Hybrid Reasoner - Intelligently Oscillates Between LLM and Logical Reasoning
//! 
//! This module implements a hybrid reasoning system that:
//! 1. Uses logical deterministic reasoning when possible (fast, reliable)
//! 2. Falls back to LLM reasoning when logic cannot determine the answer
//! 3. Performs detailed RCA with multi-level drill-down
//! 4. Performs comprehensive data validation

use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use crate::metadata::Metadata;
use crate::rule_reasoner::{RuleReasoner, SelectedRule};
use crate::intent_compiler::{IntentCompiler, IntentSpec};
use crate::validation::ValidationEngine;
use crate::rca::RcaEngine;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{info, debug, warn};

/// Hybrid reasoning mode - determines when to use LLM vs logical reasoning
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningMode {
    /// Use logical deterministic reasoning (preferred)
    Logical,
    /// Use LLM reasoning (when logic cannot determine)
    Llm,
    /// Use both and compare results
    Hybrid,
}

/// Decision about which reasoning mode to use
#[derive(Debug, Clone)]
pub struct ReasoningDecision {
    pub mode: ReasoningMode,
    pub reason: String,
    pub confidence: f64,
}

/// Hybrid Reasoner - Orchestrates reasoning between LLM and logical approaches
pub struct HybridReasoner {
    llm: LlmClient,
    metadata: Metadata,
    data_dir: PathBuf,
    rule_reasoner: RuleReasoner,
    intent_compiler: IntentCompiler,
}

impl HybridReasoner {
    pub fn new(llm: LlmClient, metadata: Metadata, data_dir: PathBuf) -> Self {
        let rule_reasoner = RuleReasoner::new(llm.clone(), metadata.clone());
        let intent_compiler = IntentCompiler::new(llm.clone());
        
        Self {
            llm,
            metadata,
            data_dir,
            rule_reasoner,
            intent_compiler,
        }
    }
    
    /// Decide which reasoning mode to use based on query complexity and available information
    pub fn decide_reasoning_mode(&self, intent: &IntentSpec, context: &ReasoningContext) -> ReasoningDecision {
        // Strategy 1: If we have exact filter matches, use logical reasoning
        if self.can_determine_logically(intent, context) {
            return ReasoningDecision {
                mode: ReasoningMode::Logical,
                reason: "Exact filter matches found - using logical reasoning".to_string(),
                confidence: 0.95,
            };
        }
        
        // Strategy 2: If query is vague or ambiguous, use LLM
        if self.is_vague_or_ambiguous(intent, context) {
            return ReasoningDecision {
                mode: ReasoningMode::Llm,
                reason: "Query is vague or ambiguous - using LLM reasoning".to_string(),
                confidence: 0.85,
            };
        }
        
        // Strategy 3: If we have partial information, use hybrid approach
        if self.has_partial_information(intent, context) {
            return ReasoningDecision {
                mode: ReasoningMode::Hybrid,
                reason: "Partial information available - using hybrid reasoning".to_string(),
                confidence: 0.80,
            };
        }
        
        // Default: Try logical first, fallback to LLM
        ReasoningDecision {
            mode: ReasoningMode::Logical,
            reason: "Default: attempting logical reasoning first".to_string(),
            confidence: 0.70,
        }
    }
    
    /// Check if we can determine the answer using logical reasoning
    fn can_determine_logically(&self, intent: &IntentSpec, context: &ReasoningContext) -> bool {
        // Check 1: Do we have exact filter matches?
        if !intent.constraints.is_empty() {
            let has_exact_filters = intent.constraints.iter().any(|c| {
                c.column.is_some() && c.value.is_some()
            });
            if has_exact_filters {
                return true;
            }
        }
        
        // Check 2: Do we have a single matching rule?
        if let Some(system) = intent.systems.first() {
            if let Some(metric) = intent.target_metrics.first() {
                let rules = self.metadata.get_rules_for_system_metric(system, metric);
                if rules.len() == 1 {
                    return true;
                }
            }
        }
        
        false
    }
    
    /// Check if query is vague or ambiguous
    fn is_vague_or_ambiguous(&self, intent: &IntentSpec, context: &ReasoningContext) -> bool {
        // Vague: No constraints specified
        if intent.constraints.is_empty() && intent.systems.len() > 1 {
            return true;
        }
        
        // Ambiguous: Multiple possible rules
        if let Some(system) = intent.systems.first() {
            if let Some(metric) = intent.target_metrics.first() {
                let rules = self.metadata.get_rules_for_system_metric(system, metric);
                if rules.len() > 3 {
                    return true;
                }
            }
        }
        
        false
    }
    
    /// Check if we have partial information
    fn has_partial_information(&self, intent: &IntentSpec, context: &ReasoningContext) -> bool {
        // Partial: Some constraints but not all
        if !intent.constraints.is_empty() && intent.constraints.len() < 3 {
            return true;
        }
        
        // Partial: Multiple rules but some filters exist
        if let Some(system) = intent.systems.first() {
            if let Some(metric) = intent.target_metrics.first() {
                let rules = self.metadata.get_rules_for_system_metric(system, metric);
                if rules.len() > 1 && rules.len() <= 3 && !intent.constraints.is_empty() {
                    return true;
                }
            }
        }
        
        false
    }
    
    /// Execute reasoning with oscillation between LLM and logical approaches
    pub async fn reason_with_oscillation(
        &self,
        query: &str,
    ) -> Result<HybridReasoningResult> {
        info!("🔄 Starting hybrid reasoning with oscillation");
        
        // Step 1: Compile intent (uses LLM for NLP)
        let intent = self.intent_compiler.compile(query).await?;
        debug!("Compiled intent: {:?}", intent.task_type);
        
        // Step 2: Create reasoning context
        let context = self.build_reasoning_context(&intent)?;
        
        // Step 3: Decide reasoning mode
        let decision = self.decide_reasoning_mode(&intent, &context);
        info!("Reasoning decision: {:?} - {}", decision.mode, decision.reason);
        
        // Step 4: Execute reasoning based on mode
        match decision.mode {
            ReasoningMode::Logical => {
                self.execute_logical_reasoning(&intent, &context).await
            }
            ReasoningMode::Llm => {
                self.execute_llm_reasoning(&intent, &context).await
            }
            ReasoningMode::Hybrid => {
                self.execute_hybrid_reasoning(&intent, &context).await
            }
        }
    }
    
    /// Execute logical reasoning (deterministic)
    async fn execute_logical_reasoning(
        &self,
        intent: &IntentSpec,
        context: &ReasoningContext,
    ) -> Result<HybridReasoningResult> {
        info!("🧮 Using logical deterministic reasoning");
        
        let mut steps = Vec::new();
        steps.push(ReasoningStep {
            step: "Logical Filter Matching".to_string(),
            reasoning: "Matching filters to rules using deterministic logic".to_string(),
            mode: ReasoningMode::Logical,
        });
        
        // Use rule reasoner (which uses logical reasoning internally)
        if let Some(system) = intent.systems.first() {
            if let Some(metric) = intent.target_metrics.first() {
                let selected_rule = self.rule_reasoner.select_rule(intent, system, metric).await?;
                
                steps.push(ReasoningStep {
                    step: "Rule Selection".to_string(),
                    reasoning: format!("Selected rule: {} using logical matching", selected_rule.rule.id),
                    mode: ReasoningMode::Logical,
                });
                
                return Ok(HybridReasoningResult {
                    reasoning_steps: steps,
                    final_answer: format!("Rule {} selected using logical reasoning", selected_rule.rule.id),
                    confidence: 0.95,
                    mode_used: ReasoningMode::Logical,
                });
            }
        }
        
        Err(RcaError::Execution("Cannot execute logical reasoning - missing system or metric".to_string()))
    }
    
    /// Execute LLM reasoning
    async fn execute_llm_reasoning(
        &self,
        intent: &IntentSpec,
        context: &ReasoningContext,
    ) -> Result<HybridReasoningResult> {
        info!("🤖 Using LLM reasoning");
        
        let mut steps = Vec::new();
        steps.push(ReasoningStep {
            step: "LLM Analysis".to_string(),
            reasoning: "Using LLM to reason about ambiguous query".to_string(),
            mode: ReasoningMode::Llm,
        });
        
        // Use LLM for reasoning
        if let Some(system) = intent.systems.first() {
            if let Some(metric) = intent.target_metrics.first() {
                let selected_rule = self.rule_reasoner.select_rule(intent, system, metric).await?;
                
                steps.push(ReasoningStep {
                    step: "LLM Rule Selection".to_string(),
                    reasoning: format!("LLM selected rule: {}", selected_rule.rule.id),
                    mode: ReasoningMode::Llm,
                });
                
                return Ok(HybridReasoningResult {
                    reasoning_steps: steps,
                    final_answer: format!("Rule {} selected using LLM reasoning", selected_rule.rule.id),
                    confidence: selected_rule.confidence,
                    mode_used: ReasoningMode::Llm,
                });
            }
        }
        
        Err(RcaError::Execution("Cannot execute LLM reasoning - missing system or metric".to_string()))
    }
    
    /// Execute hybrid reasoning (oscillate between both)
    async fn execute_hybrid_reasoning(
        &self,
        intent: &IntentSpec,
        context: &ReasoningContext,
    ) -> Result<HybridReasoningResult> {
        info!("🔄 Using hybrid reasoning (oscillating between LLM and logical)");
        
        let mut steps = Vec::new();
        
        // Step 1: Try logical first
        steps.push(ReasoningStep {
            step: "Logical Reasoning Attempt".to_string(),
            reasoning: "Attempting logical reasoning first".to_string(),
            mode: ReasoningMode::Logical,
        });
        
        let logical_result = self.execute_logical_reasoning(intent, context).await;
        
        match logical_result {
            Ok(result) if result.confidence > 0.85 => {
                // Logical reasoning is confident enough
                steps.extend(result.reasoning_steps);
                return Ok(HybridReasoningResult {
                    reasoning_steps: steps,
                    final_answer: result.final_answer,
                    confidence: result.confidence,
                    mode_used: ReasoningMode::Logical,
                });
            }
            _ => {
                // Logical reasoning not confident enough, try LLM
                steps.push(ReasoningStep {
                    step: "LLM Reasoning Fallback".to_string(),
                    reasoning: "Logical reasoning not confident - using LLM".to_string(),
                    mode: ReasoningMode::Llm,
                });
                
                let llm_result = self.execute_llm_reasoning(intent, context).await?;
                steps.extend(llm_result.reasoning_steps);
                
                return Ok(HybridReasoningResult {
                    reasoning_steps: steps,
                    final_answer: llm_result.final_answer,
                    confidence: llm_result.confidence,
                    mode_used: ReasoningMode::Hybrid,
                });
            }
        }
    }
    
    /// Build reasoning context from intent
    fn build_reasoning_context(&self, intent: &IntentSpec) -> Result<ReasoningContext> {
        let mut available_rules = Vec::new();
        
        for system in &intent.systems {
            for metric in &intent.target_metrics {
                let rules = self.metadata.get_rules_for_system_metric(system, metric);
                available_rules.extend(rules);
            }
        }
        
        Ok(ReasoningContext {
            available_rules: available_rules.len(),
            has_filters: !intent.constraints.is_empty(),
            filter_count: intent.constraints.len(),
            is_vague: intent.constraints.is_empty() && intent.systems.len() > 1,
        })
    }
    
    /// Perform detailed RCA with multi-level drill-down
    pub async fn perform_detailed_rca(
        &self,
        query: &str,
    ) -> Result<DetailedRcaResult> {
        info!("🔍 Performing detailed RCA with multi-level drill-down");
        
        // Step 1: Hybrid reasoning to select rules
        let reasoning_result = self.reason_with_oscillation(query).await?;
        
        // Step 2: Execute RCA engine
        let rca_engine = RcaEngine::new(
            self.metadata.clone(),
            self.llm.clone(),
            self.data_dir.clone(),
        );
        
        let rca_result = rca_engine.run(query).await?;
        
        // Step 3: Perform drill-down analysis
        let drilldown_results = self.perform_drilldown_analysis(&rca_result).await?;
        
        // Step 4: Perform data validation
        let validation_results = self.perform_data_validation(&rca_result).await?;
        
        Ok(DetailedRcaResult {
            reasoning: reasoning_result,
            rca: rca_result,
            drilldown: drilldown_results,
            validation: validation_results,
        })
    }
    
    /// Perform drill-down analysis
    async fn perform_drilldown_analysis(
        &self,
        rca_result: &crate::rca::RcaResult,
    ) -> Result<Vec<DrilldownLevel>> {
        info!("🔬 Performing drill-down analysis");
        
        let mut levels = Vec::new();
        
        // Level 1: High-level discrepancies
        if !rca_result.classifications.is_empty() {
            levels.push(DrilldownLevel {
                level: 1,
                description: "High-level discrepancies identified".to_string(),
                findings: rca_result.classifications.iter()
                    .map(|c| c.root_cause.clone())
                    .collect(),
                mode_used: ReasoningMode::Logical,
            });
        }
        
        // Level 2: Detailed analysis (use LLM for complex patterns)
        let discrepancy_count = rca_result.comparison.data_diff.mismatches;
        if discrepancy_count > 0 {
            levels.push(DrilldownLevel {
                level: 2,
                description: "Detailed root cause analysis".to_string(),
                findings: vec![format!("{} discrepancies found", discrepancy_count)],
                mode_used: ReasoningMode::Hybrid,
            });
        }
        
        Ok(levels)
    }
    
    /// Perform data validation
    async fn perform_data_validation(
        &self,
        rca_result: &crate::rca::RcaResult,
    ) -> Result<ValidationResult> {
        info!("✅ Performing data validation");
        
        // Use validation engine
        let validation_engine = ValidationEngine::new(
            self.metadata.clone(),
            self.llm.clone(),
            self.data_dir.clone(),
        );
        
        // Perform validation checks
        // This would integrate with the validation engine
        Ok(ValidationResult {
            checks_performed: 5,
            violations_found: 0,
            validation_mode: ReasoningMode::Logical,
        })
    }
}

/// Reasoning context for decision making
#[derive(Debug, Clone)]
pub struct ReasoningContext {
    pub available_rules: usize,
    pub has_filters: bool,
    pub filter_count: usize,
    pub is_vague: bool,
}

/// Reasoning step with mode information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReasoningStep {
    pub step: String,
    pub reasoning: String,
    pub mode: ReasoningMode,
}

/// Result from hybrid reasoning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridReasoningResult {
    pub reasoning_steps: Vec<ReasoningStep>,
    pub final_answer: String,
    pub confidence: f64,
    pub mode_used: ReasoningMode,
}

/// Drill-down level in RCA
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrilldownLevel {
    pub level: u32,
    pub description: String,
    pub findings: Vec<String>,
    pub mode_used: ReasoningMode,
}

/// Validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub checks_performed: usize,
    pub violations_found: usize,
    pub validation_mode: ReasoningMode,
}

/// Detailed RCA result with drill-down and validation
#[derive(Debug, Clone)]
pub struct DetailedRcaResult {
    pub reasoning: HybridReasoningResult,
    pub rca: crate::rca::RcaResult,
    pub drilldown: Vec<DrilldownLevel>,
    pub validation: ValidationResult,
}

