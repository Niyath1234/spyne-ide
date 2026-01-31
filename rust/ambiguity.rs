use crate::error::{RcaError, Result};
use crate::llm::{AmbiguityOption, AmbiguityQuestion};
use crate::metadata::Metadata;
use crate::metric_similarity::are_metrics_similar_via_contracts;
use std::io::{self, Write};

pub struct AmbiguityResolver {
    metadata: Metadata,
}

impl AmbiguityResolver {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
    
    /// Resolve ambiguities in query interpretation
    /// Returns resolved values or asks user questions (max 3)
    /// 
    /// If world_state is provided, checks contract column descriptions to determine
    /// if metrics are similar (same description = same meaning)
    pub fn resolve(
        &self,
        interpretation: &crate::llm::QueryInterpretation,
        world_state: Option<&crate::world_state::WorldState>,
    ) -> Result<ResolvedInterpretation> {
        // Check for ambiguities
        let mut questions = Vec::new();
        
        // Check if multiple rules exist for system/metric combination
        // Handle both same-metric and cross-metric cases
        let (metric_a, metric_b) = if interpretation.is_cross_metric {
            (
                interpretation.metric_a.as_ref().map(|s| s.as_str()).unwrap_or(""),
                interpretation.metric_b.as_ref().map(|s| s.as_str()).unwrap_or("")
            )
        } else {
            let metric = &interpretation.metric;
            (metric.as_str(), metric.as_str())
        };
        
        // Check if metrics are similar via contract column descriptions or table metadata
        // If they have the same description in contracts or table metadata, treat them as same-metric
        let mut is_cross_metric = interpretation.is_cross_metric;
        if is_cross_metric && !metric_a.is_empty() && !metric_b.is_empty() {
            match are_metrics_similar_via_contracts(
                world_state,
                &self.metadata,
                &interpretation.system_a,
                &interpretation.system_b,
                metric_a,
                metric_b,
            ) {
                    Ok(true) => {
                        // Metrics are similar based on contract descriptions
                        // Treat as same-metric instead of cross-metric
                        is_cross_metric = false;
                        println!("   ℹ️  Metrics '{}' and '{}' are similar based on contract column descriptions - treating as same-metric", metric_a, metric_b);
                    }
                    Ok(false) => {
                        // Metrics are different - keep as cross-metric
                    }
                    Err(e) => {
                        // Error checking similarity - log but continue with original interpretation
                        eprintln!("   ⚠️  Warning: Could not check metric similarity via contracts: {}", e);
                    }
                }
        }
        
        let rules_a = self.metadata.get_rules_for_system_metric(
            &interpretation.system_a,
            metric_a,
        );
        let rules_b = self.metadata.get_rules_for_system_metric(
            &interpretation.system_b,
            metric_b,
        );
        
        if rules_a.len() > 1 {
            let rules_a_refs: Vec<&crate::metadata::Rule> = rules_a.iter().collect();
            questions.push(self.build_rule_question(&interpretation.system_a, rules_a_refs)?);
        }
        
        if rules_b.len() > 1 {
            let rules_b_refs: Vec<&crate::metadata::Rule> = rules_b.iter().collect();
            questions.push(self.build_rule_question(&interpretation.system_b, rules_b_refs)?);
        }
        
        // Check for time column ambiguity
        if interpretation.as_of_date.is_none() {
            // Could ask which time column to use
            // For now, skip if not critical
        }
        
        // Ask questions (max 3)
        let mut answers = std::collections::HashMap::new();
        for (idx, question) in questions.iter().take(3).enumerate() {
            println!("\nQuestion {}: {}", idx + 1, question.question);
            for (opt_idx, option) in question.options.iter().enumerate() {
                println!("  {}. {} - {}", opt_idx + 1, option.label, option.description);
            }
            
            print!("Your choice (1-{}): ", question.options.len());
            io::stdout().flush()?;
            
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            
            let choice: usize = input.trim().parse()
                .map_err(|_| RcaError::Ambiguity("Invalid choice".to_string()))?;
            
            if choice < 1 || choice > question.options.len() {
                return Err(RcaError::Ambiguity("Invalid choice".to_string()));
            }
            
            let selected = &question.options[choice - 1];
            answers.insert(question.question.clone(), selected.id.clone());
        }
        
        // Extract rule answers based on question keys
        let rule_a_question = format!("Which rule version for {}?", interpretation.system_a);
        let rule_b_question = format!("Which rule version for {}?", interpretation.system_b);
        
        // If we determined metrics are similar, set metric to the first one for same-metric mode
        let (final_metric, final_metric_a, final_metric_b) = if !is_cross_metric && !metric_a.is_empty() {
            // Same-metric mode: use metric_a as the unified metric
            (Some(metric_a.to_string()), None, None)
        } else {
            // Cross-metric mode: keep both metrics
            (Some(interpretation.metric.clone()), interpretation.metric_a.clone(), interpretation.metric_b.clone())
        };
        
        Ok(ResolvedInterpretation {
            system_a: interpretation.system_a.clone(),
            system_b: interpretation.system_b.clone(),
            metric: final_metric,
            metric_a: final_metric_a,
            metric_b: final_metric_b,
            is_cross_metric,
            as_of_date: interpretation.as_of_date.clone(),
            rule_a: answers.get(&rule_a_question).cloned(),
            rule_b: answers.get(&rule_b_question).cloned(),
        })
    }
    
    fn build_rule_question(
        &self,
        system: &str,
        rules: Vec<&crate::metadata::Rule>,
    ) -> Result<AmbiguityQuestion> {
        let options: Vec<AmbiguityOption> = rules
            .iter()
            .map(|r| AmbiguityOption {
                id: r.id.clone(),
                label: r.id.clone(),
                description: r.computation.description.clone(),
            })
            .collect();
        
        Ok(AmbiguityQuestion {
            question: format!("Which rule version for {}?", system),
            options,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedInterpretation {
    pub system_a: String,
    pub system_b: String,
    // For same-metric (backward compatibility)
    pub metric: Option<String>,
    // For cross-metric
    pub metric_a: Option<String>,
    pub metric_b: Option<String>,
    pub is_cross_metric: bool,
    pub as_of_date: Option<String>,
    pub rule_a: Option<String>,
    pub rule_b: Option<String>,
}

