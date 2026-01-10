use crate::error::{RcaError, Result};
use crate::llm::{AmbiguityOption, AmbiguityQuestion, AmbiguityResolution};
use crate::metadata::Metadata;
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
    pub fn resolve(
        &self,
        interpretation: &crate::llm::QueryInterpretation,
    ) -> Result<ResolvedInterpretation> {
        // Check for ambiguities
        let mut questions = Vec::new();
        
        // Check if multiple rules exist for system/metric combination
        let rules_a = self.metadata.get_rules_for_system_metric(
            &interpretation.system_a,
            &interpretation.metric,
        );
        let rules_b = self.metadata.get_rules_for_system_metric(
            &interpretation.system_b,
            &interpretation.metric,
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
        
        Ok(ResolvedInterpretation {
            system_a: interpretation.system_a.clone(),
            system_b: interpretation.system_b.clone(),
            metric: interpretation.metric.clone(),
            as_of_date: interpretation.as_of_date.clone(),
            rule_a: answers.get("Which rule version?").cloned(),
            rule_b: answers.get("Which rule version?").cloned(),
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
                label: r.name.clone(),
                description: format!("Rule ID: {}", r.id),
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
    pub metric: String,
    pub as_of_date: Option<String>,
    pub rule_a: Option<String>,
    pub rule_b: Option<String>,
}

