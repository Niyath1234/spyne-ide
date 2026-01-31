use crate::error::Result;
use crate::intelligent_rule_parser::{IntelligentRuleParser, ParsedFormula};
use crate::metadata::{Metadata, Rule, ComputationDefinition};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::io::{self, Write};

/// Builder for creating rules from natural language business rules
pub struct IntelligentRuleBuilder {
    parser: IntelligentRuleParser,
    metadata: Metadata,
}

impl IntelligentRuleBuilder {
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        let parser = IntelligentRuleParser::new(metadata.clone(), data_dir);
        Self { parser, metadata }
    }

    /// Build a rule from natural language business rule
    /// Example: "TOS or total outstanding is A+B-C for all loans"
    pub async fn build_rule_from_natural_language(
        &mut self,
        business_rule: &str,
        system: &str,
        metric: &str,
        target_entity: &str,
    ) -> Result<Rule> {
        println!("\n🔍 Intelligent Rule Builder");
        println!("   Business Rule: \"{}\"", business_rule);
        println!("   System: {}", system);
        println!("   Metric: {}", metric);
        println!("   Target Entity: {}", target_entity);
        
        // Parse the formula
        let parsed = self.parser.parse_formula(business_rule, system, metric, target_entity).await?;
        
        // Handle clarifications if needed
        let resolved_formula = if parsed.needs_clarification {
            println!("\n   ⚠️  Need clarification for column matching:");
            self.resolve_clarifications(parsed.clone()).await?
        } else {
            parsed.formula.clone()
        };
        
        // Determine source entities from column matches
        let source_entities = self.determine_source_entities(&parsed)?;
        
        // Build attributes_needed from column matches
        let attributes_needed = self.build_attributes_needed(&parsed, &source_entities)?;
        
        // Create the rule
        let rule = Rule {
            id: format!("{}_{}", system, metric),
            system: system.to_string(),
            metric: metric.to_string(),
            target_entity: target_entity.to_string(),
            target_grain: vec!["loan_id".to_string()], // Default, should be inferred
            computation: ComputationDefinition {
                filter_conditions: None,
                source_table: None,
                note: None,
                description: format!("{}: {}", metric, business_rule),
                source_entities,
                attributes_needed,
                formula: resolved_formula,
                aggregation_grain: vec!["loan_id".to_string()], // Default
            },
            labels: None,
        };
        
        println!("   ✅ Rule built successfully!");
        println!("      Formula: {}", rule.computation.formula);
        println!("      Source Entities: {:?}", rule.computation.source_entities);
        
        Ok(rule)
    }

    async fn resolve_clarifications(&self, parsed: ParsedFormula) -> Result<String> {
        let mut resolved_vars = HashMap::new();
        
        for question in &parsed.clarification_questions {
            println!("\n   ❓ {}", question.question);
            println!("      Options:");
            
            // Display options with better formatting
            for (idx, option) in question.options.iter().enumerate() {
                println!("      {}: {} (table: {}, confidence: {:.2}%)", 
                    idx + 1,
                    option.column_name,
                    option.table_name,
                    option.confidence * 100.0
                );
                if let Some(ref desc) = option.description {
                    println!("         Description: {}", desc);
                }
            }
            
            // Get user input with retry logic
            let choice: usize;
            loop {
                print!("      Your choice (1-{}): ", question.options.len());
                io::stdout().flush()?;
                
                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                
                match input.trim().parse::<usize>() {
                    Ok(val) if val >= 1 && val <= question.options.len() => {
                        choice = val;
                        break;
                    }
                    Ok(_) => {
                        println!("      ⚠️  Please enter a number between 1 and {}", question.options.len());
                        continue;
                    }
                    Err(_) => {
                        println!("      ⚠️  Invalid input. Please enter a number.");
                        continue;
                    }
                }
            }
            
            let selected = &question.options[choice - 1];
            resolved_vars.insert(question.variable.clone(), selected.column_name.clone());
            
            println!("      ✅ Selected: {} from table {}", 
                selected.column_name, selected.table_name);
        }
        
        // Replace variables in formula with resolved column names
        let mut formula = parsed.formula.clone();
        for (var, col) in &resolved_vars {
            formula = formula.replace(var, col);
        }
        
        Ok(formula)
    }

    fn determine_source_entities(&self, parsed: &ParsedFormula) -> Result<Vec<String>> {
        let mut entities = HashSet::new();
        
        for col_match in parsed.column_matches.values() {
            // Find entity for this table
            if let Some(table) = self.metadata.tables.iter().find(|t| t.name == col_match.table_name) {
                entities.insert(table.entity.clone());
            }
        }
        
        Ok(entities.into_iter().collect())
    }

    fn build_attributes_needed(
        &self,
        parsed: &ParsedFormula,
        source_entities: &[String],
    ) -> Result<HashMap<String, Vec<String>>> {
        let mut attributes_needed = HashMap::new();
        
        for entity in source_entities {
            let mut entity_cols = Vec::new();
            
            // Collect columns for this entity
            for col_match in parsed.column_matches.values() {
                if let Some(table) = self.metadata.tables.iter().find(|t| t.name == col_match.table_name) {
                    if table.entity == *entity {
                        entity_cols.push(col_match.column_name.clone());
                    }
                }
            }
            
            if !entity_cols.is_empty() {
                attributes_needed.insert(entity.clone(), entity_cols);
            }
        }
        
        Ok(attributes_needed)
    }
}

