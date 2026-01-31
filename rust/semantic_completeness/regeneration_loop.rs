//! Semantic Completeness Regeneration Loop
//! 
//! Enforces completeness by regenerating SQL when entities are missing.

use crate::error::Result;
use crate::llm::LlmClient;
use crate::metadata::Metadata;
use crate::semantic::registry::SemanticRegistry;
use std::sync::Arc;
use tracing::{info, warn};
use serde_json;
use super::entity_extractor::{EntityExtractor, RequiredEntitySet};
use super::entity_mapper::EntityMapper;
use super::sql_validator::{SqlValidator, ValidationResult};

pub struct SemanticCompletenessValidator {
    entity_extractor: EntityExtractor,
    entity_mapper: EntityMapper,
    sql_validator: SqlValidator,
    llm: LlmClient,
}

impl SemanticCompletenessValidator {
    pub fn new(
        llm: LlmClient,
        metadata: Metadata,
        semantic_registry: Option<Arc<dyn SemanticRegistry>>,
    ) -> Self {
        let entity_extractor = EntityExtractor::new(llm.clone());
        let entity_mapper = EntityMapper::new(semantic_registry, metadata);
        let sql_validator = SqlValidator;

        Self {
            entity_extractor,
            entity_mapper,
            sql_validator,
            llm,
        }
    }

    pub async fn enforce_completeness(
        &self,
        question: &str,
        sql: &str,
        max_iterations: usize,
        reasoning_steps: &mut Vec<String>,
    ) -> Result<String> {
        let mut current_sql = sql.to_string();

        for iteration in 0..max_iterations {
            info!("Semantic completeness check iteration {}", iteration + 1);
            reasoning_steps.push(format!("Completeness check iteration {} of {}", iteration + 1, max_iterations));

            // Step 1: Extract required entities
            reasoning_steps.push("Extracting required entities from question...".to_string());
            let entities = match self.entity_extractor.extract_entities(question).await {
                Ok(e) => {
                    reasoning_steps.push(format!(
                        "✅ Extracted entities - anchors: {:?}, attributes: {:?}, relationships: {:?}",
                        e.anchor_entities, e.attribute_entities, e.relationship_entities
                    ));
                    e
                }
                Err(e) => {
                    let error_msg = format!("⚠️ Failed to extract entities: {}. Proceeding with SQL as-is.", e);
                    warn!("{}", error_msg);
                    reasoning_steps.push(error_msg);
                    return Ok(current_sql);
                }
            };

            // Step 2: Map entities to tables
            reasoning_steps.push("Mapping entities to database tables...".to_string());
            let required_tables = match self.entity_mapper.map_entities_to_tables(&entities) {
                Ok(t) => {
                    if t.is_empty() {
                        // No tables found - might be a valid case (e.g., system query)
                        let msg = "⚠️ No required tables found for entities. This might be a system query. Proceeding with SQL as-is.";
                        info!("{}", msg);
                        reasoning_steps.push(msg.to_string());
                        return Ok(current_sql);
                    }
                    reasoning_steps.push(format!("✅ Mapped to required tables: {:?}", t));
                    t
                }
                Err(e) => {
                    let error_msg = format!("⚠️ Failed to map entities to tables: {}. Proceeding with SQL as-is.", e);
                    warn!("{}", error_msg);
                    reasoning_steps.push(error_msg);
                    return Ok(current_sql);
                }
            };

            info!("Required tables: {:?}", required_tables);

            // Step 3: Validate SQL completeness
            reasoning_steps.push("Validating SQL completeness against required tables...".to_string());
            let validation = match self.sql_validator.validate_completeness(&current_sql, &required_tables) {
                Ok(v) => {
                    if v.is_complete {
                        reasoning_steps.push("✅ SQL completeness validated - all required tables present".to_string());
                    } else {
                        reasoning_steps.push(format!("⚠️ SQL incomplete. Missing tables: {:?}", v.missing_tables));
                    }
                    v
                }
                Err(e) => {
                    let error_msg = format!("⚠️ Failed to validate SQL completeness: {}. Proceeding with SQL as-is.", e);
                    warn!("{}", error_msg);
                    reasoning_steps.push(error_msg);
                    return Ok(current_sql);
                }
            };

            if validation.is_complete {
                info!("SQL completeness validated successfully");
                return Ok(current_sql);
            }

            info!("SQL incomplete. Missing tables: {:?}", validation.missing_tables);

            // Step 4: Regenerate with missing entities feedback
            if iteration < max_iterations - 1 {
                let feedback = format!(
                    "The SQL is incomplete.\nMissing required entities/tables: {}\nRegenerate SQL including all required entities.",
                    validation.missing_tables.join(", ")
                );

                reasoning_steps.push(format!("🔄 Regenerating SQL to include missing tables: {:?}", validation.missing_tables));
                current_sql = match self.regenerate_sql(question, &current_sql, &feedback).await {
                    Ok(new_sql) => {
                        info!("Regenerated SQL (attempt {})", iteration + 2);
                        reasoning_steps.push(format!("✅ SQL regenerated (attempt {}): {}", iteration + 2, new_sql));
                        new_sql
                    }
                    Err(e) => {
                        let error_msg = format!("⚠️ Failed to regenerate SQL: {}. Using current SQL.", e);
                        warn!("{}", error_msg);
                        reasoning_steps.push(error_msg);
                        return Ok(current_sql);
                    }
                };
            }
        }

        // After max iterations, return the best SQL we have
        let final_validation = self.entity_extractor.extract_entities(question).await
            .ok()
            .and_then(|entities| {
                self.entity_mapper.map_entities_to_tables(&entities).ok()
            })
            .and_then(|tables| {
                self.sql_validator.validate_completeness(&current_sql, &tables).ok()
            });
        
        if let Some(validation) = final_validation {
            if !validation.is_complete {
                let warning = format!(
                    "⚠️ SQL completeness validation failed after {} iterations. Missing tables: {:?}",
                    max_iterations,
                    validation.missing_tables
                );
                warn!("{}", warning);
                reasoning_steps.push(warning);
            }
        } else {
            let warning = format!(
                "⚠️ Could not validate completeness after {} iterations. Proceeding with current SQL.",
                max_iterations
            );
            warn!("{}", warning);
            reasoning_steps.push(warning);
        }

        // Return the SQL anyway - let execution handle errors
        Ok(current_sql)
    }

    async fn regenerate_sql(
        &self,
        question: &str,
        current_sql: &str,
        feedback: &str,
    ) -> Result<String> {
        let prompt = format!(
            r#"Regenerate the SQL query to include all required entities.

ORIGINAL QUESTION: "{}"

CURRENT SQL (INCOMPLETE):
{}

FEEDBACK:
{}

INSTRUCTIONS:
1. Include all required entities/tables mentioned in the feedback
2. Add necessary JOINs to connect related tables
3. Ensure all anchor entities are included in FROM clause
4. Maintain the original query intent and structure
5. Return ONLY the SQL query, no explanations, no markdown code blocks

SQL:"#,
            question, current_sql, feedback
        );

        let sql = self.llm.call_llm(&prompt).await?;
        let cleaned_sql = sql
            .trim()
            .trim_start_matches("```sql")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim()
            .trim_start_matches("SQL:")
            .trim();
        
        // Remove JSON wrapper if present (e.g., {"sql": "..."})
        let cleaned = if cleaned_sql.starts_with("{") && cleaned_sql.contains("\"sql\"") {
            // Try to extract SQL from JSON
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&cleaned_sql) {
                if let Some(sql_value) = json_value.get("sql").and_then(|v| v.as_str()) {
                    sql_value.to_string()
                } else {
                    cleaned_sql.to_string()
                }
            } else {
                // If JSON parsing fails, try simple string extraction
                // Look for "sql": "..." pattern
                if let Some(start) = cleaned_sql.find("\"sql\"") {
                    if let Some(quote_start) = cleaned_sql[start..].find('"') {
                        let after_quote = &cleaned_sql[start + quote_start + 1..];
                        if let Some(end_quote) = after_quote.find('"') {
                            after_quote[..end_quote].to_string()
                        } else {
                            cleaned_sql.to_string()
                        }
                    } else {
                        cleaned_sql.to_string()
                    }
                } else {
                    cleaned_sql.to_string()
                }
            }
        } else {
            cleaned_sql.to_string()
        };

        Ok(cleaned)
    }
}

