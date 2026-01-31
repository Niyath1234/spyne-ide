///! Simplified Intent Compiler - Enhanced to auto-detect systems from table names
///! 
///! This module extends the existing intent compiler to:
///! 1. Auto-detect systems from table names mentioned in the question
///! 2. Use table registry to infer system membership
///! 3. Generate metadata on-the-fly from uploaded tables
///! 4. Dynamically determine required number of systems based on task type
///! 5. Provide chain-of-thought reasoning for system detection

use crate::intent_compiler::{IntentSpec, TaskType};
use crate::table_upload::TableRegistry;
use crate::llm::LlmClient;
use serde::{Serialize, Deserialize};

/// Chain of thought reasoning step for system detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemDetectionStep {
    pub step: String,
    pub reasoning: String,
    pub conclusion: String,
}

/// System detection result with chain of thought
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemDetectionResult {
    pub detected_systems: Vec<String>,
    pub required_count: usize,
    pub task_type: String,
    pub chain_of_thought: Vec<SystemDetectionStep>,
    pub reasoning_summary: String,
}

/// Enhanced intent compiler that auto-detects systems
pub struct SimplifiedIntentCompiler {
    pub table_registry: TableRegistry,
    pub llm_client: Option<LlmClient>,
}

impl SimplifiedIntentCompiler {
    pub fn new(table_registry: TableRegistry, llm_client: Option<LlmClient>) -> Self {
        Self {
            table_registry,
            llm_client,
        }
    }
    
    /// Compile intent with automatic system detection and chain-of-thought reasoning
    /// 
    /// Example:
    /// Query: "TOS recon between khatabook and TB"
    /// Auto-detects: systems = ["khatabook", "tb"]
    /// Chain of thought: Shows reasoning for why these systems are needed
    pub async fn compile_with_auto_detection(
        &self,
        query: &str,
    ) -> Result<SimplifiedIntent, Box<dyn std::error::Error>> {
        // Step 1: Detect systems with chain-of-thought reasoning
        let detection_result = self.detect_systems_with_reasoning(query).await?;
        
        // Step 2: Extract metric name from question
        let metric_name = self.extract_metric_name(query).await?;
        
        // Step 3: Find all tables for each system
        let mut system_tables = std::collections::HashMap::new();
        for system in &detection_result.detected_systems {
            let tables = self.table_registry.find_tables_by_prefix(system);
            system_tables.insert(system.clone(), tables);
        }
        
        // Step 4: Generate default rules for this metric
        let suggested_rules = self.table_registry.generate_default_rules(&metric_name);
        
        // Step 5: Create simplified intent with chain of thought
        Ok(SimplifiedIntent {
            query: query.to_string(),
            metric_name,
            detected_systems: detection_result.detected_systems,
            system_tables: system_tables.into_iter()
                .map(|(k, v)| (k, v.into_iter().map(|t| t.upload.table_name.clone()).collect()))
                .collect(),
            suggested_rules,
            chain_of_thought: detection_result.chain_of_thought,
            reasoning_summary: detection_result.reasoning_summary,
        })
    }
    
    /// Detect systems with chain-of-thought reasoning
    /// Dynamically determines how many systems are needed based on task type
    async fn detect_systems_with_reasoning(
        &self,
        query: &str,
    ) -> Result<SystemDetectionResult, Box<dyn std::error::Error>> {
        let mut chain_of_thought = Vec::new();
        
        // Step 1: Analyze query to determine task type
        chain_of_thought.push(SystemDetectionStep {
            step: "Task Type Analysis".to_string(),
            reasoning: format!("Analyzing query: '{}' to determine what type of analysis is needed", query),
            conclusion: String::new(),
        });
        
        let query_lower = query.to_lowercase();
        let is_reconciliation = query_lower.contains("recon") || 
                               query_lower.contains("compare") || 
                               query_lower.contains("mismatch") ||
                               query_lower.contains("difference") ||
                               query_lower.contains("between");
        let is_validation = query_lower.contains("validate") || 
                           query_lower.contains("check") ||
                           query_lower.contains("verify");
        let is_single_system_analysis = query_lower.contains("analyze") || 
                                        query_lower.contains("show") ||
                                        query_lower.contains("list");
        
        let task_type = if is_reconciliation {
            "reconciliation"
        } else if is_validation {
            "validation"
        } else if is_single_system_analysis {
            "single_system_analysis"
        } else {
            "reconciliation" // Default to reconciliation for RCA tasks
        };
        
        chain_of_thought[0].conclusion = format!(
            "Task type determined: {} (reconciliation: {}, validation: {}, single_analysis: {})",
            task_type, is_reconciliation, is_validation, is_single_system_analysis
        );
        
        // Step 2: Determine required number of systems based on task type
        chain_of_thought.push(SystemDetectionStep {
            step: "System Requirement Analysis".to_string(),
            reasoning: format!("Determining how many systems are needed for task type: {}", task_type),
            conclusion: String::new(),
        });
        
        let required_count = match task_type {
            "reconciliation" => {
                // Reconciliation requires multiple systems to compare
                let min_required = 2;
                chain_of_thought[1].conclusion = format!(
                    "Reconciliation tasks require comparing data across multiple systems. Minimum required: {} systems",
                    min_required
                );
                min_required
            },
            "validation" => {
                // Validation can work with single or multiple systems
                let min_required = 1;
                chain_of_thought[1].conclusion = format!(
                    "Validation tasks can work with single or multiple systems. Minimum required: {} system(s)",
                    min_required
                );
                min_required
            },
            "single_system_analysis" => {
                // Single system analysis only needs one system
                let min_required = 1;
                chain_of_thought[1].conclusion = format!(
                    "Single system analysis tasks require: {} system(s)",
                    min_required
                );
                min_required
            },
            _ => {
                // Default: assume reconciliation needs multiple systems
                let min_required = 2;
                chain_of_thought[1].conclusion = format!(
                    "Default assumption for RCA tasks: {} systems required for comparison",
                    min_required
                );
                min_required
            }
        };
        
        // Step 3: Detect systems from query
        chain_of_thought.push(SystemDetectionStep {
            step: "System Detection".to_string(),
            reasoning: "Scanning query and table registry for system mentions".to_string(),
            conclusion: String::new(),
        });
        
        let detected_systems = self.table_registry.detect_systems_from_question(query);
        
        chain_of_thought[2].conclusion = format!(
            "Detected {} system(s): {}",
            detected_systems.len(),
            if detected_systems.is_empty() {
                "none".to_string()
            } else {
                detected_systems.join(", ")
            }
        );
        
        // Step 4: Validate detected systems against requirements
        chain_of_thought.push(SystemDetectionStep {
            step: "System Validation".to_string(),
            reasoning: format!(
                "Validating detected {} system(s) against required {} system(s) for {} task",
                detected_systems.len(),
                required_count,
                task_type
            ),
            conclusion: String::new(),
        });
        
        if detected_systems.is_empty() {
            let error_msg = format!(
                "Could not detect any systems from the question. Please mention system or table names in your query."
            );
            chain_of_thought[3].conclusion = format!("Validation failed: {}", error_msg);
            return Err(error_msg.into());
        }
        
        if detected_systems.len() < required_count {
            let error_msg = format!(
                "Detected {} system(s): {}. This {} task requires at least {} system(s). {}",
                detected_systems.len(),
                detected_systems.join(", "),
                task_type,
                required_count,
                if required_count > detected_systems.len() {
                    format!("Please specify {} more system(s) in your query.", required_count - detected_systems.len())
                } else {
                    String::new()
                }
            );
            chain_of_thought[3].conclusion = format!("Validation failed: {}", error_msg);
            return Err(error_msg.into());
        }
        
        chain_of_thought[3].conclusion = format!(
            "Validation passed: Detected {} system(s) meets requirement of {} system(s) for {} task",
            detected_systems.len(),
            required_count,
            task_type
        );
        
        // Step 5: Final reasoning summary
        let reasoning_summary = format!(
            "Chain of Thought Summary:\n\
            - Task Type: {} (determined from query keywords)\n\
            - Required Systems: {} (based on task type requirements)\n\
            - Detected Systems: {} ({})\n\
            - Validation: {} (detected systems meet requirement)\n\
            - Conclusion: Proceeding with {} system(s) for {} task",
            task_type,
            required_count,
            detected_systems.len(),
            detected_systems.join(", "),
            if detected_systems.len() >= required_count { "PASSED" } else { "FAILED" },
            detected_systems.len(),
            task_type
        );
        
        Ok(SystemDetectionResult {
            detected_systems,
            required_count,
            task_type: task_type.to_string(),
            chain_of_thought,
            reasoning_summary,
        })
    }
    
    /// Extract metric name from question using LLM or pattern matching
    async fn extract_metric_name(&self, query: &str) -> Result<String, Box<dyn std::error::Error>> {
        let query_lower = query.to_lowercase();
        
        // Pattern matching for common metrics
        if query_lower.contains("tos") || query_lower.contains("outstanding") {
            return Ok("total_outstanding".to_string());
        } else if query_lower.contains("recovery") {
            return Ok("recovery".to_string());
        } else if query_lower.contains("disbursement") {
            return Ok("disbursement".to_string());
        } else if query_lower.contains("recon") {
            // Generic reconciliation - try to extract metric from "X recon"
            if let Some(pos) = query_lower.find("recon") {
                let before = &query_lower[..pos].trim();
                if let Some(word_start) = before.rfind(' ') {
                    let metric = &before[word_start..].trim();
                    return Ok(metric.to_string());
                }
            }
            return Ok("amount".to_string()); // Default
        }
        
        // If we have LLM, use it to extract metric
        if let Some(ref llm) = self.llm_client {
            let prompt = format!(
                r#"Extract the metric name being reconciled from this question:

Question: {}

Common metrics:
- total_outstanding (TOS)
- recovery
- disbursement  
- balance
- amount

Return only the metric name, nothing else."#,
                query
            );
            
            // Use the LLM's call_llm method
            match llm.call_llm(&prompt).await {
                Ok(response) => {
                    let metric = response.trim().to_lowercase();
                    if !metric.is_empty() {
                        return Ok(metric);
                    }
                }
                Err(_) => {}
            }
        }
        
        // Default
        Ok("amount".to_string())
    }
    
    /// Generate full metadata JSON from table registry
    /// This creates the metadata needed by the RCA engine
    pub fn generate_metadata(&self) -> Result<String, Box<dyn std::error::Error>> {
        self.table_registry.generate_full_metadata()
    }
}

/// Simplified intent structure
#[derive(Debug, Clone)]
pub struct SimplifiedIntent {
    /// Original query
    pub query: String,
    
    /// Detected metric name
    pub metric_name: String,
    
    /// Auto-detected systems (e.g., ["khatabook", "tb"])
    pub detected_systems: Vec<String>,
    
    /// Tables for each system
    pub system_tables: std::collections::HashMap<String, Vec<String>>,
    
    /// Auto-generated business rules suggestions
    pub suggested_rules: Vec<String>,
    
    /// Chain of thought reasoning for system detection
    pub chain_of_thought: Vec<SystemDetectionStep>,
    
    /// Summary of reasoning
    pub reasoning_summary: String,
}

impl SimplifiedIntent {
    /// Convert to full IntentSpec for RCA engine
    pub fn to_intent_spec(&self) -> IntentSpec {
        IntentSpec {
            task_type: TaskType::RCA,
            systems: self.detected_systems.clone(),
            target_metrics: vec![self.metric_name.clone()],
            entities: vec![], // Will be inferred by task grounder
            grain: vec![], // Will be inferred from tables
            constraints: vec![], // Can be extracted from query if needed
            time_scope: None,
            validation_constraint: None,
            joins: vec![], // Will be inferred from query if needed
            tables: vec![], // Will be inferred from query if needed
        }
    }
    
    /// Display human-readable summary
    pub fn summary(&self) -> String {
        format!(
            r#"Detected Intent:
- Metric: {}
- Systems: {} ({} system(s) detected)
- Tables:
{}
- Suggested Rules:
{}

Chain of Thought Reasoning:
{}
"#,
            self.metric_name,
            self.detected_systems.join(" vs "),
            self.detected_systems.len(),
            self.system_tables.iter()
                .map(|(sys, tables)| format!("  {}: {}", sys, tables.join(", ")))
                .collect::<Vec<_>>()
                .join("\n"),
            self.suggested_rules.iter()
                .map(|r| format!("  - {}", r))
                .collect::<Vec<_>>()
                .join("\n"),
            self.chain_of_thought.iter()
                .enumerate()
                .map(|(i, step)| format!("  {}. {}: {}\n     Conclusion: {}", 
                    i + 1, step.step, step.reasoning, step.conclusion))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_upload::SimpleTableUpload;
    use std::path::PathBuf;
    use std::collections::HashMap;
    
    #[test]
    fn test_system_detection() {
        let mut registry = TableRegistry::new();
        
        // Manually create tables (in real use, would call register_table)
        let tables = vec![
            ("khatabook_customers", "khatabook"),
            ("khatabook_loans", "khatabook"),
            ("tb_customer_data", "tb"),
            ("tb_loan_details", "tb"),
        ];
        
        for (name, prefix) in tables {
            registry.tables.push(crate::table_upload::RegisteredTable {
                upload: SimpleTableUpload {
                    table_name: name.to_string(),
                    csv_path: PathBuf::from("test.csv"),
                    primary_keys: vec!["id".to_string()],
                    column_descriptions: HashMap::new(),
                },
                schema: crate::table_upload::TableSchema { columns: vec![] },
                table_prefix: Some(prefix.to_string()),
                row_count: 0,
            });
        }
        
        let systems = registry.detect_systems_from_question("TOS recon between khatabook and TB");
        assert!(systems.len() >= 2, "Should detect at least 2 systems for reconciliation");
        assert!(systems.contains(&"khatabook".to_string()));
        assert!(systems.contains(&"tb".to_string()));
    }
}

