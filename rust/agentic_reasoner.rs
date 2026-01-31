//! Agentic Reasoning System
//! 
//! This module provides an agentic workflow where the LLM can explore the graph
//! and knowledge base to solve problems iteratively, similar to how Cursor explores files.
//! 
//! The graph and knowledge base serve as the "knowledge space" that the agent explores.

use crate::error::{RcaError, Result};
use crate::graph::Hypergraph;
use crate::llm::LlmClient;
use crate::metadata::Metadata;
use crate::tool_system::{ToolSystem, ToolExecutionContext};
use crate::knowledge_base;
use crate::agentic_prompts;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, debug, warn};

/// Agentic reasoner that explores graph and knowledge base to solve problems
pub struct AgenticReasoner {
    llm: LlmClient,
    graph: Hypergraph,
    metadata: Metadata,
    tool_system: ToolSystem,
    knowledge_base: Option<knowledge_base::KnowledgeBase>,
    exploration_history: Vec<ExplorationStep>,
    max_iterations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplorationStep {
    pub step_type: StepType,
    pub query: String,
    pub result: ExplorationResult,
    pub reasoning: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StepType {
    GraphQuery,
    KnowledgeBaseQuery,
    PathFinding,
    TableDiscovery,
    ColumnDiscovery,
    RelationshipDiscovery,
    RuleDiscovery,
    ConceptSearch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExplorationResult {
    Tables(Vec<TableInfo>),
    Columns(Vec<ColumnInfo>),
    Path(Vec<String>),
    Concepts(Vec<ConceptInfo>),
    Rules(Vec<RuleInfo>),
    Relationships(Vec<RelationshipInfo>),
    Error(String),
    Success(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub system: String,
    pub entity: String,
    pub grain: Vec<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub table: String,
    pub name: String,
    pub data_type: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConceptInfo {
    pub id: String,
    pub name: String,
    pub concept_type: String,
    pub description: String,
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleInfo {
    pub id: String,
    pub system: String,
    pub metric: String,
    pub description: String,
    pub formula: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipInfo {
    pub from_table: String,
    pub to_table: String,
    pub join_keys: Vec<String>,
    pub relationship_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgenticPlan {
    pub goal: String,
    pub steps: Vec<PlanStep>,
    pub reasoning: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub action: Action,
    pub reasoning: String,
    pub expected_outcome: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    ExploreTable { table: String },
    ExploreColumn { table: String, column: Option<String> },
    FindPath { from: String, to: String },
    SearchConcept { term: String },
    FindRelationships { table: String },
    DiscoverRules { metric: String, system: Option<String> },
    QueryGraph { query: String },
    QueryKnowledgeBase { query: String },
}

impl AgenticReasoner {
    pub fn new(
        llm: LlmClient,
        graph: Hypergraph,
        metadata: Metadata,
        knowledge_base: Option<knowledge_base::KnowledgeBase>,
    ) -> Self {
        let tool_system = ToolSystem::new(llm.clone());
        Self {
            llm,
            graph,
            metadata,
            tool_system,
            knowledge_base,
            exploration_history: Vec::new(),
            max_iterations: 10,
        }
    }

    /// Main agentic reasoning loop - explores graph and knowledge base to solve a problem
    /// Works like Cursor: builds plans stage-wise and adapts based on results
    pub async fn reason(&mut self, problem: &str) -> Result<AgenticSolution> {
        info!("🤖 Starting agentic reasoning for problem: {}", problem);
        
        let mut solution = AgenticSolution {
            problem: problem.to_string(),
            plan: None,
            exploration_steps: Vec::new(),
            final_answer: None,
            confidence: 0.0,
        };

        let mut current_understanding = HashMap::new();
        let mut current_plan: Option<AgenticPlan> = None;
        let mut stage = 0;
        
        // Stage-wise planning and execution (like Cursor)
        loop {
            if stage >= self.max_iterations {
                warn!("⚠️  Reached max iterations ({})", self.max_iterations);
                break;
            }

            stage += 1;
            info!("\n{}", "=".repeat(80));
            info!("📋 STAGE {}: Planning and Exploration", stage);
            info!("{}", "=".repeat(80));

            // Stage 1: Create or refine plan based on current understanding
            let plan = if current_plan.is_none() {
                info!("🎯 Creating initial plan...");
                let new_plan = self.create_plan(problem, &current_understanding).await?;
                current_plan = Some(new_plan.clone());
                new_plan
            } else {
                info!("🔄 Refining plan based on discoveries...");
                let refined_plan = self.refine_plan(problem, &current_plan.unwrap(), &current_understanding).await?;
                current_plan = Some(refined_plan.clone());
                refined_plan
            };

            if solution.plan.is_none() {
                solution.plan = Some(plan.clone());
            }

            info!("✅ Plan has {} steps", plan.steps.len());
            for (i, step) in plan.steps.iter().enumerate() {
                info!("   Step {}: {:?} - {}", i + 1, step.action, step.reasoning);
            }

            // Stage 2: Execute next step(s) from plan
            let steps_to_execute = self.determine_steps_to_execute(&plan, &current_understanding);
            
            if steps_to_execute.is_empty() {
                info!("✅ All planned steps completed or no more steps needed");
                break;
            }

            info!("🔍 Executing {} step(s) from plan", steps_to_execute.len());
            
            let mut stage_results = Vec::new();
            for step_idx in steps_to_execute {
                if step_idx >= plan.steps.len() {
                    break;
                }
                
                let plan_step = &plan.steps[step_idx];
                info!("   → Executing: {:?}", plan_step.action);
                
                // Execute the action and explore
                let result = self.execute_action(&plan_step.action).await?;
                
                // Record exploration step
                let exploration_step = ExplorationStep {
                    step_type: self.get_step_type(&plan_step.action),
                    query: format!("{:?}", plan_step.action),
                    result: result.clone(),
                    reasoning: plan_step.reasoning.clone(),
                    timestamp: chrono::Utc::now().to_rfc3339(),
                };
                
                solution.exploration_steps.push(exploration_step.clone());
                self.exploration_history.push(exploration_step);
                stage_results.push((plan_step.clone(), result.clone()));

                // Update understanding based on result
                self.update_understanding(&mut current_understanding, &plan_step.action, &result);
            }

            // Stage 3: Evaluate results and decide next steps
            info!("📊 Evaluating stage results...");
            let should_continue = self.evaluate_stage_results(&stage_results, &current_understanding).await?;
            
            if !should_continue {
                info!("✅ Sufficient information gathered, proceeding to synthesis");
                break;
            }

            // Check if we need to explore more or if plan is complete
            if self.is_plan_complete(&plan, &current_understanding) {
                info!("✅ Plan execution complete");
                break;
            }
        }

        // Final stage: Synthesize answer
        info!("\n{}", "=".repeat(80));
        info!("📝 FINAL STAGE: Synthesizing answer");
        info!("{}", "=".repeat(80));
        solution.final_answer = Some(self.synthesize_answer(problem, &current_understanding).await?);
        if let Some(ref plan) = current_plan {
            solution.confidence = plan.confidence;
        }

        Ok(solution)
    }

    /// Create an initial plan based on the problem and current understanding
    async fn create_plan(&self, problem: &str, understanding: &HashMap<String, serde_json::Value>) -> Result<AgenticPlan> {
        // Determine if deep reasoning is needed (non-direct task)
        let use_deep_reasoning = agentic_prompts::requires_deep_reasoning(problem);
        
        // Use appropriate prompt based on task complexity
        let system_prompt = if use_deep_reasoning {
            info!("🧠 Using deep reasoning prompt for non-direct task");
            agentic_prompts::get_deep_reasoning_prompt()
        } else {
            self.get_planning_prompt()
        };
        
        let understanding_summary = if understanding.is_empty() {
            "No prior exploration yet.".to_string()
        } else {
            format!("Current understanding: {} exploration results available", understanding.len())
        };

        let user_prompt = if use_deep_reasoning {
            format!(
                "Problem: {}\n\n{}\n\nAvailable capabilities:\n{}\n\nThis task requires deep reasoning. Analyze the problem autonomously:\n- Infer grain requirements and aggregation needs\n- Identify columns by semantic meaning\n- Determine join strategies and business logic\n- Only ask questions if truly ambiguous\n\nCreate a step-by-step plan with autonomous reasoning.",
                problem,
                understanding_summary,
                self.get_capabilities_description()
            )
        } else {
            format!(
                "Problem: {}\n\n{}\n\nAvailable capabilities:\n{}\n\nCreate a step-by-step plan to solve this problem by exploring the graph and knowledge base. Focus on the most important first steps.",
                problem,
                understanding_summary,
                self.get_capabilities_description()
            )
        };

        let full_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
        let response = self.llm.call_llm(&full_prompt).await?;

        // Parse the plan from LLM response
        // For now, create a simple plan structure
        // In production, would parse JSON from LLM
        let plan = self.parse_plan_from_response(&response, problem)?;
        
        Ok(plan)
    }

    /// Execute an action and return exploration result
    async fn execute_action(&mut self, action: &Action) -> Result<ExplorationResult> {
        match action {
            Action::ExploreTable { table } => {
                self.explore_table(table).await
            }
            Action::ExploreColumn { table, column } => {
                self.explore_column(table, column.as_deref()).await
            }
            Action::FindPath { from, to } => {
                self.find_path(from, to).await
            }
            Action::SearchConcept { term } => {
                self.search_concept(term).await
            }
            Action::FindRelationships { table } => {
                self.find_relationships(table).await
            }
            Action::DiscoverRules { metric, system } => {
                self.discover_rules(metric, system.as_deref()).await
            }
            Action::QueryGraph { query } => {
                self.query_graph(query).await
            }
            Action::QueryKnowledgeBase { query } => {
                self.query_knowledge_base(query).await
            }
        }
    }

    /// Explore a table - get its structure, grain, relationships
    async fn explore_table(&mut self, table_name: &str) -> Result<ExplorationResult> {
        info!("🔍 Exploring table: {}", table_name);
        
        let table = self.metadata.tables
            .iter()
            .find(|t| t.name == table_name)
            .ok_or_else(|| RcaError::Graph(format!("Table not found: {}", table_name)))?;

        let table_info = TableInfo {
            name: table.name.clone(),
            system: table.system.clone(),
            entity: table.entity.clone(),
            grain: table.primary_key.clone(),
            description: None,
        };

        // Also find related tables
        let mut relationships = Vec::new();
        for edge in &self.metadata.lineage.edges {
            if edge.from == table_name {
                relationships.push(RelationshipInfo {
                    from_table: edge.from.clone(),
                    to_table: edge.to.clone(),
                    join_keys: edge.keys.keys().cloned().collect(),
                    relationship_type: edge.relationship.clone(),
                });
            } else if edge.to == table_name {
                relationships.push(RelationshipInfo {
                    from_table: edge.from.clone(),
                    to_table: edge.to.clone(),
                    join_keys: edge.keys.keys().cloned().collect(),
                    relationship_type: edge.relationship.clone(),
                });
            }
        }

        Ok(ExplorationResult::Relationships(relationships))
    }

    /// Explore a column in a table
    async fn explore_column(&mut self, table_name: &str, column_name: Option<&str>) -> Result<ExplorationResult> {
        info!("🔍 Exploring column: {}.{}", table_name, column_name.unwrap_or("*"));
        
        let table = self.metadata.tables
            .iter()
            .find(|t| t.name == table_name)
            .ok_or_else(|| RcaError::Graph(format!("Table not found: {}", table_name)))?;

        let mut columns = Vec::new();
        if let Some(cols) = &table.columns {
            for col in cols {
                if let Some(col_name) = column_name {
                    if col.name == col_name {
                        columns.push(ColumnInfo {
                            table: table_name.to_string(),
                            name: col.name.clone(),
                            data_type: col.data_type.as_ref().map(|d| d.clone()).unwrap_or_else(|| "unknown".to_string()),
                            description: col.description.clone(),
                        });
                    }
                } else {
                    columns.push(ColumnInfo {
                        table: table_name.to_string(),
                        name: col.name.clone(),
                        data_type: col.data_type.as_ref().map(|d| d.clone()).unwrap_or_else(|| "unknown".to_string()),
                        description: col.description.clone(),
                    });
                }
            }
        }

        Ok(ExplorationResult::Columns(columns))
    }

    /// Find a path between two tables
    async fn find_path(&mut self, from: &str, to: &str) -> Result<ExplorationResult> {
        info!("🛤️  Finding path from {} to {}", from, to);
        
        let adapter = self.graph.adapter()?;
        let path = adapter.find_join_path(from, to)?;
        
        if let Some(path_tables) = path {
            Ok(ExplorationResult::Path(path_tables))
        } else {
            Ok(ExplorationResult::Error(format!("No path found from {} to {}", from, to)))
        }
    }

    /// Search for concepts in knowledge base
    async fn search_concept(&mut self, term: &str) -> Result<ExplorationResult> {
        info!("🔎 Searching concept: {}", term);
        
        if let Some(ref kb) = self.knowledge_base {
            let concepts = kb.search_by_name(term);
            let concept_infos: Vec<ConceptInfo> = concepts.iter()
                .map(|c| ConceptInfo {
                    id: c.concept_id.clone(),
                    name: c.name.clone(),
                    concept_type: format!("{:?}", c.concept_type),
                    description: c.definition.clone(),
                    tables: c.related_tables.clone(),
                })
                .collect();
            
            Ok(ExplorationResult::Concepts(concept_infos))
        } else {
            Ok(ExplorationResult::Error("Knowledge base not available".to_string()))
        }
    }

    /// Find relationships for a table
    async fn find_relationships(&mut self, table_name: &str) -> Result<ExplorationResult> {
        info!("🔗 Finding relationships for: {}", table_name);
        
        let mut relationships = Vec::new();
        for edge in &self.metadata.lineage.edges {
            if edge.from == table_name || edge.to == table_name {
                relationships.push(RelationshipInfo {
                    from_table: edge.from.clone(),
                    to_table: edge.to.clone(),
                    join_keys: edge.keys.keys().cloned().collect(),
                    relationship_type: edge.relationship.clone(),
                });
            }
        }

        Ok(ExplorationResult::Relationships(relationships))
    }

    /// Discover rules for a metric
    async fn discover_rules(&mut self, metric: &str, system: Option<&str>) -> Result<ExplorationResult> {
        info!("📜 Discovering rules for metric: {} (system: {:?})", metric, system);
        
        let mut rules = Vec::new();
        for rule in &self.metadata.rules {
            if rule.metric == metric {
                if let Some(sys) = system {
                    if rule.system != sys {
                        continue;
                    }
                }
                rules.push(RuleInfo {
                    id: rule.id.clone(),
                    system: rule.system.clone(),
                    metric: rule.metric.clone(),
                    description: rule.computation.description.clone(),
                    formula: rule.computation.formula.clone(),
                });
            }
        }

        Ok(ExplorationResult::Rules(rules))
    }

    /// Query the graph with natural language
    async fn query_graph(&mut self, query: &str) -> Result<ExplorationResult> {
        info!("💬 Querying graph: {}", query);
        
        // Use LLM to interpret the query and determine what to explore
        // For now, do simple keyword matching
        let query_lower = query.to_lowercase();
        
        if query_lower.contains("table") || query_lower.contains("find table") {
            // Extract table name from query
            let words: Vec<&str> = query.split_whitespace().collect();
            for word in words {
                if let Some(table) = self.metadata.tables.iter().find(|t| t.name.contains(word)) {
                    let table_name = table.name.clone();
                    return self.explore_table(&table_name).await;
                }
            }
        }
        
        if query_lower.contains("path") || query_lower.contains("join") {
            // Try to extract table names
            let words: Vec<&str> = query.split_whitespace().collect();
            let tables: Vec<&str> = words.iter()
                .filter(|w| self.metadata.tables.iter().any(|t| t.name.contains(*w)))
                .copied()
                .collect();
            
            if tables.len() >= 2 {
                return self.find_path(tables[0], tables[1]).await;
            }
        }

        Ok(ExplorationResult::Error("Could not interpret query".to_string()))
    }

    /// Query the knowledge base
    async fn query_knowledge_base(&mut self, query: &str) -> Result<ExplorationResult> {
        info!("📚 Querying knowledge base: {}", query);
        self.search_concept(query).await
    }

    /// Update understanding based on exploration result
    fn update_understanding(
        &self,
        understanding: &mut HashMap<String, serde_json::Value>,
        action: &Action,
        result: &ExplorationResult,
    ) {
        let key = format!("{:?}", action);
        let value = serde_json::to_value(result).unwrap_or(serde_json::Value::Null);
        understanding.insert(key, value);
    }

    /// Synthesize final answer from exploration
    async fn synthesize_answer(
        &self,
        problem: &str,
        understanding: &HashMap<String, serde_json::Value>,
    ) -> Result<String> {
        let system_prompt = agentic_prompts::get_synthesis_prompt();
        
        let understanding_json = serde_json::to_string_pretty(understanding)
            .unwrap_or_else(|_| "{}".to_string());
        
        let user_prompt = format!(
            "Problem: {}\n\nExploration Results:\n{}\n\nSynthesize a comprehensive RCA answer.",
            problem,
            understanding_json
        );

        let full_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
        let response = self.llm.call_llm(&full_prompt).await?;

        Ok(response)
    }

    fn get_step_type(&self, action: &Action) -> StepType {
        match action {
            Action::ExploreTable { .. } => StepType::TableDiscovery,
            Action::ExploreColumn { .. } => StepType::ColumnDiscovery,
            Action::FindPath { .. } => StepType::PathFinding,
            Action::SearchConcept { .. } => StepType::ConceptSearch,
            Action::FindRelationships { .. } => StepType::RelationshipDiscovery,
            Action::DiscoverRules { .. } => StepType::RuleDiscovery,
            Action::QueryGraph { .. } => StepType::GraphQuery,
            Action::QueryKnowledgeBase { .. } => StepType::KnowledgeBaseQuery,
        }
    }

    fn get_planning_prompt(&self) -> &str {
        "You are an agentic reasoning system that explores a knowledge graph and knowledge base to solve problems.
        
You can:
- Explore tables to understand their structure and grain
- Explore columns to understand data types and semantics
- Find paths between tables to understand relationships
- Search for business concepts in the knowledge base
- Discover rules and formulas for metrics
- Query the graph and knowledge base with natural language

Create a step-by-step plan that explores the necessary information to solve the problem."
    }

    fn get_capabilities_description(&self) -> String {
        format!(
            "- Explore {} tables across {} systems\n- {} lineage relationships\n- {} business rules\n- Knowledge base with semantic search",
            self.metadata.tables.len(),
            self.metadata.business_labels.systems.len(),
            self.metadata.lineage.edges.len(),
            self.metadata.rules.len()
        )
    }

    /// Refine plan based on discoveries (like Cursor adapts plans)
    async fn refine_plan(
        &self,
        problem: &str,
        current_plan: &AgenticPlan,
        understanding: &HashMap<String, serde_json::Value>,
    ) -> Result<AgenticPlan> {
        // Check if deep reasoning is still needed based on discoveries
        let use_deep_reasoning = agentic_prompts::requires_deep_reasoning(problem) ||
                                 self.has_grain_mismatches(understanding) ||
                                 self.has_ambiguous_columns(understanding);
        
        let system_prompt = if use_deep_reasoning {
            info!("🧠 Using deep reasoning for plan refinement");
            agentic_prompts::get_deep_reasoning_prompt()
        } else {
            "You are refining an exploration plan based on new discoveries. Update the plan to incorporate new findings and adjust priorities."
        };
        
        let understanding_json = serde_json::to_string_pretty(understanding)
            .unwrap_or_else(|_| "{}".to_string());
        
        let current_plan_json = serde_json::to_string_pretty(current_plan)
            .unwrap_or_else(|_| "{}".to_string());
        
        let user_prompt = if use_deep_reasoning {
            format!(
                "Problem: {}\n\nCurrent Plan:\n{}\n\nNew Discoveries:\n{}\n\nRefine the plan using deep reasoning:\n- Infer grain mismatches and aggregation needs\n- Identify columns semantically\n- Adjust join strategies based on discoveries\n- Only ask questions if truly ambiguous",
                problem,
                current_plan_json,
                understanding_json
            )
        } else {
            format!(
                "Problem: {}\n\nCurrent Plan:\n{}\n\nNew Discoveries:\n{}\n\nRefine the plan: add new steps based on discoveries, remove completed steps, adjust priorities.",
                problem,
                current_plan_json,
                understanding_json
            )
        };

        let full_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
        let response = self.llm.call_llm(&full_prompt).await?;
        
        // For now, return current plan (in production would parse refined plan from LLM)
        // This is a placeholder - would need proper JSON parsing
        Ok(current_plan.clone())
    }
    
    /// Check if discoveries indicate grain mismatches requiring deep reasoning
    fn has_grain_mismatches(&self, understanding: &HashMap<String, serde_json::Value>) -> bool {
        // Check if any exploration results show grain mismatches
        // This is a heuristic - in production would parse JSON properly
        let understanding_str = format!("{:?}", understanding);
        understanding_str.contains("grain") && 
        (understanding_str.contains("mismatch") || understanding_str.contains("aggregation"))
    }
    
    /// Check if discoveries indicate ambiguous column identification
    fn has_ambiguous_columns(&self, understanding: &HashMap<String, serde_json::Value>) -> bool {
        // Check if column exploration found multiple matches or semantic ambiguity
        let understanding_str = format!("{:?}", understanding);
        understanding_str.contains("column") && 
        (understanding_str.contains("multiple") || understanding_str.contains("ambiguous") || understanding_str.contains("semantic"))
    }

    /// Determine which steps to execute next (like Cursor decides what to do next)
    fn determine_steps_to_execute(
        &self,
        plan: &AgenticPlan,
        understanding: &HashMap<String, serde_json::Value>,
    ) -> Vec<usize> {
        // Execute first 1-2 steps that haven't been completed yet
        // In production, would use LLM to decide based on understanding
        let mut steps_to_execute = Vec::new();
        
        for (idx, step) in plan.steps.iter().enumerate() {
            let step_key = format!("{:?}", step.action);
            if !understanding.contains_key(&step_key) {
                steps_to_execute.push(idx);
                // Execute 1-2 steps per stage (like Cursor)
                if steps_to_execute.len() >= 2 {
                    break;
                }
            }
        }
        
        steps_to_execute
    }

    /// Evaluate stage results and decide if we should continue (like Cursor evaluates progress)
    async fn evaluate_stage_results(
        &self,
        stage_results: &[(PlanStep, ExplorationResult)],
        understanding: &HashMap<String, serde_json::Value>,
    ) -> Result<bool> {
        // Check if we got useful results
        let has_errors = stage_results.iter().any(|(_, result)| {
            matches!(result, ExplorationResult::Error(_))
        });
        
        let has_useful_results = stage_results.iter().any(|(_, result)| {
            !matches!(result, ExplorationResult::Error(_))
        });
        
        // Continue if we have useful results and no critical errors
        // In production, would use LLM to evaluate if we have enough information
        Ok(has_useful_results && !has_errors && understanding.len() < 10)
    }

    /// Check if plan is complete (like Cursor checks if task is done)
    fn is_plan_complete(
        &self,
        plan: &AgenticPlan,
        understanding: &HashMap<String, serde_json::Value>,
    ) -> bool {
        // Plan is complete if all steps have been executed
        plan.steps.iter().all(|step| {
            let step_key = format!("{:?}", step.action);
            understanding.contains_key(&step_key)
        })
    }

    fn parse_plan_from_response(&self, response: &str, problem: &str) -> Result<AgenticPlan> {
        // Heuristic-based plan creation for RCA problems
        // Analyzes the problem and creates specific exploration steps
        
        let mut steps = Vec::new();
        let problem_lower = problem.to_lowercase();
        
        // Extract metric from problem (TOS, balance, etc.)
        let metric = if problem_lower.contains("tos") || problem_lower.contains("total outstanding") {
            "tos"
        } else if problem_lower.contains("balance") {
            "balance"
        } else {
            "tos" // Default
        };
        
        // Extract loan ID if mentioned
        let loan_id = if let Some(caps) = regex::Regex::new(r"L\d+")
            .ok()
            .and_then(|re| re.find(&problem))
        {
            Some(caps.as_str().to_string())
        } else {
            None
        };
        
        // Step 1: Discover rules for the metric (both systems)
        steps.push(PlanStep {
            action: Action::DiscoverRules {
                metric: metric.to_string(),
                system: None,
            },
            reasoning: format!("Find calculation rules for {} metric in both systems", metric),
            expected_outcome: "Rules for System A and System B showing how TOS is calculated".to_string(),
        });
        
        // Step 2: Explore main tables involved
        steps.push(PlanStep {
            action: Action::ExploreTable {
                table: "loan_summary".to_string(),
            },
            reasoning: "Understand the structure and grain of the main loan table".to_string(),
            expected_outcome: "Table structure, grain, and relationships".to_string(),
        });
        
        // Step 3: Find relationships to understand data flow
        steps.push(PlanStep {
            action: Action::FindRelationships {
                table: "loan_summary".to_string(),
            },
            reasoning: "Discover all tables related to loan_summary to understand data dependencies".to_string(),
            expected_outcome: "List of related tables and join relationships".to_string(),
        });
        
        // Step 4: If problem mentions specific tables, explore them
        if problem_lower.contains("daily") || problem_lower.contains("interest") {
            steps.push(PlanStep {
                action: Action::ExploreTable {
                    table: "daily_interest_accruals".to_string(),
                },
                reasoning: "Explore daily interest accruals table to understand grain differences".to_string(),
                expected_outcome: "Table grain and aggregation requirements".to_string(),
            });
        }
        
        if problem_lower.contains("emi") || problem_lower.contains("schedule") {
            steps.push(PlanStep {
                action: Action::ExploreTable {
                    table: "emi_schedule".to_string(),
                },
                reasoning: "Explore EMI schedule table to understand payment structure".to_string(),
                expected_outcome: "EMI table structure and relationships".to_string(),
            });
        }
        
        // Step 5: Find paths between key tables
        steps.push(PlanStep {
            action: Action::FindPath {
                from: "loan_summary".to_string(),
                to: "daily_interest_accruals".to_string(),
            },
            reasoning: "Understand how to join loan summary with daily tables".to_string(),
            expected_outcome: "Join path showing how tables connect".to_string(),
        });

        Ok(AgenticPlan {
            goal: problem.to_string(),
            steps,
            reasoning: format!("Plan created to explore {} metric calculation differences between systems", metric),
            confidence: 0.85,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgenticSolution {
    pub problem: String,
    pub plan: Option<AgenticPlan>,
    pub exploration_steps: Vec<ExplorationStep>,
    pub final_answer: Option<String>,
    pub confidence: f64,
}

