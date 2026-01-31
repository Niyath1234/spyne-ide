//! One-Shot Agentic RCA + DV Runner
//! 
//! Orchestrates all layers into a single unified UX.
//! User gives one natural-language query, system figures out the rest.

use crate::error::{RcaError, Result};
use crate::intent_compiler::{IntentCompiler, IntentSpec};
use crate::task_grounder::{TaskGrounder, GroundedTask};
use crate::execution_planner::{ExecutionPlanner, ExecutionPlan};
use crate::execution_engine::ExecutionEngine;
use crate::safety_guardrails::SafetyGuardrails;
use crate::explainability::ExplainabilityLayer;
use crate::goal_directed_explorer::GoalDirectedExplorer;
use crate::graph::Hypergraph;
use crate::metadata::Metadata;
use crate::llm::LlmClient;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{info, warn, error};

/// One-Shot Runner Result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OneShotResult {
    pub success: bool,
    pub intent: IntentSpec,
    pub grounded_task: Option<GroundedTask>,
    pub execution_plan: Option<ExecutionPlan>,
    pub result_data: Option<serde_json::Value>,
    pub explanation: crate::explainability::EnhancedExplanation,
    pub failures: Vec<FailureInfo>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureInfo {
    pub failure_type: String,
    pub message: String,
    pub context: serde_json::Value,
}

/// One-Shot Agentic RCA + DV Runner
pub struct OneShotRunner {
    metadata: Metadata,
    graph: Hypergraph,
    llm: LlmClient,
    data_dir: PathBuf,
    max_exploration_cycles: usize,
    enable_safety_guardrails: bool,
}

impl OneShotRunner {
    pub fn new(
        metadata: Metadata,
        llm: LlmClient,
        data_dir: PathBuf,
    ) -> Self {
        let graph = Hypergraph::new(metadata.clone());
        
        Self {
            metadata,
            graph,
            llm,
            data_dir,
            max_exploration_cycles: 10,
            enable_safety_guardrails: true,
        }
    }

    /// Run one-shot query - main entry point
    pub async fn run(&self, query: &str) -> Result<OneShotResult> {
        info!("🚀 One-Shot Runner: {}", query);
        
        let mut failures = Vec::new();
        
        // Step 1: Compile Intent
        let intent = match self.compile_intent(query).await {
            Ok(intent) => intent,
            Err(e) => {
                failures.push(FailureInfo {
                    failure_type: "AmbiguousIntent".to_string(),
                    message: format!("Failed to compile intent: {}", e),
                    context: serde_json::json!({"query": query}),
                });
                return Ok(OneShotResult {
                    success: false,
                    intent: IntentSpec {
                        task_type: crate::intent_compiler::TaskType::RCA,
                        target_metrics: vec![],
                        entities: vec![],
                        constraints: vec![],
                        grain: vec![],
                        time_scope: None,
                        systems: vec![],
                        validation_constraint: None,
                        joins: vec![],
                        tables: vec![],
                    },
                    grounded_task: None,
                    execution_plan: None,
                    result_data: None,
                    explanation: crate::explainability::EnhancedExplanation {
                        summary: format!("Failed to compile intent: {}", e),
                        why_tables: vec![],
                        why_joins: vec![],
                        why_grain: vec![],
                        why_rules: vec![],
                        why_constraints: vec![],
                        decision_tree: vec![],
                    },
                    failures,
                });
            }
        };
        
        // Step 2: Ground Task (with chain-of-thought reasoning)
        let grounded_task = match self.ground_task(&intent).await {
            Ok(task) => {
                if !task.unresolved_fields.is_empty() {
                    warn!("Unresolved fields: {:?}", task.unresolved_fields);
                    // Could trigger exploration here
                }
                Some(task)
            }
            Err(e) => {
                failures.push(FailureInfo {
                    failure_type: "UnresolvablePath".to_string(),
                    message: format!("Failed to ground task: {}", e),
                    context: serde_json::json!({"intent": intent}),
                });
                None
            }
        };
        
        let grounded_task = match grounded_task {
            Some(t) => t,
            None => {
                return Ok(OneShotResult {
                    success: false,
                    intent,
                    grounded_task: None,
                    execution_plan: None,
                    result_data: None,
                    explanation: crate::explainability::EnhancedExplanation {
                        summary: "Failed to ground task - could not resolve tables/columns".to_string(),
                        why_tables: vec![],
                        why_joins: vec![],
                        why_grain: vec![],
                        why_rules: vec![],
                        why_constraints: vec![],
                        decision_tree: vec![],
                    },
                    failures,
                });
            }
        };
        
        // Step 3: Goal-Directed Exploration (if needed)
        if !grounded_task.unresolved_fields.is_empty() {
            info!("Unresolved fields detected, starting goal-directed exploration");
            let mut explorer = GoalDirectedExplorer::new(self.metadata.clone(), Hypergraph::new(self.metadata.clone()));
            match explorer.explore(&grounded_task) {
                Ok(exploration_state) => {
                    info!("Exploration completed: {} cycles, {} nodes explored", 
                        exploration_state.cycles, exploration_state.explored_nodes.len());
                    // Would update grounded_task with new discoveries
                }
                Err(e) => {
                    warn!("Exploration failed: {}", e);
                    // Continue anyway
                }
            }
        }
        
        // Step 4: Build Execution Plan
        let execution_plan = match self.build_plan(&grounded_task) {
            Ok(plan) => {
                // Check safety guardrails
                if self.enable_safety_guardrails {
                    let guardrails = SafetyGuardrails::new(self.metadata.clone());
                    match guardrails.assess(&plan) {
                        Ok(assessment) => {
                            if !assessment.safe {
                                if assessment.requires_force {
                                    failures.push(FailureInfo {
                                        failure_type: "SafetyGuardrail".to_string(),
                                        message: format!("Safety check failed: requires --force flag. {}", 
                                            assessment.reasons.join("; ")),
                                        context: serde_json::json!({
                                            "estimated_rows": assessment.estimated_rows_scanned,
                                            "estimated_memory_mb": assessment.estimated_memory_mb,
                                            "join_explosion_risk": assessment.estimated_join_explosion_risk,
                                        }),
                                    });
                                    None
                                } else {
                                    warn!("Safety warnings: {:?}", assessment.warnings);
                                    Some(plan)
                                }
                            } else {
                                Some(plan)
                            }
                        }
                        Err(e) => {
                            failures.push(FailureInfo {
                                failure_type: "SafetyGuardrail".to_string(),
                                message: format!("Safety assessment failed: {}", e),
                                context: serde_json::json!({}),
                            });
                            None
                        }
                    }
                } else {
                    Some(plan)
                }
            }
            Err(e) => {
                failures.push(FailureInfo {
                    failure_type: "DangerousPlan".to_string(),
                    message: format!("Failed to build execution plan: {}", e),
                    context: serde_json::json!({}),
                });
                None
            }
        };
        
        // Step 5: Generate Enhanced Explanation
        let explainability = ExplainabilityLayer::new(Some(self.llm.clone()))
            .with_metadata(self.metadata.clone());
        let enhanced_explanation = explainability.explain(&intent, &grounded_task, &execution_plan).await
            .unwrap_or_else(|e| {
                warn!("Failed to generate enhanced explanation: {}", e);
                // Fallback to basic explanation
                self.generate_explanation(&intent, &grounded_task, &execution_plan)
            });
        
        // Step 6: Execute (if plan exists and safe)
        let result_data = if let Some(ref plan) = execution_plan {
            let engine = ExecutionEngine::new(self.metadata.clone(), self.data_dir.clone());
            match engine.execute(plan).await {
                Ok(exec_result) => {
                    if exec_result.success {
                        // Convert DataFrame to JSON
                        exec_result.data.map(|df| {
                            // Simplified - would properly serialize DataFrame
                            serde_json::json!({
                                "rows": df.height(),
                                "columns": df.width(),
                                "execution_time_ms": exec_result.execution_time_ms,
                            })
                        })
                    } else {
                        failures.push(FailureInfo {
                            failure_type: "Execution".to_string(),
                            message: format!("Execution completed with {} errors", exec_result.nodes_failed),
                            context: serde_json::json!({
                                "nodes_executed": exec_result.nodes_executed,
                                "nodes_failed": exec_result.nodes_failed,
                                "errors": exec_result.errors,
                            }),
                        });
                        None
                    }
                }
                Err(e) => {
                    failures.push(FailureInfo {
                        failure_type: "Execution".to_string(),
                        message: format!("Execution failed: {}", e),
                        context: serde_json::json!({}),
                    });
                    None
                }
            }
        } else {
            None
        };
        
        let success = result_data.is_some() && failures.is_empty();
        
        Ok(OneShotResult {
            success,
            intent,
            grounded_task: Some(grounded_task),
            execution_plan,
            result_data,
            explanation: enhanced_explanation,
            failures,
        })
    }

    async fn compile_intent(&self, query: &str) -> Result<IntentSpec> {
        let compiler = IntentCompiler::new(self.llm.clone());
        let mut intent = compiler.compile(query).await?;
        
        // SAFEGUARD: Validate against metadata to prevent hallucination
        let validation_result = IntentCompiler::validate_against_metadata(&mut intent, &self.metadata)?;
        
        if !validation_result.is_valid {
            return Err(RcaError::Execution(format!(
                "Intent validation failed (hallucination detected):\nErrors: {}\nWarnings: {}",
                validation_result.errors.join("; "),
                validation_result.warnings.join("; ")
            )));
        }
        
        // Log warnings if any
        if !validation_result.warnings.is_empty() {
            warn!("Intent validation warnings: {:?}", validation_result.warnings);
        }
        
        Ok(intent)
    }

    async fn ground_task(&self, intent: &IntentSpec) -> Result<GroundedTask> {
        let grounder = TaskGrounder::new(self.metadata.clone())
            .with_llm(self.llm.clone());
        grounder.ground(intent).await
    }

    fn build_plan(&self, task: &GroundedTask) -> Result<ExecutionPlan> {
        // Create new graph instance (Hypergraph doesn't implement Clone)
        let planner = ExecutionPlanner::new(self.metadata.clone(), Hypergraph::new(self.metadata.clone()));
        planner.build_plan(task)
    }

    fn check_safety(&self, plan: &ExecutionPlan) -> Result<()> {
        // Basic safety checks
        // In production, would estimate:
        // - Rows scanned
        // - Join explosion risk
        // - Memory footprint
        
        // Check for cartesian joins
        for node in &plan.nodes {
            if let crate::execution_planner::ExecutionNode::Join { keys, .. } = node {
                if keys.is_empty() {
                    return Err(RcaError::SafetyGuardrail(
                        "Cartesian join detected - no join keys".to_string()
                    ));
                }
            }
        }
        
        // Check for unbounded scans
        // Would check if filters are present for large tables
        
        Ok(())
    }

    async fn execute_plan(&self, plan: &ExecutionPlan) -> Result<serde_json::Value> {
        // Simplified execution - in production would use ExecutionEngine
        // For now, return plan structure as result
        Ok(serde_json::json!({
            "plan_executed": true,
            "nodes": plan.nodes.len(),
            "edges": plan.edges.len(),
        }))
    }

    fn generate_explanation(
        &self,
        intent: &IntentSpec,
        task: &GroundedTask,
        plan: &Option<ExecutionPlan>,
    ) -> crate::explainability::EnhancedExplanation {
        // Fallback basic explanation
        use crate::explainability::{EnhancedExplanation, TableExplanation, GrainExplanation};
        
        let why_tables: Vec<TableExplanation> = task.candidate_tables.iter()
            .map(|t| TableExplanation {
                table_name: t.table_name.clone(),
                system: t.system.clone(),
                reasons: vec![t.reason.clone()],
                confidence: t.confidence,
                alternatives_considered: Vec::new(),
            })
            .collect();
        
        let why_grain = vec![GrainExplanation {
            grain: task.required_grain.clone(),
            source: crate::explainability::GrainSource::InferredFromTables,
            reasons: vec!["Inferred from candidate tables".to_string()],
            alternatives_considered: Vec::new(),
        }];
        
        EnhancedExplanation {
            summary: format!("Task: {:?}, Systems: {:?}, Metrics: {:?}", 
                intent.task_type, task.systems, task.metrics),
            why_tables,
            why_joins: Vec::new(),
            why_grain,
            why_rules: Vec::new(),
            why_constraints: Vec::new(),
            decision_tree: Vec::new(),
        }
    }
}

