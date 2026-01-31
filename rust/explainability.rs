//! Enhanced Explainability Layer
//! 
//! Provides detailed explanations for:
//! - Why this table?
//! - Why this join path?
//! - Why this grain?
//! - Why this rule interpretation?

use crate::error::Result;
use crate::intent_compiler::IntentSpec;
use crate::task_grounder::GroundedTask;
use crate::execution_planner::ExecutionPlan;
use crate::llm::LlmClient;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Enhanced explanation with detailed reasoning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedExplanation {
    pub summary: String,
    pub why_tables: Vec<TableExplanation>,
    pub why_joins: Vec<JoinExplanation>,
    pub why_grain: Vec<GrainExplanation>,
    pub why_rules: Vec<RuleExplanation>,
    pub why_constraints: Vec<ConstraintExplanation>,
    pub decision_tree: Vec<DecisionNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableExplanation {
    pub table_name: String,
    pub system: String,
    pub reasons: Vec<String>,
    pub confidence: f64,
    pub alternatives_considered: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinExplanation {
    pub from_table: String,
    pub to_table: String,
    pub path: Vec<String>,
    pub reasons: Vec<String>,
    pub keys_used: Vec<String>,
    pub join_type: String,
    pub alternatives_considered: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrainExplanation {
    pub grain: Vec<String>,
    pub source: GrainSource,
    pub reasons: Vec<String>,
    pub alternatives_considered: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GrainSource {
    FromIntent,
    FromTablePrimaryKey,
    FromMetricDefinition,
    InferredFromTables,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleExplanation {
    pub rule_id: String,
    pub metric: String,
    pub system: String,
    pub interpretation: String,
    pub formula: String,
    pub reasoning: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintExplanation {
    pub constraint_type: String,
    pub column: String,
    pub interpretation: String,
    pub reasoning: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionNode {
    pub decision: String,
    pub reasoning: String,
    pub alternatives: Vec<String>,
    pub chosen: String,
}

/// Enhanced Explainability Layer
pub struct ExplainabilityLayer {
    llm: Option<LlmClient>,
    enable_llm_explanations: bool,
    metadata: Option<crate::metadata::Metadata>,
}

impl ExplainabilityLayer {
    pub fn new(llm: Option<LlmClient>) -> Self {
        let enable_llm_explanations = llm.is_some();
        Self {
            llm,
            enable_llm_explanations,
            metadata: None,
        }
    }

    pub fn with_metadata(mut self, metadata: crate::metadata::Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Generate enhanced explanation
    pub async fn explain(
        &self,
        intent: &IntentSpec,
        grounded_task: &GroundedTask,
        execution_plan: &Option<ExecutionPlan>,
    ) -> Result<EnhancedExplanation> {
        info!("Generating enhanced explanation");
        
        let why_tables = self.explain_tables(grounded_task)?;
        let why_joins = self.explain_joins(execution_plan)?;
        let why_grain = self.explain_grain(intent, grounded_task)?;
        let why_rules = self.explain_rules(grounded_task)?;
        let why_constraints = self.explain_constraints(grounded_task)?;
        let decision_tree = self.build_decision_tree(intent, grounded_task, execution_plan)?;
        
        let summary = self.generate_summary(intent, grounded_task, execution_plan)?;
        
        Ok(EnhancedExplanation {
            summary,
            why_tables,
            why_joins,
            why_grain,
            why_rules,
            why_constraints,
            decision_tree,
        })
    }

    fn explain_tables(&self, task: &GroundedTask) -> Result<Vec<TableExplanation>> {
        let mut explanations = Vec::new();
        
        for table in &task.candidate_tables {
            let mut reasons = Vec::new();
            
            // Add confidence-based reasons
            if table.confidence > 0.8 {
                reasons.push("High confidence match".to_string());
            }
            
            // Add reason from table candidate
            if !table.reason.is_empty() {
                reasons.push(table.reason.clone());
            }
            
            // Add system match reason
            if task.systems.contains(&table.system) {
                reasons.push(format!("Matches required system: {}", table.system));
            }
            
            // Add grain match reason
            if !task.required_grain.is_empty() {
                let grain_match = table.grain.iter()
                    .any(|g| task.required_grain.contains(g));
                if grain_match {
                    reasons.push("Grain matches required level".to_string());
                }
            }
            
            explanations.push(TableExplanation {
                table_name: table.table_name.clone(),
                system: table.system.clone(),
                reasons,
                confidence: table.confidence,
                alternatives_considered: Vec::new(), // Would populate from exploration
            });
        }
        
        Ok(explanations)
    }

    fn explain_joins(&self, plan: &Option<ExecutionPlan>) -> Result<Vec<JoinExplanation>> {
        let mut explanations = Vec::new();
        
        if let Some(ref plan) = plan {
            for node in &plan.nodes {
                if let crate::execution_planner::ExecutionNode::Join {
                    left_table,
                    right_table,
                    keys,
                    join_type,
                } = node {
                    let mut reasons = Vec::new();
                    
                    reasons.push(format!("Join needed to combine {} and {}", left_table, right_table));
                    
                    if !keys.is_empty() {
                        reasons.push(format!("Using keys: {:?}", keys));
                    }
                    
                    reasons.push(format!("Join type: {:?} (chosen for data completeness)", join_type));
                    
                    explanations.push(JoinExplanation {
                        from_table: left_table.clone(),
                        to_table: right_table.clone(),
                        path: vec![left_table.clone(), right_table.clone()],
                        reasons,
                        keys_used: keys.clone(),
                        join_type: format!("{:?}", join_type),
                        alternatives_considered: Vec::new(),
                    });
                }
            }
        }
        
        Ok(explanations)
    }

    fn explain_grain(&self, intent: &IntentSpec, task: &GroundedTask) -> Result<Vec<GrainExplanation>> {
        let mut explanations = Vec::new();
        
        let source = if !intent.grain.is_empty() {
            GrainSource::FromIntent
        } else if !task.required_grain.is_empty() {
            GrainSource::InferredFromTables
        } else {
            GrainSource::FromTablePrimaryKey
        };
        
        let mut reasons = Vec::new();
        match source {
            GrainSource::FromIntent => {
                reasons.push("Grain specified in user query".to_string());
            }
            GrainSource::FromTablePrimaryKey => {
                reasons.push("Grain inferred from table primary keys".to_string());
            }
            GrainSource::FromMetricDefinition => {
                reasons.push("Grain from metric definition".to_string());
            }
            GrainSource::InferredFromTables => {
                reasons.push("Grain inferred from candidate tables".to_string());
            }
        }
        
        if !task.required_grain.is_empty() {
            reasons.push(format!("Required grain: {:?}", task.required_grain));
        }
        
        explanations.push(GrainExplanation {
            grain: task.required_grain.clone(),
            source,
            reasons,
            alternatives_considered: Vec::new(),
        });
        
        Ok(explanations)
    }

    fn explain_rules(&self, task: &GroundedTask) -> Result<Vec<RuleExplanation>> {
        let mut explanations = Vec::new();
        
        for metric in &task.metrics {
            for system in &task.systems {
                // Fetch actual rules from metadata if available
                if let Some(ref metadata) = self.metadata {
                    let rules = metadata.get_rules_for_system_metric(system, metric);
                    
                    if rules.is_empty() {
                        // Fallback if no rules found
                        explanations.push(RuleExplanation {
                            rule_id: format!("{}_{}", system, metric),
                            metric: metric.clone(),
                            system: system.clone(),
                            interpretation: format!("Calculate {} for {}", metric, system),
                            formula: format!("SUM({})", metric),
                            reasoning: format!("Standard aggregation for {} metric (no specific rule found)", metric),
                        });
                    } else {
                        // Use actual rules from metadata
                        for rule in &rules {
                            let mut reasoning = format!("Rule selected: {}. ", rule.id);
                            reasoning.push_str(&rule.computation.description);
                            
                            if let Some(ref filter_conditions) = rule.computation.filter_conditions {
                                reasoning.push_str(&format!(" Applies to: {:?}", filter_conditions));
                            }
                            
                            explanations.push(RuleExplanation {
                                rule_id: rule.id.clone(),
                                metric: metric.clone(),
                                system: system.clone(),
                                interpretation: rule.computation.description.clone(),
                                formula: rule.computation.formula.clone(),
                                reasoning,
                            });
                        }
                    }
                } else {
                    // Fallback if no metadata available
                    explanations.push(RuleExplanation {
                        rule_id: format!("{}_{}", system, metric),
                        metric: metric.clone(),
                        system: system.clone(),
                        interpretation: format!("Calculate {} for {}", metric, system),
                        formula: format!("SUM({})", metric),
                        reasoning: format!("Standard aggregation for {} metric", metric),
                    });
                }
            }
        }
        
        Ok(explanations)
    }

    fn explain_constraints(&self, task: &GroundedTask) -> Result<Vec<ConstraintExplanation>> {
        let mut explanations = Vec::new();
        
        for constraint in &task.constraint_specs {
            explanations.push(ConstraintExplanation {
                constraint_type: "filter".to_string(),
                column: constraint.column.clone(),
                interpretation: constraint.description.clone(),
                reasoning: format!(
                    "Filter applied: {} {} {:?}",
                    constraint.column,
                    constraint.operator,
                    constraint.value
                ),
            });
        }
        
        Ok(explanations)
    }

    fn build_decision_tree(
        &self,
        intent: &IntentSpec,
        task: &GroundedTask,
        plan: &Option<ExecutionPlan>,
    ) -> Result<Vec<DecisionNode>> {
        let mut decisions = Vec::new();
        
        // Decision: Task type
        decisions.push(DecisionNode {
            decision: "Task Type".to_string(),
            reasoning: format!("Determined from query: {:?}", intent.task_type),
            alternatives: vec!["RCA".to_string(), "DV".to_string()],
            chosen: format!("{:?}", intent.task_type),
        });
        
        // Decision: Tables selected
        decisions.push(DecisionNode {
            decision: "Table Selection".to_string(),
            reasoning: format!("Selected {} tables based on system/metric matching", task.candidate_tables.len()),
            alternatives: Vec::new(), // Would include alternatives considered
            chosen: format!("{} tables", task.candidate_tables.len()),
        });
        
        // Decision: Join strategy
        if let Some(ref plan) = plan {
            let join_count = plan.nodes.iter()
                .filter(|n| matches!(n, crate::execution_planner::ExecutionNode::Join { .. }))
                .count();
            
            decisions.push(DecisionNode {
                decision: "Join Strategy".to_string(),
                reasoning: format!("Using {} joins to connect tables", join_count),
                alternatives: Vec::new(),
                chosen: format!("{} joins", join_count),
            });
        }
        
        Ok(decisions)
    }

    fn generate_summary(
        &self,
        intent: &IntentSpec,
        task: &GroundedTask,
        plan: &Option<ExecutionPlan>,
    ) -> Result<String> {
        let mut summary_parts = Vec::new();
        
        summary_parts.push(format!("Task Type: {:?}", intent.task_type));
        summary_parts.push(format!("Systems: {:?}", task.systems));
        summary_parts.push(format!("Metrics: {:?}", task.metrics));
        summary_parts.push(format!("Grain: {:?}", task.required_grain));
        summary_parts.push(format!("Tables Selected: {}", task.candidate_tables.len()));
        
        if let Some(ref plan) = plan {
            summary_parts.push(format!("Execution Plan: {} nodes, {} edges", plan.nodes.len(), plan.edges.len()));
        }
        
        Ok(summary_parts.join(" | "))
    }
}

