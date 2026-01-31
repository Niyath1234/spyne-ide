//! Task Grounder - Maps Intent to Tables, Columns, Grain, Constraints
//! 
//! Resolves which tables, columns, grain, and logic are needed to fulfill the intent.

use crate::error::{RcaError, Result};
use crate::intent_compiler::{IntentSpec, TaskType};
use crate::metadata::Metadata;
use crate::fuzzy_matcher::FuzzyMatcher;
use crate::rule_reasoner::RuleReasoner;
use crate::llm::LlmClient;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

/// Grounded task with resolved tables, columns, and constraints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroundedTask {
    /// Candidate tables that might fulfill the task
    pub candidate_tables: Vec<TableCandidate>,
    
    /// Fields that couldn't be resolved (need exploration)
    pub unresolved_fields: Vec<String>,
    
    /// Required grain level
    pub required_grain: Vec<String>,
    
    /// Constraint specifications
    pub constraint_specs: Vec<GroundedConstraint>,
    
    /// Task type
    pub task_type: TaskType,
    
    /// Systems involved (for RCA)
    pub systems: Vec<String>,
    
    /// Metrics involved (for RCA)
    pub metrics: Vec<String>,
    
    /// Labels extracted from problem/query (like JIRA labels)
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCandidate {
    pub table_name: String,
    pub system: String,
    pub entity: String,
    pub grain: Vec<String>,
    pub confidence: f64,
    pub reason: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroundedConstraint {
    pub table: Option<String>,
    pub column: String,
    pub operator: String,
    pub value: serde_json::Value,
    pub description: String,
}

/// Task Grounder - Maps intent to concrete tables/columns/grain
pub struct TaskGrounder {
    metadata: Metadata,
    fuzzy_matcher: FuzzyMatcher,
    rule_reasoner: Option<RuleReasoner>,
    llm_client: Option<LlmClient>,
}

impl TaskGrounder {
    pub fn new(metadata: Metadata) -> Self {
        Self {
            metadata,
            fuzzy_matcher: FuzzyMatcher::new(0.85),
            rule_reasoner: None,
            llm_client: None,
        }
    }

    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm_client = Some(llm.clone());
        self.rule_reasoner = Some(RuleReasoner::new(llm, self.metadata.clone()));
        self
    }

    /// Ground an intent specification to concrete tables/columns
    pub async fn ground(&self, intent: &IntentSpec) -> Result<GroundedTask> {
        info!("Grounding intent: {:?}", intent.task_type);
        
        let mut candidate_tables = Vec::new();
        let mut unresolved_fields = Vec::new();
        
        match intent.task_type {
            TaskType::QUERY => {
                // Direct query - single system, find tables for the system and metric
                for system in &intent.systems {
                    for metric in &intent.target_metrics {
                        match self.find_tables_for_query(system, metric, &intent.grain).await {
                            Ok(tables) => candidate_tables.extend(tables),
                            Err(e) => {
                                warn!("Failed to find tables for {} {}: {}", system, metric, e);
                                unresolved_fields.push(format!("tables for {} {}", system, metric));
                            }
                        }
                    }
                }
            }
            TaskType::RCA => {
                // Find tables for each system and metric
                for system in &intent.systems {
                    for metric in &intent.target_metrics {
                        match self.find_tables_for_rca(system, metric, &intent.grain, intent).await {
                            Ok(tables) => candidate_tables.extend(tables),
                            Err(e) => {
                                warn!("Failed to find tables for {} {}: {}", system, metric, e);
                                unresolved_fields.push(format!("tables for {} {}", system, metric));
                            }
                        }
                    }
                }
            }
            TaskType::DV => {
                // Find tables for validation constraint
                if let Some(ref constraint) = intent.validation_constraint {
                    match self.find_tables_for_dv(constraint, &intent.entities, &intent.grain) {
                        Ok(tables) => candidate_tables.extend(tables),
                        Err(e) => {
                            warn!("Failed to find tables for DV: {}", e);
                            unresolved_fields.push("tables for validation".to_string());
                        }
                    }
                }
            }
        }
        
        // Ground constraints
        let constraint_specs = self.ground_constraints(&intent.constraints, &candidate_tables)?;
        
        // Determine required grain
        let required_grain = if intent.grain.is_empty() {
            // Try to infer from candidate tables
            self.infer_grain(&candidate_tables)?
        } else {
            intent.grain.clone()
        };
        
        // Extract labels from intent (if available) or infer from problem
        let labels = self.extract_labels_from_intent(intent);
        
        Ok(GroundedTask {
            candidate_tables,
            unresolved_fields,
            required_grain,
            constraint_specs,
            task_type: intent.task_type.clone(),
            systems: intent.systems.clone(),
            metrics: intent.target_metrics.clone(),
            labels,
        })
    }

    /// Extract labels from intent (systems, metrics, entities, or explicit labels)
    fn extract_labels_from_intent(&self, intent: &crate::intent_compiler::IntentSpec) -> Vec<String> {
        let mut labels = Vec::new();
        
        // Add systems as labels
        labels.extend(intent.systems.iter().cloned());
        
        // Add metrics as labels
        labels.extend(intent.target_metrics.iter().cloned());
        
        // Add entities as labels
        labels.extend(intent.entities.iter().cloned());
        
        // Normalize labels (lowercase, remove duplicates)
        let normalized: HashSet<String> = labels.iter()
            .map(|l| l.to_lowercase())
            .collect();
        
        normalized.into_iter().collect()
    }

    async fn find_tables_for_rca(
        &self,
        system: &str,
        metric: &str,
        grain: &[String],
        intent: &IntentSpec,
    ) -> Result<Vec<TableCandidate>> {
        let mut candidates = Vec::new();
        
        // Find tables for this system
        let system_tables: Vec<_> = self.metadata.tables
            .iter()
            .filter(|t| self.fuzzy_matcher.is_match(&t.system, system))
            .collect();
        
        if system_tables.is_empty() {
            return Err(RcaError::Metadata(format!("No tables found for system: {}", system)));
        }
        
        // Use RuleReasoner with chain-of-thought reasoning to select best rule
        let selected_rule = if let Some(ref reasoner) = self.rule_reasoner {
            match reasoner.select_rule(intent, system, metric).await {
                Ok(selected) => {
                    info!("✅ RuleReasoner selected: {} (confidence: {:.2})", selected.rule.id, selected.confidence);
                    info!("📝 Reasoning: {}", selected.reasoning);
                    Some(selected.rule)
                }
                Err(e) => {
                    warn!("RuleReasoner failed: {}. Falling back to simple rule lookup.", e);
                    None
                }
            }
        } else {
            None
        };
        
        // Find rules/metrics that match
        let matching_rules = if let Some(ref rule) = selected_rule {
            vec![rule.clone()]
        } else {
            self.metadata.get_rules_for_system_metric(system, metric)
        };
        
        // Use selected rule's source_entities to filter tables
        let relevant_entities: HashSet<String> = if let Some(ref rule) = selected_rule {
            rule.computation.source_entities.iter().cloned().collect()
        } else {
            matching_rules.iter()
                .flat_map(|r| r.computation.source_entities.iter().cloned())
                .collect()
        };
        
        // Use LLM for comprehensive table selection if available
        let mut llm_scored_opt = None;
        if let Some(ref llm) = self.llm_client {
            match self.llm_reason_about_tables(llm, system, metric, &system_tables, &matching_rules, intent, grain).await {
                Ok(llm_scored) => {
                    info!("✅ LLM-based table reasoning completed for {} tables", llm_scored.len());
                    llm_scored_opt = Some(llm_scored);
                }
                Err(e) => {
                    warn!("LLM table reasoning failed: {}. Falling back to rule-based selection.", e);
                }
            }
        }
        
        // Score tables (using LLM results if available)
        if let Some(ref llm_scored) = llm_scored_opt {
            // Merge LLM scores with rule-based scores
            for table in system_tables {
                        let mut confidence = 0.5;
                        let mut reasons = Vec::new();
                        
                        // Get LLM score if available
                        let llm_score = llm_scored.iter()
                            .find(|c| c.table_name == table.name)
                            .map(|c| c.confidence)
                            .unwrap_or(0.0);
                        
                        if llm_score > 0.0 {
                            confidence = llm_score; // Use LLM score as base
                            reasons.push("LLM reasoning".to_string());
                        }
                        
                        // Check if table's entity is in relevant entities from selected rule
                        let table_in_rules = relevant_entities.contains(&table.entity) || 
                            matching_rules.iter().any(|r| {
                                r.computation.source_entities.contains(&table.entity)
                            });
                        
                        if table_in_rules {
                            confidence += 0.2; // Reduced from 0.4 since LLM already considered this
                            if let Some(ref rule) = selected_rule {
                                reasons.push(format!("used by selected rule: {}", rule.id));
                            } else {
                                reasons.push("mentioned in metric rules".to_string());
                            }
                        }
                        
                        // LABEL-BASED GUIDANCE: Check if table has labels matching task labels
                        let task_labels = self.extract_labels_from_intent(intent);
                        if !task_labels.is_empty() {
                            // Check table labels
                            if let Some(ref table_labels) = table.labels {
                                let label_score = self.calculate_label_match(table_labels, &task_labels);
                                if label_score > 0.0 {
                                    confidence += label_score * 0.15; // Reduced weight since LLM considered
                                    reasons.push(format!("label match: {:.0}%", label_score * 100.0));
                                }
                            }
                            
                            // Check rule labels for additional guidance
                            let rule_label_score = self.calculate_rule_label_match(&matching_rules, &task_labels);
                            if rule_label_score > 0.0 {
                                confidence += rule_label_score * 0.1; // Reduced weight
                                reasons.push(format!("rule label match: {:.0}%", rule_label_score * 100.0));
                            }
                        }
                        
                        // Get grain from entity
                        let entity_grain = self.metadata.entities_by_id.get(&table.entity)
                            .map(|e| e.grain.clone())
                            .unwrap_or_default();
                        
                        // Check grain match
                        let grain_match = !grain.is_empty() && 
                            grain.iter().all(|g| entity_grain.contains(g));
                        if grain_match {
                            confidence += 0.1; // Reduced weight
                            reasons.push("grain matches".to_string());
                        }
                        
                        // Get columns (with LLM reasoning if available)
                        let columns = if llm_score > 0.0 {
                            // Use LLM-recommended columns if available
                            llm_scored_opt.as_ref().unwrap().iter()
                                .find(|c| c.table_name == table.name)
                                .map(|c| c.columns.clone())
                                .unwrap_or_else(|| {
                                    table.columns.as_ref()
                                        .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
                                        .unwrap_or_default()
                                })
                        } else {
                            table.columns.as_ref()
                                .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
                                .unwrap_or_default()
                        };
                        
                        candidates.push(TableCandidate {
                            table_name: table.name.clone(),
                            system: table.system.clone(),
                            entity: table.entity.clone(),
                            grain: entity_grain,
                            confidence: confidence.min(1.0), // Cap at 1.0
                            reason: reasons.join(", "),
                            columns,
                        });
                    }
        } else {
            // Rule-based scoring (used if LLM not available or failed)
            for table in system_tables {
                let mut confidence = 0.5;
                let mut reasons = Vec::new();
                
                // Check if table's entity is in relevant entities from selected rule
                let table_in_rules = relevant_entities.contains(&table.entity) || 
                    matching_rules.iter().any(|r| {
                        r.computation.source_entities.contains(&table.entity)
                    });
                
                if table_in_rules {
                    confidence += 0.4;
                    if let Some(ref rule) = selected_rule {
                        reasons.push(format!("used by selected rule: {}", rule.id));
                    } else {
                        reasons.push("mentioned in metric rules".to_string());
                    }
                }
                
                // LABEL-BASED GUIDANCE: Check if table has labels matching task labels
                let task_labels = self.extract_labels_from_intent(intent);
                if !task_labels.is_empty() {
                    // Check table labels
                    if let Some(ref table_labels) = table.labels {
                        let label_score = self.calculate_label_match(table_labels, &task_labels);
                        if label_score > 0.0 {
                            confidence += label_score * 0.3; // Labels contribute up to 30% boost
                            reasons.push(format!("label match: {:.0}%", label_score * 100.0));
                        }
                    }
                    
                    // Check rule labels for additional guidance
                    let rule_label_score = self.calculate_rule_label_match(&matching_rules, &task_labels);
                    if rule_label_score > 0.0 {
                        confidence += rule_label_score * 0.15; // Rule labels contribute up to 15% boost
                        reasons.push(format!("rule label match: {:.0}%", rule_label_score * 100.0));
                    }
                }
                
                // Get grain from entity
                let entity_grain = self.metadata.entities_by_id.get(&table.entity)
                    .map(|e| e.grain.clone())
                    .unwrap_or_default();
                
                // Check grain match
                let grain_match = !grain.is_empty() && 
                    grain.iter().all(|g| entity_grain.contains(g));
                if grain_match {
                    confidence += 0.2;
                    reasons.push("grain matches".to_string());
                }
                
                // Get columns
                let columns: Vec<String> = table.columns.as_ref()
                    .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
                    .unwrap_or_default();
                
                candidates.push(TableCandidate {
                    table_name: table.name.clone(),
                    system: table.system.clone(),
                    entity: table.entity.clone(),
                    grain: entity_grain,
                    confidence,
                    reason: reasons.join(", "),
                    columns,
                });
            }
        }
        
        // Sort by confidence
        candidates.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(candidates)
    }

    /// Find tables for a direct query (single system, single metric)
    async fn find_tables_for_query(
        &self,
        system: &str,
        metric: &str,
        grain: &[String],
    ) -> Result<Vec<TableCandidate>> {
        let mut candidates = Vec::new();
        
        // Find tables for this system
        let system_tables: Vec<_> = self.metadata.tables
            .iter()
            .filter(|t| self.fuzzy_matcher.is_match(&t.system, system))
            .collect();
        
        if system_tables.is_empty() {
            return Err(RcaError::Metadata(format!("No tables found for system: {}", system)));
        }
        
        // Find rules for this system and metric
        let matching_rules = self.metadata.get_rules_for_system_metric(system, metric);
        
        // Use rule's source_entities to filter tables
        let relevant_entities: HashSet<String> = matching_rules.iter()
            .flat_map(|r| r.computation.source_entities.iter().cloned())
            .collect();
        
        for table in system_tables {
            if relevant_entities.contains(&table.entity) || matching_rules.is_empty() {
                let columns: Vec<String> = table.columns.as_ref()
                    .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
                    .unwrap_or_default();
                
                candidates.push(TableCandidate {
                    table_name: table.name.clone(),
                    system: system.to_string(),
                    entity: table.entity.clone(),
                    grain: grain.to_vec(),
                    confidence: 0.8,
                    reason: format!("Table for {} metric in {} system", metric, system),
                    columns,
                });
            }
        }
        
        Ok(candidates)
    }

    fn find_tables_for_dv(
        &self,
        constraint: &crate::intent_compiler::ValidationConstraintSpec,
        entities: &[String],
        grain: &[String],
    ) -> Result<Vec<TableCandidate>> {
        let mut candidates = Vec::new();
        
        // Find tables for entities
        for entity in entities {
            let entity_tables: Vec<_> = self.metadata.tables
                .iter()
                .filter(|t| t.entity == *entity)
                .collect();
            
            for table in entity_tables {
                let mut confidence = 0.5;
                let mut reasons = Vec::new();
                
                // Check if constraint column exists in table
                if let Some(col_name) = constraint.details.get("column").and_then(|v| v.as_str()) {
                    let has_column = table.columns.as_ref()
                        .map(|cols| cols.iter().any(|c| {
                            self.fuzzy_matcher.is_match(&c.name, col_name)
                        }))
                        .unwrap_or(false);
                    
                    if has_column {
                        confidence += 0.3;
                        reasons.push(format!("has column matching '{}'", col_name));
                    }
                }
                
                // Get grain from entity
                let entity_grain = self.metadata.entities_by_id.get(&table.entity)
                    .map(|e| e.grain.clone())
                    .unwrap_or_default();
                
                // Check grain match
                let grain_match = !grain.is_empty() && 
                    grain.iter().all(|g| entity_grain.contains(g));
                if grain_match {
                    confidence += 0.2;
                    reasons.push("grain matches".to_string());
                }
                
                let columns: Vec<String> = table.columns.as_ref()
                    .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
                    .unwrap_or_default();
                
                candidates.push(TableCandidate {
                    table_name: table.name.clone(),
                    system: table.system.clone(),
                    entity: table.entity.clone(),
                    grain: entity_grain,
                    confidence,
                    reason: reasons.join(", "),
                    columns,
                });
            }
        }
        
        // Sort by confidence
        candidates.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(candidates)
    }

    fn ground_constraints(
        &self,
        constraints: &[crate::intent_compiler::ConstraintSpec],
        tables: &[TableCandidate],
    ) -> Result<Vec<GroundedConstraint>> {
        let mut grounded = Vec::new();
        
        for constraint in constraints {
            // Try to find matching column in candidate tables
            let mut found_table = None;
            let mut found_column = None;
            
            if let Some(ref col_name) = constraint.column {
                for table in tables {
                    if let Some(col) = table.columns.iter()
                        .find(|c| self.fuzzy_matcher.is_match(c, col_name)) {
                        found_table = Some(table.table_name.clone());
                        found_column = Some(col.clone());
                        break;
                    }
                }
            }
            
            grounded.push(GroundedConstraint {
                table: found_table,
                column: found_column.unwrap_or_else(|| {
                    constraint.column.clone().unwrap_or_else(|| "unknown".to_string())
                }),
                operator: constraint.operator.clone().unwrap_or_else(|| "=".to_string()),
                value: constraint.value.clone().unwrap_or(serde_json::Value::Null),
                description: constraint.description.clone(),
            });
        }
        
        Ok(grounded)
    }

    fn infer_grain(&self, tables: &[TableCandidate]) -> Result<Vec<String>> {
        if tables.is_empty() {
            return Err(RcaError::Metadata("Cannot infer grain from empty table list".to_string()));
        }
        
        // Use the most common grain across tables
        let mut grain_counts: HashMap<String, usize> = HashMap::new();
        for table in tables {
            for grain_col in &table.grain {
                *grain_counts.entry(grain_col.clone()).or_insert(0) += 1;
            }
        }
        
        // Get most common grain columns
        let mut grain_vec: Vec<_> = grain_counts.into_iter().collect();
        grain_vec.sort_by(|a, b| b.1.cmp(&a.1));
        
        Ok(grain_vec.into_iter().map(|(g, _)| g).collect())
    }

    /// Calculate label match score - how well table labels match task labels
    fn calculate_label_match(&self, table_labels: &[String], task_labels: &[String]) -> f64 {
        if table_labels.is_empty() || task_labels.is_empty() {
            return 0.0;
        }
        
        let mut matches = 0;
        let normalized_task_labels: HashSet<String> = task_labels.iter()
            .map(|l| l.to_lowercase())
            .collect();
        
        for table_label in table_labels {
            let normalized = table_label.to_lowercase();
            if normalized_task_labels.contains(&normalized) {
                matches += 1;
            } else {
                // Fuzzy match
                for task_label in task_labels {
                    if self.fuzzy_matcher.is_match(&normalized, &task_label.to_lowercase()) {
                        matches += 1;
                        break;
                    }
                }
            }
        }
        
        // Return proportion of table labels that matched
        (matches as f64 / table_labels.len() as f64).min(1.0)
    }

    /// Calculate label match score from rules associated with this metric/system
    fn calculate_rule_label_match(&self, rules: &[crate::metadata::Rule], task_labels: &[String]) -> f64 {
        if rules.is_empty() || task_labels.is_empty() {
            return 0.0;
        }
        
        let mut total_score = 0.0;
        let mut rule_count = 0;
        
        for rule in rules {
            // Check if rule has labels matching task
            if let Some(ref rule_labels) = rule.labels {
                let label_match = self.calculate_label_match(rule_labels, task_labels);
                total_score += label_match;
                rule_count += 1;
            }
        }
        
        if rule_count == 0 {
            return 0.0;
        }
        
        // Average score across rules
        total_score / rule_count as f64
    }

    /// Use LLM to reason comprehensively about table selection using all table knowledge
    async fn llm_reason_about_tables(
        &self,
        llm: &LlmClient,
        system: &str,
        metric: &str,
        candidate_tables: &[&crate::metadata::Table],
        rules: &[crate::metadata::Rule],
        intent: &IntentSpec,
        grain: &[String],
    ) -> Result<Vec<TableCandidate>> {
        info!("🤖 LLM reasoning about {} candidate tables for {} {}", candidate_tables.len(), system, metric);
        
        // Build comprehensive table knowledge context
        let mut table_contexts = Vec::new();
        for table in candidate_tables {
            let mut context = serde_json::json!({
                "name": table.name,
                "system": table.system,
                "entity": table.entity,
                "primary_key": table.primary_key,
                "time_column": table.time_column,
                "labels": table.labels.as_ref().unwrap_or(&Vec::new()),
            });
            
            // Add column metadata
            if let Some(ref columns) = table.columns {
                let column_info: Vec<_> = columns.iter().map(|c| {
                    serde_json::json!({
                        "name": c.name,
                        "data_type": c.data_type.as_ref().unwrap_or(&"unknown".to_string()),
                        "description": c.description.as_ref().unwrap_or(&"".to_string()),
                        "distinct_values_count": c.distinct_values.as_ref().map(|v| v.len()).unwrap_or(0),
                    })
                }).collect();
                context["columns"] = serde_json::Value::Array(column_info);
            }
            
            // Add entity metadata
            if let Some(entity) = self.metadata.entities_by_id.get(&table.entity) {
                context["entity_grain"] = serde_json::Value::Array(
                    entity.grain.iter().map(|g| serde_json::Value::String(g.clone())).collect()
                );
                context["entity_attributes"] = serde_json::Value::Array(
                    entity.attributes.iter().map(|a| serde_json::Value::String(a.clone())).collect()
                );
            }
            
            // Add lineage relationships
            let related_tables: Vec<_> = self.metadata.lineage.edges.iter()
                .filter(|e| e.from == table.name || e.to == table.name)
                .map(|e| {
                    if e.from == table.name {
                        e.to.clone()
                    } else {
                        e.from.clone()
                    }
                })
                .collect();
            context["related_tables"] = serde_json::Value::Array(
                related_tables.iter().map(|t| serde_json::Value::String(t.clone())).collect()
            );
            
            table_contexts.push(context);
        }
        
        // Build rules context
        let rules_context: Vec<_> = rules.iter().map(|r| {
            serde_json::json!({
                "id": r.id,
                "description": r.computation.description,
                "formula": r.computation.formula,
                "source_entities": r.computation.source_entities,
                "target_entity": r.target_entity,
                "target_grain": r.target_grain,
                "labels": r.labels.as_ref().unwrap_or(&Vec::new()),
            })
        }).collect();
        
        // Build intent context
        let intent_context = serde_json::json!({
            "systems": intent.systems,
            "metrics": intent.target_metrics,
            "entities": intent.entities,
            "grain": grain,
            "constraints": intent.constraints,
            "labels": self.extract_labels_from_intent(intent),
        });
        
        // Create comprehensive prompt
        let prompt = format!(r#"
You are an expert data analyst reasoning about which tables and columns to use for an RCA (Root Cause Analysis) task.

TASK CONTEXT:
- System: {}
- Metric: {}
- Intent: {}

AVAILABLE TABLES (with full metadata):
{}

AVAILABLE RULES (business logic):
{}

YOUR TASK:
1. Analyze each table's relevance considering:
   - Table name, system, entity
   - Column metadata (names, types, descriptions)
   - Entity grain and attributes
   - Table relationships/lineage
   - Labels and semantic meaning
   - Rule requirements

2. Score each table (0.0 to 1.0) based on:
   - How well it matches the metric/system requirements
   - Column availability for the metric calculation
   - Entity grain compatibility
   - Relationship to other relevant tables
   - Semantic relevance (labels, descriptions)

3. Identify which columns from each table are most relevant for:
   - The metric calculation
   - Joining with other tables
   - Filtering/constraints

Return JSON array of table candidates with scores and relevant columns:
[
  {{
    "table_name": "table_name",
    "confidence": 0.95,
    "reasoning": "Why this table is relevant",
    "columns": ["col1", "col2", ...]
  }},
  ...
]
"#,
            system,
            metric,
            serde_json::to_string_pretty(&intent_context)?,
            serde_json::to_string_pretty(&table_contexts)?,
            serde_json::to_string_pretty(&rules_context)?,
        );
        
        // Call LLM
        let response = llm.call_llm(&prompt).await?;
        
        // Parse LLM response
        let cleaned = self.extract_json_from_response(&response);
        let llm_results: Vec<serde_json::Value> = serde_json::from_str(&cleaned)
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM table reasoning: {}. Response: {}", e, cleaned)))?;
        
        // Convert to TableCandidate
        let mut candidates = Vec::new();
        for result in llm_results {
            let table_name = result["table_name"].as_str()
                .ok_or_else(|| RcaError::Llm("Missing table_name in LLM response".to_string()))?
                .to_string();
            
            // Find the actual table to get full metadata
            let table = candidate_tables.iter()
                .find(|t| t.name == table_name)
                .ok_or_else(|| RcaError::Llm(format!("Table {} not found in candidates", table_name)))?;
            
            let confidence = result["confidence"].as_f64()
                .unwrap_or(0.5)
                .min(1.0)
                .max(0.0);
            
            let columns: Vec<String> = result["columns"]
                .as_array()
                .map(|arr| arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect())
                .unwrap_or_else(|| {
                    // Fallback to all columns if LLM didn't specify
                    table.columns.as_ref()
                        .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
                        .unwrap_or_default()
                });
            
            let entity_grain = self.metadata.entities_by_id.get(&table.entity)
                .map(|e| e.grain.clone())
                .unwrap_or_default();
            
            candidates.push(TableCandidate {
                table_name,
                system: table.system.clone(),
                entity: table.entity.clone(),
                grain: entity_grain,
                confidence,
                reason: result["reasoning"].as_str().unwrap_or("LLM reasoning").to_string(),
                columns,
            });
        }
        
        Ok(candidates)
    }

    /// Extract JSON from LLM response (handles markdown code blocks)
    fn extract_json_from_response(&self, response: &str) -> String {
        // Try to find JSON array or object
        let json_start = response.find('[').or_else(|| response.find('{'));
        let json_end = response.rfind(']').or_else(|| response.rfind('}'));
        
        if let (Some(start), Some(end)) = (json_start, json_end) {
            response[start..=end].to_string()
        } else {
            // Try to extract from markdown code blocks
            if let Some(start) = response.find("```json") {
                let after_start = &response[start + 7..];
                if let Some(end) = after_start.find("```") {
                    return after_start[..end].trim().to_string();
                }
            }
            if let Some(start) = response.find("```") {
                let after_start = &response[start + 3..];
                if let Some(end) = after_start.find("```") {
                    return after_start[..end].trim().to_string();
                }
            }
            response.to_string()
        }
    }
}

