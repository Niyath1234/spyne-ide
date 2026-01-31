//! Goal-Directed Explorer - Iterative Node Selection with Scoring
//! 
//! Iteratively selects best node to explore, stops when task is provably solvable.

use crate::error::{RcaError, Result};
use crate::task_grounder::{GroundedTask, TableCandidate};
use crate::graph::Hypergraph;
use crate::metadata::Metadata;
use crate::fuzzy_matcher::FuzzyMatcher;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{info, debug, warn};

/// Exploration state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplorationState {
    pub explored_nodes: HashSet<String>,
    pub candidate_nodes: Vec<NodeCandidate>,
    pub resolved_fields: HashSet<String>,
    pub required_fields: HashSet<String>,
    pub join_paths_found: Vec<JoinPath>,
    pub cycles: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCandidate {
    pub node_id: String,
    pub node_type: NodeType,
    pub score: f64,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeType {
    Table(String),
    Column(String, String), // table, column
    Join(String, String),   // from, to
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinPath {
    pub from: String,
    pub to: String,
    pub path: Vec<String>,
    pub confidence: f64,
}

/// Goal-Directed Explorer
pub struct GoalDirectedExplorer {
    metadata: Metadata,
    graph: Hypergraph,
    fuzzy_matcher: FuzzyMatcher,
    max_cycles: usize,
    max_time_seconds: u64,
}

impl GoalDirectedExplorer {
    pub fn new(metadata: Metadata, graph: Hypergraph) -> Self {
        Self {
            metadata,
            graph,
            fuzzy_matcher: FuzzyMatcher::new(0.85),
            max_cycles: 10,
            max_time_seconds: 300, // 5 minutes
        }
    }

    /// Explore until task is solvable or max cycles reached
    pub fn explore(&mut self, task: &GroundedTask) -> Result<ExplorationState> {
        info!("Starting goal-directed exploration");
        
        let mut state = ExplorationState {
            explored_nodes: HashSet::new(),
            candidate_nodes: Vec::new(),
            resolved_fields: HashSet::new(),
            required_fields: self.extract_required_fields(task),
            join_paths_found: Vec::new(),
            cycles: 0,
        };
        
        // Initialize candidate nodes from grounded task
        self.initialize_candidates(&mut state, task)?;
        
        // Exploration loop
        while state.cycles < self.max_cycles {
            state.cycles += 1;
            debug!("Exploration cycle {}", state.cycles);
            
            // Check if task is solvable
            if self.is_task_solvable(&state, task) {
                info!("Task is solvable after {} cycles", state.cycles);
                break;
            }
            
            // Score all unexplored nodes
            self.score_candidates(&mut state, task)?;
            
            // Select highest scoring node
            if let Some(best_node) = self.select_best_node(&state) {
                // Explore the node
                self.explore_node(&mut state, &best_node, task)?;
                
                // Mark as explored
                state.explored_nodes.insert(best_node.node_id.clone());
            } else {
                warn!("No more candidate nodes to explore");
                break;
            }
            
            // Re-score remaining candidates based on new information
            self.update_scores(&mut state, task)?;
        }
        
        Ok(state)
    }

    fn extract_required_fields(&self, task: &GroundedTask) -> HashSet<String> {
        let mut fields = HashSet::new();
        
        // Add grain fields
        for grain in &task.required_grain {
            fields.insert(format!("grain:{}", grain));
        }
        
        // Add metric fields
        for metric in &task.metrics {
            fields.insert(format!("metric:{}", metric));
        }
        
        // Add constraint fields
        for constraint in &task.constraint_specs {
            fields.insert(format!("constraint:{}", constraint.column));
        }
        
        // Add unresolved fields
        for field in &task.unresolved_fields {
            fields.insert(field.clone());
        }
        
        fields
    }

    fn initialize_candidates(&self, state: &mut ExplorationState, task: &GroundedTask) -> Result<()> {
        // Add all candidate tables as initial nodes
        for table in &task.candidate_tables {
            let node_id = format!("table:{}", table.table_name);
            if !state.explored_nodes.contains(&node_id) {
                state.candidate_nodes.push(NodeCandidate {
                    node_id: node_id.clone(),
                    node_type: NodeType::Table(table.table_name.clone()),
                    score: table.confidence,
                    reasons: vec![table.reason.clone()],
                });
            }
        }
        
        // Add related tables from graph
        for table in &task.candidate_tables {
            if let Ok(related) = self.find_related_tables(&table.table_name) {
                for related_table in related {
                    let node_id = format!("table:{}", related_table);
                    if !state.explored_nodes.contains(&node_id) &&
                       !state.candidate_nodes.iter().any(|n| n.node_id == node_id) {
                        state.candidate_nodes.push(NodeCandidate {
                            node_id: node_id.clone(),
                            node_type: NodeType::Table(related_table.clone()),
                            score: 0.3, // Lower initial score for related tables
                            reasons: vec![format!("Related to {}", table.table_name)],
                        });
                    }
                }
            }
        }
        
        Ok(())
    }

    fn score_candidates(&mut self, state: &mut ExplorationState, task: &GroundedTask) -> Result<()> {
        for candidate in &mut state.candidate_nodes {
            if state.explored_nodes.contains(&candidate.node_id) {
                continue;
            }
            
            let mut score = candidate.score;
            let mut reasons = candidate.reasons.clone();
            
            // Relevance score (how relevant to required fields)
            let relevance = self.calculate_relevance(candidate, &state.required_fields, task);
            score += relevance * 0.3;
            if relevance > 0.5 {
                reasons.push("high relevance to required fields".to_string());
            }
            
            // Proximity score (how close to already explored nodes)
            let proximity = self.calculate_proximity(candidate, &state.explored_nodes);
            score += proximity * 0.2;
            if proximity > 0.5 {
                reasons.push("close to explored nodes".to_string());
            }
            
            // Grain match score
            let grain_match = self.calculate_grain_match(candidate, &task.required_grain);
            score += grain_match * 0.2;
            if grain_match > 0.5 {
                reasons.push("grain matches".to_string());
            }
            
            // Semantic match score
            let semantic = self.calculate_semantic_match(candidate, task);
            score += semantic * 0.3;
            if semantic > 0.5 {
                reasons.push("semantic match".to_string());
            }
            
            candidate.score = if score > 1.0 { 1.0 } else { score };
            candidate.reasons = reasons;
        }
        
        // Sort by score descending
        state.candidate_nodes.sort_by(|a, b| {
            b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        Ok(())
    }

    fn calculate_relevance(
        &self,
        candidate: &NodeCandidate,
        required_fields: &HashSet<String>,
        task: &GroundedTask,
    ) -> f64 {
        match &candidate.node_type {
            NodeType::Table(table_name) => {
                // Check if table has columns matching required fields
                if let Some(table) = self.metadata.get_table(table_name) {
                    let mut matches = 0;
                    let total = required_fields.len().max(1);
                    
                    for field in required_fields {
                        if field.starts_with("grain:") {
                            let grain_col = field.strip_prefix("grain:").unwrap();
                            if table.primary_key.contains(&grain_col.to_string()) {
                                matches += 1;
                            }
                        } else if field.starts_with("metric:") {
                            // Check if table is mentioned in rules for this metric
                            let metric = field.strip_prefix("metric:").unwrap();
                            if task.metrics.contains(&metric.to_string()) {
                                // Check if table is in rules
                                for system in &task.systems {
                                    let rules = self.metadata.get_rules_for_system_metric(system, metric);
                                    if rules.iter().any(|r| {
                                        r.computation.source_entities.iter()
                                            .any(|e| {
                                                self.metadata.tables.iter()
                                                    .any(|t| t.entity == *e && t.name == *table_name)
                                            })
                                    }) {
                                        matches += 1;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    
                    matches as f64 / total as f64
                } else {
                    0.0
                }
            }
            NodeType::Column(table_name, column_name) => {
                // Check if column matches any required field
                for field in required_fields {
                    if field.contains(column_name) {
                        return 1.0;
                    }
                }
                0.0
            }
            NodeType::Join(from, to) => {
                // Check if join helps connect required tables
                let from_relevant = required_fields.iter().any(|f| f.contains(from));
                let to_relevant = required_fields.iter().any(|f| f.contains(to));
                if from_relevant && to_relevant {
                    1.0
                } else if from_relevant || to_relevant {
                    0.5
                } else {
                    0.0
                }
            }
        }
    }

    fn calculate_proximity(
        &self,
        candidate: &NodeCandidate,
        explored_nodes: &HashSet<String>,
    ) -> f64 {
        if explored_nodes.is_empty() {
            return 0.5; // Neutral if nothing explored yet
        }
        
        match &candidate.node_type {
            NodeType::Table(table_name) => {
                // Check if table is directly connected to explored tables
                let mut connections = 0;
                for explored in explored_nodes {
                    if let Some(explored_table) = explored.strip_prefix("table:") {
                        if self.are_tables_connected(explored_table, table_name) {
                            connections += 1;
                        }
                    }
                }
                
                if connections > 0 {
                    let ratio = connections as f64 / explored_nodes.len() as f64;
                    if ratio > 1.0 { 1.0 } else { ratio }
                } else {
                    0.0
                }
            }
            _ => 0.5, // Default for other types
        }
    }

    fn calculate_grain_match(
        &self,
        candidate: &NodeCandidate,
        required_grain: &[String],
    ) -> f64 {
        match &candidate.node_type {
            NodeType::Table(table_name) => {
                if let Some(table) = self.metadata.get_table(table_name) {
                    let entity = self.metadata.entities_by_id.get(&table.entity);
                    if let Some(entity) = entity {
                        let table_grain: HashSet<String> = entity.grain.iter().cloned().collect();
                        let required: HashSet<String> = required_grain.iter().cloned().collect();
                        
                        let intersection = table_grain.intersection(&required).count();
                        let union = table_grain.union(&required).count();
                        
                        if union > 0 {
                            intersection as f64 / union as f64
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            }
            _ => 0.0,
        }
    }

    fn calculate_semantic_match(
        &self,
        candidate: &NodeCandidate,
        task: &GroundedTask,
    ) -> f64 {
        match &candidate.node_type {
            NodeType::Table(table_name) => {
                // Check semantic similarity to task systems/metrics
                let mut score = 0.0;
                
                for system in &task.systems {
                    if let Some(table) = self.metadata.get_table(table_name) {
                        if self.fuzzy_matcher.is_match(&table.system, system) {
                            score += 0.3;
                        }
                    }
                }
                
                // Check if table name semantically matches metrics
                for metric in &task.metrics {
                    if self.fuzzy_matcher.is_match(table_name, metric) {
                        score += 0.3;
                    }
                }
                
                // LABEL-BASED MATCHING: Check if table has labels matching problem context
                if let Some(table) = self.metadata.get_table(table_name) {
                    if let Some(ref table_labels) = table.labels {
                        let label_score = self.calculate_label_match(table_labels, task);
                        score += label_score * 0.4; // Labels contribute 40% to semantic match
                    }
                }
                
                // Check rules associated with this table for label matches
                let rule_label_score = self.calculate_rule_label_match(table_name, task);
                score += rule_label_score * 0.2;
                
                if score > 1.0 { 1.0 } else { score }
            }
            _ => 0.0,
        }
    }
    
    /// Calculate label match score - how well table labels match problem context
    fn calculate_label_match(&self, table_labels: &[String], task: &GroundedTask) -> f64 {
        // Extract labels/keywords from task (systems, metrics, entities)
        let mut task_labels = HashSet::new();
        
        // Add systems as potential labels
        for system in &task.systems {
            task_labels.insert(system.to_lowercase());
            // Also add variations
            if system.contains('_') {
                task_labels.insert(system.replace('_', "-").to_lowercase());
            }
        }
        
        // Add metrics as potential labels
        for metric in &task.metrics {
            task_labels.insert(metric.to_lowercase());
        }
        
        // Add entities as potential labels
        if let Some(ref entity) = task.candidate_tables.first() {
            task_labels.insert(entity.entity.to_lowercase());
        }
        
        // Count matches
        let mut matches = 0;
        for table_label in table_labels {
            let normalized_label = table_label.to_lowercase();
            if task_labels.contains(&normalized_label) {
                matches += 1;
            } else {
                // Fuzzy match for partial matches
                for task_label in &task_labels {
                    if self.fuzzy_matcher.is_match(&normalized_label, task_label) {
                        matches += 1;
                        break;
                    }
                }
            }
        }
        
        if table_labels.is_empty() {
            0.0
        } else {
            (matches as f64 / table_labels.len() as f64).min(1.0)
        }
    }
    
    /// Calculate label match score from rules associated with this table
    fn calculate_rule_label_match(&self, table_name: &str, task: &GroundedTask) -> f64 {
        let mut score = 0.0;
        let mut rule_count = 0;
        
        // Find rules that use this table
        for rule in &self.metadata.rules {
            // Check if rule's source entities include this table's entity
            if let Some(table) = self.metadata.get_table(table_name) {
                if rule.computation.source_entities.contains(&table.entity) {
                    rule_count += 1;
                    
                    // Check if rule has labels matching task
                    if let Some(ref rule_labels) = rule.labels {
                        let label_match = self.calculate_label_match(rule_labels, task);
                        score += label_match;
                    }
                }
            }
        }
        
        if rule_count > 0 {
            score / rule_count as f64
        } else {
            0.0
        }
    }
    
    /// Calculate label match score for candidate node based on task labels
    fn calculate_label_match_for_task(&self, candidate: &NodeCandidate, task: &GroundedTask) -> f64 {
        if task.labels.is_empty() {
            return 0.0;
        }
        
        match &candidate.node_type {
            NodeType::Table(table_name) => {
                // Check table labels
                if let Some(table) = self.metadata.get_table(table_name) {
                    if let Some(ref table_labels) = table.labels {
                        return self.match_labels(table_labels, &task.labels);
                    }
                }
                
                // Check rules associated with this table
                self.calculate_rule_label_match(table_name, task)
            }
            _ => 0.0,
        }
    }
    
    /// Match two sets of labels and return similarity score
    fn match_labels(&self, labels1: &[String], labels2: &[String]) -> f64 {
        if labels1.is_empty() || labels2.is_empty() {
            return 0.0;
        }
        
        let mut matches = 0;
        let normalized_labels2: HashSet<String> = labels2.iter()
            .map(|l| l.to_lowercase())
            .collect();
        
        for label1 in labels1 {
            let normalized = label1.to_lowercase();
            if normalized_labels2.contains(&normalized) {
                matches += 1;
            } else {
                // Fuzzy match
                for label2 in labels2 {
                    if self.fuzzy_matcher.is_match(&normalized, &label2.to_lowercase()) {
                        matches += 1;
                        break;
                    }
                }
            }
        }
        
        (matches as f64 / labels1.len() as f64).min(1.0)
    }

    fn select_best_node(&self, state: &ExplorationState) -> Option<NodeCandidate> {
        state.candidate_nodes.iter()
            .find(|n| !state.explored_nodes.contains(&n.node_id))
            .cloned()
    }

    fn explore_node(
        &mut self,
        state: &mut ExplorationState,
        node: &NodeCandidate,
        task: &GroundedTask,
    ) -> Result<()> {
        debug!("Exploring node: {:?}", node.node_type);
        
        match &node.node_type {
            NodeType::Table(table_name) => {
                // Inspect table schema
                if let Some(table) = self.metadata.get_table(table_name) {
                    // Check if table has required columns
                    for grain in &task.required_grain {
                        if table.primary_key.contains(grain) {
                            state.resolved_fields.insert(format!("grain:{}", grain));
                        }
                    }
                    
                    // Find related tables
                    if let Ok(related) = self.find_related_tables(table_name) {
                        for related_table in related {
                            let node_id = format!("table:{}", related_table);
                            if !state.explored_nodes.contains(&node_id) &&
                               !state.candidate_nodes.iter().any(|n| n.node_id == node_id) {
                                state.candidate_nodes.push(NodeCandidate {
                                    node_id: node_id.clone(),
                                    node_type: NodeType::Table(related_table.clone()),
                                    score: 0.4,
                                    reasons: vec![format!("Related to {}", table_name)],
                                });
                            }
                        }
                    }
                }
            }
            NodeType::Column(table_name, column_name) => {
                // Mark column as resolved
                state.resolved_fields.insert(format!("column:{}:{}", table_name, column_name));
            }
            NodeType::Join(from, to) => {
                // Find join path
                if let Ok(path) = self.find_join_path(from, to) {
                    state.join_paths_found.push(JoinPath {
                        from: from.clone(),
                        to: to.clone(),
                        path,
                        confidence: 0.8,
                    });
                }
            }
        }
        
        Ok(())
    }

    fn update_scores(&mut self, state: &mut ExplorationState, task: &GroundedTask) -> Result<()> {
        // Re-score based on new discoveries
        self.score_candidates(state, task)
    }

    fn is_task_solvable(&self, state: &ExplorationState, task: &GroundedTask) -> bool {
        // Check if all required fields are resolved
        let all_resolved = state.required_fields.iter()
            .all(|field| {
                state.resolved_fields.contains(field) ||
                // Check if field can be inferred
                self.can_infer_field(field, state, task)
            });
        
        // Check if grain alignment is solvable
        let grain_solvable = !task.required_grain.is_empty() &&
            state.resolved_fields.iter()
                .any(|f| f.starts_with("grain:"));
        
        // Check if join paths are found (if needed)
        let paths_ok = if task.candidate_tables.len() > 1 {
            // Need at least one join path
            !state.join_paths_found.is_empty() || task.candidate_tables.len() == 1
        } else {
            true
        };
        
        all_resolved && grain_solvable && paths_ok
    }

    fn can_infer_field(&self, field: &str, state: &ExplorationState, task: &GroundedTask) -> bool {
        // Check if field can be inferred from explored nodes
        if field.starts_with("grain:") {
            // Grain can be inferred from table primary keys
            state.explored_nodes.iter()
                .any(|n| n.starts_with("table:"))
        } else {
            false
        }
    }

    fn find_related_tables(&self, table_name: &str) -> Result<Vec<String>> {
        let mut related = Vec::new();
        
        // Find tables connected via lineage
        for edge in &self.metadata.lineage.edges {
            if edge.from == table_name {
                related.push(edge.to.clone());
            } else if edge.to == table_name {
                related.push(edge.from.clone());
            }
        }
        
        Ok(related)
    }

    fn are_tables_connected(&self, table1: &str, table2: &str) -> bool {
        // Check lineage edges
        self.metadata.lineage.edges.iter()
            .any(|e| {
                (e.from == table1 && e.to == table2) ||
                (e.from == table2 && e.to == table1)
            })
    }

    fn find_join_path(&self, from: &str, to: &str) -> Result<Vec<String>> {
        // Use graph to find shortest path
        // Simplified - would use graph adapter in production
        if self.are_tables_connected(from, to) {
            Ok(vec![from.to_string(), to.to_string()])
        } else {
            // Try to find intermediate tables
            for edge in &self.metadata.lineage.edges {
                if edge.from == from {
                    if self.are_tables_connected(&edge.to, to) {
                        return Ok(vec![from.to_string(), edge.to.clone(), to.to_string()]);
                    }
                }
            }
            Err(RcaError::Graph(format!("No path found from {} to {}", from, to)))
        }
    }
}

