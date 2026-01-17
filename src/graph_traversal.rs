//! Dynamic Graph Traversal System
//! 
//! Implements the "Traverse → Test → Observe → Decide → Repeat" pattern for RCA.
//! 
//! Instead of a fixed pipeline, the agent dynamically navigates a graph of nodes:
//! - Tables
//! - Rules  
//! - Joins
//! - Filters
//! - Metrics
//! 
//! At each node, it:
//! 1. Chooses the next best node to visit
//! 2. Runs a small SQL probe at that node
//! 3. Observes the result
//! 4. Decides the next step based on what it learned
//! 5. Repeats until root cause is found

use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use crate::knowledge_hints::{KnowledgeHints, TermHit};
use crate::sql_engine::{SqlEngine, SqlProbeResult};
use crate::llm::LlmClient;
use crate::graph::Hypergraph;
use crate::agent_prompts::{
    build_node_selection_prompt, build_result_interpretation_prompt,
    build_sql_generation_prompt, build_hypothesis_prompt,
    NodeSelectionResponse, ResultInterpretationResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::{info, debug, warn};

/// Node types in the knowledge graph
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NodeType {
    /// Base table node
    Table(String),
    
    /// Business rule node
    Rule(String),
    
    /// Join relationship node
    Join { from: String, to: String },
    
    /// Filter node
    Filter { table: String, condition: String },
    
    /// Metric calculation node
    Metric { name: String, system: String },
}

/// A node in the traversal graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalNode {
    pub node_id: String,
    pub node_type: NodeType,
    pub visited: bool,
    pub visit_count: usize,
    pub last_probe_result: Option<SqlProbeResult>,
    pub score: f64,
    pub reasons: Vec<String>,
    /// Rich metadata for LLM context (table columns, rule formulas, join keys, etc.)
    pub metadata: Option<NodeMetadata>,
}

/// Rich metadata for a node - provides full context to LLM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    /// For Table nodes: table metadata
    pub table_info: Option<TableNodeMetadata>,
    /// For Rule nodes: rule metadata
    pub rule_info: Option<RuleNodeMetadata>,
    /// For Join nodes: join metadata
    pub join_info: Option<JoinNodeMetadata>,
    /// For Filter nodes: filter metadata
    pub filter_info: Option<FilterNodeMetadata>,
    /// For Metric nodes: metric metadata
    pub metric_info: Option<MetricNodeMetadata>,
    /// Hypergraph statistics if available
    pub hypergraph_stats: Option<HypergraphStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableNodeMetadata {
    pub name: String,
    pub system: String,
    pub entity: String,
    pub primary_key: Vec<String>,
    pub time_column: Option<String>,
    pub columns: Vec<ColumnInfo>,
    pub labels: Vec<String>,
    pub grain: Vec<String>,
    pub attributes: Vec<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: Option<String>,
    pub description: Option<String>,
    pub distinct_values_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleNodeMetadata {
    pub id: String,
    pub system: String,
    pub metric: String,
    pub description: String,
    pub formula: String,
    pub source_entities: Vec<String>,
    pub target_entity: String,
    pub target_grain: Vec<String>,
    pub filter_conditions: Option<HashMap<String, String>>,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinNodeMetadata {
    pub from_table: String,
    pub to_table: String,
    pub join_keys: HashMap<String, String>, // left_col -> right_col
    pub join_type: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterNodeMetadata {
    pub table: String,
    pub condition: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricNodeMetadata {
    pub name: String,
    pub system: String,
    pub description: String,
    pub grain: Vec<String>,
    pub precision: u32,
    pub unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HypergraphStats {
    pub row_count: Option<u64>,
    pub distinct_count: Option<f64>,
    pub null_percentage: Option<f64>,
    pub data_quality_score: Option<f64>,
    pub top_n_values: Vec<String>,
    pub join_selectivity: Option<f64>,
    pub filter_selectivity: Option<f64>,
}

/// State of the traversal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalState {
    /// All nodes in the graph
    pub nodes: HashMap<String, TraversalNode>,
    
    /// Currently visited nodes (in order)
    pub visited_path: Vec<String>,
    
    /// Root cause findings
    pub findings: Vec<Finding>,
    
    /// Hints collected from knowledge base or graph
    pub hints: Vec<TraversalHint>,
    
    /// Current hypothesis about the root cause
    pub current_hypothesis: Option<String>,
    
    /// Whether we've found the root cause
    pub root_cause_found: bool,
    
    /// Maximum traversal depth
    pub max_depth: usize,
    
    /// Current depth
    pub current_depth: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    pub node_id: String,
    pub finding_type: FindingType,
    pub description: String,
    pub evidence: SqlProbeResult,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FindingType {
    MissingRows,
    ValueMismatch,
    JoinFailure,
    FilterIssue,
    RuleDiscrepancy,
    DataQualityIssue,
}

/// Hint to guide traversal (from knowledge base, hypergraph, or heuristics)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalHint {
    pub hint_type: HintType,
    pub description: String,
    pub confidence: f64,
    pub related_nodes: Vec<String>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HintType {
    KnowledgeTerm,
    KnowledgeFilter,
    KnowledgeRelationship,
    HypergraphStat,
    LlmSuggestion,
    Heuristic,
}

/// Graph Traversal Agent
/// 
/// Dynamically navigates the knowledge graph to find root causes
pub struct GraphTraversalAgent {
    metadata: Metadata,
    graph: Hypergraph,
    sql_engine: SqlEngine,
    llm_client: Option<LlmClient>,
    knowledge_hints: Option<KnowledgeHints>,
}

// Make graph mutable for adapter access
impl GraphTraversalAgent {
    /// Get mutable reference to graph (for adapter access)
    fn graph_mut(&mut self) -> &mut Hypergraph {
        &mut self.graph
    }
}

impl GraphTraversalAgent {
    /// Create a new traversal agent
    pub fn new(
        metadata: Metadata,
        graph: Hypergraph,
        sql_engine: SqlEngine,
    ) -> Self {
        Self {
            metadata,
            graph,
            sql_engine,
            llm_client: None,
            knowledge_hints: None,
        }
    }
    
    /// Set LLM client for decision making
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm_client = Some(llm);
        self
    }
    
    /// Attach knowledge hints (from knowledge base JSON or other sources)
    pub fn with_knowledge_hints(mut self, hints: KnowledgeHints) -> Self {
        self.knowledge_hints = Some(hints);
        self
    }
    
    /// Load knowledge hints from a JSON file path
    pub fn with_knowledge_hints_from_path(
        mut self,
        path: impl AsRef<Path>,
    ) -> Result<Self> {
        if let Some(hints) = KnowledgeHints::load(path)? {
            self.knowledge_hints = Some(hints);
        }
        Ok(self)
    }
    
    /// Start traversal from initial problem
    /// 
    /// Problem example: "Why is paid_amount different on 2026-01-08 between system A and B?"
    pub async fn traverse(
        &mut self,
        problem: &str,
        initial_metric: &str,
        system_a: &str,
        system_b: &str,
        date_constraint: Option<&str>,
    ) -> Result<TraversalState> {
        info!("🚀 Starting graph traversal for problem: {}", problem);
        
        // Initialize state
        let mut state = self.initialize_state(initial_metric, system_a, system_b).await?;
        
        // Build initial graph of nodes
        self.build_initial_graph(&mut state, problem, initial_metric, system_a, system_b).await?;
        
        // Apply knowledge-base hints to seed additional nodes
        self.apply_knowledge_hints(&mut state, problem)?;
        
        // Traversal loop: Traverse → Test → Observe → Decide → Repeat
        while !state.root_cause_found && state.current_depth < state.max_depth {
            state.current_depth += 1;
            info!("📍 Traversal depth: {}", state.current_depth);
            
            // Step 1: Choose next best node to visit
            let next_node = self.choose_next_node(&state).await?;
            
            if let Some(node) = next_node {
                info!("🎯 Selected node: {} ({:?})", node.node_id, node.node_type);
                
                // Step 2: Run SQL probe at this node
                let probe_result = self.probe_node(&node, date_constraint).await?;
                
                // Step 3: Observe the result
                let observations = self.observe_probe_result(&probe_result, &node, &state).await?;
                
                // Step 4: Decide next step based on observations
                let decision = self.decide_next_step(&observations, &node, &state, &probe_result).await?;
                
                // Update state
                state.nodes.get_mut(&node.node_id).unwrap().visited = true;
                state.nodes.get_mut(&node.node_id).unwrap().visit_count += 1;
                state.nodes.get_mut(&node.node_id).unwrap().last_probe_result = Some(probe_result.clone());
                state.visited_path.push(node.node_id.clone());
                
                // Record findings
                if let Some(finding) = decision.finding {
                    state.findings.push(finding);
                }
                
                if state.current_hypothesis.is_none() {
                    if let Some(ref hypothesis) = observations.llm_hypothesis {
                        state.current_hypothesis = Some(hypothesis.clone());
                    }
                }
                
                // Check if root cause found
                if decision.root_cause_found {
                    state.root_cause_found = true;
                    state.current_hypothesis = decision.hypothesis;
                    break;
                }
                
                // Add new candidate nodes based on observations
                if let Some(new_nodes) = decision.new_candidate_nodes {
                    for new_node in new_nodes {
                        state.nodes.insert(new_node.node_id.clone(), new_node);
                    }
                }
            } else {
                warn!("No more nodes to explore");
                break;
            }
        }
        
        info!("✅ Traversal completed. Found {} findings", state.findings.len());
        
        Ok(state)
    }
    
    /// Initialize traversal state
    async fn initialize_state(
        &self,
        metric: &str,
        system_a: &str,
        system_b: &str,
    ) -> Result<TraversalState> {
        Ok(TraversalState {
            nodes: HashMap::new(),
            visited_path: Vec::new(),
            findings: Vec::new(),
            hints: Vec::new(),
            current_hypothesis: None,
            root_cause_found: false,
            max_depth: 20, // Maximum traversal depth
            current_depth: 0,
        })
    }
    
    /// Build initial graph of nodes
    async fn build_initial_graph(
        &mut self,
        state: &mut TraversalState,
        _problem: &str,
        metric: &str,
        system_a: &str,
        system_b: &str,
    ) -> Result<()> {
        info!("🔨 Building initial graph for metric: {}", metric);
        
        // Add metric nodes for both systems with rich metadata
        let metric_a_metadata = self.build_metric_metadata(metric, system_a)?;
        state.nodes.insert(
            format!("metric:{}:{}", system_a, metric).clone(),
            TraversalNode {
                node_id: format!("metric:{}:{}", system_a, metric),
                node_type: NodeType::Metric {
                    name: metric.to_string(),
                    system: system_a.to_string(),
                },
                visited: false,
                visit_count: 0,
                last_probe_result: None,
                score: 1.0, // High priority - start here
                reasons: vec!["Initial metric node for system A".to_string()],
                metadata: Some(metric_a_metadata),
            },
        );
        
        let metric_b_metadata = self.build_metric_metadata(metric, system_b)?;
        state.nodes.insert(
            format!("metric:{}:{}", system_b, metric),
            TraversalNode {
                node_id: format!("metric:{}:{}", system_b, metric),
                node_type: NodeType::Metric {
                    name: metric.to_string(),
                    system: system_b.to_string(),
                },
                visited: false,
                visit_count: 0,
                last_probe_result: None,
                score: 1.0,
                reasons: vec!["Initial metric node for system B".to_string()],
                metadata: Some(metric_b_metadata),
            },
        );
        
        // Find rules for this metric
        let rules_a = self.metadata.get_rules_for_system_metric(system_a, metric);
        let rules_b = self.metadata.get_rules_for_system_metric(system_b, metric);
        
        // Add rule nodes with rich metadata
        for rule in rules_a.iter().chain(rules_b.iter()) {
            let rule_metadata = self.build_rule_metadata(rule)?;
            state.nodes.insert(
                format!("rule:{}", rule.id),
                TraversalNode {
                    node_id: format!("rule:{}", rule.id),
                    node_type: NodeType::Rule(rule.id.clone()),
                    visited: false,
                    visit_count: 0,
                    last_probe_result: None,
                    score: 0.8,
                    reasons: vec!["Rule for metric calculation".to_string()],
                    metadata: Some(rule_metadata),
                },
            );
        }
        
        // Add table nodes from rules
        let mut relevant_tables: HashSet<String> = HashSet::new();
        for rule in rules_a.iter().chain(rules_b.iter()) {
            for entity in &rule.computation.source_entities {
                let tables: Vec<_> = self.metadata.tables
                    .iter()
                    .filter(|t| t.entity == *entity && (t.system == system_a || t.system == system_b))
                    .cloned()
                    .collect();
                
                for table in tables {
                    let table_metadata = self.build_table_metadata(&table)?;
                    state.nodes.insert(
                        format!("table:{}", table.name),
                        TraversalNode {
                            node_id: format!("table:{}", table.name),
                            node_type: NodeType::Table(table.name.clone()),
                            visited: false,
                            visit_count: 0,
                            last_probe_result: None,
                            score: 0.6,
                            reasons: vec!["Table used in rule".to_string()],
                            metadata: Some(table_metadata),
                        },
                    );
                    relevant_tables.insert(table.name.clone());
                }
            }
        }
        
        // Add filter nodes from rule filter conditions
        for rule in rules_a.iter().chain(rules_b.iter()) {
            if let Some(ref filters) = rule.computation.filter_conditions {
                for (column, value) in filters {
                    let condition = self.format_filter_condition(column, value);
                    for entity in &rule.computation.source_entities {
                        let tables: Vec<_> = self.metadata.tables
                            .iter()
                            .filter(|t| t.entity == *entity && t.system == rule.system)
                            .collect();
                        for table in tables {
                            let node_id = format!("filter:{}:{}", table.name, condition);
                            if !state.nodes.contains_key(&node_id) {
                                let filter_metadata = NodeMetadata {
                                    table_info: None,
                                    rule_info: None,
                                    join_info: None,
                                    filter_info: Some(FilterNodeMetadata {
                                        table: table.name.clone(),
                                        condition: condition.clone(),
                                        description: Some(format!("Rule filter from {}", rule.id)),
                                    }),
                                    metric_info: None,
                                    hypergraph_stats: None,
                                };
                                state.nodes.insert(
                                    node_id.clone(),
                                    TraversalNode {
                                        node_id,
                                        node_type: NodeType::Filter {
                                            table: table.name.clone(),
                                            condition: condition.clone(),
                                        },
                                        visited: false,
                                        visit_count: 0,
                                        last_probe_result: None,
                                        score: 0.55,
                                        reasons: vec![format!("Filter condition from rule {}", rule.id)],
                                        metadata: Some(filter_metadata),
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
        
        // Add join nodes for relevant tables from lineage
        let edges: Vec<_> = self.metadata.lineage.edges.iter().cloned().collect();
        for edge in edges {
            if relevant_tables.contains(&edge.from) || relevant_tables.contains(&edge.to) {
                let node_id = format!("join:{}:{}", edge.from, edge.to);
                if !state.nodes.contains_key(&node_id) {
                    let join_metadata = self.build_join_metadata(&edge.from, &edge.to, &edge.keys)?;
                    state.nodes.insert(
                        node_id.clone(),
                        TraversalNode {
                            node_id,
                            node_type: NodeType::Join {
                                from: edge.from.clone(),
                                to: edge.to.clone(),
                            },
                            visited: false,
                            visit_count: 0,
                            last_probe_result: None,
                            score: 0.45,
                            reasons: vec!["Lineage join from metadata".to_string()],
                            metadata: Some(join_metadata),
                        },
                    );
                }
            }
        }
        
        info!("✅ Built graph with {} nodes", state.nodes.len());
        
        Ok(())
    }

    /// Apply knowledge base hints to seed additional nodes and hints
    fn apply_knowledge_hints(&mut self, state: &mut TraversalState, problem: &str) -> Result<()> {
        let hints = if let Some(ref h) = self.knowledge_hints {
            h.clone()
        } else {
            return Ok(());
        };

        let mut new_hints = Vec::new();
        let term_hits = hints.find_term_hits(problem);
        for hit in term_hits {
            let mut related_nodes = Vec::new();
            let related_tables = hit.hint.related_tables.clone();
            for table_name in &related_tables {
                if let Some(table) = self.metadata.get_table(table_name).cloned() {
                    if let Ok(node) = self.make_table_node(&table, 0.7, format!("KB term: {}", hit.term)) {
                        related_nodes.push(node.node_id.clone());
                        state.nodes.entry(node.node_id.clone()).or_insert(node);
                    }
                }
            }

            let term_filters = self.collect_term_filters(&hit);
            for filter in term_filters {
                for table_name in &hit.hint.related_tables {
                    let node_id = format!("filter:{}:{}", table_name, filter);
                    if !state.nodes.contains_key(&node_id) {
                        let filter_metadata = NodeMetadata {
                            table_info: None,
                            rule_info: None,
                            join_info: None,
                            filter_info: Some(FilterNodeMetadata {
                                table: table_name.clone(),
                                condition: filter.clone(),
                                description: Some(format!("KB filter from term {}", hit.term)),
                            }),
                            metric_info: None,
                            hypergraph_stats: None,
                        };
                        state.nodes.insert(
                            node_id.clone(),
                            TraversalNode {
                                node_id: node_id.clone(),
                                node_type: NodeType::Filter {
                                    table: table_name.clone(),
                                    condition: filter.clone(),
                                },
                                visited: false,
                                visit_count: 0,
                                last_probe_result: None,
                                score: 0.6,
                                reasons: vec![format!("KB filter for {}", hit.term)],
                                metadata: Some(filter_metadata),
                            },
                        );
                        related_nodes.push(node_id);
                    }
                }
            }

            if !related_nodes.is_empty() {
                new_hints.push(TraversalHint {
                    hint_type: HintType::KnowledgeTerm,
                    description: format!("KB term '{}' matched via '{}'", hit.term, hit.matched),
                    confidence: 0.75,
                    related_nodes,
                    source: "knowledge_base".to_string(),
                });
            }
        }

        // Direct table mentions from knowledge hints
        let table_mentions = hints.find_table_mentions(problem);
        for table_name in table_mentions {
            if let Some(table) = self.metadata.get_table(&table_name).cloned() {
                if let Ok(node) = self.make_table_node(&table, 0.65, "KB table mention".to_string()) {
                    let node_id = node.node_id.clone();
                    state.nodes.entry(node_id.clone()).or_insert(node);
                    new_hints.push(TraversalHint {
                        hint_type: HintType::KnowledgeTerm,
                        description: format!("KB table '{}' mentioned in problem", table_name),
                        confidence: 0.6,
                        related_nodes: vec![node_id],
                        source: "knowledge_base".to_string(),
                    });
                }
            }
        }

        // Relationship hints from knowledge base
        let relationships: Vec<(String, _)> = hints.relationships.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for (rel_key, rel_hint) in relationships {
            if let Some((from_table, to_table)) = self.parse_relationship_key(&rel_key) {
                if let Ok(join_keys) = self.find_join_keys(&from_table, &to_table) {
                    let node_id = format!("join:{}:{}", from_table, to_table);
                    if !state.nodes.contains_key(&node_id) {
                        let join_metadata = self.build_join_metadata(&from_table, &to_table, &join_keys)?;
                        state.nodes.insert(
                            node_id.clone(),
                            TraversalNode {
                                node_id: node_id.clone(),
                                node_type: NodeType::Join {
                                    from: from_table.clone(),
                                    to: to_table.clone(),
                                },
                                visited: false,
                                visit_count: 0,
                                last_probe_result: None,
                                score: 0.55,
                                reasons: vec![format!("KB relationship hint: {}", rel_key)],
                                metadata: Some(join_metadata),
                            },
                        );
                        new_hints.push(TraversalHint {
                            hint_type: HintType::KnowledgeRelationship,
                            description: rel_hint.description.clone().unwrap_or_else(|| rel_key.clone()),
                            confidence: 0.6,
                            related_nodes: vec![node_id],
                            source: "knowledge_base".to_string(),
                        });
                    }
                }
            }
        }

        state.hints.extend(new_hints);
        Ok(())
    }
    
    /// Choose the next best node to visit
    async fn choose_next_node(&self, state: &TraversalState) -> Result<Option<TraversalNode>> {
        let mut candidates: Vec<TraversalNode> = state.nodes
            .values()
            .filter(|n| !n.visited)
            .cloned()
            .collect();
        
        if candidates.is_empty() {
            return Ok(None);
        }
        
        // Score nodes based on findings, hints, proximity, and hypergraph stats
        for candidate in &mut candidates {
            let (score, reasons) = self.score_candidate(candidate, state);
            candidate.score = score;
            candidate.reasons = reasons;
        }
        
        // Sort by score
        candidates.sort_by(|a, b| {
            b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        // Use LLM to make final decision if available
        if let Some(ref llm) = self.llm_client {
            if let Ok(llm_choice) = self.llm_choose_node(llm, &candidates, state).await {
                return Ok(Some(llm_choice));
            }
        }
        
        Ok(candidates.first().cloned())
    }
    
    fn score_candidate(&self, candidate: &TraversalNode, state: &TraversalState) -> (f64, Vec<String>) {
        let mut score = candidate.score;
        let mut reasons = candidate.reasons.clone();
        
        // Boost score if connected to recently visited nodes
        if !state.visited_path.is_empty() {
            let last_visited = &state.visited_path[state.visited_path.len() - 1];
            if self.are_nodes_connected(candidate, last_visited) {
                score += 0.2;
                reasons.push("connected to last visited node".to_string());
            }
        }
        
        // Boost score if relevant to findings
        for finding in &state.findings {
            if self.is_node_relevant_to_finding(candidate, finding) {
                score += 0.3;
                reasons.push("relevant to findings".to_string());
                break;
            }
        }
        
        // Boost score if hinted by knowledge base
        for hint in &state.hints {
            if hint.related_nodes.iter().any(|n| n == &candidate.node_id) {
                score += 0.25 * hint.confidence;
                reasons.push(format!("hint: {}", hint.description));
                break;
            }
        }
        
        // Boost score based on hypergraph statistics
        if let Some(ref metadata) = candidate.metadata {
            if let Some(ref stats) = metadata.hypergraph_stats {
                if let Some(row_count) = stats.row_count {
                    if row_count == 0 {
                        score += 0.2;
                        reasons.push("hypergraph indicates zero rows".to_string());
                    }
                }
                if let Some(null_pct) = stats.null_percentage {
                    if null_pct > 0.2 {
                        score += 0.15;
                        reasons.push("high null percentage".to_string());
                    }
                }
                if let Some(quality) = stats.data_quality_score {
                    if quality < 70.0 {
                        score += 0.1;
                        reasons.push("low data quality score".to_string());
                    }
                }
                if let Some(selectivity) = stats.join_selectivity {
                    if selectivity < 0.3 {
                        score += 0.1;
                        reasons.push("low join selectivity".to_string());
                    }
                }
            }
        }
        
        if candidate.visit_count > 0 {
            score -= 0.1;
        }
        
        (score.min(1.0).max(0.0), reasons)
    }
    
    /// Check if two nodes are connected
    fn are_nodes_connected(&self, node1: &TraversalNode, node2_id: &str) -> bool {
        match (&node1.node_type, node2_id) {
            (NodeType::Table(t1), id) if id.starts_with("table:") => {
                let t2 = id.strip_prefix("table:").unwrap();
                // Check if tables are connected via lineage
                self.metadata.lineage.edges.iter().any(|e| {
                    (e.from == *t1 && e.to == t2) || (e.from == t2 && e.to == *t1)
                })
            }
            (NodeType::Join { from, to }, id) => {
                id.starts_with("table:") && {
                    let table = id.strip_prefix("table:").unwrap();
                    from == table || to == table
                }
            }
            _ => false,
        }
    }
    
    /// Check if node is relevant to a finding
    fn is_node_relevant_to_finding(&self, node: &TraversalNode, finding: &Finding) -> bool {
        match (&node.node_type, &finding.finding_type) {
            (NodeType::Join { .. }, FindingType::JoinFailure) => true,
            (NodeType::Filter { .. }, FindingType::FilterIssue) => true,
            (NodeType::Rule(_), FindingType::RuleDiscrepancy) => true,
            (NodeType::Table(_), FindingType::MissingRows) => true,
            _ => false,
        }
    }
    
    /// Use LLM to choose next node
    async fn llm_choose_node(
        &self,
        llm: &LlmClient,
        candidates: &[TraversalNode],
        state: &TraversalState,
    ) -> Result<TraversalNode> {
        let prompt = build_node_selection_prompt(
            candidates,
            &state.findings,
            &state.visited_path,
            "RCA investigation",
            &state.hints,
            state.current_hypothesis.as_deref(),
        );
        
        let response = llm.call_llm(&prompt).await?;
        let cleaned = self.extract_json(&response);
        let choice: NodeSelectionResponse = serde_json::from_str(&cleaned)
            .map_err(|e| RcaError::Llm(format!("Failed to parse node selection: {}. Response: {}", e, &cleaned[..cleaned.len().min(500)])))?;
        
        info!("🤖 LLM selected node: {} (confidence: {:.2})", choice.node_id, choice.confidence);
        info!("   Reasoning: {}", choice.reasoning);
        info!("   Expected insight: {}", choice.expected_insight);
        
        candidates.iter()
            .find(|n| n.node_id == choice.node_id)
            .cloned()
            .ok_or_else(|| RcaError::Llm(format!("Node {} not found in candidates", choice.node_id)))
    }
    
    /// Run SQL probe at a node
    async fn probe_node(
        &self,
        node: &TraversalNode,
        date_constraint: Option<&str>,
    ) -> Result<SqlProbeResult> {
        match &node.node_type {
            NodeType::Table(table_name) => {
                // Probe: Get base rows from table
                if let Some(date) = date_constraint {
                    // Try to find date column
                    if let Some(table) = self.metadata.tables.iter().find(|t| t.name == *table_name) {
                        if !table.time_column.is_empty() {
                            let date_col = &table.time_column;
                            let condition = format!("{} = '{}'", date_col, date);
                            return self.sql_engine.probe_filter(table_name, &condition).await;
                        }
                    }
                }
                let sql = format!("SELECT * FROM {} LIMIT 100", table_name);
                self.sql_engine.execute_probe(&sql, Some(100)).await
            }
            NodeType::Join { from, to } => {
                // Probe: Test the join
                // Find join keys from metadata
                let join_keys = self.find_join_keys(from, to)?;
                self.sql_engine.probe_join_failures(from, to, &join_keys, "left").await
            }
            NodeType::Filter { table, condition } => {
                // Probe: Test the filter
                self.sql_engine.probe_filter(table, condition).await
            }
            NodeType::Rule(rule_id) => {
                // Probe: Execute rule calculation
                if let Some(rule) = self.metadata.get_rule(rule_id) {
                    // Build SQL from rule
                    let sql = self.build_rule_sql(rule, date_constraint)?;
                    self.sql_engine.execute_probe(&sql, Some(100)).await
                } else {
                    Err(RcaError::Execution(format!("Rule not found: {}", rule_id)))
                }
            }
            NodeType::Metric { name, system } => {
                // Probe: Get metric value
                // Validate system exists
                if !self.metadata.tables.iter().any(|t| &t.system == system) {
                    return Err(RcaError::Execution(format!("Invalid system '{}' - not found in metadata. Available systems: {:?}", 
                        system, 
                        self.metadata.tables.iter().map(|t| t.system.clone()).collect::<std::collections::HashSet<_>>().into_iter().collect::<Vec<_>>())));
                }
                let rules = self.metadata.get_rules_for_system_metric(system, name);
                if let Some(rule) = rules.first() {
                    let sql = self.build_rule_sql(rule, date_constraint)?;
                    self.sql_engine.execute_probe(&sql, Some(100)).await
                } else {
                    Err(RcaError::Execution(format!("No rule found for metric {} in system {}. Available rules for this system: {:?}", 
                        name, 
                        system,
                        self.metadata.rules.iter().filter(|r| &r.system == system).map(|r| format!("{}:{}", r.id, r.metric)).collect::<Vec<_>>())))
                }
            }
        }
    }
    
    /// Find join keys between two tables
    fn find_join_keys(&self, from: &str, to: &str) -> Result<HashMap<String, String>> {
        // Look for lineage edge
        for edge in &self.metadata.lineage.edges {
            if edge.from == from && edge.to == to {
                return Ok(edge.keys.clone());
            }
        }
        
        // Fallback: try to infer from primary keys
        let from_table = self.metadata.tables.iter()
            .find(|t| t.name == from)
            .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", from)))?;
        
        let to_table = self.metadata.tables.iter()
            .find(|t| t.name == to)
            .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", to)))?;
        
        // Try to match primary keys
        let mut keys = HashMap::new();
        for pk in &from_table.primary_key {
            if to_table.primary_key.contains(pk) {
                keys.insert(pk.clone(), pk.clone());
            }
        }
        
        if keys.is_empty() {
            return Err(RcaError::Execution(format!("Could not find join keys between {} and {}", from, to)));
        }
        
        Ok(keys)
    }
    
    /// Build SQL from rule
    fn build_rule_sql(&self, rule: &crate::metadata::Rule, date_constraint: Option<&str>) -> Result<String> {
        // Simplified SQL builder - in production, use proper SQL AST
        let mut sql = "SELECT ".to_string();
        
        // Add group by columns
        if !rule.target_grain.is_empty() {
            sql.push_str(&rule.target_grain.join(", "));
            sql.push_str(", ");
        }
        
        // Add aggregation
        sql.push_str(&rule.computation.formula);
        sql.push_str(" FROM ");
        
        // Add base tables
        if let Some(entity) = rule.computation.source_entities.first() {
            let tables: Vec<_> = self.metadata.tables
                .iter()
                .filter(|t| t.entity == *entity && t.system == rule.system)
                .collect();
            
            if let Some(table) = tables.first() {
                sql.push_str(&table.name);
            }
        }
        
        // Add date constraint if provided
        if let Some(date) = date_constraint {
            if let Some(time_col) = self.find_time_column_for_rule(rule) {
                sql.push_str(&format!(" WHERE {} = '{}'", time_col, date));
            } else {
                sql.push_str(&format!(" WHERE paid_date = '{}'", date));
            }
        }
        
        sql.push_str(" LIMIT 100");
        
        Ok(sql)
    }
    
    fn find_time_column_for_rule(&self, rule: &crate::metadata::Rule) -> Option<String> {
        if let Some(ref source_table) = rule.computation.source_table {
            if let Some(table) = self.metadata.tables.iter().find(|t| t.name == *source_table) {
                if !table.time_column.is_empty() {
                    return Some(table.time_column.clone());
                }
            }
        }
        
        for entity in &rule.computation.source_entities {
            if let Some(table) = self.metadata.tables.iter()
                .find(|t| t.entity == *entity && t.system == rule.system) {
                if !table.time_column.is_empty() {
                    return Some(table.time_column.clone());
                }
            }
        }
        
        None
    }
    
    /// Observe probe result and extract insights
    async fn observe_probe_result(
        &self,
        result: &SqlProbeResult,
        node: &TraversalNode,
        state: &TraversalState,
    ) -> Result<Observations> {
        let mut observations = Observations {
            row_count: result.row_count,
            has_data: result.row_count > 0,
            has_nulls: false,
            value_mismatches: Vec::new(),
            join_failures: false,
            filter_issues: false,
            insights: Vec::new(),
            data_quality_issue: None,
            llm_suggested_nodes: Vec::new(),
            llm_root_cause_found: false,
            llm_hypothesis: None,
        };
        
        // Check for nulls
        if let Some(ref summary) = result.summary {
            observations.has_nulls = summary.null_counts.values().any(|&count| count > 0);
            let null_columns: Vec<String> = summary.null_counts.iter()
                .filter(|(_, count)| **count > 0)
                .map(|(col, _)| col.clone())
                .collect();
            if !null_columns.is_empty() {
                observations.data_quality_issue = Some(format!(
                    "Nulls detected in columns: {}",
                    null_columns.join(", ")
                ));
            }
        }
        
        // Detect issues based on node type
        match &node.node_type {
            NodeType::Join { .. } => {
                if result.row_count > 0 {
                    observations.join_failures = true;
                    observations.insights.push("Join probe returned unmatched rows - possible join failure".to_string());
                }
            }
            NodeType::Filter { .. } => {
                if result.row_count == 0 {
                    observations.filter_issues = true;
                    observations.insights.push("Filter returned no rows - all rows filtered out".to_string());
                }
            }
            _ => {}
        }
        
        // Use LLM for deeper interpretation if available
        if let Some(ref llm) = self.llm_client {
            if let Ok(llm_interpretation) = self.llm_interpret_result(llm, result, node, state).await {
                // Merge LLM insights
                observations.insights.extend(llm_interpretation.observation.lines().map(|s| s.to_string()));
                
                if let Some(ref finding) = llm_interpretation.finding {
                    // LLM found something - update observations
                    match finding.finding_type.as_str() {
                        "JoinFailure" => observations.join_failures = true,
                        "FilterIssue" => observations.filter_issues = true,
                        "MissingRows" => observations.has_data = false,
                        "ValueMismatch" => observations.value_mismatches.push(finding.description.clone()),
                        "DataQualityIssue" => observations.data_quality_issue = Some(finding.description.clone()),
                        _ => {}
                    }
                }
                
                observations.llm_root_cause_found = llm_interpretation.root_cause_found;
                observations.llm_hypothesis = Some(llm_interpretation.hypothesis);
                if let Some(nodes) = llm_interpretation.new_candidate_nodes {
                    observations.llm_suggested_nodes = nodes;
                }
            }
        }
        
        Ok(observations)
    }
    
    /// Use LLM to interpret probe result
    async fn llm_interpret_result(
        &self,
        llm: &LlmClient,
        result: &SqlProbeResult,
        node: &TraversalNode,
        state: &TraversalState,
    ) -> Result<ResultInterpretationResponse> {
        let prompt = build_result_interpretation_prompt(result, node, &state.findings);
        
        let response = llm.call_llm(&prompt).await?;
        let cleaned = self.extract_json(&response);
        let interpretation: ResultInterpretationResponse = serde_json::from_str(&cleaned)
            .map_err(|e| RcaError::Llm(format!("Failed to parse result interpretation: {}. Response: {}", e, &cleaned[..cleaned.len().min(500)])))?;
        
        info!("🤖 LLM interpretation: {}", interpretation.observation);
        if interpretation.root_cause_found {
            info!("   ✅ Root cause found: {}", interpretation.hypothesis);
        }
        
        Ok(interpretation)
    }
    
    /// Decide next step based on observations
    async fn decide_next_step(
        &mut self,
        observations: &Observations,
        node: &TraversalNode,
        state: &TraversalState,
        probe_result: &SqlProbeResult,
    ) -> Result<Decision> {
        let mut decision = Decision {
            finding: None,
            root_cause_found: false,
            hypothesis: None,
            new_candidate_nodes: None,
        };
        
        // Analyze observations to make decision
        match &node.node_type {
            NodeType::Join { from, to } if observations.join_failures => {
                // Join failed - this is a finding
                decision.finding = Some(Finding {
                    node_id: node.node_id.clone(),
                    finding_type: FindingType::JoinFailure,
                    description: format!("Join between {} and {} has unmatched rows (join failure)", from, to),
                    evidence: probe_result.clone(),
                    confidence: 0.9,
                });
                
                // Root cause might be found if this explains the discrepancy
                if state.findings.len() > 0 {
                    decision.root_cause_found = true;
                    decision.hypothesis = Some(format!("Root cause: Join failure between {} and {}", from, to));
                }
            }
            NodeType::Filter { table, condition } if observations.filter_issues => {
                decision.finding = Some(Finding {
                    node_id: node.node_id.clone(),
                    finding_type: FindingType::FilterIssue,
                    description: format!("Filter on {} with condition '{}' filtered out all rows", table, condition),
                    evidence: probe_result.clone(),
                    confidence: 0.8,
                });
            }
            NodeType::Table(_) if !observations.has_data => {
                decision.finding = Some(Finding {
                    node_id: node.node_id.clone(),
                    finding_type: FindingType::MissingRows,
                    description: format!("Table {} has no data", node.node_id),
                    evidence: probe_result.clone(),
                    confidence: 0.7,
                });
            }
            _ if observations.data_quality_issue.is_some() => {
                decision.finding = Some(Finding {
                    node_id: node.node_id.clone(),
                    finding_type: FindingType::DataQualityIssue,
                    description: observations.data_quality_issue.clone().unwrap_or_else(|| "Data quality issue detected".to_string()),
                    evidence: probe_result.clone(),
                    confidence: 0.6,
                });
            }
            _ => {
                // No immediate finding, but might need to explore related nodes
                // Add candidate nodes based on current node
                let mut candidates = self.generate_candidate_nodes(node, state);
                for node_id in &observations.llm_suggested_nodes {
                    if let Some(candidate) = self.build_node_from_id(node_id) {
                        candidates.push(candidate);
                    }
                }
                if !candidates.is_empty() {
                    decision.new_candidate_nodes = Some(candidates);
                }
            }
        }
        
        if observations.llm_root_cause_found {
            decision.root_cause_found = true;
            decision.hypothesis = observations.llm_hypothesis.clone();
        }
        
        Ok(decision)
    }
    
    /// Generate candidate nodes based on current node
    fn generate_candidate_nodes(
        &mut self,
        node: &TraversalNode,
        state: &TraversalState,
    ) -> Vec<TraversalNode> {
        let mut candidates = Vec::new();
        
        match &node.node_type {
            NodeType::Table(table_name) => {
                // Add join nodes for this table (both directions)
                for edge in &self.metadata.lineage.edges {
                    if edge.from == *table_name || edge.to == *table_name {
                        let node_id = format!("join:{}:{}", edge.from, edge.to);
                        if !state.nodes.contains_key(&node_id) {
                            let join_metadata = NodeMetadata {
                                table_info: None,
                                rule_info: None,
                                join_info: Some(JoinNodeMetadata {
                                    from_table: edge.from.clone(),
                                    to_table: edge.to.clone(),
                                    join_keys: edge.keys.clone(),
                                    join_type: "left".to_string(),
                                    description: None,
                                }),
                                filter_info: None,
                                metric_info: None,
                                hypergraph_stats: None,
                            };
                            candidates.push(TraversalNode {
                                node_id: node_id.clone(),
                                node_type: NodeType::Join {
                                    from: edge.from.clone(),
                                    to: edge.to.clone(),
                                },
                                visited: false,
                                visit_count: 0,
                                last_probe_result: None,
                                score: 0.5,
                                reasons: vec!["Connected table via lineage".to_string()],
                                metadata: Some(join_metadata),
                            });
                        }
                    }
                }
                
                // Add rule nodes that use this table's entity
                if let Some(table) = self.metadata.get_table(table_name) {
                    for rule in &self.metadata.rules {
                        if rule.computation.source_entities.contains(&table.entity) {
                            let node_id = format!("rule:{}", rule.id);
                            if !state.nodes.contains_key(&node_id) {
                                if let Ok(rule_metadata) = self.build_rule_metadata(rule) {
                                    candidates.push(TraversalNode {
                                        node_id,
                                        node_type: NodeType::Rule(rule.id.clone()),
                                        visited: false,
                                        visit_count: 0,
                                        last_probe_result: None,
                                        score: 0.55,
                                        reasons: vec!["Rule uses this table".to_string()],
                                        metadata: Some(rule_metadata),
                                    });
                                }
                            }
                        }
                    }
                }
            }
            NodeType::Join { from, to } => {
                // Add table nodes on both sides of join
                for table_name in [from, to] {
                    let node_id = format!("table:{}", table_name);
                    if !state.nodes.contains_key(&node_id) {
                        if let Some(table) = self.metadata.get_table(table_name).cloned() {
                            if let Ok(node) = self.make_table_node(&table, 0.5, "Join side table".to_string()) {
                                candidates.push(node);
                            }
                        }
                    }
                }
            }
            NodeType::Rule(rule_id) => {
                if let Some(rule) = self.metadata.get_rule(rule_id).cloned() {
                    // Add tables for source entities
                    let source_entities = rule.computation.source_entities.clone();
                    let rule_system = rule.system.clone();
                    for entity in source_entities {
                        let tables: Vec<_> = self.metadata.tables
                            .iter()
                            .filter(|t| t.entity == entity && t.system == rule_system)
                            .cloned()
                            .collect();
                        for table in tables {
                            let node_id = format!("table:{}", table.name);
                            if !state.nodes.contains_key(&node_id) {
                                if let Ok(node) = self.make_table_node(&table, 0.5, "Rule source table".to_string()) {
                                    candidates.push(node);
                                }
                            }
                        }
                    }
                }
            }
            NodeType::Metric { name, system } => {
                let rules = self.metadata.get_rules_for_system_metric(system, name);
                for rule in rules {
                    let node_id = format!("rule:{}", rule.id);
                    if !state.nodes.contains_key(&node_id) {
                        if let Ok(rule_metadata) = self.build_rule_metadata(&rule) {
                            candidates.push(TraversalNode {
                                node_id,
                                node_type: NodeType::Rule(rule.id.clone()),
                                visited: false,
                                visit_count: 0,
                                last_probe_result: None,
                                score: 0.6,
                                reasons: vec!["Metric rule".to_string()],
                                metadata: Some(rule_metadata),
                            });
                        }
                    }
                }
            }
            _ => {}
        }
        
        candidates
    }
    
    fn build_node_from_id(&mut self, node_id: &str) -> Option<TraversalNode> {
        if let Some(table_name) = node_id.strip_prefix("table:") {
            let table = self.metadata.get_table(table_name)?.clone();
            return self.make_table_node(&table, 0.55, "LLM suggested table".to_string()).ok();
        }
        if let Some(rule_id) = node_id.strip_prefix("rule:") {
            let rule = self.metadata.get_rule(rule_id)?;
            let rule_metadata = self.build_rule_metadata(rule).ok()?;
            return Some(TraversalNode {
                node_id: node_id.to_string(),
                node_type: NodeType::Rule(rule_id.to_string()),
                visited: false,
                visit_count: 0,
                last_probe_result: None,
                score: 0.6,
                reasons: vec!["LLM suggested rule".to_string()],
                metadata: Some(rule_metadata),
            });
        }
        if let Some(metric_id) = node_id.strip_prefix("metric:") {
            let parts: Vec<&str> = metric_id.splitn(2, ':').collect();
            if parts.len() == 2 {
                let system = parts[0];
                let metric = parts[1];
                let metric_metadata = self.build_metric_metadata(metric, system).ok()?;
                return Some(TraversalNode {
                    node_id: node_id.to_string(),
                    node_type: NodeType::Metric { name: metric.to_string(), system: system.to_string() },
                    visited: false,
                    visit_count: 0,
                    last_probe_result: None,
                    score: 0.7,
                    reasons: vec!["LLM suggested metric".to_string()],
                    metadata: Some(metric_metadata),
                });
            }
        }
        if let Some(join_spec) = node_id.strip_prefix("join:") {
            let parts: Vec<&str> = join_spec.splitn(2, ':').collect();
            if parts.len() == 2 {
                let from = parts[0];
                let to = parts[1];
                let join_keys = self.find_join_keys(from, to).ok()?;
                let join_metadata = self.build_join_metadata(from, to, &join_keys).ok()?;
                return Some(TraversalNode {
                    node_id: node_id.to_string(),
                    node_type: NodeType::Join { from: from.to_string(), to: to.to_string() },
                    visited: false,
                    visit_count: 0,
                    last_probe_result: None,
                    score: 0.55,
                    reasons: vec!["LLM suggested join".to_string()],
                    metadata: Some(join_metadata),
                });
            }
        }
        if let Some(filter_spec) = node_id.strip_prefix("filter:") {
            let parts: Vec<&str> = filter_spec.splitn(2, ':').collect();
            if parts.len() == 2 {
                let table = parts[0];
                let condition = parts[1];
                let filter_metadata = NodeMetadata {
                    table_info: None,
                    rule_info: None,
                    join_info: None,
                    filter_info: Some(FilterNodeMetadata {
                        table: table.to_string(),
                        condition: condition.to_string(),
                        description: Some("LLM suggested filter".to_string()),
                    }),
                    metric_info: None,
                    hypergraph_stats: None,
                };
                return Some(TraversalNode {
                    node_id: node_id.to_string(),
                    node_type: NodeType::Filter { table: table.to_string(), condition: condition.to_string() },
                    visited: false,
                    visit_count: 0,
                    last_probe_result: None,
                    score: 0.55,
                    reasons: vec!["LLM suggested filter".to_string()],
                    metadata: Some(filter_metadata),
                });
            }
        }
        None
    }
    
    /// Build metadata for a table node
    fn build_table_metadata(&mut self, table: &crate::metadata::Table) -> Result<NodeMetadata> {
        let entity = self.metadata.entities_by_id.get(&table.entity);
        let grain = entity.map(|e| e.grain.clone()).unwrap_or_default();
        let attributes = entity.map(|e| e.attributes.clone()).unwrap_or_default();
        
        let columns: Vec<ColumnInfo> = table.columns.as_ref()
            .map(|cols| cols.iter().map(|c| {
                ColumnInfo {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    description: c.description.clone(),
                    distinct_values_count: c.distinct_values.as_ref().map(|v| v.len()),
                }
            }).collect())
            .unwrap_or_default();
        
        // Try to get hypergraph stats if available
        let hypergraph_stats = self.hypergraph_stats_for_table(&table.name);
        
        Ok(NodeMetadata {
            table_info: Some(TableNodeMetadata {
                name: table.name.clone(),
                system: table.system.clone(),
                entity: table.entity.clone(),
                primary_key: table.primary_key.clone(),
                time_column: if table.time_column.is_empty() { None } else { Some(table.time_column.clone()) },
                columns,
                labels: table.labels.as_ref().cloned().unwrap_or_default(),
                grain,
                attributes,
                description: None,
            }),
            rule_info: None,
            join_info: None,
            filter_info: None,
            metric_info: None,
            hypergraph_stats,
        })
    }
    
    /// Build metadata for a rule node
    fn build_rule_metadata(&self, rule: &crate::metadata::Rule) -> Result<NodeMetadata> {
        Ok(NodeMetadata {
            table_info: None,
            rule_info: Some(RuleNodeMetadata {
                id: rule.id.clone(),
                system: rule.system.clone(),
                metric: rule.metric.clone(),
                description: rule.computation.description.clone(),
                formula: rule.computation.formula.clone(),
                source_entities: rule.computation.source_entities.clone(),
                target_entity: rule.target_entity.clone(),
                target_grain: rule.target_grain.clone(),
                filter_conditions: rule.computation.filter_conditions.clone(),
                labels: rule.labels.as_ref().cloned().unwrap_or_default(),
            }),
            join_info: None,
            filter_info: None,
            metric_info: None,
            hypergraph_stats: None,
        })
    }
    
    /// Build metadata for a metric node
    fn build_metric_metadata(&self, metric_name: &str, system: &str) -> Result<NodeMetadata> {
        // Find metric in metadata
        let metric = self.metadata.metrics.iter()
            .find(|m| m.name == metric_name);
        
        Ok(NodeMetadata {
            table_info: None,
            rule_info: None,
            join_info: None,
            filter_info: None,
            metric_info: metric.map(|m| MetricNodeMetadata {
                name: m.name.clone(),
                system: system.to_string(),
                description: m.description.clone(),
                grain: m.grain.clone(),
                precision: m.precision,
                unit: m.unit.clone(),
            }),
            hypergraph_stats: None,
        })
    }
    
    fn build_join_metadata(
        &mut self,
        from: &str,
        to: &str,
        join_keys: &HashMap<String, String>,
    ) -> Result<NodeMetadata> {
        let hypergraph_stats = self.hypergraph_stats_for_join(from, to);
        Ok(NodeMetadata {
            table_info: None,
            rule_info: None,
            join_info: Some(JoinNodeMetadata {
                from_table: from.to_string(),
                to_table: to.to_string(),
                join_keys: join_keys.clone(),
                join_type: "left".to_string(),
                description: None,
            }),
            filter_info: None,
            metric_info: None,
            hypergraph_stats,
        })
    }
    
    fn make_table_node(
        &mut self,
        table: &crate::metadata::Table,
        score: f64,
        reason: String,
    ) -> Result<TraversalNode> {
        let table_metadata = self.build_table_metadata(table)?;
        Ok(TraversalNode {
            node_id: format!("table:{}", table.name),
            node_type: NodeType::Table(table.name.clone()),
            visited: false,
            visit_count: 0,
            last_probe_result: None,
            score,
            reasons: vec![reason],
            metadata: Some(table_metadata),
        })
    }
    
    fn collect_term_filters(&self, hit: &TermHit) -> Vec<String> {
        let mut filters = Vec::new();
        if let Some(filter) = &hit.hint.filter {
            filters.push(filter.clone());
        }
        filters.extend(hit.hint.filters.iter().cloned());
        filters
    }
    
    fn parse_relationship_key(&self, key: &str) -> Option<(String, String)> {
        if let Some((left, right)) = key.split_once("_to_") {
            return Some((left.to_string(), right.to_string()));
        }
        if let Some((left, right)) = key.split_once("->") {
            return Some((left.trim().to_string(), right.trim().to_string()));
        }
        None
    }
    
    fn format_filter_condition(&self, column: &str, value: &str) -> String {
        let trimmed = value.trim();
        let has_operator = trimmed.contains('=') || trimmed.contains('>') || trimmed.contains('<');
        if trimmed.to_lowercase().contains(&column.to_lowercase()) {
            return trimmed.to_string();
        }
        if has_operator {
            if trimmed.starts_with('=') || trimmed.starts_with('>') || trimmed.starts_with('<') || trimmed.starts_with("!=") {
                return format!("{} {}", column, trimmed);
            }
        }
        if trimmed.starts_with('\'') || trimmed.starts_with('"') {
            format!("{} = {}", column, trimmed)
        } else {
            format!("{} = '{}'", column, trimmed)
        }
    }
    
    fn hypergraph_stats_for_table(&mut self, table_name: &str) -> Option<HypergraphStats> {
        let adapter = self.graph.adapter().ok()?;
        let stats = adapter.get_table_stats(table_name)?;
        let top_n_values = stats.top_n_values.iter()
            .map(|v| v.value.to_string())
            .collect::<Vec<_>>();
        Some(HypergraphStats {
            row_count: Some(stats.row_count as u64),
            distinct_count: Some(stats.distinct_count),
            null_percentage: Some(stats.null_percentage),
            data_quality_score: Some(stats.data_quality_score),
            top_n_values,
            join_selectivity: stats.join_selectivity,
            filter_selectivity: stats.filter_selectivity,
        })
    }
    
    fn hypergraph_stats_for_join(&mut self, from: &str, to: &str) -> Option<HypergraphStats> {
        let adapter = self.graph.adapter().ok()?;
        let stats = adapter.get_join_stats(from, to)?;
        Some(HypergraphStats {
            row_count: None,
            distinct_count: None,
            null_percentage: None,
            data_quality_score: None,
            top_n_values: Vec::new(),
            join_selectivity: Some(stats.selectivity),
            filter_selectivity: None,
        })
    }
    
    /// Extract JSON from LLM response
    fn extract_json(&self, response: &str) -> String {
        // Try to find JSON object/array
        let json_start = response.find('{').or_else(|| response.find('['));
        let json_end = response.rfind('}').or_else(|| response.rfind(']'));
        
        if let (Some(start), Some(end)) = (json_start, json_end) {
            response[start..=end].to_string()
        } else {
            // Try markdown code blocks
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

#[derive(Debug, Clone)]
struct Observations {
    row_count: usize,
    has_data: bool,
    has_nulls: bool,
    value_mismatches: Vec<String>,
    join_failures: bool,
    filter_issues: bool,
    insights: Vec<String>,
    data_quality_issue: Option<String>,
    llm_suggested_nodes: Vec<String>,
    llm_root_cause_found: bool,
    llm_hypothesis: Option<String>,
}

#[derive(Debug, Clone)]
struct Decision {
    finding: Option<Finding>,
    root_cause_found: bool,
    hypothesis: Option<String>,
    new_candidate_nodes: Option<Vec<TraversalNode>>,
}

