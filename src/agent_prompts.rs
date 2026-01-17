//! Agent Prompts - System prompts for LLM-guided graph traversal
//! 
//! These prompts help the LLM understand how to navigate the graph,
//! choose nodes, interpret probe results, and make decisions.

use crate::graph_traversal::{TraversalNode, Finding, FindingType, TraversalHint};
use serde::{Deserialize, Serialize};

/// System prompt for graph traversal agent
pub const GRAPH_TRAVERSAL_SYSTEM_PROMPT: &str = r#"You are an intelligent RCA (Root Cause Analysis) agent operating within a dynamic graph traversal system.

## Your Role

Navigate a knowledge graph of data transformations, using small SQL probes to eliminate possibilities, until only the true root cause remains.

## Core Philosophy: Traverse → Test → Observe → Decide → Repeat

You do NOT follow a fixed pipeline. Instead, you:

1. **Traverse**: Choose the next best node to visit
2. **Test**: Run a small SQL probe at that node
3. **Observe**: Analyze the probe result
4. **Decide**: Determine the next step based on what you learned
5. **Repeat**: Continue until root cause is found

## Node Types

1. **Table Nodes**: Base data tables - probe to check if data exists
2. **Rule Nodes**: Business rule calculations - probe to see how metrics are calculated
3. **Join Nodes**: Join relationships - probe to test if joins succeed
4. **Filter Nodes**: Filter conditions - probe to check if filters work correctly
5. **Metric Nodes**: Final metric calculations - probe to get metric values

## Decision-Making Framework

When choosing next node, consider:

1. **Relevance to Current Findings**
   - Missing rows → probe joins
   - Join failures → probe related tables
   - Filter issues → probe filter conditions
   - Value mismatches → probe rules

2. **Information Gain**
   - Which node eliminates the most possibilities?
   - Which node provides the most diagnostic value?

3. **Proximity to Visited Nodes**
   - Nodes connected to recently visited nodes are more relevant
   - Follow the data flow: upstream → downstream

## Interpreting Probe Results

- **Row count == 0**: No data → likely missing rows issue
- **Join failures**: LEFT JOIN ... WHERE right.key IS NULL returns rows → join failures found
- **Filter returns 0 rows**: All rows filtered out
- **Value mismatches**: Same rows exist but values differ → probe rules

## Decision Rules

- **Join failures** → Root cause likely join issue → Record finding → Stop or probe related joins
- **Filter issues** → Root cause likely filter → Record finding → Probe filter conditions
- **Missing data** → Root cause likely missing rows → Probe joins/filters
- **Value mismatches** → Root cause likely rule calculation → Probe rules/formulas
- **No issues found** → Continue exploring → Probe next most informative node

## Best Practices

1. Start broad, then narrow - start with high-level probes, narrow down based on findings
2. Follow the data flow - probe upstream first, then downstream
3. Eliminate possibilities - each probe should eliminate some possibilities
4. Use small probes - keep probes small (LIMIT 100) for fast iteration
5. Record findings early - don't wait for complete picture
6. Stop when root cause found - don't continue exploring after root cause is clear

## Common Patterns

**Missing Rows Investigation:**
Finding: System A has 1000 rows, System B has 950 rows
→ Next: Probe joins (most likely cause)
→ Then: Probe filters (if joins pass)

**Value Mismatch Investigation:**
Finding: Same rows exist but values differ
→ Next: Probe rules (calculation differences)
→ Then: Probe formulas/transformations

**Join Failure Investigation:**
Finding: Join returns 0 rows or many NULLs
→ Next: Probe left table (check if source data exists)
→ Then: Probe right table (check if target data exists)

Remember: You are a **dynamic, adaptive agent** that chooses next step based on observations, not a fixed pipeline."#;

/// Prompt for choosing next node
pub fn build_node_selection_prompt(
    candidates: &[TraversalNode],
    findings: &[Finding],
    visited_path: &[String],
    problem: &str,
    hints: &[TraversalHint],
    current_hypothesis: Option<&str>,
) -> String {
    let candidates_json: Vec<_> = candidates.iter().map(|n| {
        let mut node_json = serde_json::json!({
            "node_id": n.node_id,
            "node_type": format!("{:?}", n.node_type),
            "score": n.score,
            "reasons": n.reasons,
            "visited": n.visited,
        });
        
        // Add rich metadata if available
        if let Some(ref metadata) = n.metadata {
            if let Some(ref table_info) = metadata.table_info {
                node_json["table_metadata"] = serde_json::json!({
                    "name": table_info.name,
                    "system": table_info.system,
                    "entity": table_info.entity,
                    "primary_key": table_info.primary_key,
                    "time_column": table_info.time_column,
                    "columns": table_info.columns.iter().map(|c| serde_json::json!({
                        "name": c.name,
                        "data_type": c.data_type,
                        "description": c.description,
                        "distinct_values_count": c.distinct_values_count,
                    })).collect::<Vec<_>>(),
                    "labels": table_info.labels,
                    "grain": table_info.grain,
                    "attributes": table_info.attributes,
                });
            }
            
            if let Some(ref rule_info) = metadata.rule_info {
                node_json["rule_metadata"] = serde_json::json!({
                    "id": rule_info.id,
                    "system": rule_info.system,
                    "metric": rule_info.metric,
                    "description": rule_info.description,
                    "formula": rule_info.formula,
                    "source_entities": rule_info.source_entities,
                    "target_entity": rule_info.target_entity,
                    "target_grain": rule_info.target_grain,
                    "filter_conditions": rule_info.filter_conditions,
                    "labels": rule_info.labels,
                });
            }
            
            if let Some(ref join_info) = metadata.join_info {
                node_json["join_metadata"] = serde_json::json!({
                    "from_table": join_info.from_table,
                    "to_table": join_info.to_table,
                    "join_keys": join_info.join_keys,
                    "join_type": join_info.join_type,
                    "description": join_info.description,
                });
            }
            
            if let Some(ref metric_info) = metadata.metric_info {
                node_json["metric_metadata"] = serde_json::json!({
                    "name": metric_info.name,
                    "system": metric_info.system,
                    "description": metric_info.description,
                    "grain": metric_info.grain,
                    "precision": metric_info.precision,
                    "unit": metric_info.unit,
                });
            }
            
            if let Some(ref stats) = metadata.hypergraph_stats {
                node_json["hypergraph_stats"] = serde_json::json!({
                    "row_count": stats.row_count,
                    "distinct_count": stats.distinct_count,
                    "null_percentage": stats.null_percentage,
                    "data_quality_score": stats.data_quality_score,
                    "top_n_values": stats.top_n_values,
                    "join_selectivity": stats.join_selectivity,
                    "filter_selectivity": stats.filter_selectivity,
                });
            }
        }
        
        node_json
    }).collect();
    
    let findings_json: Vec<_> = findings.iter().map(|f| {
        serde_json::json!({
            "finding_type": format!("{:?}", f.finding_type),
            "description": f.description,
            "confidence": f.confidence,
        })
    }).collect();
    
    let hints_json: Vec<_> = hints.iter().map(|h| {
        serde_json::json!({
            "hint_type": format!("{:?}", h.hint_type),
            "description": h.description,
            "confidence": h.confidence,
            "related_nodes": h.related_nodes,
            "source": h.source,
        })
    }).collect();
    
    format!(
        r#"You are choosing the next node to explore in an RCA investigation.

Problem: {}
Visited Path: {:?}
Current Hypothesis: {}

Current Findings:
{}

Traversal Hints:
{}

Candidate Nodes:
{}

Choose the most informative node to explore next. Consider:
1. Which node is most likely to reveal the root cause?
2. Which node builds on current findings?
3. Which node eliminates the most possibilities?
4. Which node follows the data flow?

Return JSON:
{{
  "node_id": "node_id",
  "reasoning": "why this node was chosen",
  "expected_insight": "what we expect to learn",
  "confidence": 0.0-1.0
}}

Guidelines:
- If findings show missing rows → choose join nodes
- If findings show join failures → choose related table nodes
- If findings show filter issues → choose filter nodes
- If findings show value mismatches → choose rule nodes
- If no findings yet → choose metric or base table nodes"#,
        problem,
        visited_path,
        current_hypothesis.unwrap_or("none"),
        serde_json::to_string_pretty(&findings_json).unwrap_or_default(),
        serde_json::to_string_pretty(&hints_json).unwrap_or_default(),
        serde_json::to_string_pretty(&candidates_json).unwrap_or_default(),
    )
}

/// Prompt for interpreting probe results
pub fn build_result_interpretation_prompt(
    probe_result: &crate::sql_engine::SqlProbeResult,
    node: &TraversalNode,
    findings: &[Finding],
) -> String {
    let summary_json = probe_result.summary.as_ref().map(|s| {
        serde_json::json!({
            "null_counts": s.null_counts,
            "value_ranges": s.value_ranges,
        })
    });
    
    format!(
        r#"Interpret this probe result and decide next steps.

Node: {} ({:?})
Probe Result:
- Row count: {}
- Columns: {:?}
- Sample rows: {} (showing first 3)
- Summary: {}

Current Findings:
{}

Analyze:
1. What does this probe result tell us?
2. Does it reveal a root cause?
3. What should we probe next?

Return JSON:
{{
  "observation": "what we learned from this probe",
  "finding": {{
    "type": "MissingRows|JoinFailure|FilterIssue|ValueMismatch|RuleDiscrepancy|DataQualityIssue|null",
    "description": "description if finding",
    "confidence": 0.0-1.0
  }},
  "root_cause_found": true/false,
  "hypothesis": "current hypothesis about root cause",
  "next_action": "what to do next",
  "new_candidate_nodes": ["node_id1", "node_id2"] or null
}}

Decision Rules:
- If row_count == 0 and node is Table → Finding: MissingRows
- If join probe shows NULLs → Finding: JoinFailure
- If filter probe returns 0 rows → Finding: FilterIssue
- If values differ between systems → Finding: ValueMismatch
- If no issues found → Continue exploring"#,
        node.node_id,
        node.node_type,
        probe_result.row_count,
        probe_result.columns,
        probe_result.sample_rows.len().min(3),
        serde_json::to_string_pretty(&summary_json).unwrap_or_default(),
        serde_json::to_string_pretty(&findings).unwrap_or_default(),
    )
}

/// Prompt for generating probe SQL
pub fn build_sql_generation_prompt(
    node: &TraversalNode,
    date_constraint: Option<&str>,
    context: &str,
) -> String {
    let date_clause = date_constraint
        .map(|d| format!("WHERE paid_date = '{}'", d))
        .unwrap_or_default();
    
    format!(
        r#"Generate a SQL probe query for this node.

Node: {} ({:?})
Context: {}
Date Constraint: {}

Generate a small, focused SQL query (LIMIT 100) that will help diagnose the root cause.

Node Type Guidelines:
- Table: SELECT * FROM table_name {} LIMIT 100
- Join: SELECT a.* FROM left_table a LEFT JOIN right_table b ON keys {} AND b.key IS NULL LIMIT 100
- Filter: SELECT * FROM table WHERE condition {} LIMIT 100
- Rule: Execute rule's SQL/formula {} LIMIT 100
- Metric: SELECT metric_calculation FROM tables {} LIMIT 100

Return JSON:
{{
  "sql": "SELECT ...",
  "reasoning": "why this SQL will help",
  "expected_result": "what we expect to see"
}}"#,
        node.node_id,
        node.node_type,
        context,
        date_constraint.unwrap_or("none"),
        date_clause,
        date_clause,
        date_clause,
        date_clause,
        date_clause,
    )
}

/// Prompt for forming hypothesis
pub fn build_hypothesis_prompt(
    findings: &[Finding],
    visited_path: &[String],
    problem: &str,
) -> String {
    let findings_json: Vec<_> = findings.iter().map(|f| {
        serde_json::json!({
            "type": format!("{:?}", f.finding_type),
            "description": f.description,
            "confidence": f.confidence,
        })
    }).collect();
    
    format!(
        r#"Form a hypothesis about the root cause based on current findings.

Problem: {}
Visited Nodes: {:?}

Findings:
{}

Based on these findings, form a hypothesis about what the root cause might be.

Return JSON:
{{
  "hypothesis": "hypothesis about root cause",
  "confidence": 0.0-1.0,
  "supporting_evidence": ["finding1", "finding2"],
  "next_nodes_to_probe": ["node_id1", "node_id2"],
  "reasoning": "why this hypothesis"
}}

Common Patterns:
- Missing rows + Join failures → Hypothesis: "Root cause is join failure between X and Y"
- Value mismatch + Rule differences → Hypothesis: "Root cause is different rule logic"
- Missing rows + No join failures → Hypothesis: "Root cause is missing source data"
- Filter issues → Hypothesis: "Root cause is filter condition excluding rows""#,
        problem,
        visited_path,
        serde_json::to_string_pretty(&findings_json).unwrap_or_default(),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSelectionResponse {
    pub node_id: String,
    pub reasoning: String,
    pub expected_insight: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultInterpretationResponse {
    pub observation: String,
    pub finding: Option<FindingResponse>,
    pub root_cause_found: bool,
    pub hypothesis: String,
    pub next_action: String,
    pub new_candidate_nodes: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindingResponse {
    #[serde(rename = "type")]
    pub finding_type: String,
    pub description: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlGenerationResponse {
    pub sql: String,
    pub reasoning: String,
    pub expected_result: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HypothesisResponse {
    pub hypothesis: String,
    pub confidence: f64,
    pub supporting_evidence: Vec<String>,
    pub next_nodes_to_probe: Vec<String>,
    pub reasoning: String,
}

