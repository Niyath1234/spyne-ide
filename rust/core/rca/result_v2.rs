//! RCAResult v2 - Standardized Result Model
//! 
//! New standardized result structure for grain-driven RCA results.
//! Replaces the old RcaCursorResult with a grain-focused design.

use crate::core::rca::result_formatter::FormattedDisplayResult;
use crate::core::observability::execution_trace::ExecutionTrace;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Grain-level difference between two systems
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrainDifference {
    /// Grain value (composite key values)
    pub grain_value: Vec<String>,
    /// Metric value from system A
    pub value_a: f64,
    /// Metric value from system B
    pub value_b: f64,
    /// Delta (value_b - value_a)
    pub delta: f64,
    /// Impact (absolute value of delta)
    pub impact: f64,
}

/// Attribution for a grain unit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribution {
    /// Grain value
    pub grain_value: Vec<String>,
    /// Impact score
    pub impact: f64,
    /// Contribution percentage
    pub contribution_percentage: f64,
    /// Contributing rows
    pub contributors: Vec<RowRef>,
    /// Explanation graph (simplified representation)
    pub explanation_graph: HashMap<String, serde_json::Value>,
}

/// Reference to a contributing row
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowRef {
    /// Table name
    pub table: String,
    /// Row identifier (composite key)
    pub row_id: Vec<String>,
    /// Contribution amount
    pub contribution: f64,
}

/// Lineage graph tracking data flow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageGraph {
    /// Nodes in the graph (tables, joins, filters)
    pub nodes: Vec<LineageNode>,
    /// Edges between nodes
    pub edges: Vec<LineageEdge>,
}

/// Node in the lineage graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    /// Node ID
    pub id: String,
    /// Node type (table, join, filter, aggregate)
    pub node_type: String,
    /// Node metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Edge in the lineage graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Source node ID
    pub from: String,
    /// Target node ID
    pub to: String,
    /// Edge metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Summary of RCA results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RCASummary {
    /// Total grain units analyzed
    pub total_grain_units: usize,
    /// Grain units only in system A
    pub missing_left_count: usize,
    /// Grain units only in system B
    pub missing_right_count: usize,
    /// Value mismatches
    pub mismatch_count: usize,
    /// Total aggregate difference
    pub aggregate_difference: f64,
    /// Top K differences found
    pub top_k: usize,
}

/// RCAResult v2 - Standardized result structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RCAResult {
    /// Target grain entity name
    pub grain: String,
    /// Grain key column name
    pub grain_key: String,
    /// Summary statistics
    pub summary: RCASummary,
    /// Top differences at grain level
    pub top_differences: Vec<GrainDifference>,
    /// Attributions for top differences
    pub attributions: Vec<Attribution>,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    /// Lineage graph showing data flow
    pub lineage_graph: LineageGraph,
    /// LLM-formatted display result (optional)
    pub formatted_display: Option<FormattedDisplayResult>,
    /// Execution trace for debugging
    pub execution_trace: Option<ExecutionTrace>,
}

impl RCAResult {
    /// Create a new RCAResult
    pub fn new(
        grain: String,
        grain_key: String,
        summary: RCASummary,
    ) -> Self {
        Self {
            grain,
            grain_key,
            summary,
            top_differences: Vec::new(),
            attributions: Vec::new(),
            confidence: 0.0,
            lineage_graph: LineageGraph {
                nodes: Vec::new(),
                edges: Vec::new(),
            },
            formatted_display: None,
            execution_trace: None,
        }
    }

    /// Add grain differences
    pub fn with_differences(mut self, differences: Vec<GrainDifference>) -> Self {
        self.top_differences = differences;
        self
    }

    /// Add attributions
    pub fn with_attributions(mut self, attributions: Vec<Attribution>) -> Self {
        self.attributions = attributions;
        self
    }

    /// Set confidence score
    pub fn with_confidence(mut self, confidence: f64) -> Self {
        self.confidence = confidence.max(0.0).min(1.0);
        self
    }

    /// Set lineage graph
    pub fn with_lineage_graph(mut self, lineage_graph: LineageGraph) -> Self {
        self.lineage_graph = lineage_graph;
        self
    }

    /// Set formatted display
    pub fn with_formatted_display(mut self, formatted_display: FormattedDisplayResult) -> Self {
        self.formatted_display = Some(formatted_display);
        self
    }

    /// Set execution trace
    pub fn with_execution_trace(mut self, execution_trace: ExecutionTrace) -> Self {
        self.execution_trace = Some(execution_trace);
        self
    }
}

/// Migration helper to convert from old RcaCursorResult to new RCAResult
/// 
/// This is a temporary bridge during migration. The conversion may lose some
/// information as the old structure is UUID-focused while the new is grain-focused.
#[cfg(feature = "migration")]
impl From<crate::core::agent::RcaCursorResult> for RCAResult {
    fn from(old: crate::core::agent::RcaCursorResult) -> Self {
        // Extract grain information from old result if available
        // For now, use defaults - proper migration requires context
        let grain = "unknown".to_string();
        let grain_key = "uuid".to_string(); // Old system used UUIDs
        
        let summary = RCASummary {
            total_grain_units: old.summary.total_rows,
            missing_left_count: old.summary.missing_left_count,
            missing_right_count: old.summary.missing_right_count,
            mismatch_count: old.summary.mismatch_count,
            aggregate_difference: old.summary.aggregate_mismatch,
            top_k: old.explanations.len().min(100),
        };

        RCAResult::new(grain, grain_key, summary)
            .with_formatted_display(old.formatted_display.unwrap_or_else(|| {
                FormattedDisplayResult {
                    display_content: String::new(),
                    key_identifiers: Vec::new(),
                    display_format: crate::core::rca::result_formatter::DisplayFormat::Summary,
                    summary_stats: None,
                    metadata: None,
                }
            }))
    }
}
