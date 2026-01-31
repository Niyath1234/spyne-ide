use super::node::NodeId;
use super::types::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// An edge in the hypergraph represents a join relationship
/// Edges connect nodes and store join predicates
/// Note: Cannot fully serialize due to MaterializedJoin containing non-serializable types
#[derive(Clone, Debug)]
pub struct HyperEdge {
    /// Unique edge ID
    pub id: EdgeId,
    
    /// Source node ID
    pub source: NodeId,
    
    /// Target node ID
    pub target: NodeId,
    
    /// Join type (for query planning hints only - relationships in hypergraph are bidirectional)
    /// The actual join type (INNER/LEFT/RIGHT) is determined at query time based on query requirements
    pub join_type: JoinType,
    
    /// Join predicate (e.g., "left.region_id = right.id")
    pub predicate: JoinPredicate,
    
    /// Statistics about this edge
    pub stats: EdgeStatistics,
    
    /// Whether this edge is materialized (precomputed)
    pub is_materialized: bool,
    
    /// Cached join result (if materialized)
    pub materialized_result: Option<MaterializedJoin>,
    
    /// Metadata (for pattern insights and other metadata)
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EdgeId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,  // CROSS JOIN (Cartesian product)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinPredicate {
    /// Left side: (table, column)
    pub left: (String, String),
    
    /// Right side: (table, column)
    pub right: (String, String),
    
    /// Operator (usually Equals)
    pub operator: PredicateOperator,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PredicateOperator {
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeStatistics {
    /// Estimated cardinality of join result
    pub cardinality: usize,
    
    /// Selectivity estimate (0.0 to 1.0)
    pub selectivity: f64,
    
    /// Average fan-out (rows in target per row in source)
    pub avg_fanout: f64,
    
    /// Last update timestamp
    pub last_updated: u64,
    
    // ========== Join Performance Metrics ==========
    /// Actual measured join cost in milliseconds
    pub actual_join_cost_ms: Option<f64>,
    
    /// Cardinality estimation error percentage
    pub cardinality_error_percent: Option<f64>,
    
    /// Preferred join order (true = probe left side first, false = probe right side first)
    pub preferred_probe_left: bool,
    
    /// Recommended join algorithm hint
    pub join_algorithm_hint: JoinAlgorithmHint,
    
    /// Optimal bloom filter size in bytes (if applicable)
    pub bloom_filter_size_bytes: Option<usize>,
    
    /// Cost to materialize vs compute (materialize_cost / compute_cost ratio)
    pub materialization_cost_ratio: Option<f64>,
    
    // ========== Join Pattern Metadata ==========
    /// How often this join is executed
    pub join_frequency: u64,
    
    /// Last join execution timestamp
    pub last_join_execution: u64,
    
    /// Historical join result sizes (for trend analysis)
    pub join_result_size_history: Vec<usize>,
}

/// Join algorithm recommendation
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum JoinAlgorithmHint {
    HashJoin,
    NestedLoop,
    SortMerge,
    BroadcastHash,
    Auto, // Let optimizer decide
}

/// Materialized join result (for precomputed joins)
#[derive(Clone, Debug)]
pub struct MaterializedJoin {
    /// Map from left key to list of right row indices
    pub join_map: HashMap<Value, Vec<usize>>,
    
    /// Total number of result rows
    pub result_count: usize,
}

impl HyperEdge {
    pub fn new(
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        join_type: JoinType,
        predicate: JoinPredicate,
    ) -> Self {
        Self {
            id,
            source,
            target,
            join_type,
            predicate,
            stats: EdgeStatistics::default(),
            is_materialized: false,
            materialized_result: None,
            metadata: HashMap::new(),
        }
    }
    
    /// Materialize this edge (precompute join result)
    pub fn materialize(&mut self) {
        // TODO: Precompute join and store in materialized_result
        self.is_materialized = true;
    }
    
    /// Check if this edge matches a given predicate
    pub fn matches_predicate(&self, left_table: &str, left_col: &str, right_table: &str, right_col: &str) -> bool {
        self.predicate.left.0 == left_table
            && self.predicate.left.1 == left_col
            && self.predicate.right.0 == right_table
            && self.predicate.right.1 == right_col
    }
    
    /// Record a join execution with actual metrics
    pub fn record_join_execution(&mut self, actual_cost_ms: f64, actual_cardinality: usize, result_size: usize) {
        self.stats.join_frequency += 1;
        self.stats.last_join_execution = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // Update actual join cost (exponential moving average)
        let alpha = 0.1;
        self.stats.actual_join_cost_ms = Some(
            self.stats.actual_join_cost_ms
                .map(|c| alpha * actual_cost_ms + (1.0 - alpha) * c)
                .unwrap_or(actual_cost_ms)
        );
        
        // Calculate cardinality estimation error
        if self.stats.cardinality > 0 {
            let error = ((actual_cardinality as f64 - self.stats.cardinality as f64).abs() / self.stats.cardinality as f64) * 100.0;
            self.stats.cardinality_error_percent = Some(
                self.stats.cardinality_error_percent
                    .map(|e| alpha * error + (1.0 - alpha) * e)
                    .unwrap_or(error)
            );
        }
        
        // Update join result size history
        const MAX_HISTORY_SIZE: usize = 100;
        self.stats.join_result_size_history.push(result_size);
        if self.stats.join_result_size_history.len() > MAX_HISTORY_SIZE {
            self.stats.join_result_size_history.remove(0);
        }
        
        // Update selectivity based on actual results
        // This would need source table size, but we can approximate
        self.stats.last_updated = self.stats.last_join_execution;
    }
}

impl Default for EdgeStatistics {
    fn default() -> Self {
        Self {
            cardinality: 0,
            selectivity: 1.0,
            avg_fanout: 1.0,
            last_updated: 0,
            actual_join_cost_ms: None,
            cardinality_error_percent: None,
            preferred_probe_left: true, // Default to probing left side first
            join_algorithm_hint: JoinAlgorithmHint::Auto,
            bloom_filter_size_bytes: None,
            materialization_cost_ratio: None,
            join_frequency: 0,
            last_join_execution: 0,
            join_result_size_history: vec![],
        }
    }
}

