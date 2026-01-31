use super::types::{ColumnFragment, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::collections::HashMap;

/// A node in the hypergraph represents a table or column
/// Nodes store references to column fragments
/// Note: Cannot derive Serialize/Deserialize due to ColumnFragment containing Arc<dyn Array>
#[derive(Clone, Debug)]
pub struct HyperNode {
    /// Unique node ID
    pub id: NodeId,
    
    /// Node type (Table or Column)
    pub node_type: NodeType,
    
    /// Schema name (required - must always be provided)
    pub schema_name: String,
    
    /// Table name (if this is a table node)
    pub table_name: Option<String>,
    
    /// Column name (if this is a column node)
    pub column_name: Option<String>,
    
    /// Column fragments stored in this node
    pub fragments: Vec<ColumnFragment>,
    
    /// Statistics about this node
    pub stats: NodeStatistics,
    
    /// Metadata
    pub metadata: HashMap<String, String>,
}

impl HyperNode {
    /// Get schema name (always available since schema is required)
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }
    
    /// Get qualified table name (schema.table)
    pub fn qualified_table_name(&self) -> Option<String> {
        self.table_name.as_ref().map(|table| {
            format!("{}.{}", self.schema_name, table)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NodeType {
    Table,
    Column,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatistics {
    /// Estimated row count
    pub row_count: usize,
    
    /// Estimated cardinality
    pub cardinality: usize,
    
    /// Size in bytes
    pub size_bytes: usize,
    
    /// Last update timestamp
    pub last_updated: u64,
    
    // ========== Data Distribution Statistics ==========
    /// Minimum value (for numeric columns)
    pub min_value: Option<Value>,
    
    /// Maximum value (for numeric columns)
    pub max_value: Option<Value>,
    
    /// Mean value (for numeric columns)
    pub mean_value: Option<f64>,
    
    /// Standard deviation (for numeric columns)
    pub std_dev: Option<f64>,
    
    /// Percentiles: p25, p50 (median), p75, p90, p95, p99
    pub percentiles: Percentiles,
    
    /// Percentage of NULL values (0.0 to 1.0)
    pub null_percentage: f64,
    
    /// Percentage of empty strings (for string columns, 0.0 to 1.0)
    pub empty_string_percentage: f64,
    
    /// Exact or estimated distinct count
    pub distinct_count: f64,
    
    /// Top-N most frequent values with counts
    pub top_n_values: Vec<TopNValue>,
    
    // ========== Data Quality Metrics ==========
    /// Overall data quality score (0-100)
    pub data_quality_score: f64,
    
    /// Freshness score (0-100, based on last_updated)
    pub freshness_score: f64,
    
    /// Completeness score (0-100, based on null_percentage)
    pub completeness_score: f64,
    
    /// Consistency score (0-100, based on format consistency)
    pub consistency_score: f64,
    
    /// Number of detected anomalies
    pub anomaly_count: usize,
    
    // ========== Access Pattern Metadata ==========
    /// Access frequency (number of times accessed)
    pub access_frequency: u64,
    
    /// Last access timestamp
    pub last_accessed: u64,
    
    /// Read vs write ratio (0.0 = all reads, 1.0 = all writes)
    pub read_write_ratio: f64,
    
    /// Average selectivity when used in WHERE clause (0.0 to 1.0)
    pub filter_selectivity: Option<f64>,
    
    /// Average selectivity when used in JOIN (0.0 to 1.0)
    pub join_selectivity: Option<f64>,
    
    /// Query count using this column/table
    pub query_count: u64,
    
    /// Average query execution time in milliseconds
    pub avg_query_time_ms: f64,
    
    /// 95th percentile query execution time in milliseconds
    pub p95_query_time_ms: f64,
}

/// Percentiles for data distribution
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Percentiles {
    pub p25: Option<f64>,
    pub p50: Option<f64>, // Median
    pub p75: Option<f64>,
    pub p90: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
}

/// Top-N value with frequency
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopNValue {
    /// Value
    pub value: Value,
    
    /// Frequency (count)
    pub frequency: usize,
}

impl HyperNode {
    /// Create a new table node (schema is required)
    pub fn new_table(id: NodeId, schema_name: String, table_name: String) -> Self {
        Self {
            id,
            node_type: NodeType::Table,
            schema_name,
            table_name: Some(table_name),
            column_name: None,
            fragments: vec![],
            stats: NodeStatistics::default(),
            metadata: HashMap::new(),
        }
    }
    
    /// Create a new column node (schema is required)
    pub fn new_column(id: NodeId, schema_name: String, table_name: String, column_name: String) -> Self {
        Self {
            id,
            node_type: NodeType::Column,
            schema_name,
            table_name: Some(table_name),
            column_name: Some(column_name),
            fragments: vec![],
            stats: NodeStatistics::default(),
            metadata: HashMap::new(),
        }
    }
    
    /// Add a fragment to this node
    pub fn add_fragment(&mut self, fragment: ColumnFragment) {
        self.fragments.push(fragment);
        self.update_stats();
    }
    
    /// Update statistics from fragments
    pub fn update_stats(&mut self) {
        // For table nodes, all column fragments should have the same row_count
        // So we use the first fragment's row_count (or 0 if no fragments)
        // For column nodes, there's typically one fragment, so this works correctly
        self.stats.row_count = self.fragments.first()
            .map(|f| f.len())
            .unwrap_or(0);
        self.stats.size_bytes = self.fragments.iter().map(|f| f.metadata.memory_size).sum();
        
        // Update timestamp
        self.stats.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // Recalculate quality scores
        self.stats.recalculate_quality_scores();
        
        // TODO: Compute distribution statistics from fragments
        // This would require scanning fragments, which can be expensive
        // Should be done asynchronously or on-demand
    }
    
    /// Compute distribution statistics from fragments (expensive operation)
    /// This should be called asynchronously or on-demand
    pub fn compute_distribution_stats(&mut self) {
        // TODO: Implement actual computation from fragments
        // For now, this is a placeholder
        // Would need to:
        // 1. Scan all fragments
        // 2. Compute min, max, percentiles, mean, std_dev
        // 3. Count nulls and empty strings
        // 4. Compute distinct count (using HyperLogLog or similar)
        // 5. Compute top-N values
    }
    
    /// Get total row count across all fragments
    pub fn total_rows(&self) -> usize {
        self.stats.row_count
    }
}

impl Default for NodeStatistics {
    fn default() -> Self {
        Self {
            row_count: 0,
            cardinality: 0,
            size_bytes: 0,
            last_updated: 0,
            min_value: None,
            max_value: None,
            mean_value: None,
            std_dev: None,
            percentiles: Percentiles::default(),
            null_percentage: 0.0,
            empty_string_percentage: 0.0,
            distinct_count: 0.0,
            top_n_values: vec![],
            data_quality_score: 100.0, // Start with perfect score
            freshness_score: 100.0,
            completeness_score: 100.0,
            consistency_score: 100.0,
            anomaly_count: 0,
            access_frequency: 0,
            last_accessed: 0,
            read_write_ratio: 0.0, // Default to read-only
            filter_selectivity: None,
            join_selectivity: None,
            query_count: 0,
            avg_query_time_ms: 0.0,
            p95_query_time_ms: 0.0,
        }
    }
}

impl NodeStatistics {
    /// Record an access to this node
    pub fn record_access(&mut self, is_write: bool, query_time_ms: f64) {
        self.access_frequency += 1;
        self.last_accessed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // Update read/write ratio (exponential moving average)
        let alpha = 0.1; // Smoothing factor
        if is_write {
            self.read_write_ratio = alpha * 1.0 + (1.0 - alpha) * self.read_write_ratio;
        } else {
            self.read_write_ratio = alpha * 0.0 + (1.0 - alpha) * self.read_write_ratio;
        }
        
        // Update query time statistics
        self.query_count += 1;
        let alpha_time = 0.1;
        self.avg_query_time_ms = alpha_time * query_time_ms + (1.0 - alpha_time) * self.avg_query_time_ms;
        // For p95, we'd need to track all values or use a more sophisticated approach
        // For now, approximate: if new time > current p95, update it
        if query_time_ms > self.p95_query_time_ms {
            self.p95_query_time_ms = query_time_ms;
        }
    }
    
    /// Update filter selectivity (exponential moving average)
    pub fn update_filter_selectivity(&mut self, selectivity: f64) {
        let alpha = 0.1;
        self.filter_selectivity = Some(
            self.filter_selectivity
                .map(|s| alpha * selectivity + (1.0 - alpha) * s)
                .unwrap_or(selectivity)
        );
    }
    
    /// Update join selectivity (exponential moving average)
    pub fn update_join_selectivity(&mut self, selectivity: f64) {
        let alpha = 0.1;
        self.join_selectivity = Some(
            self.join_selectivity
                .map(|s| alpha * selectivity + (1.0 - alpha) * s)
                .unwrap_or(selectivity)
        );
    }
    
    /// Recalculate quality scores
    pub fn recalculate_quality_scores(&mut self) {
        // Completeness score: based on null_percentage
        self.completeness_score = (1.0 - self.null_percentage) * 100.0;
        
        // Freshness score: based on last_updated (decay over time)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let age_seconds = now.saturating_sub(self.last_updated);
        // Decay: 100% at 0 seconds, 50% at 1 day, 0% at 1 week
        let days_old = age_seconds as f64 / 86400.0;
        self.freshness_score = (100.0 * (-days_old / 7.0).exp()).max(0.0);
        
        // Overall quality score: weighted average
        self.data_quality_score = (
            self.completeness_score * 0.4 +
            self.freshness_score * 0.3 +
            self.consistency_score * 0.3
        );
    }
}

