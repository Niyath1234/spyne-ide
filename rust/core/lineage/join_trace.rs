//! Join Path Tracer
//! 
//! Tracks which joins succeeded or failed for each row during materialization.
//! This enables root cause attribution: "Row X was dropped because join Y failed."

use crate::error::{RcaError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Join trace for a single row
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinTrace {
    /// Row identifier (key values)
    pub row_id: Vec<String>,
    
    /// Table being joined from
    pub table_from: String,
    
    /// Table being joined to
    pub table_to: String,
    
    /// Join condition columns
    pub join_condition: Vec<String>,
    
    /// Join type
    pub join_type: String,
    
    /// Whether the join succeeded
    pub succeeded: bool,
    
    /// If failed, reason why
    pub failure_reason: Option<String>,
    
    /// Number of matching rows found (for diagnostics)
    pub match_count: usize,
}

/// Join trace collection
/// 
/// Stores join traces for all rows processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinTraceCollection {
    /// Traces indexed by row ID (string representation of key)
    traces: HashMap<String, Vec<JoinTrace>>,
}

impl JoinTraceCollection {
    /// Create a new collection
    pub fn new() -> Self {
        Self {
            traces: HashMap::new(),
        }
    }
    
    /// Add a join trace
    pub fn add_trace(&mut self, trace: JoinTrace) {
        let row_id_str = self.row_id_to_string(&trace.row_id);
        self.traces
            .entry(row_id_str)
            .or_insert_with(Vec::new)
            .push(trace);
    }
    
    /// Get traces for a row
    pub fn get_traces(&self, row_id: &[String]) -> Vec<&JoinTrace> {
        let row_id_str = self.row_id_to_string(row_id);
        self.traces
            .get(&row_id_str)
            .map(|traces| traces.iter().collect())
            .unwrap_or_default()
    }
    
    /// Get all failed joins
    pub fn get_failed_joins(&self) -> Vec<&JoinTrace> {
        self.traces
            .values()
            .flatten()
            .filter(|trace| !trace.succeeded)
            .collect()
    }
    
    /// Convert row ID to string for indexing
    fn row_id_to_string(&self, row_id: &[String]) -> String {
        row_id.join("|")
    }
}

impl Default for JoinTraceCollection {
    fn default() -> Self {
        Self::new()
    }
}

/// Join tracer
/// 
/// Tracks join operations during row materialization
pub struct JoinTracer {
    /// Collection of traces
    collection: JoinTraceCollection,
}

impl JoinTracer {
    /// Create a new tracer
    pub fn new() -> Self {
        Self {
            collection: JoinTraceCollection::new(),
        }
    }
    
    /// Trace a join operation
    /// 
    /// Executes the join and records which rows succeeded/failed
    pub async fn trace_join(
        &mut self,
        left_df: &DataFrame,
        right_df: &DataFrame,
        join_keys: &[String],
        join_type: &str,
        table_from: &str,
        table_to: &str,
    ) -> Result<(DataFrame, JoinTraceCollection)> {
        // Perform the join
        let key_exprs: Vec<Expr> = join_keys.iter().map(|k| col(k)).collect();
        
        let left_lazy = left_df.clone().lazy();
        let right_lazy = right_df.clone().lazy();
        
        // Determine join type
        // Note: Polars doesn't have Right join, so we swap tables for right joins
        let (actual_left, actual_right, polars_join_type) = match join_type.to_lowercase().as_str() {
            "inner" => (left_df.clone(), right_df.clone(), JoinType::Inner),
            "left" => (left_df.clone(), right_df.clone(), JoinType::Left),
            "right" => (right_df.clone(), left_df.clone(), JoinType::Left), // Swap for right join
            "outer" | "full" => (left_df.clone(), right_df.clone(), JoinType::Outer),
            _ => (left_df.clone(), right_df.clone(), JoinType::Inner),
        };
        
        // Perform join
        let joined = left_lazy
            .join(
                right_lazy,
                key_exprs.clone(),
                key_exprs.clone(),
                JoinArgs::new(polars_join_type),
            )
            .collect()?;
        
        // Trace which rows from left succeeded/failed
        // For left/outer joins, we can see which rows didn't match
        if join_type.to_lowercase() == "left" || join_type.to_lowercase() == "outer" {
            // Find rows that didn't match (have nulls in right table columns)
            let join_keys_set: std::collections::HashSet<String> = join_keys.iter().cloned().collect();
            let column_names: Vec<String> = actual_right.get_column_names().iter().map(|s| s.to_string()).collect();
            let right_cols: Vec<String> = column_names
                .into_iter()
                .filter(|c| !join_keys_set.contains(c))
                .collect();
            
            if !right_cols.is_empty() {
                // Check for nulls in right columns
                let first_right_col = &right_cols[0];
                let unmatched = joined
                    .clone()
                    .lazy()
                    .filter(col(first_right_col).is_null())
                    .collect()?;
                
                // Trace unmatched rows
                for row_idx in 0..unmatched.height() {
                    let mut row_id = Vec::new();
                    for key in join_keys {
                        let key_val = self.extract_row_value(&unmatched, key, row_idx)?;
                        row_id.push(key_val);
                    }
                    
                    let trace = JoinTrace {
                        row_id,
                        table_from: table_from.to_string(),
                        table_to: table_to.to_string(),
                        join_condition: join_keys.to_vec(),
                        join_type: join_type.to_string(),
                        succeeded: false,
                        failure_reason: Some("No matching row in right table".to_string()),
                        match_count: 0,
                    };
                    
                    self.collection.add_trace(trace);
                }
            }
        }
        
        // Trace successful joins
        // For inner joins, all rows succeeded
        // For left/outer joins, rows with non-null right columns succeeded
        let matched = if join_type.to_lowercase() == "inner" {
            joined.clone()
            } else {
                let join_keys_set: std::collections::HashSet<String> = join_keys.iter().cloned().collect();
                let column_names: Vec<String> = actual_right.get_column_names().iter().map(|s| s.to_string()).collect();
                let right_cols: Vec<String> = column_names
                    .into_iter()
                    .filter(|c| !join_keys_set.contains(c))
                    .collect();
            
            if !right_cols.is_empty() {
                let first_right_col = &right_cols[0];
                joined
                    .clone()
                    .lazy()
                    .filter(col(first_right_col).is_not_null())
                    .collect()?
            } else {
                joined.clone()
            }
        };
        
        for row_idx in 0..matched.height() {
            let mut row_id = Vec::new();
            for key in join_keys {
                let key_val = self.extract_row_value(&matched, key, row_idx)?;
                row_id.push(key_val);
            }
            
            // Count matches (simplified - in practice would count actual matches)
            let match_count = 1; // Simplified
            
            let trace = JoinTrace {
                row_id,
                table_from: table_from.to_string(),
                table_to: table_to.to_string(),
                join_condition: join_keys.to_vec(),
                join_type: join_type.to_string(),
                succeeded: true,
                failure_reason: None,
                match_count,
            };
            
            self.collection.add_trace(trace);
        }
        
        Ok((joined, self.collection.clone()))
    }
    
    /// Extract value from a row
    fn extract_row_value(&self, df: &DataFrame, col_name: &str, row_idx: usize) -> Result<String> {
        let col_series = df.column(col_name)?;
        let val_str = match col_series.dtype() {
            DataType::String => col_series.str()?.get(row_idx).unwrap_or("").to_string(),
            DataType::Int64 => col_series.i64()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::Float64 => col_series.f64()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            _ => format!("{:?}", col_series.get(row_idx)),
        };
        Ok(val_str)
    }
    
    /// Get the trace collection
    pub fn get_collection(&self) -> &JoinTraceCollection {
        &self.collection
    }
}

impl Default for JoinTracer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_join_trace_collection() {
        let mut collection = JoinTraceCollection::new();
        
        let trace = JoinTrace {
            row_id: vec!["1".to_string()],
            table_from: "table_a".to_string(),
            table_to: "table_b".to_string(),
            join_condition: vec!["id".to_string()],
            join_type: "inner".to_string(),
            succeeded: true,
            failure_reason: None,
            match_count: 1,
        };
        
        collection.add_trace(trace);
        assert_eq!(collection.get_traces(&["1".to_string()]).len(), 1);
    }
}

