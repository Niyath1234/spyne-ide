//! Filter Tracer
//! 
//! Tracks which filters passed or failed for each row.
//! This enables root cause attribution: "Row X was dropped because filter Y failed."

use crate::error::{RcaError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Filter decision for a single row
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterDecision {
    /// Row identifier (key values)
    pub row_id: Vec<String>,
    
    /// Filter expression that was evaluated
    pub filter_expr: String,
    
    /// Whether the filter passed
    pub passed: bool,
    
    /// If failed, the actual values that caused failure
    pub failure_values: Option<HashMap<String, String>>,
    
    /// Description of the filter
    pub description: Option<String>,
}

/// Filter trace collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterTraceCollection {
    /// Traces indexed by row ID
    traces: HashMap<String, Vec<FilterDecision>>,
}

impl FilterTraceCollection {
    /// Create a new collection
    pub fn new() -> Self {
        Self {
            traces: HashMap::new(),
        }
    }
    
    /// Add a filter decision
    pub fn add_decision(&mut self, decision: FilterDecision) {
        let row_id_str = self.row_id_to_string(&decision.row_id);
        self.traces
            .entry(row_id_str)
            .or_insert_with(Vec::new)
            .push(decision);
    }
    
    /// Get decisions for a row
    pub fn get_decisions(&self, row_id: &[String]) -> Vec<&FilterDecision> {
        let row_id_str = self.row_id_to_string(row_id);
        self.traces
            .get(&row_id_str)
            .map(|decisions| decisions.iter().collect())
            .unwrap_or_default()
    }
    
    /// Get all failed filters
    pub fn get_failed_filters(&self) -> Vec<&FilterDecision> {
        self.traces
            .values()
            .flatten()
            .filter(|decision| !decision.passed)
            .collect()
    }
    
    /// Convert row ID to string
    fn row_id_to_string(&self, row_id: &[String]) -> String {
        row_id.join("|")
    }
}

impl Default for FilterTraceCollection {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter tracer
/// 
/// Tracks filter operations during row materialization
pub struct FilterTracer {
    /// Collection of traces
    collection: FilterTraceCollection,
    
    /// Key columns for row identification
    key_columns: Vec<String>,
}

impl FilterTracer {
    /// Create a new tracer
    pub fn new(key_columns: Vec<String>) -> Self {
        Self {
            collection: FilterTraceCollection::new(),
            key_columns,
        }
    }
    
    /// Trace a filter operation
    /// 
    /// Applies the filter and records which rows passed/failed
    pub async fn trace_filter(
        &mut self,
        df: &DataFrame,
        filter_expr: &str,
        description: Option<&str>,
    ) -> Result<(DataFrame, FilterTraceCollection)> {
        // For now, we'll use a simplified approach
        // In a full implementation, you'd parse the filter expression
        // and evaluate it row by row
        
        // Apply filter using Polars
        // Note: This is simplified - real implementation would need
        // to parse the expression and evaluate row-by-row
        let filtered_df = df
            .clone()
            .lazy()
            .filter(
                // Placeholder - real implementation needs expression parsing
                col("1").eq(lit(1)) // Always true for now
            )
            .collect()?;
        
        // Trace all rows as passed (simplified)
        // In real implementation, would evaluate filter per row
        for row_idx in 0..df.height() {
            let mut row_id = Vec::new();
            for key_col in &self.key_columns {
                let key_val = self.extract_row_value(df, key_col, row_idx)?;
                row_id.push(key_val);
            }
            
            // Check if row is in filtered result
            let passed = self.row_exists_in_df(&row_id, &filtered_df)?;
            
            let decision = FilterDecision {
                row_id,
                filter_expr: filter_expr.to_string(),
                passed,
                failure_values: if !passed {
                    Some(self.extract_row_values(df, row_idx)?)
                } else {
                    None
                },
                description: description.map(|s| s.to_string()),
            };
            
            self.collection.add_decision(decision);
        }
        
        Ok((filtered_df, self.collection.clone()))
    }
    
    /// Check if a row exists in dataframe
    fn row_exists_in_df(&self, row_id: &[String], df: &DataFrame) -> Result<bool> {
        // Simplified check - in practice would do proper key matching
        Ok(df.height() > 0)
    }
    
    /// Extract row values as map
    fn extract_row_values(&self, df: &DataFrame, row_idx: usize) -> Result<HashMap<String, String>> {
        let mut values = HashMap::new();
        
        for col_name in df.get_column_names() {
            let val = self.extract_row_value(df, col_name, row_idx)?;
            values.insert(col_name.to_string(), val);
        }
        
        Ok(values)
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
    pub fn get_collection(&self) -> &FilterTraceCollection {
        &self.collection
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_filter_trace_collection() {
        let mut collection = FilterTraceCollection::new();
        
        let decision = FilterDecision {
            row_id: vec!["1".to_string()],
            filter_expr: "amount > 100".to_string(),
            passed: false,
            failure_values: Some({
                let mut m = HashMap::new();
                m.insert("amount".to_string(), "50".to_string());
                m
            }),
            description: Some("Filter high amounts".to_string()),
        };
        
        collection.add_decision(decision);
        assert_eq!(collection.get_decisions(&["1".to_string()]).len(), 1);
    }
}





