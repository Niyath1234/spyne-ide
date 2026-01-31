//! Rule Execution Tracer
//! 
//! Tracks rule execution per row, recording input values, output values,
//! and whether the rule fired. This enables root cause attribution:
//! "Row X was transformed by rule Y, changing value from A to B."

use crate::error::{RcaError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Rule execution trace for a single row
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleExecution {
    /// Row identifier (key values)
    pub row_id: Vec<String>,
    
    /// Rule ID
    pub rule_id: String,
    
    /// Input values before rule application
    pub input_values: HashMap<String, String>,
    
    /// Output values after rule application
    pub output_values: HashMap<String, String>,
    
    /// Whether the rule fired (was applicable)
    pub fired: bool,
    
    /// If fired, description of what changed
    pub change_description: Option<String>,
}

/// Rule trace collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleTraceCollection {
    /// Traces indexed by row ID
    traces: HashMap<String, Vec<RuleExecution>>,
}

impl RuleTraceCollection {
    /// Create a new collection
    pub fn new() -> Self {
        Self {
            traces: HashMap::new(),
        }
    }
    
    /// Add a rule execution trace
    pub fn add_execution(&mut self, execution: RuleExecution) {
        let row_id_str = self.row_id_to_string(&execution.row_id);
        self.traces
            .entry(row_id_str)
            .or_insert_with(Vec::new)
            .push(execution);
    }
    
    /// Get executions for a row
    pub fn get_executions(&self, row_id: &[String]) -> Vec<&RuleExecution> {
        let row_id_str = self.row_id_to_string(row_id);
        self.traces
            .get(&row_id_str)
            .map(|executions| executions.iter().collect())
            .unwrap_or_default()
    }
    
    /// Get all fired rules
    pub fn get_fired_rules(&self) -> Vec<&RuleExecution> {
        self.traces
            .values()
            .flatten()
            .filter(|execution| execution.fired)
            .collect()
    }
    
    /// Convert row ID to string
    fn row_id_to_string(&self, row_id: &[String]) -> String {
        row_id.join("|")
    }
}

impl Default for RuleTraceCollection {
    fn default() -> Self {
        Self::new()
    }
}

/// Rule tracer
/// 
/// Tracks rule execution during row materialization
pub struct RuleTracer {
    /// Collection of traces
    collection: RuleTraceCollection,
    
    /// Key columns for row identification
    key_columns: Vec<String>,
}

impl RuleTracer {
    /// Create a new tracer
    pub fn new(key_columns: Vec<String>) -> Self {
        Self {
            collection: RuleTraceCollection::new(),
            key_columns,
        }
    }
    
    /// Trace rule execution
    /// 
    /// Records input/output values and whether the rule fired
    pub async fn trace_rule_execution(
        &mut self,
        before_df: &DataFrame,
        after_df: &DataFrame,
        rule_id: &str,
        affected_columns: &[String],
    ) -> Result<RuleTraceCollection> {
        // Compare before and after to detect changes
        // For each row, check if values changed
        
        // Match rows by key columns
        let key_exprs: Vec<Expr> = self.key_columns.iter().map(|k| col(k)).collect();
        
        let before_lazy = before_df.clone().lazy();
        let after_lazy = after_df.clone().lazy();
        
        // Join on keys to compare
        let joined = before_lazy
            .join(
                after_lazy,
                key_exprs.clone(),
                key_exprs.clone(),
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
        
        // For each row, check if affected columns changed
        for row_idx in 0..joined.height() {
            let mut row_id = Vec::new();
            for key_col in &self.key_columns {
                let key_val = self.extract_row_value(&joined, key_col, row_idx)?;
                row_id.push(key_val);
            }
            
            // Extract input and output values
            let mut input_values = HashMap::new();
            let mut output_values = HashMap::new();
            let mut fired = false;
            
            for col_name in affected_columns {
                // Try to get before and after values
                // In joined dataframe, columns might be renamed
                let before_val = self.try_extract_value(&joined, &format!("{}_left", col_name), row_idx)
                    .or_else(|| self.try_extract_value(&joined, col_name, row_idx));
                
                let after_val = self.try_extract_value(&joined, &format!("{}_right", col_name), row_idx)
                    .or_else(|| self.try_extract_value(&joined, col_name, row_idx));
                
                if let (Some(before), Some(after)) = (before_val, after_val) {
                    input_values.insert(col_name.clone(), before.clone());
                    output_values.insert(col_name.clone(), after.clone());
                    
                    if before != after {
                        fired = true;
                    }
                }
            }
            
            // If no affected columns found, check if row exists in both
            if input_values.is_empty() {
                // Row exists in both - rule may have fired but didn't change tracked columns
                fired = true; // Assume fired if row exists
            }
            
            let change_description = if fired {
                Some(format!("Rule {} applied, changed {} columns", rule_id, affected_columns.len()))
            } else {
                None
            };
            
            let execution = RuleExecution {
                row_id,
                rule_id: rule_id.to_string(),
                input_values,
                output_values,
                fired,
                change_description,
            };
            
            self.collection.add_execution(execution);
        }
        
        Ok(self.collection.clone())
    }
    
    /// Try to extract value, returning None if column doesn't exist
    fn try_extract_value(&self, df: &DataFrame, col_name: &str, row_idx: usize) -> Option<String> {
        df.column(col_name).ok().and_then(|col_series| {
            match col_series.dtype() {
                DataType::String => col_series.str().ok()?.get(row_idx).map(|s| s.to_string()),
                DataType::Int64 => col_series.i64().ok()?.get(row_idx).map(|v| v.to_string()),
                DataType::Float64 => col_series.f64().ok()?.get(row_idx).map(|v| v.to_string()),
                _ => Some(format!("{:?}", col_series.get(row_idx))),
            }
        })
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
    pub fn get_collection(&self) -> &RuleTraceCollection {
        &self.collection
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_rule_trace_collection() {
        let mut collection = RuleTraceCollection::new();
        
        let execution = RuleExecution {
            row_id: vec!["1".to_string()],
            rule_id: "rule_1".to_string(),
            input_values: {
                let mut m = HashMap::new();
                m.insert("amount".to_string(), "100".to_string());
                m
            },
            output_values: {
                let mut m = HashMap::new();
                m.insert("amount".to_string(), "150".to_string());
                m
            },
            fired: true,
            change_description: Some("Applied discount".to_string()),
        };
        
        collection.add_execution(execution);
        assert_eq!(collection.get_executions(&["1".to_string()]).len(), 1);
    }
}





