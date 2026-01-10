use crate::error::{RcaError, Result};
use crate::rule_compiler::{ExecutionStep, RuleExecutor};
use polars::prelude::*;

pub struct DrilldownEngine {
    executor: RuleExecutor,
}

impl DrilldownEngine {
    pub fn new(executor: RuleExecutor) -> Self {
        Self { executor }
    }
    
    /// Find first divergence point between two rule executions
    pub async fn find_divergence(
        &self,
        rule_a: &str,
        rule_b: &str,
        mismatched_keys: &[Vec<String>],
        as_of_date: Option<chrono::NaiveDate>,
    ) -> Result<DivergencePoint> {
        // Execute both rules step-by-step
        let steps_a = self.executor.execute_with_steps(rule_a, as_of_date).await?;
        let steps_b = self.executor.execute_with_steps(rule_b, as_of_date).await?;
        
        // Compare step by step
        for (idx, (step_a, step_b)) in steps_a.iter().zip(steps_b.iter()).enumerate() {
            // Filter to mismatched keys only
            let df_a_filtered = self.filter_to_keys(step_a.data.as_ref().unwrap(), mismatched_keys)?;
            let df_b_filtered = self.filter_to_keys(step_b.data.as_ref().unwrap(), mismatched_keys)?;
            
            // Compare row counts
            if df_a_filtered.height() != df_b_filtered.height() {
                return Ok(DivergencePoint {
                    step_index: idx,
                    step_name_a: step_a.step_name.clone(),
                    step_name_b: step_b.step_name.clone(),
                    operation_a: step_a.operation.clone(),
                    operation_b: step_b.operation.clone(),
                    row_count_a: df_a_filtered.height(),
                    row_count_b: df_b_filtered.height(),
                    divergence_type: "row_count_mismatch".to_string(),
                });
            }
            
            // Compare column values if same structure
            if step_a.columns == step_b.columns {
                let diff = self.compare_dataframes(&df_a_filtered, &df_b_filtered)?;
                if diff > 0 {
                    return Ok(DivergencePoint {
                        step_index: idx,
                        step_name_a: step_a.step_name.clone(),
                        step_name_b: step_b.step_name.clone(),
                        operation_a: step_a.operation.clone(),
                        operation_b: step_b.operation.clone(),
                        row_count_a: df_a_filtered.height(),
                        row_count_b: df_b_filtered.height(),
                        divergence_type: "value_mismatch".to_string(),
                    });
                }
            }
        }
        
        // No divergence found in steps - might be in final aggregation
        Ok(DivergencePoint {
            step_index: steps_a.len(),
            step_name_a: "final".to_string(),
            step_name_b: "final".to_string(),
            operation_a: "final_aggregation".to_string(),
            operation_b: "final_aggregation".to_string(),
            row_count_a: steps_a.last().map(|s| s.row_count).unwrap_or(0),
            row_count_b: steps_b.last().map(|s| s.row_count).unwrap_or(0),
            divergence_type: "final_aggregation".to_string(),
        })
    }
    
    fn filter_to_keys(&self, df: &DataFrame, keys: &[Vec<String>]) -> Result<DataFrame> {
        if keys.is_empty() {
            return Ok(df.clone());
        }
        
        // Get grain columns from first key
        let grain_cols: Vec<&str> = (0..keys[0].len())
            .map(|i| df.get_column_names()[i])
            .collect();
        
        // Build filter expression
        let mut conditions = Vec::new();
        for key in keys {
            let mut key_conditions = Vec::new();
            for (col_idx, val) in key.iter().enumerate() {
                if col_idx < grain_cols.len() {
                    let col_name = grain_cols[col_idx];
                    key_conditions.push(col(col_name).eq(lit(val.as_str())));
                }
            }
            if !key_conditions.is_empty() {
                conditions.push(key_conditions.into_iter().reduce(|a, b| a.and(b)).unwrap());
            }
        }
        
        if conditions.is_empty() {
            return Ok(df.clone());
        }
        
        let filter_expr = conditions.into_iter().reduce(|a, b| a.or(b)).unwrap();
        Ok(df.clone().lazy().filter(filter_expr).collect()?)
    }
    
    fn compare_dataframes(&self, df_a: &DataFrame, df_b: &DataFrame) -> Result<usize> {
        // Simple comparison - count rows that differ
        // In production, would do more sophisticated comparison
        if df_a.height() != df_b.height() {
            return Ok(df_a.height().max(df_b.height()));
        }
        
        // Compare values (simplified)
        Ok(0) // Placeholder
    }
}

#[derive(Debug, Clone)]
pub struct DivergencePoint {
    pub step_index: usize,
    pub step_name_a: String,
    pub step_name_b: String,
    pub operation_a: String,
    pub operation_b: String,
    pub row_count_a: usize,
    pub row_count_b: usize,
    pub divergence_type: String,
}

