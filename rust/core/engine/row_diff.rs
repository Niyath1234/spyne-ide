//! Enhanced Row Diff Engine
//! 
//! Provides detailed row-level diff results with:
//! - missing_left: Rows only in left dataframe
//! - missing_right: Rows only in right dataframe  
//! - value_mismatch: Rows in both but with different values

use crate::error::{RcaError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Detailed row diff result
#[derive(Debug, Clone)]
pub struct RowDiffResult {
    /// Rows that exist only in the left dataframe
    pub missing_left: DataFrame,
    
    /// Rows that exist only in the right dataframe
    pub missing_right: DataFrame,
    
    /// Rows that exist in both but have different values
    pub value_mismatch: DataFrame,
    
    /// Rows that match exactly (for verification)
    pub matches: DataFrame,
    
    /// Summary statistics
    pub summary: DiffSummary,
}

/// Summary of diff results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffSummary {
    /// Number of rows only in left
    pub missing_left_count: usize,
    
    /// Number of rows only in right
    pub missing_right_count: usize,
    
    /// Number of value mismatches
    pub mismatch_count: usize,
    
    /// Number of exact matches
    pub match_count: usize,
    
    /// Total difference in value columns (sum of absolute differences)
    pub total_value_diff: f64,
}

/// Enhanced row diff engine
pub struct RowDiffEngine {
    /// Precision for comparing numeric values
    pub precision: u32,
}

impl RowDiffEngine {
    /// Create a new row diff engine
    pub fn new(precision: u32) -> Self {
        Self { precision }
    }
    
    /// Diff two dataframes at row level
    /// 
    /// Returns detailed breakdown of:
    /// - Rows only in left
    /// - Rows only in right
    /// - Rows in both with different values
    pub fn diff_rows(
        &self,
        left_df: DataFrame,
        right_df: DataFrame,
        keys: &[String],
        value_columns: &[String],
    ) -> Result<RowDiffResult> {
        // Step 1: Extract keys from both dataframes
        let left_keys = self.extract_keys_set(&left_df, keys)?;
        let right_keys = self.extract_keys_set(&right_df, keys)?;
        
        // Step 2: Find missing rows
        let missing_left_keys: Vec<Vec<String>> = left_keys
            .difference(&right_keys)
            .cloned()
            .collect();
        let missing_right_keys: Vec<Vec<String>> = right_keys
            .difference(&left_keys)
            .cloned()
            .collect();
        let common_keys: Vec<Vec<String>> = left_keys
            .intersection(&right_keys)
            .cloned()
            .collect();
        
        // Step 3: Extract missing rows
        let missing_left = self.filter_by_keys(&left_df, keys, &missing_left_keys)?;
        let missing_right = self.filter_by_keys(&right_df, keys, &missing_right_keys)?;
        
        // Step 4: Compare common rows for value differences
        let (value_mismatch, matches) = self.compare_common_rows(
            &left_df,
            &right_df,
            keys,
            value_columns,
            &common_keys,
        )?;
        
        // Step 5: Calculate summary
        let total_value_diff = self.calculate_total_diff(&value_mismatch, value_columns)?;
        
        let summary = DiffSummary {
            missing_left_count: missing_left.height(),
            missing_right_count: missing_right.height(),
            mismatch_count: value_mismatch.height(),
            match_count: matches.height(),
            total_value_diff,
        };
        
        Ok(RowDiffResult {
            missing_left,
            missing_right,
            value_mismatch,
            matches,
            summary,
        })
    }
    
    /// Extract keys as a set of vectors
    fn extract_keys_set(&self, df: &DataFrame, keys: &[String]) -> Result<HashSet<Vec<String>>> {
        let mut key_set = HashSet::new();
        
        for row_idx in 0..df.height() {
            let mut key_vec = Vec::new();
            for key_col in keys {
                let col_val = df.column(key_col)?;
                let val_str = self.extract_value_as_string(col_val, row_idx)?;
                key_vec.push(val_str);
            }
            key_set.insert(key_vec);
        }
        
        Ok(key_set)
    }
    
    /// Extract value as string from a column
    fn extract_value_as_string(&self, col: &Series, row_idx: usize) -> Result<String> {
        let val_str = match col.dtype() {
            DataType::String => col.str()?.get(row_idx).unwrap_or("").to_string(),
            DataType::Int32 => col.i32()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::Int64 => col.i64()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::UInt32 => col.u32()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::UInt64 => col.u64()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::Float32 => col.f32()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::Float64 => col.f64()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::Date => col.date()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::Datetime(_, _) => {
                col.datetime()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default()
            }
            // Fallback: cast to string column and extract
            _ => {
                let string_col = col.cast(&DataType::String).ok();
                if let Some(str_col) = string_col {
                    str_col.str()?.get(row_idx).unwrap_or("").to_string()
                } else {
                    format!("{:?}", col.get(row_idx))
                }
            }
        };
        Ok(val_str)
    }
    
    /// Filter dataframe by keys
    fn filter_by_keys(
        &self,
        df: &DataFrame,
        keys: &[String],
        key_list: &[Vec<String>],
    ) -> Result<DataFrame> {
        if key_list.is_empty() {
            // Return empty dataframe with same schema
            return Ok(df.head(Some(0)));
        }
        
        // Build filter condition: (key1 == val1 AND key2 == val2) OR ...
        let mut conditions = Vec::new();
        
        for key_vec in key_list {
            let mut key_conditions = Vec::new();
            for (idx, key_col) in keys.iter().enumerate() {
                if let Some(key_val) = key_vec.get(idx) {
                    // Cast column to string for comparison (handles int/float columns)
                    key_conditions.push(col(key_col).cast(DataType::String).eq(lit(key_val.clone())));
                }
            }
            
            if !key_conditions.is_empty() {
                let combined = key_conditions
                    .into_iter()
                    .reduce(|acc, cond| acc.and(cond))
                    .unwrap();
                conditions.push(combined);
            }
        }
        
        if conditions.is_empty() {
            return Ok(df.head(Some(0)));
        }
        
        let filter_expr = conditions
            .into_iter()
            .reduce(|acc, cond| acc.or(cond))
            .unwrap();
        
        let filtered = df
            .clone()
            .lazy()
            .filter(filter_expr)
            .collect()?;
        
        Ok(filtered)
    }
    
    /// Compare common rows for value differences
    fn compare_common_rows(
        &self,
        left_df: &DataFrame,
        right_df: &DataFrame,
        keys: &[String],
        value_columns: &[String],
        common_keys: &[Vec<String>],
    ) -> Result<(DataFrame, DataFrame)> {
        // Join on keys
        let key_exprs: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
        
        let left_lazy = left_df.clone().lazy();
        let right_lazy = right_df.clone().lazy();
        
        // Rename value columns to avoid conflicts
        let mut left_renames = Vec::new();
        let mut right_renames = Vec::new();
        
        for val_col in value_columns {
            left_renames.push((val_col.clone(), format!("left_{}", val_col)));
            right_renames.push((val_col.clone(), format!("right_{}", val_col)));
        }
        
        let left_renamed = left_lazy
            .with_columns(
                left_renames.iter().map(|(old, new)| col(old).alias(new)).collect::<Vec<_>>()
            );
        
        let right_renamed = right_lazy
            .with_columns(
                right_renames.iter().map(|(old, new)| col(old).alias(new)).collect::<Vec<_>>()
            );
        
        // Join
        let joined = left_renamed
            .join(
                right_renamed,
                key_exprs.clone(),
                key_exprs.clone(),
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;
        
        // Build comparison conditions
        let mut mismatch_conditions = Vec::new();
        
        for val_col in value_columns {
            let left_col = format!("left_{}", val_col);
            let right_col = format!("right_{}", val_col);
            
            // Compare with precision tolerance
            let precision_factor = 10_f64.powi(self.precision as i32);
            let threshold = 1.0 / precision_factor;
            
            // Compute absolute value check: |left - right| > threshold
            // Equivalent to: (left - right > threshold) OR (right - left > threshold)
            let diff_expr = col(&left_col) - col(&right_col);
            mismatch_conditions.push(
                diff_expr.clone().gt(lit(threshold))
                    .or((col(&right_col) - col(&left_col)).gt(lit(threshold)))
            );
        }
        
        // Combine mismatch conditions with OR
        let mismatch_filter = mismatch_conditions
            .into_iter()
            .reduce(|acc, cond| acc.or(cond))
            .unwrap();
        
        // Split into mismatches and matches
        let mismatches = joined
            .clone()
            .lazy()
            .filter(mismatch_filter.clone())
            .collect()?;
        
        let matches = joined
            .clone()
            .lazy()
            .filter(mismatch_filter.not())
            .collect()?;
        
        Ok((mismatches, matches))
    }
    
    /// Calculate total difference in value columns
    fn calculate_total_diff(&self, mismatch_df: &DataFrame, value_columns: &[String]) -> Result<f64> {
        if mismatch_df.height() == 0 {
            return Ok(0.0);
        }
        
        let mut total = 0.0;
        
        for val_col in value_columns {
            let left_col = format!("left_{}", val_col);
            let right_col = format!("right_{}", val_col);
            
            if let (Ok(left_series), Ok(right_series)) = 
                (mismatch_df.column(&left_col), mismatch_df.column(&right_col)) {
                
                if let (Ok(left_f64), Ok(right_f64)) = 
                    (left_series.f64(), right_series.f64()) {
                    
                    for idx in 0..mismatch_df.height() {
                        if let (Some(left_val), Some(right_val)) = 
                            (left_f64.get(idx), right_f64.get(idx)) {
                            total += (left_val - right_val).abs();
                        }
                    }
                }
            }
        }
        
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_row_diff() {
        // Create test dataframes
        let left_df = DataFrame::new(vec![
            Series::new("id", vec![1, 2, 3]),
            Series::new("value", vec![100.0, 200.0, 300.0]),
        ]).unwrap();
        
        let right_df = DataFrame::new(vec![
            Series::new("id", vec![2, 3, 4]),
            Series::new("value", vec![200.0, 350.0, 400.0]),
        ]).unwrap();
        
        let engine = RowDiffEngine::new(2);
        let result = engine.diff_rows(
            left_df,
            right_df,
            &["id".to_string()],
            &["value".to_string()],
        ).unwrap();
        
        assert_eq!(result.summary.missing_left_count, 1); // id=1
        assert_eq!(result.summary.missing_right_count, 1); // id=4
        assert_eq!(result.summary.mismatch_count, 1); // id=3: 300 vs 350
        assert_eq!(result.summary.match_count, 1); // id=2: 200 vs 200
    }
}

