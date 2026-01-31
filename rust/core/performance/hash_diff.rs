//! Hash-Based Diff
//! 
//! Uses hash-based comparison for fast diff of large datasets.
//! Computes hash of each row and compares hashes instead of full row comparison.

use crate::error::{RcaError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Hash-based diff result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashDiffResult {
    /// Row keys that exist only in left
    pub missing_left_keys: Vec<Vec<String>>,
    
    /// Row keys that exist only in right
    pub missing_right_keys: Vec<Vec<String>>,
    
    /// Row keys that exist in both but have different hashes (value mismatch)
    pub mismatch_keys: Vec<Vec<String>>,
    
    /// Row keys that match exactly
    pub match_keys: Vec<Vec<String>>,
    
    /// Summary statistics
    pub summary: HashDiffSummary,
}

/// Summary of hash diff
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashDiffSummary {
    pub missing_left_count: usize,
    pub missing_right_count: usize,
    pub mismatch_count: usize,
    pub match_count: usize,
}

/// Hash-based diff engine
pub struct HashDiffEngine;

impl HashDiffEngine {
    /// Create a new hash diff engine
    pub fn new() -> Self {
        Self
    }
    
    /// Diff two dataframes using hash-based comparison
    /// 
    /// This is much faster than full row comparison for large datasets.
    /// Computes a hash for each row and compares hashes.
    pub fn hash_diff(
        &self,
        left_df: DataFrame,
        right_df: DataFrame,
        keys: &[String],
        value_columns: &[String],
    ) -> Result<HashDiffResult> {
        // Step 1: Compute hashes for left dataframe
        let left_hash_map = self.compute_row_hashes(&left_df, keys, value_columns)?;
        
        // Step 2: Compute hashes for right dataframe
        let right_hash_map = self.compute_row_hashes(&right_df, keys, value_columns)?;
        
        // Step 3: Extract keys from both maps
        let left_keys: HashSet<Vec<String>> = left_hash_map.keys().cloned().collect();
        let right_keys: HashSet<Vec<String>> = right_hash_map.keys().cloned().collect();
        
        // Step 4: Find differences
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
        
        // Step 5: Compare hashes for common keys
        let mut mismatch_keys = Vec::new();
        let mut match_keys = Vec::new();
        
        for key in &common_keys {
            let left_hash = left_hash_map.get(key).unwrap();
            let right_hash = right_hash_map.get(key).unwrap();
            
            if left_hash == right_hash {
                match_keys.push(key.clone());
            } else {
                mismatch_keys.push(key.clone());
            }
        }
        
        let summary = HashDiffSummary {
            missing_left_count: missing_left_keys.len(),
            missing_right_count: missing_right_keys.len(),
            mismatch_count: mismatch_keys.len(),
            match_count: match_keys.len(),
        };
        
        Ok(HashDiffResult {
            missing_left_keys,
            missing_right_keys,
            mismatch_keys,
            match_keys,
            summary,
        })
    }
    
    /// Compute hash for each row
    fn compute_row_hashes(
        &self,
        df: &DataFrame,
        keys: &[String],
        value_columns: &[String],
    ) -> Result<HashMap<Vec<String>, u64>> {
        let mut hash_map = HashMap::new();
        
        for row_idx in 0..df.height() {
            // Extract key
            let mut key_vec = Vec::new();
            for key_col in keys {
                let col_val = df.column(key_col)?;
                let val_str = self.extract_value_as_string(col_val, row_idx)?;
                key_vec.push(val_str);
            }
            
            // Compute hash of value columns
            let mut hasher = DefaultHasher::new();
            
            for val_col in value_columns {
                let col_val = df.column(val_col)?;
                let val_str = self.extract_value_as_string(col_val, row_idx)?;
                val_str.hash(&mut hasher);
            }
            
            let hash = hasher.finish();
            hash_map.insert(key_vec, hash);
        }
        
        Ok(hash_map)
    }
    
    /// Extract value as string from a column
    fn extract_value_as_string(&self, col: &Series, row_idx: usize) -> Result<String> {
        let val_str = match col.dtype() {
            DataType::String => col.str()?.get(row_idx).unwrap_or("").to_string(),
            DataType::Int64 => col.i64()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::Float64 => col.f64()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::Date => col.date()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default(),
            DataType::Datetime(_, _) => {
                col.datetime()?.get(row_idx).map(|v| v.to_string()).unwrap_or_default()
            }
            _ => format!("{:?}", col.get(row_idx)),
        };
        Ok(val_str)
    }
}

impl Default for HashDiffEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hash_diff() {
        let left_df = DataFrame::new(vec![
            Series::new("id", vec![1, 2, 3]),
            Series::new("value", vec![100.0, 200.0, 300.0]),
        ]).unwrap();
        
        let right_df = DataFrame::new(vec![
            Series::new("id", vec![2, 3, 4]),
            Series::new("value", vec![200.0, 350.0, 400.0]),
        ]).unwrap();
        
        let engine = HashDiffEngine::new();
        let result = engine.hash_diff(
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

