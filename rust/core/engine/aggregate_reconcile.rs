//! Aggregation Reconciliation
//! 
//! Proves that the aggregate mismatch equals the sum of row-level differences.
//! This provides the trust layer: we can verify that our row diff explains
//! the aggregate-level discrepancy.

use crate::error::{RcaError, Result};
use crate::core::engine::row_diff::{RowDiffResult, RowDiffEngine};
use polars::prelude::*;
use serde::{Deserialize, Serialize};

/// Aggregation reconciliation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateReconciliation {
    /// Reported aggregate mismatch (from original comparison)
    pub reported_mismatch: f64,
    
    /// Calculated mismatch from row diff
    pub calculated_mismatch: f64,
    
    /// Difference between reported and calculated (should be ~0)
    pub reconciliation_error: f64,
    
    /// Whether reconciliation passes (within tolerance)
    pub passes: bool,
    
    /// Breakdown by category
    pub breakdown: ReconciliationBreakdown,
}

/// Breakdown of reconciliation by category
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationBreakdown {
    /// Contribution from missing_left rows
    pub missing_left_contribution: f64,
    
    /// Contribution from missing_right rows
    pub missing_right_contribution: f64,
    
    /// Contribution from value mismatches
    pub mismatch_contribution: f64,
}

/// Aggregate reconciliation engine
pub struct AggregateReconciliationEngine {
    /// Precision tolerance for reconciliation
    pub precision: u32,
}

impl AggregateReconciliationEngine {
    /// Create a new reconciliation engine
    pub fn new(precision: u32) -> Self {
        Self { precision }
    }
    
    /// Reconcile aggregates from row diff
    /// 
    /// Proves: sum(left) - sum(right) == reported_mismatch
    /// 
    /// This is calculated as:
    /// - Sum of missing_left values (positive contribution)
    /// - Sum of missing_right values (negative contribution)
    /// - Sum of value differences (positive if left > right, negative otherwise)
    pub fn reconcile_aggregates(
        &self,
        row_diff: &RowDiffResult,
        value_columns: &[String],
        reported_mismatch: f64,
    ) -> Result<AggregateReconciliation> {
        // Calculate contributions from each category
        let missing_left_sum = self.sum_value_columns(&row_diff.missing_left, value_columns)?;
        let missing_right_sum = self.sum_value_columns(&row_diff.missing_right, value_columns)?;
        
        // For value mismatches, sum the differences (left - right)
        let mismatch_diff_sum = self.sum_value_differences(&row_diff.value_mismatch, value_columns)?;
        
        // Total calculated mismatch
        // missing_left adds to the mismatch (left has more)
        // missing_right subtracts from the mismatch (right has more)
        // mismatch_diff adds/subtracts based on direction
        let calculated_mismatch = missing_left_sum - missing_right_sum + mismatch_diff_sum;
        
        // Calculate reconciliation error
        let reconciliation_error = (reported_mismatch - calculated_mismatch).abs();
        
        // Check if passes (within precision tolerance)
        let precision_factor = 10_f64.powi(self.precision as i32);
        let threshold = 1.0 / precision_factor;
        let passes = reconciliation_error <= threshold;
        
        let breakdown = ReconciliationBreakdown {
            missing_left_contribution: missing_left_sum,
            missing_right_contribution: missing_right_sum,
            mismatch_contribution: mismatch_diff_sum,
        };
        
        Ok(AggregateReconciliation {
            reported_mismatch,
            calculated_mismatch,
            reconciliation_error,
            passes,
            breakdown,
        })
    }
    
    /// Sum value columns in a dataframe
    fn sum_value_columns(&self, df: &DataFrame, value_columns: &[String]) -> Result<f64> {
        if df.height() == 0 {
            return Ok(0.0);
        }
        
        let mut total = 0.0;
        
        for val_col in value_columns {
            if let Ok(series) = df.column(val_col) {
                if let Ok(f64_series) = series.f64() {
                    for idx in 0..df.height() {
                        if let Some(val) = f64_series.get(idx) {
                            total += val;
                        }
                    }
                }
            }
        }
        
        Ok(total)
    }
    
    /// Sum value differences from mismatch dataframe
    /// 
    /// The mismatch dataframe has columns like "left_value" and "right_value"
    fn sum_value_differences(&self, mismatch_df: &DataFrame, value_columns: &[String]) -> Result<f64> {
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
                            total += left_val - right_val;
                        }
                    }
                }
            }
        }
        
        Ok(total)
    }
    
    /// Verify reconciliation passes
    /// 
    /// Returns detailed explanation if reconciliation fails
    pub fn verify(&self, reconciliation: &AggregateReconciliation) -> Result<VerificationResult> {
        if reconciliation.passes {
            Ok(VerificationResult {
                passes: true,
                message: "Reconciliation verified: row diff matches aggregate mismatch".to_string(),
            })
        } else {
            Ok(VerificationResult {
                passes: false,
                message: format!(
                    "Reconciliation failed: reported mismatch ({:.2}) does not match calculated ({:.2}). Error: {:.2}",
                    reconciliation.reported_mismatch,
                    reconciliation.calculated_mismatch,
                    reconciliation.reconciliation_error
                ),
            })
        }
    }
}

/// Verification result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    pub passes: bool,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::engine::row_diff::RowDiffEngine;
    
    #[test]
    fn test_reconciliation() {
        // Create test dataframes
        let left_df = DataFrame::new(vec![
            Series::new("id", vec![1, 2]),
            Series::new("value", vec![100.0, 200.0]),
        ]).unwrap();
        
        let right_df = DataFrame::new(vec![
            Series::new("id", vec![2]),
            Series::new("value", vec![250.0]),
        ]).unwrap();
        
        // Calculate row diff
        let diff_engine = RowDiffEngine::new(2);
        let row_diff = diff_engine.diff_rows(
            left_df.clone(),
            right_df.clone(),
            &["id".to_string()],
            &["value".to_string()],
        ).unwrap();
        
        // Calculate reported mismatch: sum(left) - sum(right)
        let left_sum: f64 = left_df.column("value").unwrap().f64().unwrap().sum().unwrap();
        let right_sum: f64 = right_df.column("value").unwrap().f64().unwrap().sum().unwrap();
        let reported_mismatch = left_sum - right_sum; // 300 - 250 = 50
        
        // Reconcile
        let reconcile_engine = AggregateReconciliationEngine::new(2);
        let reconciliation = reconcile_engine.reconcile_aggregates(
            &row_diff,
            &["value".to_string()],
            reported_mismatch,
        ).unwrap();
        
        // Should pass: missing_left (100) - missing_right (0) + mismatch (200-250=-50) = 50
        assert!(reconciliation.passes || reconciliation.reconciliation_error < 1.0);
    }
}





