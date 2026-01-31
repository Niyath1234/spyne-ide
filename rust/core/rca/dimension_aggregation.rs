//! Dimension Aggregation for Fast Mode
//! 
//! Provides contribution analysis by dimension (date, bucket, product, system)
//! to identify dominant sources of mismatch in Fast mode.

use crate::error::{RcaError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Dimension contribution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionContribution {
    /// Dimension name
    pub dimension: String,
    
    /// Dimension value
    pub value: String,
    
    /// Contribution to mismatch (absolute value)
    pub contribution: f64,
    
    /// Percentage contribution
    pub percentage: f64,
    
    /// Number of rows contributing
    pub row_count: usize,
}

/// Dimension aggregation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionAggregationResult {
    /// Contributions by dimension
    pub contributions: Vec<DimensionContribution>,
    
    /// Top contributing dimensions
    pub top_contributors: Vec<DimensionContribution>,
    
    /// Total mismatch magnitude
    pub total_mismatch: f64,
}

/// Dimension aggregator for Fast mode
pub struct DimensionAggregator;

impl DimensionAggregator {
    /// Aggregate mismatch by dimension
    /// 
    /// Analyzes which dimensions (date, bucket, product, etc.) contribute most to the mismatch.
    pub fn aggregate_by_dimension(
        &self,
        missing_left: &DataFrame,
        missing_right: &DataFrame,
        value_mismatch: &DataFrame,
        value_columns: &[String],
        dimension_columns: &[String],
    ) -> Result<DimensionAggregationResult> {
        let mut contributions = Vec::new();
        let mut total_mismatch = 0.0;
        
        // Aggregate missing_left contributions
        for dim_col in dimension_columns {
            if missing_left.height() > 0 {
                let dim_contributions = self.aggregate_dimension(
                    missing_left,
                    dim_col,
                    value_columns,
                    "missing_left",
                )?;
                contributions.extend(dim_contributions);
            }
        }
        
        // Aggregate missing_right contributions (subtract from total)
        for dim_col in dimension_columns {
            if missing_right.height() > 0 {
                let dim_contributions = self.aggregate_dimension(
                    missing_right,
                    dim_col,
                    value_columns,
                    "missing_right",
                )?;
                contributions.extend(dim_contributions);
            }
        }
        
        // Aggregate value_mismatch contributions
        for dim_col in dimension_columns {
            if value_mismatch.height() > 0 {
                let dim_contributions = self.aggregate_value_mismatch(
                    value_mismatch,
                    dim_col,
                    value_columns,
                )?;
                contributions.extend(dim_contributions);
            }
        }
        
        // Calculate total mismatch
        total_mismatch = contributions.iter()
            .map(|c| c.contribution.abs())
            .sum();
        
        // Calculate percentages
        if total_mismatch > 0.0 {
            for contrib in &mut contributions {
                contrib.percentage = (contrib.contribution.abs() / total_mismatch) * 100.0;
            }
        }
        
        // Sort by contribution (descending)
        contributions.sort_by(|a, b| b.contribution.abs().partial_cmp(&a.contribution.abs()).unwrap_or(std::cmp::Ordering::Equal));
        
        // Get top 10 contributors
        let top_contributors = contributions.iter().take(10).cloned().collect();
        
        Ok(DimensionAggregationResult {
            contributions,
            top_contributors,
            total_mismatch,
        })
    }
    
    /// Aggregate contribution for a single dimension column
    fn aggregate_dimension(
        &self,
        df: &DataFrame,
        dimension_col: &str,
        value_columns: &[String],
        source: &str,
    ) -> Result<Vec<DimensionContribution>> {
        if df.height() == 0 {
            return Ok(Vec::new());
        }
        
        // Check if dimension column exists
        if df.column(dimension_col).is_err() {
            return Ok(Vec::new());
        }
        
        // Group by dimension and sum value columns
        let mut group_exprs = vec![col(dimension_col)];
        let mut agg_exprs = Vec::new();
        
        for val_col in value_columns {
            if df.column(val_col).is_ok() {
                agg_exprs.push(col(val_col).sum().alias(&format!("sum_{}", val_col)));
            }
        }
        
        if agg_exprs.is_empty() {
            return Ok(Vec::new());
        }
        
        let grouped = df
            .clone()
            .lazy()
            .group_by(group_exprs)
            .agg(agg_exprs)
            .collect()?;
        
        // Extract contributions
        let mut contributions = Vec::new();
        
        for row_idx in 0..grouped.height() {
            let dim_value = grouped.column(dimension_col)?
                .str()?
                .get(row_idx)
                .unwrap_or("")
                .to_string();
            
            let mut total_contribution = 0.0;
            let mut row_count = 0;
            
            for val_col in value_columns {
                let sum_col = format!("sum_{}", val_col);
                if let Ok(sum_series) = grouped.column(&sum_col) {
                    if let Ok(sum_f64) = sum_series.f64() {
                        if let Some(val) = sum_f64.get(row_idx) {
                            total_contribution += val;
                            row_count += 1;
                        }
                    }
                }
            }
            
            // For missing_right, contribution is negative
            let contribution = if source == "missing_right" {
                -total_contribution
            } else {
                total_contribution
            };
            
            contributions.push(DimensionContribution {
                dimension: dimension_col.to_string(),
                value: dim_value,
                contribution,
                percentage: 0.0, // Will be calculated later
                row_count,
            });
        }
        
        Ok(contributions)
    }
    
    /// Aggregate value mismatch contributions
    fn aggregate_value_mismatch(
        &self,
        df: &DataFrame,
        dimension_col: &str,
        value_columns: &[String],
    ) -> Result<Vec<DimensionContribution>> {
        if df.height() == 0 {
            return Ok(Vec::new());
        }
        
        // Check if dimension column exists
        if df.column(dimension_col).is_err() {
            return Ok(Vec::new());
        }
        
        // For value mismatch, we need to calculate the difference
        // Assuming df has columns like "left_value" and "right_value"
        let mut group_exprs = vec![col(dimension_col)];
        let mut agg_exprs = Vec::new();
        
        for val_col in value_columns {
            let left_col = format!("left_{}", val_col);
            let right_col = format!("right_{}", val_col);
            
            if df.column(&left_col).is_ok() && df.column(&right_col).is_ok() {
                let diff_expr = col(&left_col) - col(&right_col);
                agg_exprs.push(diff_expr.sum().alias(&format!("diff_{}", val_col)));
            }
        }
        
        if agg_exprs.is_empty() {
            return Ok(Vec::new());
        }
        
        let grouped = df
            .clone()
            .lazy()
            .group_by(group_exprs)
            .agg(agg_exprs)
            .collect()?;
        
        // Extract contributions
        let mut contributions = Vec::new();
        
        for row_idx in 0..grouped.height() {
            let dim_value = grouped.column(dimension_col)?
                .str()?
                .get(row_idx)
                .unwrap_or("")
                .to_string();
            
            let mut total_contribution = 0.0;
            let mut row_count = 0;
            
            for val_col in value_columns {
                let diff_col = format!("diff_{}", val_col);
                if let Ok(diff_series) = grouped.column(&diff_col) {
                    if let Ok(diff_f64) = diff_series.f64() {
                        if let Some(val) = diff_f64.get(row_idx) {
                            total_contribution += val;
                            row_count += 1;
                        }
                    }
                }
            }
            
            contributions.push(DimensionContribution {
                dimension: dimension_col.to_string(),
                value: dim_value,
                contribution: total_contribution,
                percentage: 0.0, // Will be calculated later
                row_count,
            });
        }
        
        Ok(contributions)
    }
}





