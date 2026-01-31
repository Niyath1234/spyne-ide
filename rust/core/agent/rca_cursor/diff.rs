//! Grain-Level Diff Engine
//! 
//! Computes grain-normalized differences between two systems by joining on grain_key,
//! computing delta and impact, and selecting top K grain units.

use crate::core::agent::rca_cursor::executor::ExecutionResult;
use crate::core::rca::result_v2::GrainDifference;
use crate::error::{RcaError, Result};
use polars::prelude::*;

/// Grain-level diff result
#[derive(Debug, Clone)]
pub struct GrainDiffResult {
    /// Target grain entity name
    pub grain: String,
    /// Grain key column name
    pub grain_key: String,
    /// Top differences at grain level
    pub differences: Vec<GrainDifference>,
    /// Top K selected
    pub top_k: usize,
    /// Total grain units in system A
    pub total_grain_units_a: usize,
    /// Total grain units in system B
    pub total_grain_units_b: usize,
    /// Missing grain units in system A (only in B)
    pub missing_left_count: usize,
    /// Missing grain units in system B (only in A)
    pub missing_right_count: usize,
    /// Mismatch count (same grain_key but different values)
    pub mismatch_count: usize,
}

/// Grain-level diff engine
pub struct GrainDiffEngine {
    /// Top K to select (default: 100)
    top_k: usize,
}

impl GrainDiffEngine {
    /// Create a new grain diff engine
    pub fn new(top_k: usize) -> Self {
        Self { top_k }
    }

    /// Compute grain-level differences between two execution results
    /// 
    /// Joins system A and B on grain_key, computes delta and impact,
    /// and selects top K grain units by impact.
    pub fn compute_diff(
        &self,
        result_a: &ExecutionResult,
        result_b: &ExecutionResult,
        metric_column: &str,
    ) -> Result<GrainDiffResult> {
        // Validate that both results have the same grain_key
        if result_a.grain_key != result_b.grain_key {
            return Err(RcaError::Execution(format!(
                "Grain keys don't match: '{}' vs '{}'",
                result_a.grain_key, result_b.grain_key
            )));
        }

        let grain_key = &result_a.grain_key;
        let df_a = &result_a.dataframe;
        let df_b = &result_b.dataframe;

        // Validate metric column exists in both dataframes
        if df_a.column(metric_column).is_err() {
            return Err(RcaError::Execution(format!(
                "Metric column '{}' not found in system A",
                metric_column
            )));
        }
        if df_b.column(metric_column).is_err() {
            return Err(RcaError::Execution(format!(
                "Metric column '{}' not found in system B",
                metric_column
            )));
        }

        // Rename metric columns to avoid conflicts during join
        let metric_a = format!("{}_a", metric_column);
        let metric_b = format!("{}_b", metric_column);

        let df_a_renamed = df_a
            .clone()
            .lazy()
            .with_columns([
                col(metric_column).alias(&metric_a),
            ])
            .collect()?;

        let df_b_renamed = df_b
            .clone()
            .lazy()
            .with_columns([
                col(metric_column).alias(&metric_b),
            ])
            .collect()?;

        // Perform outer join on grain_key to capture all grain units
        let joined_lazy = df_a_renamed
            .lazy()
            .join(
                df_b_renamed.lazy(),
                [col(grain_key)],
                [col(grain_key)],
                JoinArgs::new(JoinType::Outer),
            );

        // After outer join, Polars might create duplicate columns with suffixes
        // Check if we have duplicate grain_key columns and coalesce them
        let joined = joined_lazy.collect()?;
        
        // Check for duplicate grain_key columns (Polars might add suffixes)
        let grain_key_col_name = if joined.column(grain_key).is_ok() {
            grain_key.to_string()
        } else {
            // Try to find grain_key with suffix
            let possible_names: Vec<String> = joined.get_column_names()
                .iter()
                .filter(|name| name.starts_with(grain_key))
                .map(|s| s.to_string())
                .collect();
            if !possible_names.is_empty() {
                possible_names[0].clone()
            } else {
                return Err(RcaError::Execution(format!(
                    "Grain key column '{}' not found after join",
                    grain_key
                )));
            }
        };

        // Extract grain values and metric values
        let grain_key_series = joined.column(&grain_key_col_name)?;
        let metric_a_series = joined.column(&metric_a);
        let metric_b_series = joined.column(&metric_b);

        // Handle null values (missing grain units)
        let mut differences = Vec::new();
        let mut missing_left_count = 0;
        let mut missing_right_count = 0;
        let mut mismatch_count = 0;

        for i in 0..joined.height() {
            // Extract grain value (handle composite keys)
            let grain_value = self.extract_grain_value(&joined, &grain_key_col_name, i)?;

            // Extract metric values (handle nulls)
            let (value_a, is_missing_left) = match &metric_a_series {
                Ok(s) => {
                    match s.get(i) {
                        Ok(AnyValue::Null) => (0.0, true),
                        Ok(AnyValue::Float64(f)) => (f, false),
                        Ok(AnyValue::Int64(i)) => (i as f64, false),
                        Ok(AnyValue::UInt64(u)) => (u as f64, false),
                        Ok(AnyValue::Float32(f)) => (f as f64, false),
                        Ok(AnyValue::Int32(i)) => (i as f64, false),
                        Ok(AnyValue::UInt32(u)) => (u as f64, false),
                        Ok(_) => (0.0, true),
                        Err(_) => (0.0, true),
                    }
                }
                Err(_) => (0.0, true),
            };

            let (value_b, is_missing_right) = match &metric_b_series {
                Ok(s) => {
                    match s.get(i) {
                        Ok(AnyValue::Null) => (0.0, true),
                        Ok(AnyValue::Float64(f)) => (f, false),
                        Ok(AnyValue::Int64(i)) => (i as f64, false),
                        Ok(AnyValue::UInt64(u)) => (u as f64, false),
                        Ok(AnyValue::Float32(f)) => (f as f64, false),
                        Ok(AnyValue::Int32(i)) => (i as f64, false),
                        Ok(AnyValue::UInt32(u)) => (u as f64, false),
                        Ok(_) => (0.0, true),
                        Err(_) => (0.0, true),
                    }
                }
                Err(_) => (0.0, true),
            };

            if is_missing_left && !is_missing_right {
                missing_left_count += 1;
            } else if !is_missing_left && is_missing_right {
                missing_right_count += 1;
            } else if !is_missing_left && !is_missing_right {
                // Both present - check for mismatch
                if (value_a - value_b).abs() > 1e-10 {
                    mismatch_count += 1;
                }
            }

            // Compute delta and impact
            let delta = value_b - value_a;
            let impact = delta.abs();

            differences.push(GrainDifference {
                grain_value,
                value_a,
                value_b,
                delta,
                impact,
            });
        }

        // Sort by impact descending
        differences.sort_by(|a, b| b.impact.partial_cmp(&a.impact).unwrap_or(std::cmp::Ordering::Equal));

        // Select top K
        let top_k = self.top_k.min(differences.len());
        let top_differences = differences.into_iter().take(top_k).collect();

        // Get total counts
        let total_grain_units_a = df_a.height();
        let total_grain_units_b = df_b.height();

        Ok(GrainDiffResult {
            grain: grain_key.clone(), // Use grain_key as grain name for now
            grain_key: grain_key.clone(),
            differences: top_differences,
            top_k,
            total_grain_units_a,
            total_grain_units_b,
            missing_left_count,
            missing_right_count,
            mismatch_count,
        })
    }

    /// Extract grain value from a row
    /// 
    /// Handles both single and composite grain keys.
    /// For outer joins, handles null grain_keys by checking if there are duplicate columns.
    fn extract_grain_value(
        &self,
        df: &DataFrame,
        grain_key: &str,
        row_idx: usize,
    ) -> Result<Vec<String>> {
        // For now, handle single grain key
        // In the future, this could handle composite keys
        let grain_key_series = df.column(grain_key)?;
        
        let value = grain_key_series.get(row_idx)?;
        let value_str = match value {
            AnyValue::String(s) => s.to_string(),
            AnyValue::Int64(i) => i.to_string(),
            AnyValue::UInt64(u) => u.to_string(),
            AnyValue::Int32(i) => i.to_string(),
            AnyValue::UInt32(u) => u.to_string(),
            AnyValue::Float64(f) => f.to_string(),
            AnyValue::Float32(f) => f.to_string(),
            AnyValue::Null => {
                // In an outer join, if grain_key is null, try to find it from duplicate columns
                // Polars might create columns with suffixes like "loan_id" and "loan_id_right"
                let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
                for col_name in column_names {
                    if col_name != grain_key && col_name.starts_with(grain_key) {
                        if let Ok(alt_series) = df.column(&col_name) {
                            if let Ok(alt_value) = alt_series.get(row_idx) {
                                if !matches!(alt_value, AnyValue::Null) {
                                    // Found non-null value in alternate column
                                    return self.extract_grain_value_from_anyvalue(alt_value);
                                }
                            }
                        }
                    }
                }
                // If still null, this is an error
                return Err(RcaError::Execution(format!(
                    "Grain key '{}' is null at row {} and no alternate column found",
                    grain_key, row_idx
                )));
            }
            _ => value.to_string(),
        };

        Ok(vec![value_str])
    }

    /// Extract grain value string from AnyValue
    fn extract_grain_value_from_anyvalue(&self, value: AnyValue) -> Result<Vec<String>> {
        let value_str = match value {
            AnyValue::String(s) => s.to_string(),
            AnyValue::Int64(i) => i.to_string(),
            AnyValue::UInt64(u) => u.to_string(),
            AnyValue::Int32(i) => i.to_string(),
            AnyValue::UInt32(u) => u.to_string(),
            AnyValue::Float64(f) => f.to_string(),
            AnyValue::Float32(f) => f.to_string(),
            AnyValue::Null => {
                return Err(RcaError::Execution("Cannot extract grain value from null".to_string()));
            }
            _ => value.to_string(),
        };
        Ok(vec![value_str])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grain_diff_creation() {
        // Test would require mock execution results
    }
}

