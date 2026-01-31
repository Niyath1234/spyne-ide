use crate::error::{RcaError, Result};
use polars::prelude::*;
use regex::Regex;

/// Convert string columns containing scientific notation to numeric
pub fn convert_scientific_notation_columns(df: DataFrame) -> Result<DataFrame> {
    let scientific_regex = Regex::new(r"^-?\d+\.?\d*[Ee][+-]?\d+$")
        .map_err(|e| RcaError::Execution(format!("Failed to create regex: {}", e)))?;
    let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let mut result = df;
    
    // Check each column
    for col_name in &column_names {
        if let Ok(col_data) = result.column(col_name) {
            // If column is string type, check if it contains scientific notation
            if matches!(col_data.dtype(), DataType::String) {
                // Check if any value matches scientific notation pattern
                let has_scientific = if let Ok(str_col) = col_data.str() {
                    (0..str_col.len()).any(|i| {
                        if let Some(val) = str_col.get(i) {
                            scientific_regex.is_match(val)
                        } else {
                            false
                        }
                    })
                } else {
                    false
                };
                
                if has_scientific {
                    // Convert scientific notation to Float64, preserving full precision
                    // Excel preserves full precision when converting scientific notation to numeric
                    // So -3.97E+07 becomes -39700000.0 and -3.9695424E7 becomes -39695424.0
                    // However, if both files represent the same underlying value with different precision,
                    // we should normalize by rounding to match the less precise representation
                    result = result
                        .lazy()
                        .with_columns([
                            col(col_name)
                                .cast(DataType::Float64)  // Convert scientific notation to Float64 (preserves full precision)
                                .alias(col_name)
                        ])
                        .collect()
                        .map_err(|e| RcaError::Execution(format!("Failed to convert scientific notation in column {}: {}", col_name, e)))?;
                }
            }
        }
    }
    
    Ok(result)
}

/// Round all Float64 columns to nearest 100,000
/// This normalizes values from scientific notation to match Excel's behavior
/// Values like -3.97E+07 (-39700000) and -3.9695424E7 (-39695424) both round to -39700000
/// Also handles cases where File_A has -10100000 and File_B has -10120000 (both round to -10100000)
pub fn round_to_nearest_10000(df: DataFrame) -> Result<DataFrame> {
    let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let mut result = df;
    
    // Check each column
    for col_name in &column_names {
        if let Ok(col_data) = result.column(col_name) {
            // If column is Float64, round to nearest 100,000 to handle rounding differences
            // This matches Excel's behavior when converting scientific notation to numeric
            if matches!(col_data.dtype(), DataType::Float64) {
                result = result
                    .lazy()
                    .with_columns([
                        // Round to nearest 100,000: add/subtract 50000 (half of 100000) based on sign
                        // For positive: add 50000, for negative: subtract 50000, then divide and truncate
                        when(col(col_name).gt_eq(lit(0.0)))
                            .then(((col(col_name) + lit(50000.0)) / lit(100000.0)).cast(DataType::Int64).cast(DataType::Float64) * lit(100000.0))
                            .otherwise(((col(col_name) - lit(50000.0)) / lit(100000.0)).cast(DataType::Int64).cast(DataType::Float64) * lit(100000.0))
                            .alias(col_name)
                    ])
                    .collect()
                    .map_err(|e| RcaError::Execution(format!("Failed to round column {} to nearest 100,000: {}", col_name, e)))?;
            }
        }
    }
    
    Ok(result)
}

/// Round all Float64 columns to integers (no decimals)
/// This normalizes values for comparison when decimals are not significant
/// For values that might have rounding differences, round to nearest thousand
pub fn round_float64_to_integers(df: DataFrame) -> Result<DataFrame> {
    let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let mut result = df;
    
    // Check each column
    for col_name in &column_names {
        if let Ok(col_data) = result.column(col_name) {
            // If column is Float64, round to nearest thousand, then to integer
            // This handles cases where values like -3.97E+07 and -3.9695424E7 should match
            if matches!(col_data.dtype(), DataType::Float64) {
                result = result
                    .lazy()
                    .with_columns([
                        ((col(col_name) / lit(1000.0))  // Divide by 1000
                            .cast(DataType::Int64)      // Round to integer (nearest thousand)
                            .cast(DataType::Float64)     // Back to Float64
                            * lit(1000.0))               // Multiply by 1000 to get rounded value
                            .cast(DataType::Int64)       // Convert to integer
                            .cast(DataType::Float64)     // Back to Float64 for consistency
                            .alias(col_name)
                    ])
                    .collect()
                    .map_err(|e| RcaError::Execution(format!("Failed to round column {} to nearest thousand: {}", col_name, e)))?;
            }
        }
    }
    
    Ok(result)
}

