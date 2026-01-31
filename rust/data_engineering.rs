use crate::error::{RcaError, Result};
use polars::prelude::*;
use std::collections::HashMap;

/// Data Engineering module for data validation, cleaning, and type operations

/// Inspect columns to check top values, data types, nulls, etc.
pub fn inspect_columns(
    df: &DataFrame,
    columns: &[String],
    top_n: usize,
) -> Result<InspectionResult> {
    let mut results = HashMap::new();
    
    for col_name in columns {
        if !df.column(col_name).is_ok() {
            return Err(RcaError::Execution(format!("Column '{}' not found", col_name)));
        }
        
        let col = df.column(col_name)?;
        let dtype = col.dtype();
        
        // Get top N values
        let top_values = get_top_values(df, col_name, top_n)?;
        
        // Count nulls
        let null_count = col.null_count();
        let total_count = df.height();
        let null_percentage = if total_count > 0 {
            (null_count as f64 / total_count as f64) * 100.0
        } else {
            0.0
        };
        
        // Get sample values
        let sample_values: Vec<String> = df
            .column(col_name)?
            .head(Some(5))
            .iter()
            .map(|v| format!("{:?}", v))
            .collect();
        
        results.insert(col_name.clone(), ColumnInspection {
            data_type: format!("{:?}", dtype),
            null_count,
            null_percentage,
            total_count,
            top_values,
            sample_values,
        });
    }
    
    Ok(InspectionResult { columns: results })
}

fn get_top_values(df: &DataFrame, col_name: &str, top_n: usize) -> Result<Vec<TopValue>> {
    let col_series = df.column(col_name)?;
    
    match col_series.dtype() {
        DataType::String => {
            // For string columns, get value counts
            // Collect first, then sort in Rust (simpler than Polars sort API)
            let value_counts = df
                .clone()
                .lazy()
                .group_by([col(col_name)])
                .agg([len().alias("count")])
                .collect()?;
            
            // Convert to Vec and sort by count descending
            let mut value_vec: Vec<(String, u32)> = Vec::new();
            let height = value_counts.height();
            for i in 0..height {
                let val_col = value_counts.column(col_name)?;
                let count_col = value_counts.column("count")?;
                if let (Ok(str_chunked), Ok(u32_chunked)) = (val_col.str(), count_col.u32()) {
                    if let (Some(val), Some(cnt)) = (str_chunked.get(i), u32_chunked.get(i)) {
                        value_vec.push((val.to_string(), cnt));
                    }
                }
            }
            
            // Sort by count descending and take top N
            value_vec.sort_by(|a, b| b.1.cmp(&a.1));
            let top_values: Vec<TopValue> = value_vec
                .into_iter()
                .take(top_n)
                .map(|(val, cnt)| TopValue {
                    value: val,
                    count: cnt as usize,
                })
                .collect();
            
            Ok(top_values)
        }
        DataType::Int64 | DataType::Float64 => {
            // For numeric, compute basic stats using LazyFrame
            let stats_df = df
                .clone()
                .lazy()
                .select([
                    col(col_name).min().alias("min"),
                    col(col_name).max().alias("max"),
                    col(col_name).mean().alias("mean"),
                ])
                .collect()?;
            
            let min_val = stats_df.column("min")?.get(0).ok();
            let max_val = stats_df.column("max")?.get(0).ok();
            let mean_val = stats_df.column("mean")?.get(0).ok();
            
            Ok(vec![TopValue {
                value: format!("Min: {:?}, Max: {:?}, Mean: {:?}", min_val, max_val, mean_val),
                count: 0,
            }])
        }
        _ => {
            // For other types, just get sample values
            let sample_size = df.height().min(top_n);
            let mut sample_values = Vec::new();
            for i in 0..sample_size {
                if let Ok(val) = col_series.get(i) {
                    sample_values.push(format!("{:?}", val));
                }
            }
            Ok(vec![TopValue {
                value: format!("Sample values: {:?}", sample_values),
                count: 0,
            }])
        }
    }
}

/// Validate schema compatibility between two dataframes
pub fn validate_schema_compatibility(
    df_a: &DataFrame,
    df_b: &DataFrame,
    join_columns: &HashMap<String, String>,
) -> Result<SchemaValidationResult> {
    let mut issues = Vec::new();
    let mut compatible = true;
    
    for (left_col, right_col) in join_columns {
        // Check if columns exist
        let left_exists = df_a.column(left_col).is_ok();
        let right_exists = df_b.column(right_col).is_ok();
        
        if !left_exists {
            issues.push(SchemaIssue {
                severity: "error".to_string(),
                message: format!("Column '{}' not found in left table", left_col),
                column: left_col.clone(),
            });
            compatible = false;
            continue;
        }
        
        if !right_exists {
            issues.push(SchemaIssue {
                severity: "error".to_string(),
                message: format!("Column '{}' not found in right table", right_col),
                column: right_col.clone(),
            });
            compatible = false;
            continue;
        }
        
        // Check type compatibility
        let left_dtype = df_a.column(left_col)?.dtype();
        let right_dtype = df_b.column(right_col)?.dtype();
        
        if !are_types_compatible(left_dtype, right_dtype) {
            issues.push(SchemaIssue {
                severity: "warning".to_string(),
                message: format!(
                    "Type mismatch: '{}' ({:?}) vs '{}' ({:?})",
                    left_col, left_dtype, right_col, right_dtype
                ),
                column: format!("{} -> {}", left_col, right_col),
            });
            // Not necessarily incompatible - can cast
        }
        
        // Check for nulls in join keys
        let left_nulls = df_a.column(left_col)?.null_count();
        let right_nulls = df_b.column(right_col)?.null_count();
        
        if left_nulls > 0 {
            issues.push(SchemaIssue {
                severity: "warning".to_string(),
                message: format!("{} null values found in join key '{}' (left)", left_nulls, left_col),
                column: left_col.clone(),
            });
        }
        
        if right_nulls > 0 {
            issues.push(SchemaIssue {
                severity: "warning".to_string(),
                message: format!("{} null values found in join key '{}' (right)", right_nulls, right_col),
                column: right_col.clone(),
            });
        }
    }
    
    Ok(SchemaValidationResult {
        compatible,
        issues,
    })
}

fn are_types_compatible(dtype1: &DataType, dtype2: &DataType) -> bool {
    match (dtype1, dtype2) {
        (DataType::String, DataType::String) => true,
        (DataType::Int64, DataType::Int64) => true,
        (DataType::Int64, DataType::Float64) => true, // Can cast int to float
        (DataType::Float64, DataType::Float64) => true,
        (DataType::Float64, DataType::Int64) => true, // Can cast int to float
        (DataType::Date, DataType::Date) => true,
        (DataType::Datetime(_, _), DataType::Datetime(_, _)) => true,
        _ => false,
    }
}

/// Clean data by removing commas, trimming whitespace, etc.
/// Note: For Polars 0.40, string operations are limited. This is a simplified version.
/// In production, consider using map operations or upgrading to a newer Polars version.
pub fn clean_dataframe(
    df: DataFrame,
    columns: &[String],
    operations: &[String],
) -> Result<DataFrame> {
    // For now, return the dataframe as-is and log the cleaning request
    // Full implementation would require more complex Polars operations or version upgrade
    println!("   📝 Data cleaning requested for columns: {:?} with operations: {:?}", columns, operations);
    println!("   ⚠️  Note: Full cleaning implementation requires Polars string operation support");
    
    // Return original dataframe - actual cleaning would be implemented with proper Polars API
    Ok(df)
}

/// Cast columns to specified types
pub fn cast_dataframe_types(
    df: DataFrame,
    type_mapping: &HashMap<String, String>,
) -> Result<DataFrame> {
    let df_clone = df.clone();
    let mut result = df.lazy();
    
    for (col_name, target_type_str) in type_mapping {
        if df_clone.column(col_name).is_err() {
            continue; // Skip if column doesn't exist
        }
        
        let target_type = parse_data_type(target_type_str)?;
        result = result.with_columns([col(col_name).cast(target_type).alias(col_name)]);
    }
    
    Ok(result.collect()?)
}

fn parse_data_type(type_str: &str) -> Result<DataType> {
    match type_str.to_lowercase().as_str() {
        "string" | "str" => Ok(DataType::String),
        "int64" | "int" | "integer" => Ok(DataType::Int64),
        "float64" | "float" | "double" => Ok(DataType::Float64),
        "date" => Ok(DataType::Date),
        "bool" | "boolean" => Ok(DataType::Boolean),
        _ => Err(RcaError::Execution(format!("Unknown data type: {}", type_str))),
    }
}

/// Validate join keys before joining
pub fn validate_join_keys(
    df_a: &DataFrame,
    df_b: &DataFrame,
    join_keys: &HashMap<String, String>,
    join_type: &str,
) -> Result<JoinValidationResult> {
    let mut issues = Vec::new();
    let mut can_join = true;
    
    for (left_col, right_col) in join_keys {
        // Check column existence
        if df_a.column(left_col).is_err() {
            issues.push(JoinIssue {
                severity: "error".to_string(),
                message: format!("Join key '{}' not found in left table", left_col),
            });
            can_join = false;
            continue;
        }
        
        if df_b.column(right_col).is_err() {
            issues.push(JoinIssue {
                severity: "error".to_string(),
                message: format!("Join key '{}' not found in right table", right_col),
            });
            can_join = false;
            continue;
        }
        
        // Check type compatibility
        let left_dtype = df_a.column(left_col)?.dtype();
        let right_dtype = df_b.column(right_col)?.dtype();
        
        if !are_types_compatible(left_dtype, right_dtype) {
            issues.push(JoinIssue {
                severity: "warning".to_string(),
                message: format!(
                    "Type mismatch in join keys: '{}' ({:?}) vs '{}' ({:?})",
                    left_col, left_dtype, right_col, right_dtype
                ),
            });
        }
        
        // Check for nulls
        let left_nulls = df_a.column(left_col)?.null_count();
        let right_nulls = df_b.column(right_col)?.null_count();
        
        if left_nulls > 0 {
            issues.push(JoinIssue {
                severity: "warning".to_string(),
                message: format!("{} null values in join key '{}' (left)", left_nulls, left_col),
            });
        }
        
        if right_nulls > 0 {
            issues.push(JoinIssue {
                severity: "warning".to_string(),
                message: format!("{} null values in join key '{}' (right)", right_nulls, right_col),
            });
        }
        
        // Check value overlap (for inner joins)
        if join_type == "inner" {
            let left_unique: Vec<String> = extract_unique_values(df_a, left_col)?;
            let right_unique: Vec<String> = extract_unique_values(df_b, right_col)?;
            
            let overlap_count = left_unique.iter()
                .filter(|v| right_unique.contains(v))
                .count();
            
            if overlap_count == 0 {
                issues.push(JoinIssue {
                    severity: "error".to_string(),
                    message: format!(
                        "No overlapping values in join keys '{}' and '{}' (inner join will return 0 rows)",
                        left_col, right_col
                    ),
                });
                can_join = false;
            } else {
                issues.push(JoinIssue {
                    severity: "info".to_string(),
                    message: format!(
                        "Join key overlap: {} common values between '{}' and '{}'",
                        overlap_count, left_col, right_col
                    ),
                });
            }
        }
    }
    
    Ok(JoinValidationResult {
        can_join,
        issues,
    })
}

fn extract_unique_values(df: &DataFrame, col_name: &str) -> Result<Vec<String>> {
    let col = df.column(col_name)?;
    let unique = col.unique()?;
    
    let mut values = Vec::new();
    for i in 0..unique.len() {
        let val_str = match col.dtype() {
            DataType::String => unique.str()?.get(i).map(|s| s.to_string()),
            DataType::Int64 => unique.i64()?.get(i).map(|v| v.to_string()),
            DataType::Float64 => unique.f64()?.get(i).map(|v| v.to_string()),
            _ => Some(format!("{:?}", unique.get(i))),
        };
        if let Some(v) = val_str {
            values.push(v);
        }
    }
    
    Ok(values)
}

/// Detect anomalies in data
pub fn detect_anomalies(
    df: &DataFrame,
    columns: Option<&[String]>,
    checks: &[String],
) -> Result<AnomalyDetectionResult> {
    let cols_to_check = if let Some(cols) = columns {
        cols.to_vec()
    } else {
        df.get_column_names().iter().map(|s| s.to_string()).collect()
    };
    
    let mut anomalies = Vec::new();
    
    for col_name in &cols_to_check {
        if df.column(col_name).is_err() {
            continue;
        }
        
        for check in checks {
            match check.as_str() {
                "nulls" => {
                    let null_count = df.column(col_name)?.null_count();
                    if null_count > 0 {
                        anomalies.push(Anomaly {
                            column: col_name.clone(),
                            check_type: "nulls".to_string(),
                            message: format!("{} null values found", null_count),
                            severity: if null_count > df.height() / 2 {
                                "error".to_string()
                            } else {
                                "warning".to_string()
                            },
                        });
                    }
                }
                "duplicates" => {
                    let duplicates = df
                        .clone()
                        .lazy()
                        .group_by([col(col_name)])
                        .agg([len().alias("count")])
                        .filter(col("count").gt(lit(1)))
                        .collect()?;
                    
                    if duplicates.height() > 0 {
                        anomalies.push(Anomaly {
                            column: col_name.clone(),
                            check_type: "duplicates".to_string(),
                            message: format!("{} duplicate values found", duplicates.height()),
                            severity: "warning".to_string(),
                        });
                    }
                }
                "formats" => {
                    // Check for common format issues (e.g., commas in numbers)
                    if matches!(df.column(col_name)?.dtype(), DataType::String) {
                        let sample = df.column(col_name)?.str()?.head(Some(100));
                        let has_commas = sample.iter().any(|opt| {
                            opt.map(|s| s.contains(',')).unwrap_or(false)
                        });
                        
                        if has_commas {
                            anomalies.push(Anomaly {
                                column: col_name.clone(),
                                check_type: "formats".to_string(),
                                message: "Commas found in string column (may need cleaning)".to_string(),
                                severity: "warning".to_string(),
                            });
                        }
                    }
                }
                _ => {
                    // Unknown check type
                }
            }
        }
    }
    
    Ok(AnomalyDetectionResult { anomalies })
}

// Result structures
#[derive(Debug, Clone)]
pub struct InspectionResult {
    pub columns: HashMap<String, ColumnInspection>,
}

#[derive(Debug, Clone)]
pub struct ColumnInspection {
    pub data_type: String,
    pub null_count: usize,
    pub null_percentage: f64,
    pub total_count: usize,
    pub top_values: Vec<TopValue>,
    pub sample_values: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TopValue {
    pub value: String,
    pub count: usize,
}

#[derive(Debug, Clone)]
pub struct SchemaValidationResult {
    pub compatible: bool,
    pub issues: Vec<SchemaIssue>,
}

#[derive(Debug, Clone)]
pub struct SchemaIssue {
    pub severity: String,
    pub message: String,
    pub column: String,
}

#[derive(Debug, Clone)]
pub struct JoinValidationResult {
    pub can_join: bool,
    pub issues: Vec<JoinIssue>,
}

#[derive(Debug, Clone)]
pub struct JoinIssue {
    pub severity: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct AnomalyDetectionResult {
    pub anomalies: Vec<Anomaly>,
}

#[derive(Debug, Clone)]
pub struct Anomaly {
    pub column: String,
    pub check_type: String,
    pub message: String,
    pub severity: String,
}

