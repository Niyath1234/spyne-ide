//! Query Result - Standardized result format from execution engines

use crate::error::Result;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Standardized query result from any execution engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Success status
    pub success: bool,
    
    /// Result data (as DataFrame for internal use, JSON for API)
    #[serde(skip)]
    pub data: Option<DataFrame>,
    
    /// Result data as JSON (for serialization)
    pub data_json: Option<serde_json::Value>,
    
    /// Number of rows returned
    pub row_count: usize,
    
    /// Column names
    pub columns: Vec<String>,
    
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    
    /// Engine that executed the query
    pub engine_name: String,
    
    /// Engine-specific metadata
    pub engine_metadata: HashMap<String, serde_json::Value>,
    
    /// Warnings (non-fatal issues)
    pub warnings: Vec<String>,
    
    /// Errors (if any)
    pub errors: Vec<String>,
}

impl QueryResult {
    /// Create a successful result
    pub fn success(
        data: DataFrame,
        engine_name: String,
        execution_time_ms: u64,
    ) -> Result<Self> {
        let columns: Vec<String> = data.get_column_names().iter().map(|s| s.to_string()).collect();
        let row_count = data.height();
        
        // Convert to JSON (sample for large results)
        let data_json = if row_count <= 1000 {
            Some(dataframe_to_json(&data)?)
        } else {
            // Only include sample
            let sample = data.head(Some(100));
            Some(dataframe_to_json(&sample)?)
        };
        
        Ok(Self {
            success: true,
            data: Some(data),
            data_json,
            row_count,
            columns,
            execution_time_ms,
            engine_name,
            engine_metadata: HashMap::new(),
            warnings: Vec::new(),
            errors: Vec::new(),
        })
    }
    
    /// Create an error result
    pub fn error(
        engine_name: String,
        error: String,
        execution_time_ms: u64,
    ) -> Self {
        Self {
            success: false,
            data: None,
            data_json: None,
            row_count: 0,
            columns: Vec::new(),
            execution_time_ms,
            engine_name,
            engine_metadata: HashMap::new(),
            warnings: Vec::new(),
            errors: vec![error],
        }
    }
}

/// Convert DataFrame to JSON value
fn dataframe_to_json(df: &DataFrame) -> Result<serde_json::Value> {
    let mut rows = Vec::new();
    let columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    
    for row_idx in 0..df.height() {
        let mut row = serde_json::Map::new();
        for col_name in &columns {
            if let Ok(series) = df.column(col_name) {
                let value = series_to_json_value(series, row_idx)?;
                row.insert(col_name.clone(), value);
            }
        }
        rows.push(serde_json::Value::Object(row));
    }
    
    Ok(serde_json::json!({
        "rows": rows,
        "columns": columns
    }))
}

fn series_to_json_value(series: &Series, row_idx: usize) -> Result<serde_json::Value> {
    use polars::prelude::AnyValue;
    
    // Check if null - is_null() returns a ChunkedArray<BooleanType>
    let null_mask = series.is_null();
    // Get the boolean value at the row index
    if let Some(is_null) = null_mask.get(row_idx) {
        if is_null {
            return Ok(serde_json::Value::Null);
        }
    }
    
    let any_val = series.get(row_idx)
        .map_err(|e| crate::error::RcaError::Execution(format!("Failed to get value: {}", e)))?;
    
    // Handle null case explicitly
    if any_val.is_null() {
        return Ok(serde_json::Value::Null);
    }
    
    match any_val {
        AnyValue::Null => Ok(serde_json::Value::Null),
        AnyValue::Boolean(b) => Ok(serde_json::Value::Bool(b)),
        AnyValue::String(s) => Ok(serde_json::Value::String(s.to_string())),
        AnyValue::Int8(i) => Ok(serde_json::Value::Number(i.into())),
        AnyValue::Int16(i) => Ok(serde_json::Value::Number(i.into())),
        AnyValue::Int32(i) => Ok(serde_json::Value::Number(i.into())),
        AnyValue::Int64(i) => Ok(serde_json::Value::Number(i.into())),
        AnyValue::UInt8(u) => Ok(serde_json::Value::Number(u.into())),
        AnyValue::UInt16(u) => Ok(serde_json::Value::Number(u.into())),
        AnyValue::UInt32(u) => Ok(serde_json::Value::Number(u.into())),
        AnyValue::UInt64(u) => Ok(serde_json::Value::Number(u.into())),
        AnyValue::Float32(f) => {
            Ok(serde_json::Number::from_f64(f as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        },
        AnyValue::Float64(f) => {
            Ok(serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        },
        _ => Ok(serde_json::Value::String(format!("{:?}", any_val))),
    }
}

