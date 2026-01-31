use crate::data_engineering::{
    inspect_columns, validate_schema_compatibility, validate_join_keys,
    detect_anomalies, clean_dataframe, cast_dataframe_types,
    InspectionResult, SchemaValidationResult, JoinValidationResult, AnomalyDetectionResult,
};
use crate::error::Result;
use polars::prelude::*;
use std::collections::HashMap;

/// Data Engineering Tool Executor
/// Executes DE tools and logs results
pub struct DeExecutor;

impl DeExecutor {
    /// Execute inspection tool and log results
    pub fn execute_inspect(
        df: &DataFrame,
        table_name: &str,
        columns: &[String],
        top_n: usize,
    ) -> Result<InspectionResult> {
        println!("\n   🔍 INSPECTING COLUMNS");
        println!("      Table: {}", table_name);
        println!("      Columns: {:?}", columns);
        
        let result = inspect_columns(df, columns, top_n)?;
        
        // Log results
        for (col_name, inspection) in &result.columns {
            println!("\n      Column: {}", col_name);
            println!("         Data Type: {}", inspection.data_type);
            println!("         Total Rows: {}", inspection.total_count);
            println!("         Null Count: {} ({:.2}%)", 
                inspection.null_count, inspection.null_percentage);
            
            if !inspection.top_values.is_empty() {
                println!("         Top Values:");
                for (idx, top_val) in inspection.top_values.iter().take(5).enumerate() {
                    println!("            {}. {} (count: {})", idx + 1, top_val.value, top_val.count);
                }
            }
            
            if !inspection.sample_values.is_empty() {
                println!("         Sample Values: {:?}", 
                    inspection.sample_values.iter().take(3).collect::<Vec<_>>());
            }
        }
        
        Ok(result)
    }
    
    /// Execute schema validation and log results
    pub fn execute_validate_schema(
        df_a: &DataFrame,
        df_b: &DataFrame,
        table_a: &str,
        table_b: &str,
        join_columns: &HashMap<String, String>,
    ) -> Result<SchemaValidationResult> {
        println!("\n   ✅ VALIDATING SCHEMA");
        println!("      Left Table: {}", table_a);
        println!("      Right Table: {}", table_b);
        println!("      Join Columns: {:?}", join_columns);
        
        let result = validate_schema_compatibility(df_a, df_b, join_columns)?;
        
        // Log results
        if result.compatible {
            println!("      ✅ Schema is compatible");
        } else {
            println!("      ❌ Schema compatibility issues found");
        }
        
        for issue in &result.issues {
            let icon = match issue.severity.as_str() {
                "error" => "❌",
                "warning" => "⚠️",
                _ => "ℹ️",
            };
            println!("      {} {}: {}", icon, issue.severity.to_uppercase(), issue.message);
        }
        
        Ok(result)
    }
    
    /// Execute join key validation and log results
    pub fn execute_validate_join_keys(
        df_a: &DataFrame,
        df_b: &DataFrame,
        table_a: &str,
        table_b: &str,
        join_keys: &HashMap<String, String>,
        join_type: &str,
    ) -> Result<JoinValidationResult> {
        println!("\n   ✅ VALIDATING JOIN KEYS");
        println!("      Left Table: {}", table_a);
        println!("      Right Table: {}", table_b);
        println!("      Join Type: {}", join_type);
        println!("      Join Keys: {:?}", join_keys);
        
        let result = validate_join_keys(df_a, df_b, join_keys, join_type)?;
        
        // Log results
        if result.can_join {
            println!("      ✅ Join keys are valid - join can proceed");
        } else {
            println!("      ❌ Join keys have issues - join may fail");
        }
        
        for issue in &result.issues {
            let icon = match issue.severity.as_str() {
                "error" => "❌",
                "warning" => "⚠️",
                "info" => "ℹ️",
                _ => "•",
            };
            println!("      {} {}: {}", icon, issue.severity.to_uppercase(), issue.message);
        }
        
        Ok(result)
    }
    
    /// Execute anomaly detection and log results
    pub fn execute_detect_anomalies(
        df: &DataFrame,
        table_name: &str,
        columns: Option<&[String]>,
        checks: &[String],
    ) -> Result<AnomalyDetectionResult> {
        println!("\n   🔍 DETECTING ANOMALIES");
        println!("      Table: {}", table_name);
        if let Some(cols) = columns {
            println!("      Columns: {:?}", cols);
        } else {
            println!("      Columns: ALL");
        }
        println!("      Checks: {:?}", checks);
        
        let result = detect_anomalies(df, columns, checks)?;
        
        // Log results
        if result.anomalies.is_empty() {
            println!("      ✅ No anomalies detected");
        } else {
            println!("      ⚠️  Found {} anomaly/ies:", result.anomalies.len());
            for anomaly in &result.anomalies {
                let icon = match anomaly.severity.as_str() {
                    "error" => "❌",
                    "warning" => "⚠️",
                    _ => "ℹ️",
                };
                println!("      {} [{}] {}: {}", 
                    icon, anomaly.check_type, anomaly.column, anomaly.message);
            }
        }
        
        Ok(result)
    }
    
    /// Execute data cleaning (placeholder - full implementation pending)
    pub fn execute_clean_data(
        df: DataFrame,
        table_name: &str,
        columns: &[String],
        operations: &[String],
    ) -> Result<DataFrame> {
        println!("\n   🧹 CLEANING DATA");
        println!("      Table: {}", table_name);
        println!("      Columns: {:?}", columns);
        println!("      Operations: {:?}", operations);
        println!("      ⚠️  Note: Full cleaning implementation requires Polars string operation support");
        
        let result = clean_dataframe(df, columns, operations)?;
        println!("      ✅ Data cleaning completed (simplified implementation)");
        
        Ok(result)
    }
    
    /// Execute type casting and log results
    pub fn execute_cast_types(
        df: DataFrame,
        table_name: &str,
        type_mapping: &HashMap<String, String>,
    ) -> Result<DataFrame> {
        println!("\n   🔄 CASTING TYPES");
        println!("      Table: {}", table_name);
        println!("      Type Mappings: {:?}", type_mapping);
        
        let result = cast_dataframe_types(df, type_mapping)?;
        println!("      ✅ Type casting completed");
        
        Ok(result)
    }
}

