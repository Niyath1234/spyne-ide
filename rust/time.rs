use crate::error::{RcaError, Result};
use crate::metadata::{AsOfRule, Metadata, TimeRules};
use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;
use std::collections::HashMap;

pub struct TimeResolver {
    metadata: Metadata,
}

impl TimeResolver {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
    
    /// Get as-of rule for a table
    pub fn get_as_of_rule(&self, table_name: &str) -> Option<&AsOfRule> {
        self.metadata.time_rules.as_of_rules
            .iter()
            .find(|r| r.table == table_name)
    }
    
    /// Apply as-of filtering to a dataframe
    /// Returns dataframe unchanged if no rule exists (graceful degradation)
    pub fn apply_as_of(
        &self,
        df: DataFrame,
        table_name: &str,
        as_of_date: Option<NaiveDate>,
    ) -> Result<DataFrame> {
        let rule = match self.get_as_of_rule(table_name) {
            Some(r) => r,
            None => {
                // No rule found - return dataframe as-is (graceful degradation)
                return Ok(df);
            }
        };
        
        let time_col = &rule.as_of_column;
        
        // Check if time column exists
        if df.column(time_col).is_err() {
            return Ok(df); // No time column, return as-is
        }
        
        match as_of_date {
            Some(date) => {
                // Filter to rows where time_col <= as_of_date
                // Check column data type to handle Date vs String columns
                let time_col_type = df.column(time_col)?.dtype();
                
                let filter_expr = match time_col_type {
                    DataType::Date => {
                        // For Date columns, use date comparison directly
                        // Convert NaiveDate to days since epoch for comparison
                        let days_since_epoch = date.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
                        col(time_col).lt_eq(lit(days_since_epoch))
                    }
                    DataType::Datetime(_, _) => {
                        // For Datetime columns, convert to timestamp
                        let datetime = date.and_hms_opt(0, 0, 0).unwrap();
                        let timestamp_ns = datetime.and_utc().timestamp_nanos_opt().unwrap_or(0);
                        col(time_col).lt_eq(lit(timestamp_ns))
                    }
                    _ => {
                        // For String columns, use string comparison
                        let date_str = date.format("%Y-%m-%d").to_string();
                        col(time_col).cast(DataType::String).lt_eq(lit(date_str))
                    }
                };
                
                Ok(df.lazy().filter(filter_expr).collect()?)
            }
            None => {
                // Use default behavior
                match rule.default.as_str() {
                    "latest" => {
                        // Get max date and filter to that
                        // Simplified - would need proper date handling
                        // For now, just return the dataframe
                        Ok(df)
                    }
                    _ => Ok(df), // No filtering
                }
            }
        }
    }
    
    /// Detect temporal misalignment between two dataframes
    pub fn detect_temporal_misalignment(
        &self,
        _df_a: &DataFrame,
        _df_b: &DataFrame,
        table_a: &str,
        table_b: &str,
    ) -> Result<Option<TemporalMisalignment>> {
        let rule_a = self.get_as_of_rule(table_a);
        let rule_b = self.get_as_of_rule(table_b);
        
        if rule_a.is_none() || rule_b.is_none() {
            return Ok(None);
        }
        
        let _time_col_a = &rule_a.unwrap().as_of_column;
        let _time_col_b = &rule_b.unwrap().as_of_column;
        
        // Get date ranges (simplified - would need proper date column handling)
        // For now, return None as placeholder
        let min_a: Option<NaiveDate> = None;
        let max_a: Option<NaiveDate> = None;
        let min_b: Option<NaiveDate> = None;
        let max_b: Option<NaiveDate> = None;
        
        if min_a.is_none() || max_a.is_none() || min_b.is_none() || max_b.is_none() {
            return Ok(None);
        }
        
        let min_a = min_a.unwrap();
        let max_a = max_a.unwrap();
        let min_b = min_b.unwrap();
        let max_b = max_b.unwrap();
        
        // Check for misalignment
        if max_a != max_b || min_a != min_b {
            return Ok(Some(TemporalMisalignment {
                table_a: table_a.to_string(),
                table_b: table_b.to_string(),
                min_a: Some(min_a),
                max_a: Some(max_a),
                min_b: Some(min_b),
                max_b: Some(max_b),
            }));
        }
        
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct TemporalMisalignment {
    pub table_a: String,
    pub table_b: String,
    pub min_a: Option<NaiveDate>,
    pub max_a: Option<NaiveDate>,
    pub min_b: Option<NaiveDate>,
    pub max_b: Option<NaiveDate>,
}

