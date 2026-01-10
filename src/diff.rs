use crate::error::{RcaError, Result};
use polars::prelude::*;
use std::collections::HashSet;

pub struct DiffEngine;

impl DiffEngine {
    /// Compare two dataframes and find differences
    pub fn compare(
        &self,
        df_a: DataFrame,
        df_b: DataFrame,
        grain: &[String],
        metric_col: &str,
        precision: u32,
    ) -> Result<ComparisonResult> {
        // Population diff
        let population_diff = self.population_diff(&df_a, &df_b, grain)?;
        
        // Data diff (for common keys)
        let data_diff = self.data_diff(&df_a, &df_b, grain, metric_col, precision)?;
        
        Ok(ComparisonResult {
            population_diff,
            data_diff,
        })
    }
    
    fn population_diff(
        &self,
        df_a: &DataFrame,
        df_b: &DataFrame,
        grain: &[String],
    ) -> Result<PopulationDiff> {
        // Get unique keys from both dataframes
        let keys_a: HashSet<Vec<String>> = self.extract_keys(df_a, grain)?;
        let keys_b: HashSet<Vec<String>> = self.extract_keys(df_b, grain)?;
        
        // Find missing and extra entities
        let missing_in_b: Vec<Vec<String>> = keys_a.difference(&keys_b).cloned().collect();
        let extra_in_b: Vec<Vec<String>> = keys_b.difference(&keys_a).cloned().collect();
        let common_keys: Vec<Vec<String>> = keys_a.intersection(&keys_b).cloned().collect();
        
        // Check for duplicates
        let duplicates_a = self.find_duplicates(df_a, grain)?;
        let duplicates_b = self.find_duplicates(df_b, grain)?;
        
        Ok(PopulationDiff {
            missing_in_b,
            extra_in_b,
            common_count: common_keys.len(),
            duplicates_a,
            duplicates_b,
        })
    }
    
    fn data_diff(
        &self,
        df_a: &DataFrame,
        df_b: &DataFrame,
        grain: &[String],
        metric_col: &str,
        precision: u32,
    ) -> Result<DataDiff> {
        // Join on grain columns
        let grain_cols: Vec<Expr> = grain.iter().map(|c| col(c)).collect();
        
        let df_a_lazy = df_a.clone().lazy();
        let df_b_lazy = df_b.clone().lazy();
        
        // Rename metric columns to avoid conflict
        let df_a_renamed = df_a_lazy
            .with_columns([col(metric_col).alias("metric_a")]);
        let df_b_renamed = df_b_lazy
            .with_columns([col(metric_col).alias("metric_b")]);
        
        // Join
        let joined = df_a_renamed
            .join(
                df_b_renamed,
                grain_cols.clone(),
                grain_cols.clone(),
                JoinType::Inner,
            )
            .with_columns([
                (col("metric_a") - col("metric_b")).alias("diff"),
                (col("metric_a") - col("metric_b")).abs().alias("abs_diff"),
            ])
            .collect()?;
        
        // Filter to mismatches (considering precision)
        let precision_factor = 10_f64.powi(precision as i32);
        let threshold = 1.0 / precision_factor;
        
        let mismatches_df = joined
            .clone()
            .lazy()
            .filter(col("abs_diff").gt(lit(threshold)))
            .collect()?;
        
        let matches_df = joined
            .clone()
            .lazy()
            .filter(col("abs_diff").le(lit(threshold)))
            .collect()?;
        
        let mismatches = mismatches_df.height();
        let matches = matches_df.height();
        
        Ok(DataDiff {
            mismatches,
            matches,
            mismatch_details: mismatches_df,
        })
    }
    
    fn extract_keys(&self, df: &DataFrame, grain: &[String]) -> Result<HashSet<Vec<String>>> {
        let mut keys = HashSet::new();
        
        for row_idx in 0..df.height() {
            let mut key = Vec::new();
            for col_name in grain {
                let col_val = df.column(col_name)?;
                let val_str = match col_val.dtype() {
                    DataType::Utf8 => col_val.str().unwrap().get(row_idx).unwrap().to_string(),
                    DataType::Int64 => col_val.i64().unwrap().get(row_idx).unwrap().to_string(),
                    DataType::Float64 => col_val.f64().unwrap().get(row_idx).unwrap().to_string(),
                    _ => format!("{:?}", col_val.get(row_idx)),
                };
                key.push(val_str);
            }
            keys.insert(key);
        }
        
        Ok(keys)
    }
    
    fn find_duplicates(&self, df: &DataFrame, grain: &[String]) -> Result<Vec<Vec<String>>> {
        let grain_cols: Vec<Expr> = grain.iter().map(|c| col(c)).collect();
        
        let duplicates = df
            .clone()
            .lazy()
            .group_by(grain_cols.clone())
            .agg([count().alias("count")])
            .filter(col("count").gt(lit(1)))
            .collect()?;
        
        let mut dup_keys = Vec::new();
        for row_idx in 0..duplicates.height() {
            let mut key = Vec::new();
            for col_name in grain {
                let col_val = duplicates.column(col_name)?;
                let val_str = match col_val.dtype() {
                    DataType::Utf8 => col_val.str().unwrap().get(row_idx).unwrap().to_string(),
                    DataType::Int64 => col_val.i64().unwrap().get(row_idx).unwrap().to_string(),
                    DataType::Float64 => col_val.f64().unwrap().get(row_idx).unwrap().to_string(),
                    _ => format!("{:?}", col_val.get(row_idx)),
                };
                key.push(val_str);
            }
            dup_keys.push(key);
        }
        
        Ok(dup_keys)
    }
}

#[derive(Debug, Clone)]
pub struct ComparisonResult {
    pub population_diff: PopulationDiff,
    pub data_diff: DataDiff,
}

#[derive(Debug, Clone)]
pub struct PopulationDiff {
    pub missing_in_b: Vec<Vec<String>>,
    pub extra_in_b: Vec<Vec<String>>,
    pub common_count: usize,
    pub duplicates_a: Vec<Vec<String>>,
    pub duplicates_b: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct DataDiff {
    pub mismatches: usize,
    pub matches: usize,
    pub mismatch_details: DataFrame,
}

