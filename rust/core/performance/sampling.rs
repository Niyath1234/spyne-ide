//! Sampling Strategies
//! 
//! Provides various sampling strategies for quick analysis of large datasets.
//! Useful for exploratory analysis before full RCA.

use crate::error::{RcaError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use rand::seq::SliceRandom;
use rand::rngs::StdRng;
use rand::SeedableRng;

/// Sampling strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SamplingStrategy {
    /// Random sampling - sample N random rows
    Random { sample_size: usize },
    
    /// Stratified sampling - sample proportionally from each group
    Stratified { 
        /// Column to stratify by
        stratify_column: String,
        /// Sample size per stratum
        sample_size_per_stratum: usize,
    },
    
    /// Top-N sampling - sample top N rows by a value column
    TopN {
        /// Column to order by
        order_by: String,
        /// Number of rows
        n: usize,
        /// Whether to order descending
        descending: bool,
    },
    
    /// Systematic sampling - sample every Nth row
    Systematic {
        /// Interval between samples
        interval: usize,
        /// Starting offset
        offset: usize,
    },
}

/// Data sampler
pub struct Sampler;

impl Sampler {
    /// Sample data according to strategy
    pub fn sample(&self, df: DataFrame, strategy: &SamplingStrategy) -> Result<DataFrame> {
        match strategy {
            SamplingStrategy::Random { sample_size } => {
                self.random_sample(df, *sample_size)
            }
            SamplingStrategy::Stratified { stratify_column, sample_size_per_stratum } => {
                self.stratified_sample(df, stratify_column, *sample_size_per_stratum)
            }
            SamplingStrategy::TopN { order_by, n, descending } => {
                self.top_n_sample(df, order_by, *n, *descending)
            }
            SamplingStrategy::Systematic { interval, offset } => {
                self.systematic_sample(df, *interval, *offset)
            }
        }
    }
    
    /// Random sampling
    fn random_sample(&self, df: DataFrame, sample_size: usize) -> Result<DataFrame> {
        let total_rows = df.height();
        if sample_size >= total_rows {
            return Ok(df);
        }
        
        // Note: Polars DataFrame.sample_n API may vary by version
        // For now, use a simple approach: take first n rows after shuffling indices
        // In production, this should use the correct Polars API for the version
        let indices: Vec<usize> = (0..total_rows).collect();
        let mut rng = StdRng::seed_from_u64(42);
        let mut shuffled = indices;
        shuffled.shuffle(&mut rng);
        let selected_indices = shuffled.into_iter().take(sample_size).collect::<Vec<_>>();
        
        // Select rows by index
        let mut sampled_rows = Vec::new();
        for idx in selected_indices {
            sampled_rows.push(df.slice(idx as i64, 1));
        }
        
        if sampled_rows.is_empty() {
            Ok(df.head(Some(0)))
        } else {
            let mut combined = sampled_rows[0].clone();
            for chunk in sampled_rows.iter().skip(1) {
                combined = combined
                    .vstack(chunk)
                    .map_err(|e| RcaError::Execution(format!("Failed to concatenate samples: {}", e)))?;
            }
            Ok(combined)
        }
    }
    
    /// Stratified sampling
    fn stratified_sample(
        &self,
        df: DataFrame,
        stratify_column: &str,
        sample_size_per_stratum: usize,
    ) -> Result<DataFrame> {
        // Group by stratify column
        let groups = df
            .clone()
            .lazy()
            .group_by([col(stratify_column)])
            .agg([col("*").first()]) // Placeholder - we'll sample from each group
            .collect()?;
        
        // For each group, sample
        let mut sampled_chunks = Vec::new();
        
        // Get unique values in stratify column
        let unique_values = df
            .column(stratify_column)?
            .unique()?
            .str()?
            .into_iter()
            .filter_map(|v| v.map(|s| s.to_string()))
            .collect::<Vec<_>>();
        
        for value in unique_values {
            // Filter to this stratum
            let stratum = df
                .clone()
                .lazy()
                .filter(col(stratify_column).eq(lit(value.clone())))
                .collect()?;
            
            // Sample from stratum
            let stratum_size = stratum.height();
            let sample_size = sample_size_per_stratum.min(stratum_size);
            
            if sample_size > 0 {
                // Use same random sampling approach as random_sample
                let stratum_size = stratum.height();
                if sample_size >= stratum_size {
                    sampled_chunks.push(stratum);
                } else {
                    // Simple random selection
                    let mut rng = StdRng::seed_from_u64(42);
                    let mut indices: Vec<usize> = (0..stratum_size).collect();
                    indices.shuffle(&mut rng);
                    let selected = indices.into_iter().take(sample_size).collect::<Vec<_>>();
                    
                    let mut sampled_rows = Vec::new();
                    for idx in selected {
                        sampled_rows.push(stratum.slice(idx as i64, 1));
                    }
                    
                    if !sampled_rows.is_empty() {
                        let mut combined = sampled_rows[0].clone();
                        for chunk in sampled_rows.iter().skip(1) {
                            combined = combined.vstack(chunk)
                                .map_err(|e| RcaError::Execution(format!("Failed to concatenate: {}", e)))?;
                        }
                        sampled_chunks.push(combined);
                    }
                }
            }
        }
        
        // Concatenate all sampled strata
        if sampled_chunks.is_empty() {
            Ok(df.head(Some(0)))
        } else {
            // Use vstack to concatenate vertically
            let mut combined = sampled_chunks[0].clone();
            for chunk in sampled_chunks.iter().skip(1) {
                combined = combined
                    .vstack(chunk)
                    .map_err(|e| RcaError::Execution(format!("Failed to concatenate samples: {}", e)))?;
            }
            
            Ok(combined)
        }
    }
    
    /// Top-N sampling
    fn top_n_sample(
        &self,
        df: DataFrame,
        order_by: &str,
        n: usize,
        descending: bool,
    ) -> Result<DataFrame> {
        // Use sort_by_exprs with correct signature
        let sort_expr = col(order_by).sort(SortOptions::default().with_order_descending(descending));
        
        let sorted = df
            .clone()
            .lazy()
            .sort_by_exprs(vec![sort_expr], SortMultipleOptions::default())
            .limit(n as u32)
            .collect()?;
        
        Ok(sorted)
    }
    
    /// Systematic sampling
    fn systematic_sample(
        &self,
        df: DataFrame,
        interval: usize,
        offset: usize,
    ) -> Result<DataFrame> {
        let total_rows = df.height();
        if offset >= total_rows {
            return Ok(df.head(Some(0)));
        }
        
        // Collect row indices to sample
        let mut indices = Vec::new();
        let mut current = offset;
        
        while current < total_rows {
            indices.push(current);
            current += interval;
        }
        
        // Use slice to extract rows at calculated indices
        let mut sampled_rows = Vec::new();
        for &idx in &indices {
            if idx < total_rows {
                let row = df.slice(idx as i64, 1);
                sampled_rows.push(row);
            }
        }
        
        if sampled_rows.is_empty() {
            Ok(df.head(Some(0)))
        } else {
            // Use vstack to concatenate vertically
            let mut combined = sampled_rows[0].clone();
            for chunk in sampled_rows.iter().skip(1) {
                combined = combined
                    .vstack(chunk)
                    .map_err(|e| RcaError::Execution(format!("Failed to concatenate samples: {}", e)))?;
            }
            Ok(combined)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_random_sample() {
        let df = DataFrame::new(vec![
            Series::new("id", (0..100).collect::<Vec<i32>>()),
            Series::new("value", (0..100).map(|i| i as f64).collect::<Vec<f64>>()),
        ]).unwrap();
        
        let sampler = Sampler;
        let sampled = sampler.sample(
            df,
            &SamplingStrategy::Random { sample_size: 10 },
        ).unwrap();
        
        assert_eq!(sampled.height(), 10);
    }
}

