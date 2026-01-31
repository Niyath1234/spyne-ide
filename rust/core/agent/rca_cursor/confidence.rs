//! Confidence Model
//! 
//! Computes confidence score based on:
//! - Join completeness
//! - Null rates
//! - Filter coverage
//! - Data freshness
//! - Sampling ratio

use crate::core::agent::rca_cursor::executor::ExecutionMetadata;
use crate::error::{RcaError, Result};
use std::time::{Duration, SystemTime};

/// Confidence factors
#[derive(Debug, Clone)]
pub struct ConfidenceFactors {
    /// Join completeness (0.0-1.0): fraction of expected joins that succeeded
    pub join_completeness: f64,
    /// Null rate (0.0-1.0): fraction of null values in key columns
    pub null_rate: f64,
    /// Filter coverage (0.0-1.0): fraction of filters successfully applied
    pub filter_coverage: f64,
    /// Data freshness (0.0-1.0): how recent the data is (1.0 = very recent, 0.0 = stale)
    pub data_freshness: f64,
    /// Sampling ratio (0.0-1.0): fraction of data sampled (1.0 = full scan)
    pub sampling_ratio: f64,
}

/// Confidence model
pub struct ConfidenceModel {
    /// Weights for each factor
    weights: ConfidenceWeights,
}

/// Weights for confidence factors
#[derive(Debug, Clone)]
pub struct ConfidenceWeights {
    /// Weight for join completeness (default: 0.3)
    pub join_completeness: f64,
    /// Weight for null rate (default: 0.2)
    pub null_rate: f64,
    /// Weight for filter coverage (default: 0.2)
    pub filter_coverage: f64,
    /// Weight for data freshness (default: 0.15)
    pub data_freshness: f64,
    /// Weight for sampling ratio (default: 0.15)
    pub sampling_ratio: f64,
}

impl Default for ConfidenceWeights {
    fn default() -> Self {
        Self {
            join_completeness: 0.3,
            null_rate: 0.2,
            filter_coverage: 0.2,
            data_freshness: 0.15,
            sampling_ratio: 0.15,
        }
    }
}

impl ConfidenceModel {
    /// Create a new confidence model with default weights
    pub fn new() -> Self {
        Self {
            weights: ConfidenceWeights::default(),
        }
    }

    /// Create a new confidence model with custom weights
    pub fn with_weights(weights: ConfidenceWeights) -> Self {
        Self { weights }
    }

    /// Compute confidence score from factors
    /// 
    /// Returns a confidence score between 0.0 and 1.0, where:
    /// - 1.0 = high confidence (all joins succeeded, no nulls, filters applied, fresh data, full scan)
    /// - 0.0 = low confidence (joins failed, many nulls, filters not applied, stale data, small sample)
    pub fn compute_confidence(&self, factors: &ConfidenceFactors) -> f64 {
        // Normalize each factor and apply weights
        let join_score = factors.join_completeness * self.weights.join_completeness;
        
        // Null rate is inverted (lower null rate = higher confidence)
        let null_score = (1.0 - factors.null_rate.min(1.0)) * self.weights.null_rate;
        
        let filter_score = factors.filter_coverage * self.weights.filter_coverage;
        let freshness_score = factors.data_freshness * self.weights.data_freshness;
        let sampling_score = factors.sampling_ratio * self.weights.sampling_ratio;

        // Sum all scores
        let confidence = join_score + null_score + filter_score + freshness_score + sampling_score;

        // Clamp to [0.0, 1.0]
        confidence.max(0.0).min(1.0)
    }

    /// Compute confidence from execution metadata
    /// 
    /// Extracts confidence factors from execution metadata and computes confidence score
    pub fn compute_from_metadata(
        &self,
        metadata_a: &ExecutionMetadata,
        metadata_b: &ExecutionMetadata,
        expected_joins: usize,
        data_timestamp: Option<SystemTime>,
    ) -> Result<f64> {
        // Compute join completeness
        // Assume joins succeeded if join_selectivity is Some (simplified)
        let join_completeness = if expected_joins > 0 {
            let joins_succeeded_a = if metadata_a.join_selectivity.is_some() { 1 } else { 0 };
            let joins_succeeded_b = if metadata_b.join_selectivity.is_some() { 1 } else { 0 };
            ((joins_succeeded_a + joins_succeeded_b) as f64) / (2.0 * expected_joins as f64)
        } else {
            1.0
        };

        // Compute null rate (simplified - would need actual null counts)
        // For now, assume low null rate if filter selectivity is reasonable
        let null_rate = {
            let sel_a = metadata_a.filter_selectivity.unwrap_or(1.0);
            let sel_b = metadata_b.filter_selectivity.unwrap_or(1.0);
            // Lower selectivity might indicate nulls filtered out
            (1.0 - sel_a).max(1.0 - sel_b) * 0.5
        };

        // Compute filter coverage
        // Assume filters were applied if filter_selectivity is Some
        let filter_coverage = {
            let filters_applied_a = if metadata_a.filter_selectivity.is_some() { 1.0 } else { 0.0 };
            let filters_applied_b = if metadata_b.filter_selectivity.is_some() { 1.0 } else { 0.0 };
            (filters_applied_a + filters_applied_b) / 2.0
        };

        // Compute data freshness
        let data_freshness = if let Some(timestamp) = data_timestamp {
            let age = SystemTime::now()
                .duration_since(timestamp)
                .unwrap_or(Duration::from_secs(0));
            
            // Freshness decays over time
            // 1 hour = 0.95, 1 day = 0.8, 1 week = 0.5, 1 month = 0.2
            let hours = age.as_secs() as f64 / 3600.0;
            if hours < 1.0 {
                1.0
            } else if hours < 24.0 {
                1.0 - (hours - 1.0) * 0.01
            } else if hours < 168.0 {
                0.8 - ((hours - 24.0) / 144.0) * 0.3
            } else {
                0.5 - ((hours - 168.0) / 720.0) * 0.3
            }.max(0.0).min(1.0)
        } else {
            0.5 // Unknown freshness - assume moderate
        };

        // Compute sampling ratio
        // Assume full scan if no explicit sampling info
        // In practice, this would come from execution plan
        let sampling_ratio = 1.0; // Default to full scan

        let factors = ConfidenceFactors {
            join_completeness,
            null_rate,
            filter_coverage,
            data_freshness,
            sampling_ratio,
        };

        Ok(self.compute_confidence(&factors))
    }
}

impl Default for ConfidenceModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_confidence_computation() {
        let model = ConfidenceModel::new();
        
        let factors = ConfidenceFactors {
            join_completeness: 1.0,
            null_rate: 0.0,
            filter_coverage: 1.0,
            data_freshness: 1.0,
            sampling_ratio: 1.0,
        };

        let confidence = model.compute_confidence(&factors);
        assert!(confidence > 0.9); // Should be very high confidence
    }

    #[test]
    fn test_confidence_with_poor_factors() {
        let model = ConfidenceModel::new();
        
        let factors = ConfidenceFactors {
            join_completeness: 0.5,
            null_rate: 0.5,
            filter_coverage: 0.5,
            data_freshness: 0.0,
            sampling_ratio: 0.1,
        };

        let confidence = model.compute_confidence(&factors);
        assert!(confidence < 0.5); // Should be lower confidence
    }
}





