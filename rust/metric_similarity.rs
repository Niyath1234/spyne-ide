//! Metric Similarity Detection via Contract Column Descriptions
//! 
//! This module provides functionality to detect if two metrics from different systems
//! are similar/equivalent based on their column descriptions in contracts.
//! When making a contract, if two metric columns have the same description, they mean the same thing.

use crate::error::{RcaError, Result};
use crate::world_state::WorldState;
use crate::metadata::Metadata;

/// Check if two metrics are similar based on contract column descriptions or table metadata
/// 
/// This function:
/// 1. Finds tables containing metric_a in system_a and metric_b in system_b
/// 2. Gets contracts for those tables (if WorldState is available)
/// 3. Also checks table metadata column descriptions (always checked)
/// 4. Compares column descriptions for the metric columns
/// 5. Returns true if descriptions indicate they mean the same thing
/// 
/// Priority: Contract descriptions > Table metadata descriptions
/// When creating contracts, if both metric columns have the same description,
/// they are considered equivalent (same meaning).
pub fn are_metrics_similar_via_contracts(
    world_state: Option<&WorldState>,
    metadata: &Metadata,
    system_a: &str,
    system_b: &str,
    metric_a: &str,
    metric_b: &str,
) -> Result<bool> {
    // Find tables containing metric_a in system_a
    let tables_a: Vec<&str> = metadata.tables
        .iter()
        .filter(|t| {
            t.system.to_lowercase() == system_a.to_lowercase() &&
            t.columns.as_ref().map_or(false, |cols| {
                cols.iter().any(|c| c.name.to_lowercase() == metric_a.to_lowercase())
            })
        })
        .map(|t| t.name.as_str())
        .collect();
    
    // Find tables containing metric_b in system_b
    let tables_b: Vec<&str> = metadata.tables
        .iter()
        .filter(|t| {
            t.system.to_lowercase() == system_b.to_lowercase() &&
            t.columns.as_ref().map_or(false, |cols| {
                cols.iter().any(|c| c.name.to_lowercase() == metric_b.to_lowercase())
            })
        })
        .map(|t| t.name.as_str())
        .collect();
    
    if tables_a.is_empty() || tables_b.is_empty() {
        // Can't compare if we can't find the metrics
        return Ok(false);
    }
    
    // Get descriptions for metric_a from contracts (if available) and table metadata
    let mut descriptions_a = Vec::new();
    for table_name in &tables_a {
        // First check contracts (higher priority - user explicitly defined)
        if let Some(ws) = world_state {
            if let Some(contract) = ws.contract_registry.get_table_contract(table_name) {
                // Find the column mapping for metric_a
                for mapping in &contract.column_mappings {
                    // Check if this mapping corresponds to metric_a
                    // metric_a could be the api_column or table_column
                    if mapping.api_column.to_lowercase() == metric_a.to_lowercase() ||
                       mapping.table_column.to_lowercase() == metric_a.to_lowercase() {
                        if let Some(ref desc) = mapping.description {
                            descriptions_a.push(desc.clone());
                        }
                    }
                }
            }
        }
        
        // Also check table metadata for column descriptions (always checked)
        if let Some(table) = metadata.tables.iter().find(|t| t.name == *table_name) {
            if let Some(ref cols) = table.columns {
                for col in cols {
                    if col.name.to_lowercase() == metric_a.to_lowercase() {
                        if let Some(ref desc) = col.description {
                            descriptions_a.push(desc.clone());
                        }
                    }
                }
            }
        }
    }
    
    // Get descriptions for metric_b from contracts (if available) and table metadata
    let mut descriptions_b = Vec::new();
    for table_name in &tables_b {
        // First check contracts (higher priority - user explicitly defined)
        if let Some(ws) = world_state {
            if let Some(contract) = ws.contract_registry.get_table_contract(table_name) {
                // Find the column mapping for metric_b
                for mapping in &contract.column_mappings {
                    // Check if this mapping corresponds to metric_b
                    if mapping.api_column.to_lowercase() == metric_b.to_lowercase() ||
                       mapping.table_column.to_lowercase() == metric_b.to_lowercase() {
                        if let Some(ref desc) = mapping.description {
                            descriptions_b.push(desc.clone());
                        }
                    }
                }
            }
        }
        
        // Also check table metadata for column descriptions (always checked)
        if let Some(table) = metadata.tables.iter().find(|t| t.name == *table_name) {
            if let Some(ref cols) = table.columns {
                for col in cols {
                    if col.name.to_lowercase() == metric_b.to_lowercase() {
                        if let Some(ref desc) = col.description {
                            descriptions_b.push(desc.clone());
                        }
                    }
                }
            }
        }
    }
    
    // Compare descriptions - if any description from metric_a matches any from metric_b,
    // they are considered similar
    for desc_a in &descriptions_a {
        for desc_b in &descriptions_b {
            if descriptions_match(desc_a, desc_b) {
                return Ok(true);
            }
        }
    }
    
    Ok(false)
}

/// Check if two descriptions match (same or very similar)
fn descriptions_match(desc_a: &str, desc_b: &str) -> bool {
    // Normalize descriptions: lowercase, trim whitespace
    let lower_a = desc_a.to_lowercase();
    let lower_b = desc_b.to_lowercase();
    let normalized_a = lower_a.trim();
    let normalized_b = lower_b.trim();
    
    // Exact match
    if normalized_a == normalized_b {
        return true;
    }
    
    // Check if one contains the other (for partial matches)
    // This handles cases like "Total Outstanding" vs "Total Outstanding Amount"
    if normalized_a.contains(normalized_b) || normalized_b.contains(normalized_a) {
        // Only consider it a match if the shorter description is substantial (> 10 chars)
        // to avoid false positives from very short descriptions
        let shorter_len = normalized_a.len().min(normalized_b.len());
        if shorter_len > 10 {
            return true;
        }
    }
    
    // Check for semantic similarity using simple keyword matching
    // Split into words and check if they share significant words
    let words_a: std::collections::HashSet<&str> = normalized_a
        .split_whitespace()
        .filter(|w| w.len() > 3) // Only consider words longer than 3 chars
        .collect();
    
    let words_b: std::collections::HashSet<&str> = normalized_b
        .split_whitespace()
        .filter(|w| w.len() > 3)
        .collect();
    
    if !words_a.is_empty() && !words_b.is_empty() {
        let common_words: Vec<&str> = words_a.intersection(&words_b).copied().collect();
        // If they share at least 2 significant words, consider them similar
        if common_words.len() >= 2 {
            return true;
        }
        
        // If one description has most of its words in common with the other
        let overlap_ratio_a = common_words.len() as f64 / words_a.len() as f64;
        let overlap_ratio_b = common_words.len() as f64 / words_b.len() as f64;
        
        // If overlap is > 50% in either direction, consider them similar
        if overlap_ratio_a > 0.5 || overlap_ratio_b > 0.5 {
            return true;
        }
    }
    
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_descriptions_match_exact() {
        assert!(descriptions_match("Total Outstanding", "Total Outstanding"));
        assert!(descriptions_match("Interest Amount", "Interest Amount"));
    }
    
    #[test]
    fn test_descriptions_match_partial() {
        assert!(descriptions_match("Total Outstanding", "Total Outstanding Amount"));
        assert!(descriptions_match("Interest Amount", "Total Interest Amount"));
    }
    
    #[test]
    fn test_descriptions_match_semantic() {
        assert!(descriptions_match("Total Outstanding Principal", "Outstanding Principal Balance"));
        assert!(descriptions_match("Interest Amount Calculated", "Calculated Interest Amount"));
    }
    
    #[test]
    fn test_descriptions_no_match() {
        assert!(!descriptions_match("Total Outstanding", "Interest Amount"));
        assert!(!descriptions_match("Principal", "Interest"));
    }
}

