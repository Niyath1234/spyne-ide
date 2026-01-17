//! Canonical Mapping Layer
//! 
//! Maps dataframes from different systems to the canonical entity format
//! for comparison.

use crate::core::models::CanonicalEntity;
use crate::error::{RcaError, Result};
use polars::prelude::*;
use std::collections::HashMap;

/// Canonical mapper
/// 
/// Transforms dataframes to match the canonical entity schema
pub struct CanonicalMapper;

impl Default for CanonicalMapper {
    fn default() -> Self {
        CanonicalMapper
    }
}

impl CanonicalMapper {
    /// Create a new canonical mapper
    pub fn new() -> Self {
        Self::default()
    }
    /// Canonicalize a dataframe to match the canonical entity schema
    /// 
    /// This performs:
    /// - Column renaming to match canonical names
    /// - Type casting to ensure compatibility
    /// - Null normalization (consistent null handling)
    /// - Sorting by keys for deterministic comparison
    pub fn canonicalize(
        &self,
        df: DataFrame,
        entity: &CanonicalEntity,
        column_mapping: &HashMap<String, String>, // Maps canonical column -> source column
    ) -> Result<DataFrame> {
        let mut result = df;
        
        // Step 1: Rename columns to canonical names
        result = self.rename_columns(result, column_mapping)?;
        
        // Step 2: Ensure all required columns exist
        result = self.ensure_required_columns(result, entity)?;
        
        // Step 3: Cast types to ensure compatibility
        result = self.cast_types(result, entity)?;
        
        // Step 4: Normalize nulls
        result = self.normalize_nulls(result, entity)?;
        
        // Step 5: Sort by keys
        result = self.sort_by_keys(result, entity)?;
        
        Ok(result)
    }
    
    /// Rename columns to match canonical schema
    fn rename_columns(
        &self,
        df: DataFrame,
        mapping: &HashMap<String, String>,
    ) -> Result<DataFrame> {
        let mut renames = Vec::new();
        
        for (canonical_name, source_name) in mapping {
            if df.column(source_name).is_ok() {
                renames.push((source_name.clone(), canonical_name.clone()));
            }
        }
        
        if renames.is_empty() {
            return Ok(df);
        }
        
        let mut result = df;
        for (old_name, new_name) in renames {
            result.rename(&old_name, &new_name)
                .map_err(|e| RcaError::Execution(format!("Failed to rename column {} to {}: {}", old_name, new_name, e)))?;
        }
        
        Ok(result)
    }
    
    /// Ensure all required columns exist (add missing with nulls)
    fn ensure_required_columns(
        &self,
        df: DataFrame,
        entity: &CanonicalEntity,
    ) -> Result<DataFrame> {
        let required_cols = entity.all_columns();
        let mut result = df;
        
        for col_name in required_cols {
            if result.column(&col_name).is_err() {
                // Column missing - add it as null
                // Create a null series with same length as dataframe
                let null_series = Series::new_null(&col_name, result.height());
                result.with_column(null_series)
                    .map_err(|e| RcaError::Execution(format!("Failed to add column {}: {}", col_name, e)))?;
            }
        }
        
        Ok(result)
    }
    
    /// Cast types to ensure compatibility
    /// 
    /// For now, we'll keep original types. In a full implementation,
    /// you'd want to ensure numeric types match, dates are consistent, etc.
    fn cast_types(&self, df: DataFrame, _entity: &CanonicalEntity) -> Result<DataFrame> {
        // Placeholder - real implementation would:
        // - Cast numeric types to consistent precision
        // - Normalize date/time formats
        // - Ensure string encoding is consistent
        Ok(df)
    }
    
    /// Normalize null handling
    /// 
    /// Ensures consistent representation of nulls across systems
    fn normalize_nulls(&self, df: DataFrame, _entity: &CanonicalEntity) -> Result<DataFrame> {
        // For now, Polars handles nulls consistently
        // In a full implementation, you might:
        // - Convert empty strings to nulls
        // - Convert "NULL" strings to nulls
        // - Normalize NaN handling for floats
        Ok(df)
    }
    
    /// Sort by keys for deterministic comparison
    fn sort_by_keys(&self, df: DataFrame, entity: &CanonicalEntity) -> Result<DataFrame> {
        let key_exprs: Vec<Expr> = entity.keys.iter().map(|k| col(k)).collect();
        
        if key_exprs.is_empty() {
            return Ok(df);
        }
        
        let sorted = if key_exprs.is_empty() {
            df
        } else {
            df.lazy()
                .sort_by_exprs(key_exprs, SortMultipleOptions::default())
                .collect()?
        };
        
        Ok(sorted)
    }
    
    /// Infer column mapping from dataframe and entity
    /// 
    /// Attempts to automatically map source columns to canonical columns
    /// using fuzzy matching or heuristics.
    pub fn infer_mapping(
        &self,
        df: &DataFrame,
        entity: &CanonicalEntity,
    ) -> Result<HashMap<String, String>> {
        let mut mapping = HashMap::new();
        let df_columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        
        // Try to match each canonical column to a source column
        for canonical_col in entity.all_columns() {
            // Exact match first
            if df_columns.contains(&canonical_col) {
                mapping.insert(canonical_col.clone(), canonical_col.clone());
                continue;
            }
            
            // Try case-insensitive match
            if let Some(matched) = df_columns.iter().find(|c| {
                c.eq_ignore_ascii_case(&canonical_col)
            }) {
                mapping.insert(canonical_col.clone(), matched.clone());
                continue;
            }
            
            // Try substring match (e.g., "paid_amount" matches "amount_paid")
            if let Some(matched) = df_columns.iter().find(|c| {
                c.contains(canonical_col.as_str()) || canonical_col.contains(c.as_str())
            }) {
                mapping.insert(canonical_col.clone(), matched.clone());
                continue;
            }
            
            // No match found - will be added as null column in ensure_required_columns
        }
        
        Ok(mapping)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_infer_mapping() {
        // Create a test dataframe
        let df = DataFrame::new(vec![
            Series::new("uuid", vec!["1", "2", "3"]),
            Series::new("amount", vec![100.0, 200.0, 300.0]),
        ]).unwrap();
        
        let entity = CanonicalEntity::new(
            "payment_event",
            vec!["uuid".to_string()],
            vec!["paid_amount".to_string()],
            vec![],
        );
        
        let mapper = CanonicalMapper;
        let mapping = mapper.infer_mapping(&df, &entity).unwrap();
        
        assert!(mapping.contains_key("uuid"));
        // "paid_amount" should map to "amount" via substring match
        assert!(mapping.contains_key("paid_amount"));
    }
}

