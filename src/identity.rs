use crate::error::{RcaError, Result};
use crate::metadata::{Identity, Metadata};
use polars::prelude::*;
use std::path::PathBuf;

pub struct IdentityResolver {
    metadata: Metadata,
    data_dir: PathBuf,
}

impl IdentityResolver {
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self { metadata, data_dir }
    }
    
    /// Resolve canonical key for an entity
    pub fn get_canonical_key(&self, entity: &str) -> Result<String> {
        let canonical = self.metadata.identity.canonical_keys
            .iter()
            .find(|k| k.entity == entity)
            .ok_or_else(|| RcaError::Identity(format!("No canonical key found for entity: {}", entity)))?;
        
        Ok(canonical.canonical.clone())
    }
    
    /// Get alternate key for a system
    pub fn get_alternate_key(&self, entity: &str, system: &str) -> Result<Option<String>> {
        let canonical = self.metadata.identity.canonical_keys
            .iter()
            .find(|k| k.entity == entity)
            .ok_or_else(|| RcaError::Identity(format!("No canonical key found for entity: {}", entity)))?;
        
        let alternate = canonical.alternates
            .iter()
            .find(|a| a.system == system)
            .map(|a| a.key.clone());
        
        Ok(alternate)
    }
    
    /// Resolve key mapping between two systems
    pub fn resolve_key_mapping(
        &self,
        from_system: &str,
        to_system: &str,
        from_key: &str,
    ) -> Result<Option<String>> {
        let mapping = self.metadata.identity.key_mappings
            .iter()
            .find(|m| m.from_system == from_system && m.to_system == to_system && m.from_key == from_key);
        
        if let Some(mapping) = mapping {
            // In a real implementation, we'd load the mapping table
            // For now, assume direct mapping
            Ok(Some(mapping.to_key.clone()))
        } else {
            // Try to find reverse mapping
            let reverse = self.metadata.identity.key_mappings
                .iter()
                .find(|m| m.from_system == to_system && m.to_system == from_system && m.to_key == from_key);
            
            Ok(reverse.map(|m| m.from_key.clone()))
        }
    }
    
    /// Normalize keys to canonical form
    pub async fn normalize_keys(
        &self,
        df: DataFrame,
        table_name: &str,
        target_grain: &[String],
    ) -> Result<DataFrame> {
        let table = self.metadata
            .get_table(table_name)
            .ok_or_else(|| RcaError::Identity(format!("Table not found: {}", table_name)))?;
        
        // Get entity
        let entity = &table.entity;
        
        // Get canonical key
        let canonical_key = self.get_canonical_key(entity)?;
        
        // Check if we need to join to get canonical key
        let mut result = df.clone();
        
        // If table's primary key doesn't match canonical, we might need to join
        // For now, assume primary key is already canonical
        if !table.primary_key.contains(&canonical_key) {
            // Would need to join with identity mapping table
            // Simplified for now
        }
        
        // Ensure we have all columns in target_grain
        let mut select_cols = Vec::new();
        for col in target_grain {
            if result.column(col).is_ok() {
                select_cols.push(col.clone());
            } else {
                return Err(RcaError::Identity(format!("Column {} not found in table {}", col, table_name)));
            }
        }
        
        // Select only grain columns and metric columns
        let metric_cols: Vec<String> = result.get_column_names()
            .iter()
            .filter(|c| !target_grain.contains(&c.to_string()))
            .map(|s| s.to_string())
            .collect();
        
        select_cols.extend(metric_cols);
        
        let select_exprs: Vec<Expr> = select_cols.iter().map(|c| col(c)).collect();
        Ok(result.lazy().select(select_exprs).collect()?)
    }
}

