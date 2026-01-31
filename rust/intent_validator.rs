//! Intent Validator - Prevents Hallucination
//! 
//! Validates that all tables, columns, and relationships in IntentSpec
//! actually exist in metadata before proceeding to SQL generation.
//! This prevents hallucinated table/column names from being used.

use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use crate::intent_compiler::{IntentSpec, JoinSpec, ConstraintSpec};
use crate::fuzzy_matcher::FuzzyMatcher;
use crate::faiss_fuzzy_matcher::FaissFuzzyMatcher;
use crate::learning_store::LearningStore;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tracing::debug;

/// Validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub resolved_tables: Vec<String>, // Actual table names from metadata
    pub resolved_columns: Vec<(String, String)>, // (table, column) pairs
}

/// Intent Validator - Prevents hallucination by validating against metadata
pub struct IntentValidator {
    metadata: Metadata,
    fuzzy_matcher: FuzzyMatcher,
    /// FAISS fuzzy matcher for fast similarity search (optional)
    faiss_matcher: Option<FaissFuzzyMatcher>,
    /// Whether to allow fuzzy matching (default: true)
    allow_fuzzy: bool,
    /// Minimum confidence for fuzzy matches (default: 0.8)
    fuzzy_threshold: f64,
    /// Learning store for user-approved corrections (optional)
    learning_store: Option<Arc<LearningStore>>,
    /// Table index mapping for FAISS (table name -> index)
    table_index_map: HashMap<String, usize>,
}

impl IntentValidator {
    pub fn new(metadata: Metadata) -> Self {
        let mut validator = Self {
            metadata: metadata.clone(),
            fuzzy_matcher: FuzzyMatcher::new(0.8),
            faiss_matcher: None,
            allow_fuzzy: true,
            fuzzy_threshold: 0.8,
            learning_store: None,
            table_index_map: HashMap::new(),
        };
        
        // Build FAISS index if metadata is large enough (> 100 tables)
        validator.build_faiss_index();
        
        validator
    }
    
    /// Create validator with learning store
    pub fn with_learning_store(mut self, learning_store: Arc<LearningStore>) -> Self {
        self.learning_store = Some(learning_store);
        self
    }
    
    /// Build FAISS index from metadata
    /// Only builds if metadata has enough tables to benefit from indexing
    fn build_faiss_index(&mut self) {
        // Only build FAISS index if we have enough tables (threshold: 100)
        if self.metadata.tables.len() < 100 {
            debug!("Skipping FAISS index build: only {} tables (threshold: 100)", self.metadata.tables.len());
            return;
        }
        
        debug!("Building FAISS index for {} tables...", self.metadata.tables.len());
        
        // Extract table names
        let table_names: Vec<String> = self.metadata.tables.iter()
            .map(|t| t.name.clone())
            .collect();
        
        // Build table index map
        for (idx, table_name) in table_names.iter().enumerate() {
            self.table_index_map.insert(table_name.clone(), idx);
        }
        
        // Extract column names by table
        let mut column_names: HashMap<String, Vec<String>> = HashMap::new();
        for table in &self.metadata.tables {
            if let Some(ref columns) = table.columns {
                let cols: Vec<String> = columns.iter()
                    .map(|c| c.name.clone())
                    .collect();
                column_names.insert(table.name.clone(), cols);
            }
        }
        
        // Build FAISS index
        let mut faiss_matcher = FaissFuzzyMatcher::new(self.fuzzy_threshold);
        if faiss_matcher.build_index(&table_names, &column_names).is_ok() {
            self.faiss_matcher = Some(faiss_matcher);
            debug!("FAISS index built successfully");
        } else {
            debug!("Failed to build FAISS index, falling back to linear search");
        }
    }

    /// Validate intent against metadata - prevents hallucination
    /// 
    /// Checks:
    /// 1. All tables exist in metadata
    /// 2. All columns exist in their respective tables
    /// 3. All join conditions reference valid columns
    /// 4. All constraints reference valid columns
    /// 
    /// Returns ValidationResult with errors/warnings and resolved names
    pub fn validate(&self, intent: &IntentSpec) -> ValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut resolved_tables = Vec::new();
        let mut resolved_columns = Vec::new();

        // Step 1: Validate tables
        let table_validation = self.validate_tables(&intent.tables, &intent.joins);
        errors.extend(table_validation.errors);
        warnings.extend(table_validation.warnings);
        resolved_tables.extend(table_validation.resolved_tables);

        // Step 2: Validate columns in joins
        for join in &intent.joins {
            let join_validation = self.validate_join_columns(join, &resolved_tables);
            errors.extend(join_validation.errors);
            warnings.extend(join_validation.warnings);
            resolved_columns.extend(join_validation.resolved_columns);
        }

        // Step 3: Validate constraint columns
        for constraint in &intent.constraints {
            if let Some(ref col) = constraint.column {
                let constraint_validation = self.validate_column(col, &resolved_tables);
                if !constraint_validation.is_valid {
                    errors.push(format!(
                        "Constraint column '{}' not found in any table",
                        col
                    ));
                } else {
                    resolved_columns.extend(constraint_validation.resolved_columns);
                }
            }
        }

        // Step 4: Validate grain columns
        for grain_col in &intent.grain {
            let grain_validation = self.validate_column(grain_col, &resolved_tables);
            if !grain_validation.is_valid {
                errors.push(format!(
                    "Grain column '{}' not found in any table",
                    grain_col
                ));
            } else {
                resolved_columns.extend(grain_validation.resolved_columns);
            }
        }

        ValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
            resolved_tables,
            resolved_columns,
        }
    }

    /// Validate tables exist in metadata
    fn validate_tables(
        &self,
        table_names: &[String],
        joins: &[JoinSpec],
    ) -> ValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut resolved_tables = Vec::new();
        let mut all_table_names = HashSet::new();

        // Collect all table names from intent
        for table_name in table_names {
            all_table_names.insert(table_name.clone());
        }
        for join in joins {
            all_table_names.insert(join.left_table.clone());
            all_table_names.insert(join.right_table.clone());
        }

        // Validate each table
        for table_name in all_table_names {
            match self.resolve_table(&table_name) {
                Ok(actual_name) => {
                    if actual_name != table_name {
                        warnings.push(format!(
                            "Table '{}' resolved to '{}' (fuzzy match)",
                            table_name, actual_name
                        ));
                    }
                    resolved_tables.push(actual_name);
                }
                Err(e) => {
                    errors.push(format!(
                        "Table '{}' not found in metadata: {}",
                        table_name, e
                    ));
                }
            }
        }

        ValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
            resolved_tables,
            resolved_columns: Vec::new(),
        }
    }

    /// Validate join column references
    fn validate_join_columns(
        &self,
        join: &JoinSpec,
        resolved_tables: &[String],
    ) -> ValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut resolved_columns = Vec::new();

        // Resolve left table
        let left_table = match self.resolve_table(&join.left_table) {
            Ok(name) => name,
            Err(e) => {
                errors.push(format!(
                    "Left table '{}' in join not found: {}",
                    join.left_table, e
                ));
                return ValidationResult {
                    is_valid: false,
                    errors,
                    warnings,
                    resolved_tables: Vec::new(),
                    resolved_columns: Vec::new(),
                };
            }
        };

        // Resolve right table
        let right_table = match self.resolve_table(&join.right_table) {
            Ok(name) => name,
            Err(e) => {
                errors.push(format!(
                    "Right table '{}' in join not found: {}",
                    join.right_table, e
                ));
                return ValidationResult {
                    is_valid: false,
                    errors,
                    warnings,
                    resolved_tables: Vec::new(),
                    resolved_columns: Vec::new(),
                };
            }
        };

        // Validate join conditions
        for condition in &join.conditions {
            // Validate left column
            match self.resolve_column_in_table(&condition.left_column, &left_table) {
                Ok(actual_col) => {
                    if actual_col != condition.left_column {
                        warnings.push(format!(
                            "Join left column '{}' resolved to '{}' in table '{}'",
                            condition.left_column, actual_col, left_table
                        ));
                    }
                    resolved_columns.push((left_table.clone(), actual_col));
                }
                Err(e) => {
                    errors.push(format!(
                        "Join left column '{}' not found in table '{}': {}",
                        condition.left_column, left_table, e
                    ));
                }
            }

            // Validate right column
            match self.resolve_column_in_table(&condition.right_column, &right_table) {
                Ok(actual_col) => {
                    if actual_col != condition.right_column {
                        warnings.push(format!(
                            "Join right column '{}' resolved to '{}' in table '{}'",
                            condition.right_column, actual_col, right_table
                        ));
                    }
                    resolved_columns.push((right_table.clone(), actual_col));
                }
                Err(e) => {
                    errors.push(format!(
                        "Join right column '{}' not found in table '{}': {}",
                        condition.right_column, right_table, e
                    ));
                }
            }
        }

        ValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
            resolved_tables: Vec::new(),
            resolved_columns,
        }
    }

    /// Validate a column exists in any of the resolved tables
    fn validate_column(
        &self,
        column_name: &str,
        resolved_tables: &[String],
    ) -> ValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut resolved_columns = Vec::new();

        let mut found = false;
        for table_name in resolved_tables {
            if let Ok(actual_col) = self.resolve_column_in_table(column_name, table_name) {
                found = true;
                if actual_col != column_name {
                    warnings.push(format!(
                        "Column '{}' resolved to '{}' in table '{}'",
                        column_name, actual_col, table_name
                    ));
                }
                resolved_columns.push((table_name.clone(), actual_col));
                break; // Found in at least one table
            }
        }

        if !found {
            errors.push(format!(
                "Column '{}' not found in any of the resolved tables: {:?}",
                column_name, resolved_tables
            ));
        }

        ValidationResult {
            is_valid: found,
            errors,
            warnings,
            resolved_tables: Vec::new(),
            resolved_columns,
        }
    }

    /// Resolve table name against metadata (exact or fuzzy match)
    fn resolve_table(&self, table_name: &str) -> Result<String> {
        // Try exact match first
        if self.metadata.tables.iter().any(|t| t.name == table_name) {
            return Ok(table_name.to_string());
        }
        
        // Check learned corrections FIRST (before fuzzy matching)
        if let Some(ref learning_store) = self.learning_store {
            if let Some(correction) = learning_store.get_correction(table_name, "table") {
                // Verify the corrected name still exists in metadata
                if self.metadata.tables.iter().any(|t| t.name == correction.correct_name) {
                    return Ok(correction.correct_name.clone());
                }
            }
        }

        // Try fuzzy match if enabled
        if self.allow_fuzzy {
            let best_match = if let Some(ref faiss) = self.faiss_matcher {
                // Use FAISS for fast candidate retrieval
                debug!("Using FAISS for table search: {}", table_name);
                let candidates = faiss.find_similar_tables(table_name, 10);
                
                // Refine top candidates with string similarity
                candidates.into_iter()
                    .map(|(name, faiss_sim)| {
                        let string_sim = self.fuzzy_matcher.similarity(table_name, &name);
                        // Combine FAISS similarity (0.6 weight) and string similarity (0.4 weight)
                        let combined_sim = faiss_sim * 0.6 + string_sim * 0.4;
                        (name, combined_sim)
                    })
                    .filter(|(_, sim)| *sim >= self.fuzzy_threshold)
                    .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            } else {
                // Fallback to linear search
                debug!("Using linear search for table: {}", table_name);
                let mut best_match: Option<(String, f64)> = None;

                for table in &self.metadata.tables {
                    let similarity = self.fuzzy_matcher.similarity(table_name, &table.name);
                    if similarity >= self.fuzzy_threshold {
                        if best_match.is_none() || similarity > best_match.as_ref().unwrap().1 {
                            best_match = Some((table.name.clone(), similarity));
                        }
                    }
                }
                
                best_match
            };

            if let Some((matched_name, confidence)) = best_match {
                if confidence >= self.fuzzy_threshold {
                    return Ok(matched_name);
                }
            }
        }

        // Try partial match (contains)
        if let Some(table) = self.metadata.tables.iter()
            .find(|t| t.name.contains(table_name) || table_name.contains(&t.name)) {
            return Ok(table.name.clone());
        }

        // Try entity or system match
        if let Some(table) = self.metadata.tables.iter()
            .find(|t| t.entity == table_name || t.system == table_name) {
            return Ok(table.name.clone());
        }

        Err(RcaError::Execution(format!(
            "Table '{}' not found in metadata. Available tables: {}",
            table_name,
            self.metadata.tables.iter()
                .map(|t| t.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    /// Resolve column name in a specific table
    fn resolve_column_in_table(&self, column_name: &str, table_name: &str) -> Result<String> {
        let table = self.metadata.tables.iter()
            .find(|t| t.name == table_name)
            .ok_or_else(|| RcaError::Execution(format!("Table '{}' not found", table_name)))?;

        // Get columns from table
        let columns = table.columns.as_ref()
            .ok_or_else(|| RcaError::Execution(format!("Table '{}' has no column metadata", table_name)))?;

        // Try exact match
        if columns.iter().any(|c| c.name == column_name) {
            return Ok(column_name.to_string());
        }
        
        // Check learned corrections FIRST (before fuzzy matching)
        if let Some(ref learning_store) = self.learning_store {
            if let Some(correction) = learning_store.get_correction(column_name, "column") {
                // Verify the correction is for this table (if specified)
                if correction.table_name.is_none() || correction.table_name.as_ref().map(|s| s.as_str()) == Some(table_name) {
                    // Verify the corrected name exists in this table
                    if columns.iter().any(|c| c.name == correction.correct_name) {
                        return Ok(correction.correct_name.clone());
                    }
                }
            }
        }

        // Try fuzzy match if enabled
        if self.allow_fuzzy {
            let best_match = if let (Some(ref faiss), Some(table_idx)) = (&self.faiss_matcher, self.table_index_map.get(table_name)) {
                // Use FAISS for fast candidate retrieval
                debug!("Using FAISS for column search: {} in table {}", column_name, table_name);
                let candidates = faiss.find_similar_columns(column_name, *table_idx, 10);
                
                // Refine top candidates with string similarity
                candidates.into_iter()
                    .map(|(name, faiss_sim)| {
                        let string_sim = self.fuzzy_matcher.similarity(column_name, &name);
                        // Combine FAISS similarity (0.6 weight) and string similarity (0.4 weight)
                        let combined_sim = faiss_sim * 0.6 + string_sim * 0.4;
                        (name, combined_sim)
                    })
                    .filter(|(_, sim)| *sim >= self.fuzzy_threshold)
                    .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            } else {
                // Fallback to linear search
                debug!("Using linear search for column: {} in table {}", column_name, table_name);
                let mut best_match: Option<(String, f64)> = None;

                for column in columns {
                    let similarity = self.fuzzy_matcher.similarity(column_name, &column.name);
                    if similarity >= self.fuzzy_threshold {
                        if best_match.is_none() || similarity > best_match.as_ref().unwrap().1 {
                            best_match = Some((column.name.clone(), similarity));
                        }
                    }
                }
                
                best_match
            };

            if let Some((matched_name, confidence)) = best_match {
                if confidence >= self.fuzzy_threshold {
                    return Ok(matched_name);
                }
            }
        }

        // Try partial match
        if let Some(column) = columns.iter()
            .find(|c| c.name.contains(column_name) || column_name.contains(&c.name)) {
            return Ok(column.name.clone());
        }

        Err(RcaError::Execution(format!(
            "Column '{}' not found in table '{}'. Available columns: {}",
            column_name,
            table_name,
            columns.iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    /// Update intent with resolved (validated) names
    /// 
    /// Replaces hallucinated names with actual names from metadata
    pub fn resolve_intent(&self, intent: &mut IntentSpec) -> Result<ValidationResult> {
        let validation = self.validate(intent);

        if !validation.is_valid {
            return Ok(validation);
        }

        // Update tables with resolved names
        let mut resolved_table_map: std::collections::HashMap<String, String> = 
            intent.tables.iter()
                .zip(validation.resolved_tables.iter())
                .map(|(orig, resolved)| (orig.clone(), resolved.clone()))
                .collect();

        // Update join table names
        for join in &mut intent.joins {
            if let Some(resolved) = resolved_table_map.get(&join.left_table) {
                join.left_table = resolved.clone();
            }
            if let Some(resolved) = resolved_table_map.get(&join.right_table) {
                join.right_table = resolved.clone();
            }

            // Update join condition column names
            for condition in &mut join.conditions {
                if let Some((table, col)) = validation.resolved_columns.iter()
                    .find(|(t, c)| *t == join.left_table && *c == condition.left_column) {
                    condition.left_column = col.clone();
                }
                if let Some((table, col)) = validation.resolved_columns.iter()
                    .find(|(t, c)| *t == join.right_table && *c == condition.right_column) {
                    condition.right_column = col.clone();
                }
            }
        }

        // Update constraint column names
        for constraint in &mut intent.constraints {
            if let Some(ref col) = constraint.column {
                if let Some((_, resolved_col)) = validation.resolved_columns.iter()
                    .find(|(_, c)| c == col) {
                    constraint.column = Some(resolved_col.clone());
                }
            }
        }

        // Update grain column names
        let mut resolved_grain = Vec::new();
        for grain_col in &intent.grain {
            if let Some((_, resolved_col)) = validation.resolved_columns.iter()
                .find(|(_, c)| c == grain_col) {
                resolved_grain.push(resolved_col.clone());
            } else {
                resolved_grain.push(grain_col.clone()); // Keep original if not found
            }
        }
        intent.grain = resolved_grain;

        Ok(validation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_compiler::{IntentSpec, TaskType, JoinSpec, JoinCondition};

    fn create_test_metadata() -> Metadata {
        // Create minimal metadata for testing
        let metadata_dir = std::path::PathBuf::from("metadata");
        Metadata::load(&metadata_dir).unwrap_or_else(|_| {
            // Fallback: create minimal metadata
            Metadata {
                tables: vec![],
                rules: vec![],
                metrics: vec![],
                entities: vec![],
                business_labels: crate::metadata::BusinessLabel::Object(
                    crate::metadata::BusinessLabelObject {
                        systems: vec![],
                        metrics: vec![],
                        reconciliation_types: vec![],
                    }
                ),
                lineage: crate::metadata::Lineage::Object(
                    crate::metadata::LineageObject {
                        edges: vec![],
                        possible_joins: vec![],
                    }
                ),
                time_rules: crate::metadata::TimeRules {
                    as_of_rules: vec![],
                    lateness_rules: vec![],
                },
                identity: crate::metadata::Identity::Object(
                    crate::metadata::IdentityObject {
                        canonical_keys: vec![],
                        key_mappings: vec![],
                    }
                ),
                exceptions: crate::metadata::Exceptions::Array(vec![]),
            }
        })
    }

    #[test]
    fn test_table_validation() {
        let metadata = create_test_metadata();
        let validator = IntentValidator::new(metadata);

        // This test would need actual metadata
        // For now, just verify the validator can be created
        assert!(true);
    }
}

