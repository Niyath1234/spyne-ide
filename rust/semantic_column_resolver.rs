//! Semantic Column Resolver - Automatically infers table/column mappings from metadata
//!
//! This module enables automatic inference of which table and column to use for a metric
//! based on:
//! 1. User's natural language query (e.g., "social category")
//! 2. Column descriptions in metadata (e.g., "Social/Minority category")
//! 3. Column names (e.g., "social_category")
//! 4. Table metadata (which tables contain which columns)
//! 5. System information (which system each table belongs to)
//!
//! This eliminates the need for explicit rules like:
//!   "Social category from LOS system asset_sourcing_personal_details_v2 table"
//!
//! Instead, the system automatically infers this from the knowledge base.

use crate::error::{RcaError, Result};
use crate::metadata::{Metadata, Table, ColumnMetadata};
use std::collections::HashMap;

/// Result of semantic column resolution
#[derive(Debug, Clone)]
pub struct ColumnResolution {
    /// Table name where the column was found
    pub table_name: String,
    /// Column name
    pub column_name: String,
    /// System the table belongs to
    pub system: String,
    /// Entity the table represents
    pub entity: String,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f64,
    /// Reason for the match
    pub match_reason: String,
}

/// Semantic Column Resolver - Finds columns by semantic meaning
pub struct SemanticColumnResolver {
    metadata: Metadata,
}

impl SemanticColumnResolver {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }

    /// Resolve a metric name to table and column using semantic matching
    /// 
    /// Searches through all tables and columns to find the best match based on:
    /// 1. Column descriptions (semantic similarity)
    /// 2. Column names (exact/fuzzy match)
    /// 3. System context (if provided)
    /// 
    /// Returns the best match(es) sorted by confidence.
    pub fn resolve_metric_to_column(
        &self,
        metric_name: &str,
        system_filter: Option<&str>,
    ) -> Result<Vec<ColumnResolution>> {
        let metric_lower = metric_name.to_lowercase();
        let mut candidates: Vec<ColumnResolution> = Vec::new();

        // Search through all tables
        for table in &self.metadata.tables {
            // Apply system filter if provided
            if let Some(system) = system_filter {
                if table.system != system {
                    continue;
                }
            }

            // Search columns in this table
            if let Some(columns) = &table.columns {
                for column in columns {
                    let resolution = self.score_column_match(
                        &metric_lower,
                        column,
                        table,
                    );

                    if resolution.confidence > 0.0 {
                        candidates.push(resolution);
                    }
                }
            }
        }

        // Sort by confidence (descending)
        candidates.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap_or(std::cmp::Ordering::Equal));

        // Return top matches (confidence > 0.3)
        Ok(candidates.into_iter()
            .filter(|c| c.confidence >= 0.3)
            .collect())
    }

    /// Score how well a column matches a metric name
    fn score_column_match(
        &self,
        metric_name: &str,
        column: &ColumnMetadata,
        table: &Table,
    ) -> ColumnResolution {
        let mut confidence = 0.0;
        let mut reasons = Vec::new();

        let column_name_lower = column.name.to_lowercase();

        // 1. Exact column name match (highest confidence)
        if column_name_lower == metric_name {
            confidence = 1.0;
            reasons.push("Exact column name match".to_string());
        }
        // 2. Column name contains metric name or vice versa
        else if column_name_lower.contains(metric_name) || metric_name.contains(&column_name_lower) {
            confidence = 0.9;
            reasons.push(format!("Column name '{}' contains metric '{}'", column.name, metric_name));
        }
        // 3. Fuzzy match on column name (using simple similarity)
        else {
            let name_similarity = self.simple_similarity(&column_name_lower, metric_name);
            if name_similarity > 0.7 {
                confidence = name_similarity * 0.8; // Slightly lower than exact match
                reasons.push(format!("Column name similarity: {:.0}%", name_similarity * 100.0));
            }
        }

        // 4. Check column description (semantic matching)
        if let Some(description) = &column.description {
            let desc_lower = description.to_lowercase();
            
            // Exact phrase match in description
            if desc_lower.contains(metric_name) {
                confidence = confidence.max(0.85);
                reasons.push(format!("Metric found in column description: '{}'", description));
            }
            // Check if description words match metric words
            else {
                let desc_words: Vec<&str> = desc_lower.split_whitespace().collect();
                let metric_words: Vec<&str> = metric_name.split_whitespace().collect();
                
                let matching_words: usize = metric_words.iter()
                    .filter(|mw| desc_words.iter().any(|dw| dw.contains(*mw) || mw.contains(dw)))
                    .count();
                
                if matching_words > 0 {
                    let word_match_score = matching_words as f64 / metric_words.len().max(1) as f64;
                    confidence = confidence.max(0.6 + word_match_score * 0.2);
                    reasons.push(format!("{} matching words in description", matching_words));
                }
            }
        }

        // 5. Check if column name is a common variation of metric name
        // e.g., "social_category" matches "social category"
        let normalized_metric = metric_name.replace(" ", "_").replace("-", "_");
        if column_name_lower == normalized_metric {
            confidence = confidence.max(0.95);
            reasons.push("Normalized name match".to_string());
        }

        // 6. Check if metric name is a common variation of column name
        // e.g., "social category" matches "social_category"
        let normalized_column = column_name_lower.replace("_", " ").replace("-", " ");
        if normalized_column.contains(metric_name) || metric_name.contains(&normalized_column) {
            confidence = confidence.max(0.9);
            reasons.push("Normalized variation match".to_string());
        }

        ColumnResolution {
            table_name: table.name.clone(),
            column_name: column.name.clone(),
            system: table.system.clone(),
            entity: table.entity.clone(),
            confidence,
            match_reason: reasons.join("; "),
        }
    }

    /// Simple string similarity (Jaro-Winkler-like, simplified)
    fn simple_similarity(&self, s1: &str, s2: &str) -> f64 {
        if s1 == s2 {
            return 1.0;
        }

        // Check if one contains the other
        if s1.contains(s2) || s2.contains(s1) {
            return 0.8;
        }

        // Check common prefix
        let min_len = s1.len().min(s2.len());
        let mut common_prefix = 0;
        for i in 0..min_len {
            if s1.chars().nth(i) == s2.chars().nth(i) {
                common_prefix += 1;
            } else {
                break;
            }
        }

        if common_prefix > 0 {
            let prefix_score = common_prefix as f64 / min_len.max(1) as f64;
            return prefix_score * 0.7;
        }

        // Check common words
        let s1_words: Vec<String> = s1.split_whitespace().map(|w| w.to_string()).collect();
        let s2_words: Vec<String> = s2.split_whitespace().map(|w| w.to_string()).collect();
        
        let common_words: usize = s1_words
            .iter()
            .filter(|w1| {
                let w1s = w1.as_str();
                s2_words.iter().any(|w2| {
                    let w2s = w2.as_str();
                    w1s == w2s || w1s.contains(w2s) || w2s.contains(w1s)
                })
            })
            .count();

        if common_words > 0 {
            let word_score = common_words as f64 / s1_words.len().max(s2_words.len()).max(1) as f64;
            return word_score * 0.6;
        }

        0.0
    }

    /// Find all columns matching a metric across all systems
    /// Returns a map: system -> Vec<ColumnResolution>
    pub fn find_columns_for_metric(
        &self,
        metric_name: &str,
    ) -> HashMap<String, Vec<ColumnResolution>> {
        let mut result: HashMap<String, Vec<ColumnResolution>> = HashMap::new();

        // Get all unique systems
        let systems: Vec<String> = self.metadata.tables.iter()
            .map(|t| t.system.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        for system in systems {
            if let Ok(resolutions) = self.resolve_metric_to_column(metric_name, Some(&system)) {
                if !resolutions.is_empty() {
                    result.insert(system, resolutions);
                }
            }
        }

        result
    }

    /// Auto-generate a rule definition for a metric based on semantic resolution
    /// 
    /// This allows the system to automatically create rules instead of requiring
    /// explicit rule definitions in metadata.
    pub fn auto_generate_rule(
        &self,
        metric_name: &str,
        system: &str,
        target_entity: Option<&str>,
        target_grain: Option<&[String]>,
    ) -> Result<Option<crate::metadata::Rule>> {
        // Find best column match for this metric in the specified system
        let resolutions = self.resolve_metric_to_column(metric_name, Some(system))?;

        if resolutions.is_empty() {
            return Ok(None);
        }

        // Use the highest confidence match
        let best_match = &resolutions[0];

        // Determine target entity and grain if not provided
        let entity = target_entity.map(|e| e.to_string())
            .unwrap_or_else(|| best_match.entity.clone());

        // Find entity definition to get default grain
        let entity_def = self.metadata.entities.iter()
            .find(|e| e.id == entity || e.name == entity);

        let grain = if let Some(grain_cols) = target_grain {
            grain_cols.to_vec()
        } else if let Some(entity_def) = entity_def {
            entity_def.grain.clone()
        } else {
            // Try to infer grain from table primary key
            let table = self.metadata.tables.iter()
                .find(|t| t.name == best_match.table_name);
            
            if let Some(table) = table {
                table.primary_key.clone()
            } else {
                vec!["uuid".to_string()] // Default fallback
            }
        };

        // Build rule
        let rule = crate::metadata::Rule {
            id: format!("auto_{}_{}_rule", system, metric_name.replace(" ", "_")),
            system: system.to_string(),
            metric: metric_name.to_string(),
            target_entity: entity,
            target_grain: grain.clone(),
            computation: crate::metadata::ComputationDefinition {
                description: format!(
                    "Auto-generated rule: {} from {} system {} table",
                    metric_name, system, best_match.table_name
                ),
                source_entities: vec![best_match.entity.clone()],
                attributes_needed: {
                    let mut attrs = HashMap::new();
                    attrs.insert(best_match.entity.clone(), vec![best_match.column_name.clone()]);
                    attrs
                },
                formula: best_match.column_name.clone(),
                aggregation_grain: grain,
                filter_conditions: None,
                source_table: Some(best_match.table_name.clone()),
                note: Some(format!(
                    "Auto-generated via semantic resolution (confidence: {:.0}%, reason: {})",
                    best_match.confidence * 100.0,
                    best_match.match_reason
                )),
            },
            labels: Some(vec![metric_name.to_string(), system.to_string()]),
        };

        Ok(Some(rule))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Metadata, Table, ColumnMetadata};

    fn create_test_metadata() -> Metadata {
        let mut metadata = Metadata {
            tables: vec![
                Table {
                    name: "asset_sourcing_personal_details_v2".to_string(),
                    entity: "customer".to_string(),
                    primary_key: vec!["cif".to_string()],
                    time_column: None,
                    system: "los_system".to_string(),
                    path: "test.csv".to_string(),
                    columns: Some(vec![
                        ColumnMetadata {
                            name: "social_category".to_string(),
                            description: Some("Social/Minority category".to_string()),
                            data_type: Some("string".to_string()),
                            distinct_values: None,
                        },
                    ]),
                    labels: None,
                },
            ],
            entities: vec![],
            metrics: vec![],
            rules: vec![],
            lineage: crate::metadata::LineageGraph {
                nodes: vec![],
                edges: vec![],
            },
            business_labels: crate::metadata::BusinessLabel::Array(vec![]),
        };
        metadata
    }

    #[test]
    fn test_resolve_social_category() {
        let metadata = create_test_metadata();
        let resolver = SemanticColumnResolver::new(metadata);

        // Test exact match
        let results = resolver.resolve_metric_to_column("social_category", Some("los_system"))
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].column_name, "social_category");
        assert_eq!(results[0].table_name, "asset_sourcing_personal_details_v2");
        assert!(results[0].confidence > 0.9);

        // Test semantic match via description
        let results = resolver.resolve_metric_to_column("social category", Some("los_system"))
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].column_name, "social_category");
        assert!(results[0].confidence > 0.8);
    }

    #[test]
    fn test_auto_generate_rule() {
        let metadata = create_test_metadata();
        let resolver = SemanticColumnResolver::new(metadata);

        let rule = resolver.auto_generate_rule("social category", "los_system", None, None)
            .unwrap();

        assert!(rule.is_some());
        let rule = rule.unwrap();
        assert_eq!(rule.metric, "social category");
        assert_eq!(rule.system, "los_system");
        assert_eq!(rule.computation.formula, "social_category");
        assert_eq!(rule.computation.source_table, Some("asset_sourcing_personal_details_v2".to_string()));
    }
}

