use crate::data_utils;
use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use crate::graph::{Hypergraph, JoinStep};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};

/// Intelligent grain resolver that can automatically discover join paths
/// to resolve granularity mismatches between systems
pub struct GrainResolver {
    metadata: Metadata,
    graph: Hypergraph,
}

#[derive(Debug, Clone)]
pub struct GrainResolutionPlan {
    pub source_grain: Vec<String>,
    pub target_grain: Vec<String>,
    pub join_path: Vec<JoinStep>,
    pub aggregation_required: bool,
    pub description: String,
}

impl GrainResolver {
    pub fn new(metadata: Metadata) -> Self {
        let graph = Hypergraph::new(metadata.clone());
        Self { metadata, graph }
    }

    /// Resolve grain mismatch by finding join paths to get target grain columns
    /// 
    /// Example:
    /// - System A has loan-level grain: ["loan_id"]
    /// - System B has customer-level grain: ["customer_id"]
    /// - This will find: loans → customers join path
    /// - Then aggregate loan-level metrics to customer-level
    pub fn resolve_grain_mismatch(
        &self,
        system: &str,
        source_grain: &[String],
        target_grain: &[String],
        root_table: &str,
    ) -> Result<Option<GrainResolutionPlan>> {
        // If grains match, no resolution needed
        if source_grain == target_grain {
            return Ok(None);
        }

        println!("    Detecting grain mismatch:");
        println!("      Source Grain: {:?}", source_grain);
        println!("      Target Grain: {:?}", target_grain);
        println!("      Root Table: {}", root_table);

        // Check if we already have all target grain columns in root table
        let root_table_obj = self.metadata
            .get_table(root_table)
            .ok_or_else(|| RcaError::Graph(format!("Table not found: {}", root_table)))?;

        // Get entity for root table
        let root_entity = &root_table_obj.entity;
        
        // Check if target grain columns exist in root table's entity attributes
        let root_entity_obj = self.metadata.entities_by_id
            .get(root_entity)
            .ok_or_else(|| RcaError::Graph(format!("Entity not found: {}", root_entity)))?;

        // Get actual columns in the root table (not just entity attributes)
        let root_table_obj = self.metadata
            .get_table(root_table)
            .ok_or_else(|| RcaError::Graph(format!("Root table not found: {}", root_table)))?;
        
        let root_table_columns: HashSet<String> = root_table_obj.columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default();
        
        let mut missing_columns = Vec::new();
        for target_col in target_grain {
            if !source_grain.contains(target_col) {
                // Check if column actually exists in the root table (not just entity attributes)
                if !root_table_columns.contains(target_col) {
                    missing_columns.push(target_col.clone());
                }
            }
        }

        // Check if aggregation is required (e.g., loan_id → customer_id)
        let aggregation_needed = source_grain != target_grain && 
            !target_grain.iter().all(|col| source_grain.contains(col));
        
        // If all target columns exist in root table (no joins needed for columns)
        if missing_columns.is_empty() {
            if aggregation_needed {
                return Ok(Some(GrainResolutionPlan {
                    source_grain: source_grain.to_vec(),
                    target_grain: target_grain.to_vec(),
                    join_path: vec![],
                    aggregation_required: true,
                    description: format!("All target grain columns available in root table. Aggregate from {:?} to {:?}", source_grain, target_grain),
                }));
            } else {
                return Ok(Some(GrainResolutionPlan {
                    source_grain: source_grain.to_vec(),
                    target_grain: target_grain.to_vec(),
                    join_path: vec![],
                    aggregation_required: false,
                    description: format!("All target grain columns available in root table. Just select: {:?}", target_grain),
                }));
            }
        }

        println!("   ️  Missing columns for target grain: {:?}", missing_columns);

        // Find join paths to get missing columns
        // Strategy: For each missing column, find which entity/table has it
        let mut join_path = Vec::new();
        let mut current_table = root_table.to_string();
        let mut found_columns = HashSet::new();
        found_columns.extend(source_grain.iter().cloned());

        for missing_col in &missing_columns {
            println!("    Looking for column '{}' in system '{}'", missing_col, system);
            
            // First: Find tables that actually have this column in their column metadata
            let tables_with_column: Vec<&crate::metadata::Table> = self.metadata.tables
                .iter()
                .filter(|t| {
                    t.system == system && 
                    t.columns.as_ref().map_or(false, |cols| {
                        cols.iter().any(|c| c.name == *missing_col)
                    })
                })
                .collect();
            
            if !tables_with_column.is_empty() {
                println!("      Found column in tables: {:?}", 
                    tables_with_column.iter().map(|t| &t.name).collect::<Vec<_>>());
                
                // Try to find join path from current table to any of these tables
                for target_table in &tables_with_column {
                    println!("      Trying join path from {} to {}", current_table, target_table.name);
                    
                    if let Ok(Some(path)) = self.graph.find_join_path(&current_table, &target_table.name) {
                        println!("       Found join path with {} steps", path.len());
                        join_path.extend(path);
                        current_table = target_table.name.clone();
                        found_columns.insert(missing_col.clone());
                        break;
                    } else {
                        // Try direct join using common keys (like loan_id)
                        // Check if both tables share a common key column
                        let current_table_obj = self.metadata.get_table(&current_table);
                        if let Some(curr) = current_table_obj {
                            if let (Some(curr_cols), Some(target_cols)) = (&curr.columns, &target_table.columns) {
                                let curr_col_names: HashSet<String> = curr_cols.iter().map(|c| c.name.clone()).collect();
                                let target_col_names: HashSet<String> = target_cols.iter().map(|c| c.name.clone()).collect();
                                
                                let common_cols: Vec<String> = curr_col_names.intersection(&target_col_names)
                                    .filter(|c| **c != *missing_col) // Don't use the column we're looking for
                                    .cloned()
                                    .collect();
                                
                                if !common_cols.is_empty() {
                                    println!("       Found common columns for direct join: {:?}", common_cols);
                                    let mut keys = std::collections::HashMap::new();
                                    for key in &common_cols {
                                        keys.insert(key.clone(), key.clone());
                                    }
                                    join_path.push(crate::graph::JoinStep {
                                        from: current_table.clone(),
                                        to: target_table.name.clone(),
                                        keys,
                                    });
                                    current_table = target_table.name.clone();
                                    found_columns.insert(missing_col.clone());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            
            // Fallback: Find which entity has this column (old approach)
            if !found_columns.contains(missing_col) {
                let target_entity = self.find_entity_with_column(missing_col, system)?;
                
                if let Some(entity) = target_entity {
                    let target_tables: Vec<&crate::metadata::Table> = self.metadata.tables
                        .iter()
                        .filter(|t| t.entity == entity && t.system == system)
                        .collect();

                    for target_table in target_tables {
                        if let Ok(Some(path)) = self.graph.find_join_path(&current_table, &target_table.name) {
                            join_path.extend(path);
                            current_table = target_table.name.clone();
                            found_columns.insert(missing_col.clone());
                            break;
                        }
                    }
                }
            }
        }

        // Check if we found all required columns
        let all_found = missing_columns.iter().all(|col| found_columns.contains(col));

        if !all_found {
            return Err(RcaError::Graph(format!(
                "Cannot resolve grain mismatch: Missing columns {:?} cannot be found via join paths",
                missing_columns.iter().filter(|c| !found_columns.contains(*c)).collect::<Vec<_>>()
            )));
        }

        // Determine if aggregation is required
        // Aggregation is needed if we're going from finer grain to coarser grain
        // e.g., loan_id → customer_id (many loans per customer)
        let aggregation_required = source_grain.len() > target_grain.len() ||
            !target_grain.iter().all(|col| source_grain.contains(col));

        let description = if aggregation_required {
            format!(
                "Join to {} tables, then aggregate from {:?} to {:?}",
                join_path.len(),
                source_grain,
                target_grain
            )
        } else {
            format!(
                "Join to {} tables to get columns {:?}",
                join_path.len(),
                missing_columns
            )
        };

        Ok(Some(GrainResolutionPlan {
            source_grain: source_grain.to_vec(),
            target_grain: target_grain.to_vec(),
            join_path,
            aggregation_required,
            description,
        }))
    }

    /// Find which entity has a specific column
    fn find_entity_with_column(&self, column: &str, system: &str) -> Result<Option<String>> {
        // Check all entities in the system
        for table in &self.metadata.tables {
            if table.system != system {
                continue;
            }

            // Check entity attributes
            if let Some(entity) = self.metadata.entities_by_id.get(&table.entity) {
                if entity.attributes.contains(&column.to_string()) {
                    return Ok(Some(table.entity.clone()));
                }
            }

            // Check table columns metadata if available
            if let Some(ref columns) = table.columns {
                for col_meta in columns {
                    if col_meta.name == column {
                        return Ok(Some(table.entity.clone()));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Apply grain resolution plan to a dataframe
    pub async fn apply_grain_resolution(
        &self,
        df: DataFrame,
        plan: &GrainResolutionPlan,
        metric_column: &str,
        root_table: &str,
        data_dir: &std::path::Path,
    ) -> Result<DataFrame> {
        let mut result = df;

        // Step 1: Apply joins if needed
        // Note: In a full implementation, we would:
        // 1. Load target tables from parquet files
        // 2. Use the operators module to perform actual joins
        // 3. Handle join types (left, inner, etc.)
        // For now, we'll log the plan and let the rule compiler handle joins
        // The joins should ideally be done during pipeline construction, not here
        if !plan.join_path.is_empty() {
            println!("    Join path required ({} steps):", plan.join_path.len());
            for (idx, join_step) in plan.join_path.iter().enumerate() {
                println!("      {}. {} → {} on {:?}", 
                    idx + 1,
                    join_step.from, 
                    join_step.to,
                    join_step.keys
                );
            }
            println!("   ️  Note: Joins should be handled during pipeline construction");
            println!("   ℹ️  This grain resolution plan will be used to modify the pipeline");
        }

        // Step 2: Apply aggregation if needed
        if plan.aggregation_required {
            println!("    Aggregating from {:?} to {:?}", 
                plan.source_grain, 
                plan.target_grain
            );

            // Check if target grain columns exist in the dataframe
            let existing_cols: HashSet<String> = result.get_column_names().iter().map(|s| s.to_string()).collect();
            let missing_cols: Vec<String> = plan.target_grain
                .iter()
                .filter(|col| !existing_cols.contains(*col))
                .cloned()
                .collect();

            if !missing_cols.is_empty() {
                // Target grain columns are missing - we need to get them via joins
                // Check if we have source grain columns to join
                let has_source_grain = plan.source_grain.iter().all(|col| existing_cols.contains(col));
                
                if has_source_grain && !plan.join_path.is_empty() {
                    // Use the join path to get the missing columns
                    println!("    Executing join path to get target grain columns:");
                    
                    for (idx, join_step) in plan.join_path.iter().enumerate() {
                        println!("      {}. {} → {} on {:?}", 
                            idx + 1, join_step.from, join_step.to, join_step.keys);
                        
                        // Load the target table
                        let join_table = self.metadata
                            .get_table(&join_step.to)
                            .ok_or_else(|| RcaError::Graph(format!("Join table not found: {}", join_step.to)))?;
                        
                        let join_table_path = data_dir.join(&join_table.path);
                        
                        // Load based on file extension
                        let join_df = if join_table_path.extension().and_then(|s| s.to_str()) == Some("csv") {
                            LazyCsvReader::new(&join_table_path)
                                .with_try_parse_dates(true)
                                .with_infer_schema_length(Some(1000))
                                .finish()
                                .map_err(|e| RcaError::Graph(format!("Failed to load CSV join table {}: {}", join_step.to, e)))?
                                .collect()
                                .map_err(|e| RcaError::Graph(format!("Failed to collect CSV join table {}: {}", join_step.to, e)))?
                        } else {
                            LazyFrame::scan_parquet(&join_table_path, ScanArgsParquet::default())
                                .map_err(|e| RcaError::Graph(format!("Failed to load join table {}: {}", join_step.to, e)))?
                                .collect()
                                .map_err(|e| RcaError::Graph(format!("Failed to collect join table {}: {}", join_step.to, e)))?
                        };
                        let join_df = data_utils::convert_scientific_notation_columns(join_df)
                            .map_err(|e| RcaError::Graph(format!("Failed to convert scientific notation in join table {}: {}", join_step.to, e)))?;
                        
                        // Build join expressions from key mapping
                        let left_cols: Vec<Expr> = join_step.keys.keys().map(|c| col(c)).collect();
                        let right_cols: Vec<Expr> = join_step.keys.values().map(|c| col(c)).collect();
                        
                        result = result
                            .lazy()
                            .join(
                                join_df.lazy(),
                                left_cols,
                                right_cols,
                                JoinArgs::new(JoinType::Left)
                            )
                            .collect()?;
                        
                        println!("       Joined to {} successfully", join_step.to);
                    }
                    
                    println!("    Join path completed - target grain columns should now be available");
                    
                } else if has_source_grain && plan.join_path.is_empty() {
                    // Columns should be available in the root table - load it and join
                    let root_table_obj = self.metadata
                        .get_table(root_table)
                        .ok_or_else(|| RcaError::Graph(format!("Table not found: {}", root_table)))?;
                    
                    let root_table_path = data_dir.join(&root_table_obj.path);
                    
                    // Load based on file extension
                    let root_df = if root_table_path.extension().and_then(|s| s.to_str()) == Some("csv") {
                        LazyCsvReader::new(&root_table_path)
                            .with_try_parse_dates(true)
                            .with_infer_schema_length(Some(1000))
                            .finish()
                            .map_err(|e| RcaError::Graph(format!("Failed to load CSV root table {}: {}", root_table, e)))?
                            .collect()
                            .map_err(|e| RcaError::Graph(format!("Failed to collect CSV root table {}: {}", root_table, e)))?
                    } else {
                        LazyFrame::scan_parquet(&root_table_path, ScanArgsParquet::default())
                            .map_err(|e| RcaError::Graph(format!("Failed to load root table {}: {}", root_table, e)))?
                            .collect()
                            .map_err(|e| RcaError::Graph(format!("Failed to collect root table {}: {}", root_table, e)))?
                    };
                    let root_df = data_utils::convert_scientific_notation_columns(root_df)
                        .map_err(|e| RcaError::Graph(format!("Failed to convert scientific notation in root table {}: {}", root_table, e)))?;
                    
                    // Join on source grain columns
                    let left_cols: Vec<Expr> = plan.source_grain.iter().map(|c| col(c)).collect();
                    let right_cols: Vec<Expr> = plan.source_grain.iter().map(|c| col(c)).collect();
                    result = result
                        .lazy()
                        .join(
                            root_df.lazy(),
                            left_cols,
                            right_cols,
                            JoinArgs::new(JoinType::Left)
                        )
                        .collect()?;
                    
                    println!("    Joined back to {} to get target grain columns", root_table);
                } else {
                    // Try to find a mapping table that can provide the missing columns
                    println!("    Looking for mapping table to provide missing columns: {:?}", missing_cols);
                    
                    // Search for a table in the same system that has both the source grain and target grain columns
                    let mut mapping_table_found = false;
                    for table in &self.metadata.tables {
                        if let Some(ref cols) = table.columns {
                            let table_col_names: HashSet<String> = cols.iter().map(|c| c.name.clone()).collect();
                            let has_source = plan.source_grain.iter().all(|c| table_col_names.contains(c));
                            let has_target = plan.target_grain.iter().all(|c| table_col_names.contains(c));
                            
                            if has_source && has_target {
                                println!("    Found mapping table: {}", table.name);
                                
                                let mapping_table_path = data_dir.join(&table.path);
                                let mapping_df = if mapping_table_path.extension().and_then(|s| s.to_str()) == Some("csv") {
                                    LazyCsvReader::new(&mapping_table_path)
                                        .with_try_parse_dates(true)
                                        .with_infer_schema_length(Some(1000))
                                        .finish()
                                        .map_err(|e| RcaError::Graph(format!("Failed to load CSV mapping table {}: {}", table.name, e)))?
                                        .collect()
                                        .map_err(|e| RcaError::Graph(format!("Failed to collect CSV mapping table {}: {}", table.name, e)))?
                                } else {
                                    LazyFrame::scan_parquet(&mapping_table_path, ScanArgsParquet::default())
                                        .map_err(|e| RcaError::Graph(format!("Failed to load mapping table {}: {}", table.name, e)))?
                                        .collect()
                                        .map_err(|e| RcaError::Graph(format!("Failed to collect mapping table {}: {}", table.name, e)))?
                                };
                                let mapping_df = data_utils::convert_scientific_notation_columns(mapping_df)
                                    .map_err(|e| RcaError::Graph(format!("Failed to convert scientific notation in mapping table {}: {}", table.name, e)))?;
                                
                                // Join on source grain columns
                                let left_cols: Vec<Expr> = plan.source_grain.iter().map(|c| col(c)).collect();
                                let right_cols: Vec<Expr> = plan.source_grain.iter().map(|c| col(c)).collect();
                                result = result
                                    .lazy()
                                    .join(
                                        mapping_df.lazy(),
                                        left_cols,
                                        right_cols,
                                        JoinArgs::new(JoinType::Left)
                                    )
                                    .collect()?;
                                
                                println!("    Joined to mapping table {} to get target grain columns", table.name);
                                mapping_table_found = true;
                                break;
                            }
                        }
                    }
                    
                    if !mapping_table_found {
                        return Err(RcaError::Graph(format!(
                            "Cannot aggregate: target grain columns {:?} are missing from dataframe and no mapping table found. \
                            Available columns: {:?}",
                            missing_cols, existing_cols
                        )));
                    }
                }
            }

            // Group by target grain columns
            let group_by_cols: Vec<Expr> = plan.target_grain
                .iter()
                .map(|col_name| col(col_name.as_str()))
                .collect();

            // Aggregate metric column
            let agg_exprs = vec![
                col(metric_column).sum().alias(metric_column),
            ];

            result = result
                .lazy()
                .group_by(group_by_cols)
                .agg(agg_exprs)
                .collect()?;
        }

        // Step 3: Select only target grain columns + metric
        let mut select_cols = plan.target_grain.clone();
        select_cols.push(metric_column.to_string());

        let select_exprs: Vec<Expr> = select_cols
            .iter()
            .map(|c| col(c.as_str()))
            .collect();

        result = result
            .lazy()
            .select(select_exprs)
            .collect()?;

        Ok(result)
    }

    /// Find common grain between two systems
    /// This tries to find the coarsest grain that both systems can support
    pub fn find_common_grain(
        &self,
        system_a: &str,
        grain_a: &[String],
        system_b: &str,
        grain_b: &[String],
        root_table_a: &str,
        root_table_b: &str,
    ) -> Result<Vec<String>> {
        // If grains match, use that
        if grain_a == grain_b {
            return Ok(grain_a.to_vec());
        }

        println!("    Finding common grain between:");
        println!("      System A Grain: {:?} (table: {})", grain_a, root_table_a);
        println!("      System B Grain: {:?} (table: {})", grain_b, root_table_b);

        // Strategy 1: Check if one grain is a subset of the other
        let grain_a_set: HashSet<&String> = grain_a.iter().collect();
        let grain_b_set: HashSet<&String> = grain_b.iter().collect();

        // If grain_a is subset of grain_b, we can aggregate B to A
        if grain_a_set.is_subset(&grain_b_set) {
            println!("    System A grain is subset of System B - can aggregate B to A");
            return Ok(grain_a.to_vec());
        }

        // If grain_b is subset of grain_a, we can aggregate A to B
        if grain_b_set.is_subset(&grain_a_set) {
            println!("    System B grain is subset of System A - can aggregate A to B");
            return Ok(grain_b.to_vec());
        }

        // Strategy 2: Find intersection (common columns)
        let intersection: Vec<String> = grain_a_set
            .intersection(&grain_b_set)
            .cloned()
            .cloned()
            .collect();

        if !intersection.is_empty() {
            println!("    Found common columns: {:?}", intersection);
            // Check if both systems can resolve to this grain
            let can_resolve_a = self.can_resolve_to_grain(system_a, grain_a, &intersection, root_table_a)?;
            let can_resolve_b = self.can_resolve_to_grain(system_b, grain_b, &intersection, root_table_b)?;

            if can_resolve_a && can_resolve_b {
                return Ok(intersection);
            }
        }

        // Strategy 3: Check if one system can resolve to the other's grain
        // Try System A → System B grain
        let can_resolve_a_to_b = self.can_resolve_to_grain(system_a, grain_a, grain_b, root_table_a)?;
        // Try System B → System A grain
        let can_resolve_b_to_a = self.can_resolve_to_grain(system_b, grain_b, grain_a, root_table_b)?;

        if can_resolve_a_to_b && can_resolve_b_to_a {
            // Both can resolve - prefer the coarser grain (fewer columns typically means coarser)
            // In case of loan_id vs customer_id, customer_id is coarser (one customer can have many loans)
            // We'll prefer the grain with fewer columns as it's typically coarser
            if grain_b.len() <= grain_a.len() {
                println!("    Both systems can resolve to each other's grain - using System B grain (coarser)");
                return Ok(grain_b.to_vec());
            } else {
                println!("    Both systems can resolve to each other's grain - using System A grain (coarser)");
                return Ok(grain_a.to_vec());
            }
        } else if can_resolve_a_to_b {
            // Only System A can resolve to System B's grain
            println!("    System A can resolve to System B grain - using System B grain");
            return Ok(grain_b.to_vec());
        } else if can_resolve_b_to_a {
            // Only System B can resolve to System A's grain
            println!("    System B can resolve to System A grain - using System A grain");
            return Ok(grain_a.to_vec());
        }

        // Strategy 4: If no resolution possible, return error
        Err(RcaError::Graph(format!(
            "Cannot find common grain. System A: {:?}, System B: {:?}. Need manual resolution.",
            grain_a.clone(), grain_b.clone()
        )))
    }

    /// Check if a system can resolve to a target grain
    /// 
    /// Important: Going from coarser grain to finer grain is NOT possible
    /// (e.g., customer_id → loan_id is not possible because one customer can have many loans)
    /// We can only go from finer to coarser (aggregation) or same grain (no change)
    pub fn can_resolve_to_grain(
        &self,
        system: &str,
        source_grain: &[String],
        target_grain: &[String],
        root_table: &str,
    ) -> Result<bool> {
        let source_set: HashSet<&String> = source_grain.iter().collect();
        let target_set: HashSet<&String> = target_grain.iter().collect();

        // If target is subset of source, can aggregate (finer → coarser)
        if target_set.is_subset(&source_set) {
            return Ok(true);
        }

        // If source is subset of target, cannot resolve (coarser → finer is not possible)
        // Example: customer_id → loan_id is not possible because one customer can have many loans
        if source_set.is_subset(&target_set) {
            return Ok(false);
        }

        // Check if we can join to get missing columns
        let missing: Vec<String> = target_grain
            .iter()
            .filter(|col| !source_grain.contains(*col))
            .cloned()
            .collect();

        if missing.is_empty() {
            return Ok(true);
        }

        // Check if target columns exist in the root table's entity attributes
        // If they do, we can aggregate (finer to coarser)
        let root_table_obj = self.metadata
            .get_table(root_table)
            .ok_or_else(|| RcaError::Graph(format!("Table not found: {}", root_table)))?;
        let root_entity = &root_table_obj.entity;
        let root_entity_obj = self.metadata.entities_by_id
            .get(root_entity)
            .ok_or_else(|| RcaError::Graph(format!("Entity not found: {}", root_entity)))?;

        // Check if all missing columns exist in entity attributes
        let all_missing_in_attributes = missing.iter()
            .all(|col| root_entity_obj.attributes.contains(col));

        if all_missing_in_attributes {
            // All missing columns exist in entity attributes - we can aggregate
            // This is finer to coarser (e.g., loan_id → customer_id)
            return Ok(true);
        }

        // Try to resolve grain mismatch (this handles cases where we need joins)
        // This is for cases like: loan_id → customer_id (finer to coarser via join + aggregation)
        // If resolve_grain_mismatch returns an error, we can't resolve (return false, not error)
        match self.resolve_grain_mismatch(system, source_grain, target_grain, root_table) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(_) => Ok(false), // If resolution fails, we can't resolve to this grain
        }
    }
}

