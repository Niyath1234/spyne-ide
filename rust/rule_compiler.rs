use crate::error::{RcaError, Result};
use crate::metadata::{Metadata, Rule, PipelineOp, Table};
use crate::operators::RelationalEngine;
use crate::time::TimeResolver;
use crate::semantic_column_resolver::SemanticColumnResolver;
use polars::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;

pub struct RuleCompiler {
    metadata: Metadata,
    engine: RelationalEngine,
    time_resolver: TimeResolver,
}

impl RuleCompiler {
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self {
            metadata: metadata.clone(),
            engine: RelationalEngine::new(data_dir),
            time_resolver: TimeResolver::new(metadata),
        }
    }
    
    /// Compile a rule into an execution plan by automatically constructing pipeline
    /// from rule specification + metadata
    /// 
    /// If rule is not found, attempts automatic inference using semantic column resolution
    pub fn compile(&self, rule_id: &str) -> Result<ExecutionPlan> {
        // First, try to find the rule in metadata
        let rule = if let Some(rule) = self.metadata.get_rule(rule_id) {
            rule.clone()
        } else {
            // Rule not found - try to infer it using semantic resolution
            // Parse rule_id to extract system and metric (format: "{system}_{metric}_rule" or similar)
            self.try_infer_rule_from_id(rule_id)?
                .ok_or_else(|| RcaError::Execution(format!(
                    "Rule not found: {}. Tried automatic inference but could not resolve table/column mapping.",
                    rule_id
                )))?
        };
        
        // Automatically construct pipeline from rule specification
        let steps = self.construct_pipeline(&rule)?;
        
        Ok(ExecutionPlan {
            rule_id: rule_id.to_string(),
            rule: rule.clone(),
            steps,
        })
    }

    /// Try to infer a rule from its ID using semantic column resolution
    /// 
    /// Rule IDs typically follow patterns like:
    /// - "{system}_{metric}_rule" (e.g., "los_system_social_category_rule")
    /// - "{system}_{metric}" (e.g., "los_system_social_category")
    fn try_infer_rule_from_id(&self, rule_id: &str) -> Result<Option<Rule>> {
        let resolver = SemanticColumnResolver::new(self.metadata.clone());
        
        // Try to parse system and metric from rule_id
        // Common patterns:
        // 1. "{system}_{metric}_rule" -> extract system and metric
        // 2. "{system}_{metric}" -> extract system and metric
        
        let parts: Vec<&str> = rule_id.split('_').collect();
        if parts.len() < 2 {
            return Ok(None);
        }

        // Try to find system and metric
        // Look for known systems in metadata
        let known_systems: Vec<String> = self.metadata.tables.iter()
            .map(|t| t.system.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        // Try different parsing strategies
        for system in &known_systems {
            // Strategy 1: "{system}_{metric}_rule"
            if rule_id.starts_with(&format!("{}_", system)) && rule_id.ends_with("_rule") {
                let metric_part = &rule_id[system.len() + 1..rule_id.len() - 5]; // Remove "{system}_" and "_rule"
                if let Some(rule) = resolver.auto_generate_rule(metric_part, system, None, None)? {
                    return Ok(Some(rule));
                }
            }
            
            // Strategy 2: "{system}_{metric}"
            if rule_id.starts_with(&format!("{}_", system)) {
                let metric_part = &rule_id[system.len() + 1..];
                if let Some(rule) = resolver.auto_generate_rule(metric_part, system, None, None)? {
                    return Ok(Some(rule));
                }
            }
        }

        // Strategy 3: Try to find metric name in rule_id and search across all systems
        // Extract potential metric name (everything after last system match or common patterns)
        let potential_metrics = vec![
            rule_id.to_string(),
            rule_id.replace("_rule", ""),
            rule_id.replace("_", " "),
        ];

        for metric in potential_metrics {
            let all_resolutions = resolver.find_columns_for_metric(&metric);
            if !all_resolutions.is_empty() {
                // Use the first system found (or could use highest confidence)
                for (system, resolutions) in all_resolutions {
                    if !resolutions.is_empty() {
                        if let Some(rule) = resolver.auto_generate_rule(&metric, &system, None, None)? {
                            return Ok(Some(rule));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Compile a rule by metric name and system (with automatic inference)
    /// 
    /// This is the preferred method when you have a metric name and system
    /// but don't know the exact rule ID. It will:
    /// 1. Try to find an existing rule
    /// 2. If not found, use semantic resolution to auto-generate one
    pub fn compile_by_metric(
        &self,
        metric_name: &str,
        system: &str,
    ) -> Result<ExecutionPlan> {
        // First, try to find existing rule
        let existing_rule = self.metadata.rules.iter()
            .find(|r| r.metric == metric_name && r.system == system);

        let rule = if let Some(rule) = existing_rule {
            rule.clone()
        } else {
            // No existing rule - use semantic resolution to auto-generate
            let resolver = SemanticColumnResolver::new(self.metadata.clone());
            resolver.auto_generate_rule(metric_name, system, None, None)?
                .ok_or_else(|| RcaError::Execution(format!(
                    "Could not find or infer rule for metric '{}' in system '{}'. \
                    No matching column found in metadata.",
                    metric_name, system
                )))?
        };

        // Construct pipeline
        let steps = self.construct_pipeline(&rule)?;
        
        Ok(ExecutionPlan {
            rule_id: format!("{}_{}_rule", system, metric_name.replace(" ", "_")),
            rule: rule.clone(),
            steps,
        })
    }
    
    /// Automatically construct pipeline from rule's computation definition
    fn construct_pipeline(&self, rule: &Rule) -> Result<Vec<PipelineOp>> {
        let mut steps = Vec::new();
        
        // Step 1: Check if formula is natural language and parse it first
        let is_natural_language = !rule.computation.formula.to_uppercase().contains("SUM(") &&
                                  !rule.computation.formula.to_uppercase().contains("AVG(") &&
                                  !rule.computation.formula.to_uppercase().contains("COUNT(");
        
        // If natural language, parse it to get structured formula, then extract columns
        let parsed_formula = if is_natural_language {
            self.parse_natural_language_formula(
                &rule.computation.formula,
                &rule.computation.description,
                &rule.computation.attributes_needed
            )?
        } else {
            rule.computation.formula.clone()
        };
        
        // Step 2: Extract column names from the (possibly parsed) formula
        let formula_columns = self.extract_formula_columns(&parsed_formula);
        
        // Step 3: Find all tables in this system that contain the needed columns
        let required_tables = self.find_tables_with_columns(&formula_columns, &rule.system);
        
        // Step 4: Map source entities to tables for this system (for fallback)
        let entity_to_tables: HashMap<String, Vec<&Table>> = rule.computation.source_entities
            .iter()
            .map(|entity| {
                let tables: Vec<&Table> = self.metadata.tables
                    .iter()
                    .filter(|t| t.entity == *entity && t.system == rule.system)
                    .collect();
                (entity.clone(), tables)
            })
            .collect();
        
        // Check that all entities have at least one table
        for entity in &rule.computation.source_entities {
            if entity_to_tables.get(entity).map_or(true, |t| t.is_empty()) {
                return Err(RcaError::Execution(format!(
                    "No table found for entity '{}' in system '{}'",
                    entity, rule.system
                )));
            }
        }
        
        // Step 5: Determine root table - prefer table that has the target grain or most formula columns
        let root_entity = &rule.target_entity;
        let root_tables = entity_to_tables.get(root_entity)
            .ok_or_else(|| RcaError::Execution(format!("No tables for root entity: {}", root_entity)))?;
        
        // Choose root table based on: has target grain columns, and ideally some formula columns
        let root_table = root_tables.iter()
            .max_by_key(|t| {
                let has_grain = rule.target_grain.iter()
                    .all(|g| t.columns.as_ref().map_or(false, |cols| cols.iter().any(|c| c.name == *g)));
                let formula_col_count = if let Some(cols) = &t.columns {
                    cols.iter().filter(|c| formula_columns.contains(&c.name)).count()
                } else {
                    0
                };
                (has_grain as usize * 100) + formula_col_count
            })
            .ok_or_else(|| RcaError::Execution(format!("No root table found for entity: {}", root_entity)))?;
        
        // Step 6: Build join plan to include all required tables
        let mut visited_tables = HashSet::new();
        visited_tables.insert(root_table.name.clone());
        
        // Start with root table scan
        steps.push(PipelineOp::Scan { table: root_table.name.clone() });
        
        // First, join tables that have formula columns (even within the same entity)
        for (table_name, _columns) in &required_tables {
            if visited_tables.contains(table_name) {
                continue;
            }
            
            // Find join path from root to this table
            match self.find_join_path_to_table(&root_table.name, table_name, &visited_tables) {
                Ok(join_path) => {
                    for join_step in join_path {
                        if !visited_tables.contains(&join_step.to) {
                            let join_type = self.determine_join_type(&join_step.from, &join_step.to)?;
                            let join_keys: Vec<String> = join_step.keys.keys().cloned().collect();
                            
                            steps.push(PipelineOp::Join {
                                table: join_step.to.clone(),
                                on: join_keys,
                                join_type,
                            });
                            
                            visited_tables.insert(join_step.to.clone());
                        }
                    }
                }
                Err(_) => {
                    // If no direct lineage path, try joining on common grain columns
                    if let Some(table) = self.metadata.tables.iter().find(|t| t.name == *table_name) {
                        // Find common grain columns between root table and this table
                        let common_grain: Vec<String> = rule.target_grain.iter()
                            .filter(|g| {
                                table.columns.as_ref().map_or(false, |cols| cols.iter().any(|c| c.name == **g))
                            })
                            .cloned()
                            .collect();
                        
                        if !common_grain.is_empty() {
                            steps.push(PipelineOp::Join {
                                table: table_name.clone(),
                                on: common_grain,
                                join_type: "left".to_string(),
                            });
                            visited_tables.insert(table_name.clone());
                        }
                    }
                }
            }
        }
        
        // Also join tables for other entities (original logic)
        for entity in &rule.computation.source_entities {
            if *entity == *root_entity {
                continue;
            }
            
            let entity_tables = entity_to_tables.get(entity)
                .ok_or_else(|| RcaError::Execution(format!("No tables for entity: {}", entity)))?;
            
            for entity_table in entity_tables {
                if visited_tables.contains(&entity_table.name) {
                    continue;
                }
                
                // Find join path from root to this entity table
                // BFS will find path through any intermediate nodes
                let join_path = self.find_join_path_to_table(&root_table.name, &entity_table.name, &visited_tables)?;
                
                for join_step in join_path {
                    if !visited_tables.contains(&join_step.to) {
                        // Determine join type from lineage relationship
                        let join_type = self.determine_join_type(&join_step.from, &join_step.to)?;
                        let join_keys: Vec<String> = join_step.keys.keys().cloned().collect();
                        
                        // Note: Aggregation will be handled inline during join execution
                        // if the table grain is higher than target grain
                        steps.push(PipelineOp::Join {
                            table: join_step.to.clone(),
                            on: join_keys,
                            join_type,
                        });
                        
                        visited_tables.insert(join_step.to.clone());
                    }
                }
            }
        }
        
        // Step 7: Parse formula to determine if we need derive + aggregate or just select
        // If formula contains SUM/AVG/etc, it means: derive intermediate, then aggregate
        // If formula is just a column name, just select that column (with optional group by)
        
        // Use the parsed formula (already converted from natural language if needed)
        let formula_without_where = parsed_formula
            .split(" WHERE ")
            .next()
            .unwrap_or(&parsed_formula)
            .to_string();
        
        let formula_upper = formula_without_where.to_uppercase();
        let has_aggregation = formula_upper.contains("SUM(") || formula_upper.contains("AVG(") || 
                             formula_upper.contains("COUNT(") || formula_upper.contains("MAX(") || 
                             formula_upper.contains("MIN(");
        
        // Count how many aggregation functions are in the formula
        let sum_count = formula_upper.matches("SUM(").count();
        let avg_count = formula_upper.matches("AVG(").count();
        let count_count = formula_upper.matches("COUNT(").count();
        let total_agg_count = sum_count + avg_count + count_count;
        
        if has_aggregation && total_agg_count > 1 {
            // Complex formula with multiple aggregations like:
            // "SUM(account_balance) + SUM(transaction_amount) - SUM(writeoff_amount)"
            // Strategy: 
            // 1. Extract all column names from aggregation functions
            // 2. Create aggregation for each column
            // 3. Derive the final metric by combining the aggregated values
            
            let mut agg_map = HashMap::new();
            let mut derive_formula = formula_without_where.clone();
            
            // Use regex to extract all SUM(column), AVG(column), etc.
            if let Ok(re) = regex::Regex::new(r"(SUM|AVG|COUNT|MAX|MIN)\((\w+)\)") {
                for caps in re.captures_iter(&formula_without_where) {
                    if let (Some(agg_func), Some(col_name)) = (caps.get(1), caps.get(2)) {
                        let func = agg_func.as_str().to_uppercase();
                        let col = col_name.as_str();
                        let agg_alias = format!("_agg_{}", col);
                        
                        // Add to aggregation map
                        agg_map.insert(agg_alias.clone(), format!("{}({})", func, col));
                        
                        // Replace in derive formula: SUM(col) -> _agg_col
                        let pattern = format!("{}({})", agg_func.as_str(), col);
                        derive_formula = derive_formula.replace(&pattern, &agg_alias);
                        
                        // Also handle uppercase version
                        let pattern_upper = format!("{}({})", func, col);
                        derive_formula = derive_formula.replace(&pattern_upper, &agg_alias);
                    }
                }
            }
            
            // Step 4a: Group and aggregate all columns first
            steps.push(PipelineOp::Group {
                by: rule.computation.aggregation_grain.clone(),
                agg: agg_map,
            });
            
            // Step 4b: Derive the final metric by combining aggregated values
            steps.push(PipelineOp::Derive {
                expr: derive_formula.trim().to_string(),
                r#as: rule.metric.clone(),
            });
        } else if has_aggregation {
            // Single aggregation formula like "SUM(emi_amount - COALESCE(transaction_amount, 0))"
            // Step 4a: Derive intermediate column first
            // Extract inner expression from the single aggregation function
            if let Ok(re) = regex::Regex::new(r"(SUM|AVG|COUNT|MAX|MIN)\((.+)\)") {
                if let Some(caps) = re.captures(&formula_without_where) {
                    if let (Some(agg_func), Some(inner)) = (caps.get(1), caps.get(2)) {
                        let func = agg_func.as_str().to_uppercase();
                        let inner_expr = inner.as_str().to_string();
                        
                        let intermediate_col = "computed_value".to_string();
                        steps.push(PipelineOp::Derive {
                            expr: inner_expr,
                            r#as: intermediate_col.clone(),
                        });
                        
                        // Step 4b: Group and aggregate
                        let mut agg_map = HashMap::new();
                        agg_map.insert(rule.metric.clone(), format!("{}({})", func, intermediate_col));
                        
                        steps.push(PipelineOp::Group {
                            by: rule.computation.aggregation_grain.clone(),
                            agg: agg_map,
                        });
                    }
                }
            } else {
                // Fallback: use old parsing logic
                let agg_func_start = formula_upper.find('(').unwrap_or(0);
                let mut inner_expr = formula_without_where[agg_func_start+1..].to_string();
                if inner_expr.ends_with(')') {
                    inner_expr.pop();
                }
                
                let intermediate_col = "computed_value".to_string();
                steps.push(PipelineOp::Derive {
                    expr: inner_expr,
                    r#as: intermediate_col.clone(),
                });
                
                let mut agg_map = HashMap::new();
                agg_map.insert(rule.metric.clone(), format!("SUM({})", intermediate_col));
                
                steps.push(PipelineOp::Group {
                    by: rule.computation.aggregation_grain.clone(),
                    agg: agg_map,
                });
            }
        } else {
            // Formula is a direct column reference like "total_outstanding"
            // If we need aggregation grain, group by it, otherwise just rename in select
            if !rule.computation.aggregation_grain.is_empty() && 
               rule.computation.aggregation_grain != rule.target_grain {
                // Need to group by aggregation grain
                let mut agg_map = HashMap::new();
                agg_map.insert(rule.metric.clone(), formula_without_where.clone());
                steps.push(PipelineOp::Group {
                    by: rule.computation.aggregation_grain.clone(),
                    agg: agg_map,
                });
            }
            // If no special aggregation needed, we'll rename the column in the select step
        }
        
        // Step 6: Select final columns (grain + metric)
        let mut final_columns = rule.target_grain.clone();
        // For direct column formulas, alias the column to the metric name
        if !has_aggregation {
            final_columns.push(format!("{} as {}", rule.computation.formula, rule.metric));
        } else {
            final_columns.push(rule.metric.clone());
        }
        steps.push(PipelineOp::Select { columns: final_columns });
        
        Ok(steps)
    }
    
    /// Find join path from a source table to a target table using lineage
    /// Returns the shortest path through lineage edges (can include intermediate nodes)
    fn find_join_path_to_table(
        &self,
        from: &str,
        to: &str,
        visited: &HashSet<String>,
    ) -> Result<Vec<JoinPathStep>> {
        // BFS to find shortest path - intermediate nodes are allowed
        let mut queue = VecDeque::new();
        queue.push_back((from.to_string(), vec![]));
        let mut seen = HashSet::new();
        seen.insert(from.to_string());
        // Don't add visited to seen - allow traversal through already-visited nodes
        // We just need to find a path, not avoid visited nodes
        
        while let Some((current, path)) = queue.pop_front() {
            if current == to {
                return Ok(path);
            }
            
            // Check all lineage edges from current node
            for edge in &self.metadata.lineage.edges {
                // Forward direction
                if edge.from == current && !seen.contains(&edge.to) {
                    let mut new_path = path.clone();
                    new_path.push(JoinPathStep {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                        keys: edge.keys.clone(),
                    });
                    
                    if edge.to == to {
                        return Ok(new_path);
                    }
                    
                    seen.insert(edge.to.clone());
                    queue.push_back((edge.to.clone(), new_path));
                }
                
                // Reverse direction (if bidirectional joins are supported)
                if edge.to == current && !seen.contains(&edge.from) {
                    // Create reverse edge keys
                    let mut reverse_keys = HashMap::new();
                    for (k, v) in &edge.keys {
                        reverse_keys.insert(v.clone(), k.clone());
                    }
                    
                    let mut new_path = path.clone();
                    new_path.push(JoinPathStep {
                        from: edge.to.clone(),
                        to: edge.from.clone(),
                        keys: reverse_keys,
                    });
                    
                    if edge.from == to {
                        return Ok(new_path);
                    }
                    
                    seen.insert(edge.from.clone());
                    queue.push_back((edge.from.clone(), new_path));
                }
            }
        }
        
        Err(RcaError::Execution(format!(
            "No join path found from {} to {} (checked {} edges)",
            from, to, self.metadata.lineage.edges.len()
        )))
    }
    
    /// Determine join type from lineage relationship
    fn determine_join_type(&self, from: &str, to: &str) -> Result<String> {
        // Check lineage edges for relationship type
        for edge in &self.metadata.lineage.edges {
            if edge.from == from && edge.to == to {
                match edge.relationship.as_str() {
                    "one_to_many" | "one_to_one" => return Ok("left".to_string()),
                    "many_to_one" => return Ok("inner".to_string()),
                    "many_to_many" => return Ok("inner".to_string()),
                    _ => return Ok("left".to_string()), // Default to left join
                }
            }
            // Check reverse
            if edge.from == to && edge.to == from {
                match edge.relationship.as_str() {
                    "one_to_many" | "many_to_one" => return Ok("inner".to_string()),
                    "one_to_one" => return Ok("left".to_string()),
                    "many_to_many" => return Ok("inner".to_string()),
                    _ => return Ok("left".to_string()),
                }
            }
        }
        
        // Default to left join if relationship not specified
        Ok("left".to_string())
    }
    
    /// Check if a table needs to be aggregated before joining
    /// Returns true if the table's grain (primary_key) is significantly higher (more granular) than the target grain
    /// We only aggregate tables that are at a much higher grain (like date-level) to avoid join explosions
    /// Tables that are close to target grain (like loan_id + emi_number) should be joined first, then aggregated
    fn table_needs_aggregation(&self, table: &Table, target_grain: &[String]) -> bool {
        // Use primary_key as proxy for grain (grain is often same as primary_key)
        let table_grain = &table.primary_key;
        
        // If table grain has significantly more elements than target grain (3+ more), it needs aggregation
        // This catches date-level tables like loan_id + date + type
        if table_grain.len() >= target_grain.len() + 2 {
            return true;
        }
        
        // If table grain has 1-2 more elements, check if the extra columns are date-related
        // Date-related tables should be aggregated before joining to avoid explosion
        if table_grain.len() > target_grain.len() {
            let extra_cols: Vec<_> = table_grain.iter()
                .filter(|col| !target_grain.contains(col))
                .collect();
            
            // If extra columns include date-related columns, aggregate
            for col in &extra_cols {
                if col.contains("date") || col.contains("Date") || col.contains("_date") {
                    return true;
                }
            }
        }
        
        // For tables close to target grain (like loan_id + emi_number), don't aggregate before joining
        // They'll be joined first, then aggregated together in the final step
        false
    }
    
    /// Extract column names from a formula (supports both SQL-like and natural language)
    fn extract_formula_columns(&self, formula: &str) -> Vec<String> {
        let mut columns = Vec::new();
        
        // Check if formula is SQL-like (contains SUM, AVG, etc.) or natural language
        let is_sql_like = formula.to_uppercase().contains("SUM(") || 
                         formula.to_uppercase().contains("AVG(") ||
                         formula.to_uppercase().contains("COUNT(");
        
        if is_sql_like {
            // SQL-like formula: Extract column names from aggregation functions
            if let Ok(re) = regex::Regex::new(r"(SUM|AVG|COUNT|MAX|MIN)\((\w+)\)") {
                for caps in re.captures_iter(formula) {
                    if let Some(col_match) = caps.get(2) {
                        columns.push(col_match.as_str().to_string());
                    }
                }
            }
        } else {
            // Natural language formula: Extract column names from text
            // Look for patterns like "account balance", "transaction amount", "writeoff amount"
            // Common patterns: "X of Y", "Y amounts", "Y values", etc.
            
            // First, try to extract from common natural language patterns
            let patterns = vec![
                r"(\w+)\s+balances?",           // "account balances"
                r"(\w+)\s+amounts?",           // "transaction amounts", "writeoff amounts"
                r"(\w+)\s+values?",            // "interest values"
                r"(\w+)\s+totals?",            // "disbursed totals"
                r"(\w+)\s+interests?",         // "accrued interests"
                r"(\w+)\s+penalties?",         // "waived penalties"
                r"(\w+)\s+repayments?",        // "repaid repayments"
                r"(\w+)\s+disbursements?",     // "disbursed disbursements"
            ];
            
            for pattern in patterns {
                if let Ok(re) = regex::Regex::new(pattern) {
                    for caps in re.captures_iter(&formula.to_lowercase()) {
                        if let Some(col_match) = caps.get(1) {
                            let word = col_match.as_str();
                            // Skip common stop words
                            if !["sum", "of", "the", "all", "for", "only", "and", "or", "minus", "plus"].contains(&word) {
                                // Convert to column name format (snake_case)
                                let col_name = format!("{}_amount", word);
                                // Try to find matching column in metadata
                                if self.column_exists_in_system(&col_name) {
                                    columns.push(col_name);
                                } else {
                                    // Try alternative: just the word
                                    if self.column_exists_in_system(word) {
                                        columns.push(word.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            // If no patterns matched, try to extract column names directly from description
            // Look for compound words that might be column names
            if columns.is_empty() {
                // Extract potential column names (words that might be column names)
                if let Ok(re) = regex::Regex::new(r"\b([a-z]+_[a-z_]+)\b") {
                    for caps in re.captures_iter(&formula.to_lowercase()) {
                        if let Some(col_match) = caps.get(1) {
                            let col = col_match.as_str();
                            // Skip common phrases
                            if !["account_status", "loan_status", "for_all", "only_for"].contains(&col) {
                                if self.column_exists_in_system(col) {
                                    columns.push(col.to_string());
                                }
                            }
                        }
                    }
                }
                
                // Also try single words that might be column prefixes
                if columns.is_empty() {
                    let words: Vec<&str> = formula.split_whitespace().collect();
                    for word in words {
                        let word_lower = word.to_lowercase();
                        // Try common column name patterns
                        let candidates = vec![
                            format!("{}_amount", word_lower),
                            format!("{}_balance", word_lower),
                            format!("{}_value", word_lower),
                            word_lower.clone(),
                        ];
                        
                        for candidate in candidates {
                            if self.column_exists_in_system(&candidate) {
                                columns.push(candidate);
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        // Remove duplicates and return
        let mut unique_columns: Vec<String> = columns.into_iter().collect::<std::collections::HashSet<_>>().into_iter().collect();
        unique_columns.sort();
        unique_columns
    }
    
    /// Check if a column exists in the system's tables
    fn column_exists_in_system(&self, column_name: &str) -> bool {
        self.metadata.tables.iter().any(|t| {
            t.columns.as_ref().map_or(false, |cols| {
                cols.iter().any(|c| c.name.to_lowercase() == column_name.to_lowercase())
            })
        })
    }
    
    /// Parse natural language formula and convert to SQL-like format
    /// Uses attributes_needed to map natural language to actual column names
    /// Example: "Sum of account balances plus transaction amounts minus writeoff amounts"
    /// -> "SUM(account_balance) + SUM(transaction_amount) - SUM(writeoff_amount)"
    fn parse_natural_language_formula(
        &self,
        formula: &str,
        description: &str,
        attributes_needed: &std::collections::HashMap<String, Vec<String>>,
    ) -> Result<String> {
        // Get all numeric columns from attributes_needed (exclude grain and status columns)
        let all_columns: Vec<String> = attributes_needed.values()
            .flat_map(|v| v.iter().cloned())
            .filter(|c| {
                let c_lower = c.to_lowercase();
                !c_lower.contains("_id") && 
                !c_lower.contains("status") && 
                !c_lower.contains("date") &&
                !c_lower.contains("type") &&
                !c_lower.contains("name")
            })
            .collect();
        
        // Simple approach: parse the formula to understand operations, then map to columns
        let formula_lower = formula.to_lowercase();
        
        // Count how many "plus" and "minus" operations
        let plus_count = formula_lower.matches(" plus ").count();
        let minus_count = formula_lower.matches(" minus ").count();
        let total_ops = plus_count + minus_count;
        
        // If we have the right number of columns, map them in order
        // Pattern: "X plus Y minus Z" -> use first 3 columns
        if all_columns.len() >= total_ops + 1 {
            let mut result = String::new();
            let mut col_idx = 0;
            
            // Build formula based on operations found
            for (i, part) in formula_lower.split(" plus ").enumerate() {
                if i > 0 {
                    result.push_str(" + ");
                }
                
                // Check if this part contains "minus"
                let subparts: Vec<&str> = part.split(" minus ").collect();
                for (j, subpart) in subparts.iter().enumerate() {
                    if j > 0 {
                        result.push_str(" - ");
                    }
                    
                    if col_idx < all_columns.len() {
                        result.push_str(&format!("SUM({})", all_columns[col_idx]));
                        col_idx += 1;
                    }
                }
            }
            
            if !result.is_empty() {
                return Ok(result);
            }
        }
        
        // Fallback: use description to extract column names
        // Description usually has: "Sum of X plus Y minus Z"
        let desc_lower = description.to_lowercase();
        
        // Try to extract column names from description
        let mut extracted_cols = Vec::new();
        for col in &all_columns {
            let col_phrase = col.replace("_", " ");
            if desc_lower.contains(&col_phrase) || desc_lower.contains(col) {
                extracted_cols.push(col.clone());
            }
        }
        
        // If we found columns, build formula
        if extracted_cols.len() >= 2 {
            let mut result = format!("SUM({})", extracted_cols[0]);
            for i in 1..extracted_cols.len() {
                // Determine operator from formula
                let op = if formula_lower.contains("minus") && i == extracted_cols.len() - 1 {
                    " - "
                } else {
                    " + "
                };
                result.push_str(&format!("{}SUM({})", op, extracted_cols[i]));
            }
            return Ok(result);
        }
        
        // Last resort: use first N columns from attributes_needed
        if all_columns.len() >= 3 {
            Ok(format!("SUM({}) + SUM({}) - SUM({})", all_columns[0], all_columns[1], all_columns[2]))
        } else if all_columns.len() == 2 {
            Ok(format!("SUM({}) + SUM({})", all_columns[0], all_columns[1]))
        } else if all_columns.len() == 1 {
            Ok(format!("SUM({})", all_columns[0]))
        } else {
            Err(RcaError::Execution(format!("Could not parse natural language formula: {}", formula)))
        }
    }
    
    /// Find column name that matches natural language phrase
    fn find_column_in_natural_language(&self, phrase: &str, columns: &[String]) -> Result<String> {
        let phrase_lower = phrase.to_lowercase();
        
        // Remove common words
        let cleaned = phrase_lower
            .replace("sum of", "")
            .replace("total of", "")
            .replace("all", "")
            .replace("the", "")
            .replace("for", "")
            .replace("only", "")
            .replace("amounts", "amount")
            .replace("balances", "balance")
            .replace("values", "value")
            .trim()
            .to_string();
        
        // Try exact match first
        for col in columns {
            let col_lower = col.to_lowercase();
            if col_lower == cleaned || cleaned == col_lower {
                return Ok(col.clone());
            }
        }
        
        // Try partial match
        for col in columns {
            let col_lower = col.to_lowercase();
            let col_words: Vec<&str> = col_lower.split('_').collect();
            let phrase_words: Vec<&str> = cleaned.split_whitespace().collect();
            
            // Check if all phrase words appear in column name
            if phrase_words.iter().all(|pw| {
                col_words.iter().any(|cw| cw.contains(pw) || pw.contains(cw))
            }) {
                return Ok(col.clone());
            }
        }
        
        // Try reverse: column words in phrase
        for col in columns {
            let col_lower = col.to_lowercase();
            let col_words: Vec<&str> = col_lower.split('_').collect();
            
            if col_words.iter().all(|cw| cleaned.contains(cw)) {
                return Ok(col.clone());
            }
        }
        
        // Fallback: construct from phrase
        let words: Vec<&str> = cleaned.split_whitespace().collect();
        if words.len() >= 2 {
            Ok(words.join("_"))
        } else if words.len() == 1 {
            Ok(words[0].to_string())
        } else {
            Err(RcaError::Execution(format!("Could not find column for phrase: {}", phrase)))
        }
    }
    
    
    /// Find all tables in the system that contain the given columns
    fn find_tables_with_columns(&self, columns: &[String], system: &str) -> HashMap<String, Vec<String>> {
        let mut result = HashMap::new();
        
        for table in &self.metadata.tables {
            if table.system != system {
                continue;
            }
            
            if let Some(table_cols) = &table.columns {
                let matching_cols: Vec<String> = columns.iter()
                    .filter(|col| table_cols.iter().any(|tc| tc.name == **col))
                    .cloned()
                    .collect();
                
                if !matching_cols.is_empty() {
                    result.insert(table.name.clone(), matching_cols);
                }
            }
        }
        
        result
    }
    
    /// Get aggregation columns for a table when aggregating to target grain
    /// Sums all numeric columns, skips non-numeric columns that aren't in target grain
    fn get_aggregation_columns(&self, table: &Table, target_grain: &[String]) -> HashMap<String, String> {
        let mut agg_map = HashMap::new();
        
        // For each column in the table, determine aggregation
        if let Some(columns) = &table.columns {
            for col in columns {
                // Skip grain columns (they're in the GROUP BY)
                if target_grain.contains(&col.name) {
                    continue;
                }
                
                // Determine aggregation based on column type
                // Use data_type if available, otherwise default to string
                let col_type = col.data_type.as_deref().unwrap_or("string");
                match col_type {
                    "float" | "integer" | "numeric" | "double" => {
                        // Sum numeric columns
                        agg_map.insert(col.name.clone(), format!("SUM({})", col.name));
                    }
                    _ => {
                        // Skip non-numeric columns that aren't in target grain
                        // They won't be needed for the final aggregation
                    }
                }
            }
        }
        
        agg_map
    }
}

#[derive(Debug, Clone)]
struct JoinPathStep {
    from: String,
    to: String,
    keys: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub rule_id: String,
    pub rule: Rule,
    pub steps: Vec<crate::metadata::PipelineOp>,
}

pub struct RuleExecutor {
    compiler: RuleCompiler,
}

impl RuleExecutor {
    pub fn new(compiler: RuleCompiler) -> Self {
        Self { compiler }
    }
    
    /// Execute a rule and return the result dataframe
    pub async fn execute(
        &self,
        rule_id: &str,
        as_of_date: Option<chrono::NaiveDate>,
    ) -> Result<DataFrame> {
        let plan = self.compiler.compile(rule_id)?;
        
        let mut result: Option<DataFrame> = None;
        let mut current_table: Option<String> = None;
        
        for (step_idx, step) in plan.steps.iter().enumerate() {
            // Apply time filtering for scan operations
            if let crate::metadata::PipelineOp::Scan { table } = step {
                // Use metadata to get correct table path
                let mut df = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
                
                // Apply as-of filtering
                if let Some(date) = as_of_date {
                    df = self.compiler.time_resolver.apply_as_of(df, table, Some(date))?;
                }
                
                result = Some(df);
                current_table = Some(table.clone());
                continue;
            }
            
            // Execute operation - for joins, we also need to use metadata for table paths
            if let crate::metadata::PipelineOp::Join { table, on, join_type } = step {
                // Check if this table needs aggregation before joining
                let target_table = self.compiler.metadata.tables.iter()
                    .find(|t| t.name == *table)
                    .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", table)))?;
                
                let rule = &plan.rule;
                let needs_aggregation = self.compiler.table_needs_aggregation(target_table, &rule.computation.aggregation_grain);
                
                let mut right = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
                
                // Apply as-of filtering if needed
                if let Some(date) = as_of_date {
                    right = self.compiler.time_resolver.apply_as_of(right, table, Some(date))?;
                }
                
                // Aggregate if needed before joining
                // Include join keys in GROUP BY to preserve them for the join
                if needs_aggregation {
                    // Combine target grain and join keys for GROUP BY
                    let mut group_by_cols = rule.computation.aggregation_grain.clone();
                    for join_key in on {
                        if !group_by_cols.contains(join_key) {
                            group_by_cols.push(join_key.clone());
                        }
                    }
                    
                    // Check if GROUP BY matches the table's original grain (primary_key)
                    // If so, no aggregation is needed - the table is already at this grain
                    let table_grain = &target_table.primary_key;
                    let group_by_matches_grain = group_by_cols.len() == table_grain.len() &&
                        group_by_cols.iter().all(|col| table_grain.contains(col)) &&
                        table_grain.iter().all(|col| group_by_cols.contains(col));
                    
                    if !group_by_matches_grain {
                        let agg_columns = self.compiler.get_aggregation_columns(target_table, &group_by_cols);
                        // Only aggregate if we have columns to aggregate
                        if !agg_columns.is_empty() {
                            right = self.compiler.engine.execute_op(
                                &crate::metadata::PipelineOp::Group {
                                    by: group_by_cols,
                                    agg: agg_columns,
                                },
                                Some(right),
                                None,
                            ).await?;
                        }
                    }
                    // If GROUP BY matches original grain, no aggregation needed - use table as-is
                }
                
                let left = result.unwrap();
                result = Some(
                    self.compiler.engine.join(left, right, on, join_type).await?
                );
                continue;
            }
            
            // For other operations
            result = Some(
                self.compiler.engine.execute_op(step, result, None).await?
            );
        }
        
        result.ok_or_else(|| RcaError::Execution("No result from rule execution".to_string()))
    }
    
    /// Execute with step-by-step tracking for drilldown
    pub async fn execute_with_steps(
        &self,
        rule_id: &str,
        as_of_date: Option<chrono::NaiveDate>,
    ) -> Result<Vec<ExecutionStep>> {
        let plan = self.compiler.compile(rule_id)?;
        
        let mut steps = Vec::new();
        let mut result: Option<DataFrame> = None;
        let mut current_table: Option<String> = None;
        
        for (step_idx, step) in plan.steps.iter().enumerate() {
            let step_name = format!("step_{}", step_idx);
            
            if let crate::metadata::PipelineOp::Scan { table } = step {
                let mut df = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
                
                if let Some(date) = as_of_date {
                    df = self.compiler.time_resolver.apply_as_of(df, table, Some(date))?;
                }
                
                steps.push(ExecutionStep {
                    step_name: step_name.clone(),
                    operation: format!("{:?}", step),
                    row_count: df.height(),
                    columns: df.get_column_names().iter().map(|s| s.to_string()).collect(),
                    data: Some(df.clone()),
                });
                
                result = Some(df);
                current_table = Some(table.clone());
                continue;
            }
            
            // Handle join separately to use metadata
            if let crate::metadata::PipelineOp::Join { table, on, join_type } = step {
                // Check if this table needs aggregation before joining
                let target_table = self.compiler.metadata.tables.iter()
                    .find(|t| t.name == *table)
                    .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", table)))?;
                
                let rule = &plan.rule;
                let needs_aggregation = self.compiler.table_needs_aggregation(target_table, &rule.computation.aggregation_grain);
                
                let mut right = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
                
                // Apply as-of filtering if needed
                if let Some(date) = as_of_date {
                    right = self.compiler.time_resolver.apply_as_of(right, table, Some(date))?;
                }
                
                // Aggregate if needed before joining
                // Include join keys in GROUP BY to preserve them for the join
                if needs_aggregation {
                    // Combine target grain and join keys for GROUP BY
                    let mut group_by_cols = rule.computation.aggregation_grain.clone();
                    for join_key in on {
                        if !group_by_cols.contains(join_key) {
                            group_by_cols.push(join_key.clone());
                        }
                    }
                    
                    // Check if GROUP BY matches the table's original grain (primary_key)
                    // If so, no aggregation is needed - the table is already at this grain
                    let table_grain = &target_table.primary_key;
                    let group_by_matches_grain = group_by_cols.len() == table_grain.len() &&
                        group_by_cols.iter().all(|col| table_grain.contains(col)) &&
                        table_grain.iter().all(|col| group_by_cols.contains(col));
                    
                    if !group_by_matches_grain {
                        let agg_columns = self.compiler.get_aggregation_columns(target_table, &group_by_cols);
                        // Only aggregate if we have columns to aggregate
                        if !agg_columns.is_empty() {
                            right = self.compiler.engine.execute_op(
                                &crate::metadata::PipelineOp::Group {
                                    by: group_by_cols,
                                    agg: agg_columns,
                                },
                                Some(right),
                                None,
                            ).await?;
                        }
                    }
                    // If GROUP BY matches original grain, no aggregation needed - use table as-is
                }
                
                let left = result.unwrap();
                let df = self.compiler.engine.join(left, right, on, join_type).await?;
                
                steps.push(ExecutionStep {
                    step_name: step_name.clone(),
                    operation: format!("{:?}", step),
                    row_count: df.height(),
                    columns: df.get_column_names().iter().map(|s| s.to_string()).collect(),
                    data: Some(df.clone()),
                });
                
                result = Some(df);
                continue;
            }
            
            // For other operations
            let df = self.compiler.engine.execute_op(step, result.clone(), None).await?;
            
            steps.push(ExecutionStep {
                step_name: step_name.clone(),
                operation: format!("{:?}", step),
                row_count: df.height(),
                columns: df.get_column_names().iter().map(|s| s.to_string()).collect(),
                data: Some(df.clone()),
            });
            
            result = Some(df);
        }
        
        Ok(steps)
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionStep {
    pub step_name: String,
    pub operation: String,
    pub row_count: usize,
    pub columns: Vec<String>,
    pub data: Option<DataFrame>,
}

