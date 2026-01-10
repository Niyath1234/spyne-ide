use crate::error::{RcaError, Result};
use crate::metadata::{Metadata, Rule, PipelineOp, Table};
use crate::operators::RelationalEngine;
use crate::time::TimeResolver;
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
    pub fn compile(&self, rule_id: &str) -> Result<ExecutionPlan> {
        let rule = self.metadata
            .get_rule(rule_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", rule_id)))?;
        
        // Automatically construct pipeline from rule specification
        let steps = self.construct_pipeline(rule)?;
        
        Ok(ExecutionPlan {
            rule_id: rule_id.to_string(),
            rule: rule.clone(),
            steps,
        })
    }
    
    /// Automatically construct pipeline from rule's computation definition
    fn construct_pipeline(&self, rule: &Rule) -> Result<Vec<PipelineOp>> {
        let mut steps = Vec::new();
        
        // Step 1: Map source entities to tables for this system
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
        
        // Step 2: Determine root table (usually the target entity's table)
        let root_entity = &rule.target_entity;
        let root_tables = entity_to_tables.get(root_entity)
            .ok_or_else(|| RcaError::Execution(format!("No tables for root entity: {}", root_entity)))?;
        let root_table = root_tables.first()
            .ok_or_else(|| RcaError::Execution(format!("No root table found for entity: {}", root_entity)))?;
        
        // Step 3: Build join plan - find shortest paths from root to all other entity tables
        let mut visited_tables = HashSet::new();
        visited_tables.insert(root_table.name.clone());
        
        // Start with root table scan
        steps.push(PipelineOp::Scan { table: root_table.name.clone() });
        
        // For each other entity, find join path and add joins
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
                
                // Find join path from root or any visited table to this entity table
                let join_path = self.find_join_path_to_table(&root_table.name, &entity_table.name, &visited_tables)?;
                
                for join_step in join_path {
                    if !visited_tables.contains(&join_step.to) {
                        // Determine join type from lineage relationship
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
        }
        
        // Step 4: Parse formula to determine if we need derive + aggregate or just select
        // If formula contains SUM/AVG/etc, it means: derive intermediate, then aggregate
        // If formula is just a column name, just select that column (with optional group by)
        
        let formula_upper = rule.computation.formula.to_uppercase();
        let has_aggregation = formula_upper.contains("SUM(") || formula_upper.contains("AVG(") || 
                             formula_upper.contains("COUNT(") || formula_upper.contains("MAX(") || 
                             formula_upper.contains("MIN(");
        
        if has_aggregation {
            // Formula like "SUM(emi_amount - COALESCE(transaction_amount, 0))"
            // Step 4a: Derive intermediate column first
            let agg_func_start = formula_upper.find("(").unwrap_or(0);
            let inner_expr = rule.computation.formula[agg_func_start+1..]
                .trim_start_matches('(')
                .trim_end_matches(')')
                .to_string();
            
            let intermediate_col = "computed_value".to_string(); // Temporary column
            steps.push(PipelineOp::Derive {
                expr: inner_expr.clone(),
                r#as: intermediate_col.clone(),
            });
            
            // Step 4b: Group and aggregate
            let mut agg_map = HashMap::new();
            if formula_upper.starts_with("SUM") {
                agg_map.insert(rule.metric.clone(), format!("SUM({})", intermediate_col));
            } else if formula_upper.starts_with("AVG") {
                agg_map.insert(rule.metric.clone(), format!("AVG({})", intermediate_col));
            } else if formula_upper.starts_with("COUNT") {
                agg_map.insert(rule.metric.clone(), format!("COUNT({})", intermediate_col));
            } else {
                // Default to SUM
                agg_map.insert(rule.metric.clone(), format!("SUM({})", intermediate_col));
            }
            
            steps.push(PipelineOp::Group {
                by: rule.computation.aggregation_grain.clone(),
                agg: agg_map,
            });
        } else {
            // Formula is a direct column reference like "total_outstanding"
            // If we need aggregation grain, group by it, otherwise just select
            if !rule.computation.aggregation_grain.is_empty() && 
               rule.computation.aggregation_grain != rule.target_grain {
                // Need to group by aggregation grain
                let mut agg_map = HashMap::new();
                agg_map.insert(rule.metric.clone(), rule.computation.formula.clone());
                steps.push(PipelineOp::Group {
                    by: rule.computation.aggregation_grain.clone(),
                    agg: agg_map,
                });
            }
            // If no special aggregation needed, formula column is already selected in final step
        }
        
        // Step 6: Select final columns (grain + metric)
        let mut final_columns = rule.target_grain.clone();
        final_columns.push(rule.metric.clone());
        steps.push(PipelineOp::Select { columns: final_columns });
        
        Ok(steps)
    }
    
    /// Find join path from a source table to a target table using lineage
    fn find_join_path_to_table(
        &self,
        from: &str,
        to: &str,
        visited: &HashSet<String>,
    ) -> Result<Vec<JoinPathStep>> {
        // BFS to find shortest path
        let mut queue = VecDeque::new();
        queue.push_back((from.to_string(), vec![]));
        let mut seen = HashSet::new();
        seen.insert(from.to_string());
        seen.extend(visited.iter().cloned());
        
        while let Some((current, mut path)) = queue.pop_front() {
            if current == to {
                return Ok(path);
            }
            
            // Check all lineage edges
            for edge in &self.metadata.lineage.edges {
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
                
                // Also check reverse direction for symmetric joins
                if edge.to == current && !seen.contains(&edge.from) {
                    // Create reverse edge
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
            "No join path found from {} to {}",
            from, to
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
                let right = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
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
                let right = self.compiler.engine.scan_with_metadata(table, &self.compiler.metadata).await?;
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

