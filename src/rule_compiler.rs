use crate::error::{RcaError, Result};
use crate::metadata::{Metadata, Rule};
use crate::operators::RelationalEngine;
use crate::time::TimeResolver;
use polars::prelude::*;
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
    
    /// Compile a rule into an execution plan
    pub fn compile(&self, rule_id: &str) -> Result<ExecutionPlan> {
        let rule = self.metadata
            .get_rule(rule_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", rule_id)))?;
        
        Ok(ExecutionPlan {
            rule_id: rule_id.to_string(),
            rule: rule.clone(),
            steps: rule.pipeline.clone(),
        })
    }
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
                let mut df = self.compiler.engine.scan(table, None).await?;
                
                // Apply as-of filtering
                if let Some(date) = as_of_date {
                    df = self.compiler.time_resolver.apply_as_of(df, table, Some(date))?;
                }
                
                result = Some(df);
                current_table = Some(table.clone());
                continue;
            }
            
            // Execute operation
            let table_path = current_table.as_deref();
            result = Some(
                self.compiler.engine.execute_op(step, result, table_path).await?
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
                let mut df = self.compiler.engine.scan(table, None).await?;
                
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
            
            let table_path = current_table.as_deref();
            let df = self.compiler.engine.execute_op(step, result.clone(), table_path).await?;
            
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

