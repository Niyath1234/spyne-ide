//! Query Engine - Direct Query Execution
//! 
//! Executes direct queries like "What is the TOS for khatabook as of date?"
//! Single-system queries that return metric values without comparison.

use crate::error::{RcaError, Result};
use crate::intent_compiler::IntentSpec;
use crate::metadata::Metadata;
use crate::rule_compiler::{RuleCompiler, RuleExecutor};
use crate::graph::Hypergraph;
use crate::time::TimeResolver;
use crate::llm::LlmClient;
use chrono::NaiveDate;
use polars::prelude::*;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use tracing::{info, debug, warn};

/// Result of a direct query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// System queried
    pub system: String,
    
    /// Metric queried
    pub metric: String,
    
    /// Grain level
    pub grain: Vec<String>,
    
    /// As-of date (if specified)
    pub as_of_date: Option<String>,
    
    /// Query result data
    pub data: QueryData,
    
    /// Summary statistics
    pub summary: QuerySummary,
    
    /// Execution metadata
    pub metadata: QueryMetadata,
}

/// Query result data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryData {
    /// Number of rows
    pub row_count: usize,
    
    /// Column names
    pub columns: Vec<String>,
    
    /// Sample rows (first 100)
    pub sample_rows: Vec<serde_json::Value>,
    
    /// Full data available (if not too large)
    pub full_data: Option<Vec<serde_json::Value>>,
}

/// Query summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySummary {
    /// Total metric value (sum if applicable)
    pub total: Option<f64>,
    
    /// Average metric value
    pub average: Option<f64>,
    
    /// Minimum metric value
    pub min: Option<f64>,
    
    /// Maximum metric value
    pub max: Option<f64>,
    
    /// Count of distinct grain values
    pub distinct_count: usize,
}

/// Query execution metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetadata {
    /// Rule used for execution
    pub rule_id: String,
    
    /// Tables involved
    pub tables: Vec<String>,
    
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    
    /// Number of pipeline steps
    pub pipeline_steps: usize,
}

/// Query Engine for executing direct queries
pub struct QueryEngine {
    metadata: Metadata,
    data_dir: PathBuf,
    llm_client: Option<LlmClient>,
}

impl QueryEngine {
    /// Create a new Query Engine
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self {
            metadata,
            data_dir,
            llm_client: None,
        }
    }
    
    /// Create a new Query Engine with LLM client for general query fallback
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm_client = Some(llm);
        self
    }
    
    /// Execute a direct query from an IntentSpec
    pub async fn execute(&self, intent: &IntentSpec) -> Result<QueryResult> {
        info!("🔍 Executing direct query: system={:?}, metric={:?}", 
            intent.systems, intent.target_metrics);
        
        // Validate intent is QUERY type
        if !matches!(intent.task_type, crate::intent_compiler::TaskType::QUERY) {
            return Err(RcaError::Execution(
                format!("QueryEngine can only execute QUERY tasks, got {:?}", intent.task_type)
            ));
        }
        
        // Get system and metric
        let system = intent.systems.first()
            .ok_or_else(|| RcaError::Execution("QUERY requires at least one system".to_string()))?;
        let metric = intent.target_metrics.first()
            .ok_or_else(|| RcaError::Execution("QUERY requires at least one metric".to_string()))?;
        
        let start_time = std::time::Instant::now();
        
        // Step 1: Find rule for this system and metric
        info!("📋 Finding rule for system={}, metric={}", system, metric);
        
        // Find rules for this system and metric
        let rules = self.metadata.get_rules_for_system_metric(system, metric);
        
        if rules.is_empty() {
            // No rule found - try LLM fallback for general queries
            warn!("⚠️  No rule found for system={}, metric={}. Attempting LLM fallback...", system, metric);
            
            if let Some(ref llm) = self.llm_client {
                return self.execute_with_llm_fallback(intent, system, metric, start_time).await;
            } else {
                return Err(RcaError::Execution(
                    format!("No rule found for system={}, metric={}. LLM fallback not available (no LLM client provided).", system, metric)
                ));
            }
        }
        
        let rule_id = rules[0].id.clone();
        let rule = self.metadata.get_rule(&rule_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", rule_id)))?;
        
        info!("✅ Found rule: {} - {}", rule_id, rule.computation.description);
        
        // Step 2: Parse as-of date if specified
        let as_of_date = intent.time_scope.as_ref()
            .and_then(|ts| ts.as_of_date.as_ref())
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());
        
        if let Some(date) = as_of_date {
            info!("📅 As-of date: {}", date);
        }
        
        // Step 3: Compile and execute rule
        info!("⚙️  Compiling execution plan...");
        let compiler = RuleCompiler::new(self.metadata.clone(), self.data_dir.clone());
        let plan = compiler.compile(&rule_id)?;
        
        info!("📊 Execution plan has {} steps", plan.steps.len());
        for (idx, step) in plan.steps.iter().enumerate() {
            debug!("   Step {}: {:?}", idx + 1, step);
        }
        
        // Step 4: Execute pipeline
        info!("🚀 Executing pipeline...");
        let executor = RuleExecutor::new(compiler);
        let df = executor.execute(&rule_id, as_of_date).await?;
        
        info!("✅ Query executed: {} rows, {} columns", df.height(), df.width());
        
        // Step 5: Apply constraints if specified
        let mut filtered_df = df;
        if !intent.constraints.is_empty() {
            info!("🔍 Applying {} constraints...", intent.constraints.len());
            filtered_df = self.apply_constraints(filtered_df, &intent.constraints)?;
            info!("✅ After constraints: {} rows", filtered_df.height());
        }
        
        // Step 6: Extract metric column (usually the last column or named after metric)
        let metric_col = self.find_metric_column(&filtered_df, metric)?;
        
        // Step 7: Build result
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        // Convert DataFrame to JSON
        let (columns, sample_rows, full_data) = self.dataframe_to_json(&filtered_df, 100)?;
        
        // Calculate summary statistics
        let summary = self.calculate_summary(&filtered_df, &metric_col)?;
        
        // Build metadata
        let tables: Vec<String> = plan.steps.iter()
            .filter_map(|step| {
                match step {
                    crate::metadata::PipelineOp::Scan { table } => Some(table.clone()),
                    crate::metadata::PipelineOp::Join { table, .. } => Some(table.clone()),
                    _ => None,
                }
            })
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        
        Ok(QueryResult {
            system: system.clone(),
            metric: metric.clone(),
            grain: intent.grain.clone(),
            as_of_date: intent.time_scope.as_ref()
                .and_then(|ts| ts.as_of_date.clone()),
            data: QueryData {
                row_count: filtered_df.height(),
                columns,
                sample_rows,
                full_data: if filtered_df.height() <= 1000 {
                    Some(full_data)
                } else {
                    None
                },
            },
            summary,
            metadata: QueryMetadata {
                rule_id,
                tables,
                execution_time_ms: execution_time,
                pipeline_steps: plan.steps.len(),
            },
        })
    }
    
    /// Apply constraints to DataFrame
    fn apply_constraints(
        &self,
        df: DataFrame,
        constraints: &[crate::intent_compiler::ConstraintSpec],
    ) -> Result<DataFrame> {
        let mut result = df;
        
        for constraint in constraints {
            if let (Some(col), Some(op), Some(val)) = (
                constraint.column.as_ref(),
                constraint.operator.as_ref(),
                constraint.value.as_ref(),
            ) {
                result = self.apply_single_constraint(result, col, op, val)?;
            }
        }
        
        Ok(result)
    }
    
    /// Apply a single constraint
    fn apply_single_constraint(
        &self,
        df: DataFrame,
        column: &str,
        operator: &str,
        value: &serde_json::Value,
    ) -> Result<DataFrame> {
        use polars::prelude::*;
        
        let filter_expr = match operator {
            "=" => {
                if let Some(s) = value.as_str() {
                    col(column).eq(lit(s))
                } else if let Some(n) = value.as_f64() {
                    col(column).eq(lit(n))
                } else {
                    return Err(RcaError::Execution(
                        format!("Unsupported value type for = operator: {:?}", value)
                    ));
                }
            }
            "!=" => {
                if let Some(s) = value.as_str() {
                    col(column).neq(lit(s))
                } else if let Some(n) = value.as_f64() {
                    col(column).neq(lit(n))
                } else {
                    return Err(RcaError::Execution(
                        format!("Unsupported value type for != operator: {:?}", value)
                    ));
                }
            }
            ">" => {
                if let Some(n) = value.as_f64() {
                    col(column).gt(lit(n))
                } else {
                    return Err(RcaError::Execution("> operator requires numeric value".to_string()));
                }
            }
            "<" => {
                if let Some(n) = value.as_f64() {
                    col(column).lt(lit(n))
                } else {
                    return Err(RcaError::Execution("< operator requires numeric value".to_string()));
                }
            }
            ">=" => {
                if let Some(n) = value.as_f64() {
                    col(column).gt_eq(lit(n))
                } else {
                    return Err(RcaError::Execution(">= operator requires numeric value".to_string()));
                }
            }
            "<=" => {
                if let Some(n) = value.as_f64() {
                    col(column).lt_eq(lit(n))
                } else {
                    return Err(RcaError::Execution("<= operator requires numeric value".to_string()));
                }
            }
            "in" => {
                if let Some(arr) = value.as_array() {
                    let values: Vec<serde_json::Value> = arr.iter().cloned().collect();
                    // Convert to Polars expression
                    // For now, use a simple approach
                    return Err(RcaError::Execution("'in' operator not yet implemented".to_string()));
                } else {
                    return Err(RcaError::Execution("'in' operator requires array value".to_string()));
                }
            }
            _ => {
                return Err(RcaError::Execution(
                    format!("Unsupported operator: {}", operator)
                ));
            }
        };
        
        Ok(df.lazy().filter(filter_expr).collect()?)
    }
    
    /// Find metric column in DataFrame
    fn find_metric_column(&self, df: &DataFrame, metric: &str) -> Result<String> {
        let cols = df.get_column_names();
        
        // Try exact match first
        if cols.iter().any(|c| c.eq_ignore_ascii_case(metric)) {
            return Ok(metric.to_string());
        }
        
        // Try common metric column names
        let metric_lower = metric.to_lowercase();
        for col in &cols {
            let col_lower = col.to_lowercase();
            if col_lower.contains(&metric_lower) || 
               metric_lower.contains(&col_lower) {
                return Ok(col.to_string());
            }
        }
        
        // If no match, use last numeric column
        for col in cols.iter().rev() {
            if let Ok(series) = df.column(col) {
                if matches!(series.dtype(), DataType::Float64 | DataType::Int64 | DataType::UInt64) {
                    return Ok(col.to_string());
                }
            }
        }
        
        // Fallback: use last column
        cols.last()
            .ok_or_else(|| RcaError::Execution("DataFrame has no columns".to_string()))
            .map(|s| s.to_string())
    }
    
    /// Convert DataFrame to JSON
    fn dataframe_to_json(
        &self,
        df: &DataFrame,
        max_rows: usize,
    ) -> Result<(Vec<String>, Vec<serde_json::Value>, Vec<serde_json::Value>)> {
        let columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        
        let height = df.height().min(max_rows);
        let mut sample_rows = Vec::new();
        let mut full_rows = Vec::new();
        
        for i in 0..df.height() {
            let mut row = serde_json::Map::new();
            for col_name in &columns {
                if let Ok(series) = df.column(col_name) {
                    let value = if let Ok(val) = series.get(i) {
                        self.polars_value_to_json(val)
                    } else {
                        serde_json::Value::Null
                    };
                    row.insert(col_name.clone(), value);
                }
            }
            let json_row = serde_json::Value::Object(row);
            
            if i < height {
                sample_rows.push(json_row.clone());
            }
            full_rows.push(json_row);
        }
        
        Ok((columns, sample_rows, full_rows))
    }
    
    /// Convert Polars value to JSON
    fn polars_value_to_json(&self, val: polars::prelude::AnyValue) -> serde_json::Value {
        use polars::prelude::AnyValue;
        match val {
            AnyValue::Null => serde_json::Value::Null,
            AnyValue::Boolean(b) => serde_json::Value::Bool(b),
            AnyValue::String(s) => serde_json::Value::String(s.to_string()),
            AnyValue::Int64(i) => serde_json::Value::Number(i.into()),
            AnyValue::UInt64(u) => serde_json::Value::Number(u.into()),
            AnyValue::Float64(f) => {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
            _ => serde_json::Value::String(format!("{:?}", val)),
        }
    }
    
    /// Calculate summary statistics
    fn calculate_summary(&self, df: &DataFrame, metric_col: &str) -> Result<QuerySummary> {
        use polars::prelude::*;
        
        let series = df.column(metric_col)?;
        
        // Get distinct count of grain columns (use first column as grain)
        let column_names = df.get_column_names();
        let grain_col = column_names.first()
            .ok_or_else(|| RcaError::Execution("No columns in DataFrame".to_string()))?;
        let distinct_count = df.column(grain_col)?.n_unique()?;
        
        // Calculate statistics if numeric
        let (total, average, min, max): (Option<f64>, Option<f64>, Option<f64>, Option<f64>) = 
            if matches!(series.dtype(), DataType::Float64 | DataType::Int64 | DataType::UInt64) {
                let sum = series.sum::<f64>().ok();
                let mean = series.mean();
                let min_val = series.min::<f64>().ok().flatten();
                let max_val = series.max::<f64>().ok().flatten();
                (sum, mean, min_val, max_val)
            } else {
                (None, None, None, None)
            };
        
        Ok(QuerySummary {
            total,
            average,
            min,
            max,
            distinct_count,
        })
    }
    
    /// Execute query using LLM fallback when no rule exists
    /// This allows general queries like "what is the total outstanding of khatabook as of date"
    async fn execute_with_llm_fallback(
        &self,
        intent: &IntentSpec,
        system: &str,
        metric: &str,
        start_time: std::time::Instant,
    ) -> Result<QueryResult> {
        let llm = self.llm_client.as_ref()
            .ok_or_else(|| RcaError::Execution("LLM client not available".to_string()))?;
        
        info!("🤖 Using LLM to answer general query: system={}, metric={}", system, metric);
        
        // Step 1: Find relevant tables for this system
        let system_tables: Vec<_> = self.metadata.tables.iter()
            .filter(|t| t.system.eq_ignore_ascii_case(system))
            .collect();
        
        if system_tables.is_empty() {
            return Err(RcaError::Execution(
                format!("No tables found for system={}. Available systems: {:?}", 
                    system, 
                    self.metadata.tables.iter().map(|t| &t.system).collect::<std::collections::HashSet<_>>())
            ));
        }
        
        info!("📊 Found {} tables for system {}", system_tables.len(), system);
        
        // Step 2: Build context for LLM
        let table_info: Vec<String> = system_tables.iter().map(|t| {
            let columns = t.columns.as_ref()
                .map(|cols| cols.iter().map(|c| {
                    let dtype = c.data_type.as_ref().map(|d| d.as_str()).unwrap_or("unknown");
                    format!("{} ({})", c.name, dtype)
                }).collect::<Vec<_>>().join(", "))
                .unwrap_or_else(|| "unknown columns".to_string());
            format!("- {}: {} (path: {})", t.name, columns, t.path)
        }).collect();
        
        // Step 3: Parse as-of date if specified
        let as_of_date_str = intent.time_scope.as_ref()
            .and_then(|ts| ts.as_of_date.as_ref())
            .map(|s| s.as_str());
        
        // Step 4: Ask LLM to generate a query plan
        let query_prompt = format!(
            r#"You are a data query assistant. Answer the following query by analyzing available tables and generating a query plan.

QUERY: "What is the {} for {} as of {}?"

AVAILABLE TABLES FOR SYSTEM '{}':
{}

METRIC TO FIND: {}
AS-OF DATE: {}

INSTRUCTIONS:
1. Analyze which tables contain the metric '{}'
2. Identify the relevant columns (look for columns related to: outstanding, balance, amount, total, etc.)
3. Determine if joins are needed between tables
4. Consider date filtering if as_of_date is provided
5. Generate a JSON response with:
   - table_name: primary table to query
   - metric_column: column name containing the metric
   - grain_columns: columns for grouping (e.g., customer_id, loan_id)
   - join_tables: list of tables to join (if any)
   - join_keys: join conditions
   - date_filter_column: column to filter by date (if applicable)
   - aggregation: "sum", "avg", "count", or "direct" (if already aggregated)

OUTPUT FORMAT (JSON only, no markdown):
{{
  "table_name": "table_name",
  "metric_column": "column_name",
  "grain_columns": ["col1", "col2"],
  "join_tables": [{{"table": "table2", "on": ["key1", "key2"]}}],
  "date_filter_column": "date_column or null",
  "aggregation": "sum|avg|count|direct"
}}

Be specific and use actual table/column names from the available tables."#,
            metric, system, 
            as_of_date_str.unwrap_or("latest"),
            system,
            table_info.join("\n"),
            metric,
            as_of_date_str.unwrap_or("latest"),
            metric
        );
        
        let llm_response = llm.call_llm(&query_prompt).await
            .map_err(|e| RcaError::Llm(format!("LLM query planning failed: {}", e)))?;
        
        // Step 5: Parse LLM response
        let cleaned_response = llm_response
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        
        let query_plan: serde_json::Value = serde_json::from_str(cleaned_response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM query plan: {}. Response: {}", e, cleaned_response)))?;
        
        let table_name = query_plan["table_name"].as_str()
            .ok_or_else(|| RcaError::Llm("Missing table_name in LLM response".to_string()))?;
        let metric_column = query_plan["metric_column"].as_str()
            .ok_or_else(|| RcaError::Llm("Missing metric_column in LLM response".to_string()))?;
        let aggregation = query_plan["aggregation"].as_str().unwrap_or("sum");
        
        info!("✅ LLM query plan: table={}, metric_column={}, aggregation={}", 
            table_name, metric_column, aggregation);
        
        // Step 6: Find the table
        let table = system_tables.iter()
            .find(|t| t.name.eq_ignore_ascii_case(table_name))
            .ok_or_else(|| RcaError::Execution(format!("Table {} not found in system {}", table_name, system)))?;
        
        // Step 7: Load and query the table
        let csv_path = self.data_dir.join(&table.path);
        if !csv_path.exists() {
            return Err(RcaError::Execution(format!("CSV file not found: {}", csv_path.display())));
        }
        
        info!("📂 Loading CSV: {}", csv_path.display());
        let mut df = LazyCsvReader::new(&csv_path)
            .with_has_header(true)
            .finish()
            .map_err(|e| RcaError::Execution(format!("Failed to load CSV {}: {}", csv_path.display(), e)))?
            .collect()?;
        
        info!("✅ Loaded {} rows, {} columns", df.height(), df.width());
        
        // Step 8: Apply date filter if specified
        if let Some(date_str) = as_of_date_str {
            if let Some(date_col) = query_plan["date_filter_column"].as_str() {
                let col_names = df.get_column_names();
                if col_names.iter().any(|c| c.eq_ignore_ascii_case(date_col)) {
                    info!("📅 Filtering by {} = {}", date_col, date_str);
                    // Try to parse date and filter
                    if let Ok(filter_date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                        df = df.lazy()
                            .filter(col(date_col).eq(lit(filter_date.format("%Y-%m-%d").to_string())))
                            .collect()?;
                    }
                }
            }
        }
        
        // Step 9: Apply aggregation if needed
        if aggregation != "direct" && df.height() > 0 {
            let col_names = df.get_column_names();
            let metric_col = col_names.iter()
                .find(|c| c.eq_ignore_ascii_case(metric_column))
                .ok_or_else(|| RcaError::Execution(format!("Metric column {} not found in table {}", metric_column, table_name)))?;
            let metric_col = metric_col.to_string();
            
            match aggregation {
                "sum" => {
                    let sum_val = df.column(&metric_col)?.sum::<f64>()?;
                    df = DataFrame::new(vec![
                        Series::new(&metric_col, vec![sum_val])
                    ])?;
                }
                "avg" => {
                    let avg_val = df.column(&metric_col)?.mean();
                    df = DataFrame::new(vec![
                        Series::new(&metric_col, vec![avg_val])
                    ])?;
                }
                "count" => {
                    let count_val = df.height() as f64;
                    df = DataFrame::new(vec![
                        Series::new(&metric_col, vec![count_val])
                    ])?;
                }
                _ => {
                    warn!("Unknown aggregation type: {}. Using direct values.", aggregation);
                }
            }
        }
        
        // Step 10: Apply constraints if specified
        if !intent.constraints.is_empty() {
            info!("🔍 Applying {} constraints...", intent.constraints.len());
            df = self.apply_constraints(df, &intent.constraints)?;
        }
        
        // Step 11: Build result
        let execution_time = start_time.elapsed().as_millis() as u64;
        let (columns, sample_rows, full_data) = self.dataframe_to_json(&df, 100)?;
        let metric_col = self.find_metric_column(&df, metric_column)?;
        let summary = self.calculate_summary(&df, &metric_col)?;
        
        info!("✅ LLM query executed: {} rows, {} columns", df.height(), df.width());
        
        Ok(QueryResult {
            system: system.to_string(),
            metric: metric.to_string(),
            grain: intent.grain.clone(),
            as_of_date: intent.time_scope.as_ref()
                .and_then(|ts| ts.as_of_date.clone()),
            data: QueryData {
                row_count: df.height(),
                columns,
                sample_rows,
                full_data: if df.height() <= 1000 {
                    Some(full_data)
                } else {
                    None
                },
            },
            summary,
            metadata: QueryMetadata {
                rule_id: format!("llm_generated_{}_{}", system, metric),
                tables: vec![table_name.to_string()],
                execution_time_ms: execution_time,
                pipeline_steps: 1,
            },
        })
    }
}


