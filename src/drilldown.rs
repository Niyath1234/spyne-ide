use crate::error::{RcaError, Result};
use crate::rule_compiler::{ExecutionStep, RuleExecutor};
use crate::llm::LlmClient;
use crate::metadata::Metadata;
use crate::core::llm::{LlmStrategyEngine, DrilldownStrategy, DrilldownDimension};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{info, warn};

pub struct DrilldownEngine {
    executor: RuleExecutor,
    llm: LlmClient,
    metadata: Metadata,
    data_dir: PathBuf,
    strategy_engine: Option<LlmStrategyEngine>,
}

impl DrilldownEngine {
    pub fn new(executor: RuleExecutor) -> Self {
        // Default metadata will be replaced by with_metadata() in actual usage
        let metadata = Metadata::load("metadata").unwrap_or_else(|_| {
            // Create minimal metadata if load fails
            Metadata {
                entities: Vec::new(),
                tables: Vec::new(),
                metrics: Vec::new(),
                business_labels: crate::metadata::BusinessLabelObject {
                    systems: Vec::new(),
                    metrics: Vec::new(),
                    reconciliation_types: Vec::new(),
                },
                rules: Vec::new(),
                lineage: crate::metadata::LineageObject {
                    edges: Vec::new(),
                    possible_joins: Vec::new(),
                },
                time_rules: crate::metadata::TimeRules {
                    as_of_rules: Vec::new(),
                    lateness_rules: Vec::new(),
                },
                identity: crate::metadata::IdentityObject {
                    canonical_keys: Vec::new(),
                    key_mappings: Vec::new(),
                },
                exceptions: crate::metadata::ExceptionsObject {
                    exceptions: Vec::new(),
                },
                tables_by_name: HashMap::new(),
                tables_by_entity: HashMap::new(),
                tables_by_system: HashMap::new(),
                rules_by_id: HashMap::new(),
                rules_by_system_metric: HashMap::new(),
                metrics_by_id: HashMap::new(),
                entities_by_id: HashMap::new(),
            }
        });
        
        let llm = LlmClient::new("dummy-api-key".to_string(), "gpt-4o-mini".to_string(), "https://api.openai.com/v1".to_string());
        let llm_clone = LlmClient::new(
            "dummy-api-key".to_string(),
            "gpt-4o-mini".to_string(),
            "https://api.openai.com/v1".to_string()
        );
        Self {
            executor,
            llm: llm_clone.clone(),
            metadata: metadata.clone(),
            data_dir: PathBuf::from("data"),
            strategy_engine: Some(LlmStrategyEngine::new(llm_clone, metadata)),
        }
    }
    
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm = llm;
        // Strategy engine will be created lazily when needed
        self.strategy_engine = None;
        self
    }
    
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        // Strategy engine will be created lazily when needed
        self.strategy_engine = None;
        self
    }
    
    /// Get or create strategy engine
    fn get_strategy_engine(&self) -> Option<LlmStrategyEngine> {
        // Create strategy engine on demand
        Some(LlmStrategyEngine::new(
            // We can't clone LlmClient, so we need to recreate it
            // This is a limitation - in production, LlmClient should implement Clone
            LlmClient::new(
                "dummy-api-key".to_string(),
                "gpt-4o-mini".to_string(),
                "https://api.openai.com/v1".to_string()
            ),
            self.metadata.clone()
        ))
    }
    
    pub fn with_data_dir(mut self, data_dir: PathBuf) -> Self {
        self.data_dir = data_dir;
        self
    }
    
    /// LLM-guided drilldown analysis
    /// 
    /// Uses LLM to suggest optimal drilldown dimensions and strategies
    /// based on RCA results and problem description.
    pub async fn llm_guided_drilldown(
        &self,
        problem_description: &str,
        metric: &str,
        rca_summary: &crate::core::llm::strategy::RcaSummary,
        available_columns: &[String],
    ) -> Result<DrilldownStrategy> {
        info!("🔍 LLM-guided drilldown for metric: {}", metric);
        
        if let Some(strategy_engine) = self.get_strategy_engine() {
            strategy_engine.generate_drilldown_strategy(
                problem_description,
                metric,
                rca_summary,
                available_columns,
            ).await
        } else {
            warn!("Strategy engine not available, falling back to basic drilldown");
            // Fallback to basic drilldown
            Ok(DrilldownStrategy {
                dimensions: vec![],
                priority_order: vec![],
                reasoning: "Basic drilldown - LLM strategy not available".to_string(),
                expected_insights: vec![],
            })
        }
    }
    
    /// Execute drilldown on a specific dimension
    /// 
    /// Given a drilldown dimension, filters and groups data to reveal patterns.
    pub async fn execute_drilldown(
        &self,
        dimension: &DrilldownDimension,
        rule_a_id: &str,
        rule_b_id: &str,
        as_of_date: Option<chrono::NaiveDate>,
    ) -> Result<DrilldownResult> {
        info!("🔍 Executing drilldown on dimension: {} (column: {})", dimension.name, dimension.column);
        
        // Execute both rules
        let steps_a = self.executor.execute_with_steps(rule_a_id, as_of_date).await?;
        let steps_b = self.executor.execute_with_steps(rule_b_id, as_of_date).await?;
        
        // Get final dataframes
        let df_a = steps_a.last()
            .and_then(|s| s.data.as_ref())
            .ok_or_else(|| RcaError::Execution("No data from rule A execution".to_string()))?;
        let df_b = steps_b.last()
            .and_then(|s| s.data.as_ref())
            .ok_or_else(|| RcaError::Execution("No data from rule B execution".to_string()))?;
        
        // Check if column exists
        if !df_a.get_column_names().contains(&dimension.column.as_str()) {
            return Err(RcaError::Execution(format!(
                "Column {} not found in rule A data. Available columns: {:?}",
                dimension.column,
                df_a.get_column_names()
            )));
        }
        
        if !df_b.get_column_names().contains(&dimension.column.as_str()) {
            return Err(RcaError::Execution(format!(
                "Column {} not found in rule B data. Available columns: {:?}",
                dimension.column,
                df_b.get_column_names()
            )));
        }
        
        // Group by dimension and aggregate
        let grouped_a = df_a.clone()
            .lazy()
            .group_by([col(&dimension.column)])
            .agg([
                col("*").count().alias("count_a"),
                // Try to sum numeric columns if they exist
            ])
            .collect()?;
        
        let grouped_b = df_b.clone()
            .lazy()
            .group_by([col(&dimension.column)])
            .agg([
                col("*").count().alias("count_b"),
            ])
            .collect()?;
        
        // Join to compare
        let comparison = grouped_a
            .lazy()
            .join(
                grouped_b.lazy(),
                [col(&dimension.column)],
                [col(&dimension.column)],
                JoinArgs::new(JoinType::Outer),
            )
            .with_columns([
                (col("count_a").fill_null(lit(0))).alias("count_a"),
                (col("count_b").fill_null(lit(0))).alias("count_b"),
            ])
            .with_column((col("count_a") - col("count_b")).alias("difference"))
            .collect()?;
        
        let insights = self.generate_insights(&comparison, dimension).await?;
        
        Ok(DrilldownResult {
            dimension: dimension.clone(),
            comparison_data: comparison,
            insights,
        })
    }
    
    /// Generate insights from drilldown results using LLM
    async fn generate_insights(
        &self,
        comparison_df: &DataFrame,
        dimension: &DrilldownDimension,
    ) -> Result<Vec<String>> {
        // Sample data for LLM context
        let sample_size = comparison_df.height().min(10);
        let sample_data = comparison_df.head(Some(sample_size));
        
        // Convert to text representation
        let mut data_text = format!("Dimension: {} (column: {})\n\n", dimension.name, dimension.column);
        data_text.push_str("Sample comparison data:\n");
        
        // Convert DataFrame to text representation manually
        let col_names = sample_data.get_column_names();
        data_text.push_str(&col_names.join(", "));
        data_text.push_str("\n");
        
        // Add sample rows (limit to 5 rows for token efficiency)
        for row_idx in 0..sample_data.height().min(5) {
            let mut row_values = Vec::new();
            for col_name in &col_names {
                if let Ok(col_series) = sample_data.column(col_name) {
                    let val_str = match col_series.dtype() {
                        polars::prelude::DataType::String => {
                            col_series.str().ok()
                                .and_then(|s| s.get(row_idx))
                                .unwrap_or("")
                                .to_string()
                        }
                        polars::prelude::DataType::Int64 => {
                            col_series.i64().ok()
                                .and_then(|s| s.get(row_idx))
                                .map(|v| v.to_string())
                                .unwrap_or_default()
                        }
                        polars::prelude::DataType::Float64 => {
                            col_series.f64().ok()
                                .and_then(|s| s.get(row_idx))
                                .map(|v| v.to_string())
                                .unwrap_or_default()
                        }
                        _ => format!("{:?}", col_series.get(row_idx)),
                    };
                    row_values.push(val_str);
                }
            }
            data_text.push_str(&row_values.join(", "));
            data_text.push_str("\n");
        }
        
        let prompt = format!(
            r#"You are analyzing drilldown results for root cause analysis.

DRILLDOWN DIMENSION: {}
COLUMN: {}
IMPORTANCE: {}

DATA SUMMARY:
{}

TASK:
Analyze the drilldown results and identify:
1. Patterns in the differences
2. Which values of the dimension show the largest discrepancies
3. Potential root causes suggested by the patterns

Provide 3-5 specific insights as bullet points.
"#,
            dimension.name,
            dimension.column,
            dimension.importance,
            data_text
        );
        
        let response = self.llm.call_llm(&prompt).await?;
        
        // Extract insights (bullet points)
        let mut insights = Vec::new();
        for line in response.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("- ") || trimmed.starts_with("* ") || trimmed.starts_with("• ") {
                insights.push(trimmed[2..].trim().to_string());
            } else if trimmed.len() > 20 && !trimmed.starts_with("TASK:") && !trimmed.starts_with("DRILLDOWN") {
                // Also capture standalone insight lines
                insights.push(trimmed.to_string());
            }
        }
        
        if insights.is_empty() {
            insights.push(format!(
                "Drilldown on {} revealed differences across {} distinct values",
                dimension.name,
                comparison_df.height()
            ));
        }
        
        Ok(insights)
    }
    
    /// Find first divergence point between two rule executions
    pub async fn find_divergence(
        &self,
        rule_a: &str,
        rule_b: &str,
        mismatched_keys: &[Vec<String>],
        as_of_date: Option<chrono::NaiveDate>,
    ) -> Result<DivergencePoint> {
        // Execute both rules step-by-step
        let steps_a = self.executor.execute_with_steps(rule_a, as_of_date).await?;
        let steps_b = self.executor.execute_with_steps(rule_b, as_of_date).await?;
        
        // Compare step by step
        for (idx, (step_a, step_b)) in steps_a.iter().zip(steps_b.iter()).enumerate() {
            // Filter to mismatched keys only
            let df_a_filtered = self.filter_to_keys(step_a.data.as_ref().unwrap(), mismatched_keys)?;
            let df_b_filtered = self.filter_to_keys(step_b.data.as_ref().unwrap(), mismatched_keys)?;
            
            // Compare row counts
            if df_a_filtered.height() != df_b_filtered.height() {
                return Ok(DivergencePoint {
                    step_index: idx,
                    step_name_a: step_a.step_name.clone(),
                    step_name_b: step_b.step_name.clone(),
                    operation_a: step_a.operation.clone(),
                    operation_b: step_b.operation.clone(),
                    row_count_a: df_a_filtered.height(),
                    row_count_b: df_b_filtered.height(),
                    divergence_type: "row_count_mismatch".to_string(),
                    root_cause_details: None,
                });
            }
            
            // Compare column values if same structure
            if step_a.columns == step_b.columns {
                let diff = self.compare_dataframes(&df_a_filtered, &df_b_filtered)?;
                if diff > 0 {
                    return Ok(DivergencePoint {
                        step_index: idx,
                        step_name_a: step_a.step_name.clone(),
                        step_name_b: step_b.step_name.clone(),
                        operation_a: step_a.operation.clone(),
                        operation_b: step_b.operation.clone(),
                        row_count_a: df_a_filtered.height(),
                        row_count_b: df_b_filtered.height(),
                        divergence_type: "value_mismatch".to_string(),
                        root_cause_details: None,
                    });
                }
            }
        }
        
        // No divergence found in steps - might be in final aggregation
        Ok(DivergencePoint {
            step_index: steps_a.len(),
            step_name_a: "final".to_string(),
            step_name_b: "final".to_string(),
            operation_a: "final_aggregation".to_string(),
            operation_b: "final_aggregation".to_string(),
            row_count_a: steps_a.last().map(|s| s.row_count).unwrap_or(0),
            row_count_b: steps_b.last().map(|s| s.row_count).unwrap_or(0),
            divergence_type: "final_aggregation".to_string(),
            root_cause_details: None,
        })
    }
    
    fn filter_to_keys(&self, df: &DataFrame, keys: &[Vec<String>]) -> Result<DataFrame> {
        if keys.is_empty() {
            return Ok(df.clone());
        }
        
        // Get grain columns from first key
        let grain_cols: Vec<&str> = (0..keys[0].len())
            .map(|i| df.get_column_names()[i])
            .collect();
        
        // Build filter expression
        let mut conditions = Vec::new();
        for key in keys {
            let mut key_conditions = Vec::new();
            for (col_idx, val) in key.iter().enumerate() {
                if col_idx < grain_cols.len() {
                    let col_name = grain_cols[col_idx];
                    key_conditions.push(col(col_name).eq(lit(val.as_str())));
                }
            }
            if !key_conditions.is_empty() {
                conditions.push(key_conditions.into_iter().reduce(|a, b| a.and(b)).unwrap());
            }
        }
        
        if conditions.is_empty() {
            return Ok(df.clone());
        }
        
        let filter_expr = conditions.into_iter().reduce(|a, b| a.or(b)).unwrap();
        Ok(df.clone().lazy().filter(filter_expr).collect()?)
    }
    
    fn compare_dataframes(&self, df_a: &DataFrame, df_b: &DataFrame) -> Result<usize> {
        // Simple comparison - count rows that differ
        // In production, would do more sophisticated comparison
        if df_a.height() != df_b.height() {
            return Ok(df_a.height().max(df_b.height()));
        }
        
        // Compare values (simplified)
        Ok(0) // Placeholder
    }
    
    /// Analyze table-level contributions to identify specific root causes
    pub async fn analyze_root_causes(
        &self,
        rule_a_id: &str,
        rule_b_id: &str,
        mismatched_keys: &[Vec<String>],
        as_of_date: Option<chrono::NaiveDate>,
    ) -> Result<Vec<RootCauseDetail>> {
        let rule_a = self.metadata.get_rule(rule_a_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", rule_a_id)))?;
        
        let mut root_causes = Vec::new();
        
        // Get grain column name (usually first column in target_grain)
        let grain_col = rule_a.target_grain.first()
            .ok_or_else(|| RcaError::Execution("No grain column found".to_string()))?;
        
        // Analyze each mismatched loan
        for key in mismatched_keys {
            if key.is_empty() {
                continue;
            }
            
            let loan_id = &key[0];
            
            // Analyze table-level contributions
            let mut table_contributions = HashMap::new();
            
            // Get all source entities from rule
            for entity in &rule_a.computation.source_entities {
                // Find tables for this entity in System A
                let tables: Vec<_> = self.metadata.tables
                    .iter()
                    .filter(|t| t.entity == *entity && t.system == rule_a.system)
                    .collect();
                
                for table in tables {
                    // Load table data
                    let table_path = self.data_dir.join(&table.path);
                    if !table_path.exists() {
                        continue;
                    }
                    
                    // Load based on file extension
                    let df = if table_path.extension().and_then(|s| s.to_str()) == Some("csv") {
                        LazyCsvReader::new(&table_path)
                            .with_try_parse_dates(true)
                            .with_infer_schema_length(Some(1000))
                            .finish()
                            .map_err(|e| RcaError::Execution(format!("Failed to load CSV {}: {}", table.name, e)))?
                            .collect()?
                    } else {
                        LazyFrame::scan_parquet(&table_path, ScanArgsParquet::default())
                            .map_err(|e| RcaError::Execution(format!("Failed to load {}: {}", table.name, e)))?
                            .collect()?
                    };
                    
                    // Filter to this loan
                    let loan_df = df.clone()
                        .lazy()
                        .filter(col(grain_col).eq(lit(loan_id.as_str())))
                        .collect()?;
                    
                    // Calculate contribution based on table type
                    let contribution = self.calculate_table_contribution(&table.name, &loan_df, &rule_a.computation)?;
                    table_contributions.insert(table.name.clone(), contribution);
                }
            }
            
            // Compare with System B expected value
            let rule_b = self.metadata.get_rule(rule_b_id)
                .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", rule_b_id)))?;
            
            // Get System B table
            let system_b_table = self.metadata.tables
                .iter()
                .find(|t| t.system == rule_b.system && t.entity == rule_b.target_entity)
                .ok_or_else(|| RcaError::Execution("System B table not found".to_string()))?;
            
            let system_b_path = self.data_dir.join(&system_b_table.path);
            
            // Load based on file extension
            let system_b_df = if system_b_path.extension().and_then(|s| s.to_str()) == Some("csv") {
                LazyCsvReader::new(&system_b_path)
                    .with_try_parse_dates(true)
                    .with_infer_schema_length(Some(1000))
                    .finish()
                    .map_err(|e| RcaError::Execution(format!("Failed to load CSV {}: {}", system_b_table.name, e)))?
                    .filter(col(grain_col).eq(lit(loan_id.as_str())))
                    .collect()?
            } else {
                LazyFrame::scan_parquet(&system_b_path, ScanArgsParquet::default())
                    .map_err(|e| RcaError::Execution(format!("Failed to load {}: {}", system_b_table.name, e)))?
                    .filter(col(grain_col).eq(lit(loan_id.as_str())))
                    .collect()?
            };
            
            let system_b_value = if system_b_df.height() > 0 {
                // Get the metric column (usually total_outstanding or similar)
                let metric_col = if system_b_df.get_column_names().contains(&"total_outstanding") {
                    "total_outstanding"
                } else if system_b_df.get_column_names().iter().any(|c| c == &rule_b.metric) {
                    &rule_b.metric
                } else {
                    continue;
                };
                
                system_b_df.column(metric_col)
                    .ok()
                    .and_then(|c| c.f64().ok())
                    .and_then(|s| s.get(0))
                    .unwrap_or(0.0)
            } else {
                continue;
            };
            
            // Calculate System A total
            let system_a_total: f64 = table_contributions.values().sum();
            let difference = system_a_total - system_b_value;
            
            if difference.abs() > 0.01 {
                // Identify specific issues
                let issues = self.identify_specific_issues(
                    loan_id,
                    &table_contributions,
                    system_a_total,
                    system_b_value,
                    &rule_a.computation,
                ).await?;
                
                root_causes.push(RootCauseDetail {
                    loan_id: loan_id.clone(),
                    system_a_value: system_a_total,
                    system_b_value,
                    difference,
                    table_contributions,
                    specific_issues: issues,
                });
            }
        }
        
        Ok(root_causes)
    }
    
    fn calculate_table_contribution(
        &self,
        table_name: &str,
        df: &DataFrame,
        computation: &crate::metadata::ComputationDefinition,
    ) -> Result<f64> {
        // Determine contribution based on table name and formula
        if table_name.contains("transaction") {
            // Sum transaction amounts
            if df.get_column_names().contains(&"transaction_amount") {
                Ok(df.column("transaction_amount")?
                    .f64()?
                    .sum()
                    .unwrap_or(0.0) * -1.0) // Transactions reduce TOS
            } else {
                Ok(0.0)
            }
        } else if table_name.contains("penalty") {
            // Sum penalty amounts
            if df.get_column_names().contains(&"penalty_amount") {
                Ok(df.column("penalty_amount")?
                    .f64()?
                    .sum()
                    .unwrap_or(0.0))
            } else {
                Ok(0.0)
            }
        } else if table_name.contains("adjustment") {
            // Sum adjustment amounts
            if df.get_column_names().contains(&"adjustment_amount") {
                Ok(df.column("adjustment_amount")?
                    .f64()?
                    .sum()
                    .unwrap_or(0.0))
            } else {
                Ok(0.0)
            }
        } else if table_name.contains("charge") {
            // Sum charge amounts (negative contribution)
            if df.get_column_names().contains(&"charge_amount") {
                Ok(df.column("charge_amount")?
                    .f64()?
                    .sum()
                    .unwrap_or(0.0) * -1.0)
            } else {
                Ok(0.0)
            }
        } else if table_name.contains("interest") || table_name.contains("accrual") {
            // Sum interest accruals
            if df.get_column_names().contains(&"accrued_interest") {
                Ok(df.column("accrued_interest")?
                    .f64()?
                    .sum()
                    .unwrap_or(0.0))
            } else {
                Ok(0.0)
            }
        } else if table_name.contains("fee") {
            // Sum fees (negative contribution)
            if df.get_column_names().contains(&"fee_amount") {
                Ok(df.column("fee_amount")?
                    .f64()?
                    .sum()
                    .unwrap_or(0.0) * -1.0)
            } else {
                Ok(0.0)
            }
        } else if table_name.contains("emi") {
            // Sum EMI amounts
            if df.get_column_names().contains(&"emi_amount") {
                Ok(df.column("emi_amount")?
                    .f64()?
                    .sum()
                    .unwrap_or(0.0))
            } else {
                Ok(0.0)
            }
        } else {
            Ok(0.0)
        }
    }
    
    async fn identify_specific_issues(
        &self,
        loan_id: &str,
        table_contributions: &HashMap<String, f64>,
        system_a_total: f64,
        system_b_total: f64,
        computation: &crate::metadata::ComputationDefinition,
    ) -> Result<Vec<String>> {
        let difference = system_a_total - system_b_total;
        
        // Build analysis prompt for LLM
        let mut analysis_text = format!(
            "You are analyzing a Total Outstanding (TOS) discrepancy between two systems.\n\n");
        analysis_text.push_str(&format!(
            "Entity ID: {}\nSystem A TOS: {:.2}\nSystem B TOS: {:.2}\nDifference: {:.2}\n\n",
            loan_id, system_a_total, system_b_total, difference
        ));
        
        analysis_text.push_str("Table Contributions (System A):\n");
        for (table, contribution) in table_contributions {
            analysis_text.push_str(&format!("  - {}: {:.2}\n", table, contribution));
        }
        
        analysis_text.push_str("\nCalculation Formula: ");
        analysis_text.push_str(&computation.formula);
        analysis_text.push_str("\n\n");
        
        analysis_text.push_str("TASK: Identify the SPECIFIC root causes of this discrepancy.\n\n");
        analysis_text.push_str("Based on the difference and table contributions, analyze what could be wrong:\n");
        analysis_text.push_str("- Missing transactions (expected transaction not found)\n");
        analysis_text.push_str("- Wrong penalty amounts (penalty recorded incorrectly)\n");
        analysis_text.push_str("- Missing interest accruals (interest not accrued)\n");
        analysis_text.push_str("- Wrong adjustment amounts (adjustment recorded incorrectly)\n");
        analysis_text.push_str("- Missing fees (expected fee not recorded)\n");
        analysis_text.push_str("- Duplicate records (same transaction recorded twice)\n");
        analysis_text.push_str("- Extra records (unexpected transactions present)\n\n");
        
        analysis_text.push_str("Provide clear, natural language descriptions of the specific issues.\n");
        analysis_text.push_str("Be specific: mention which table, which record type, and what's wrong.\n");
        analysis_text.push_str("Example format:\n");
        analysis_text.push_str("- Missing transaction: EMI installment #2 (expected amount: 5,200)\n");
        analysis_text.push_str("- Wrong penalty amount: Penalty recorded as 1,500 but should be 500\n");
        analysis_text.push_str("- Missing interest accrual: Interest accrual for period not recorded\n\n");
        analysis_text.push_str("List each issue as a separate bullet point. If you cannot determine specific issues, explain what the discrepancy indicates.");
        
        // Call LLM to generate root cause descriptions
        let prompt = format!(
            "Analyze this TOS discrepancy and identify specific root causes in natural language:\n\n{}",
            analysis_text
        );
        
        let llm_response = self.llm.call_llm(&prompt).await?;
        
        // Parse LLM response into issue list
        // Look for bullet points, numbered lists, or lines that describe issues
        let mut issues: Vec<String> = Vec::new();
        
        for line in llm_response.lines() {
            let trimmed = line.trim();
            
            // Skip empty lines and headers
            if trimmed.is_empty() || 
               trimmed.starts_with("Loan ID:") || 
               trimmed.starts_with("System A") || 
               trimmed.starts_with("System B") ||
               trimmed.starts_with("Difference:") ||
               trimmed.starts_with("Table Contributions:") ||
               trimmed.starts_with("Calculation Formula:") ||
               trimmed.starts_with("TASK:") ||
               trimmed.starts_with("Based on") ||
               trimmed.starts_with("Example format:") ||
               trimmed.starts_with("List each") ||
               trimmed.starts_with("Provide clear") ||
               trimmed.starts_with("Be specific:") ||
               trimmed.starts_with("If you cannot") {
                continue;
            }
            
            // Extract bullet points (various formats)
            if trimmed.starts_with("- ") || 
               trimmed.starts_with("* ") || 
               trimmed.starts_with("• ") ||
               trimmed.starts_with("1. ") ||
               trimmed.starts_with("2. ") ||
               trimmed.starts_with("3. ") ||
               trimmed.starts_with("4. ") ||
               trimmed.starts_with("5. ") {
                let issue = trimmed
                    .trim_start_matches("- ")
                    .trim_start_matches("* ")
                    .trim_start_matches("• ")
                    .trim_start_matches("1. ")
                    .trim_start_matches("2. ")
                    .trim_start_matches("3. ")
                    .trim_start_matches("4. ")
                    .trim_start_matches("5. ")
                    .trim()
                    .to_string();
                
                if !issue.is_empty() && issue.len() > 10 { // Filter out very short lines
                    issues.push(issue);
                }
            } else if trimmed.len() > 20 && 
                      !trimmed.starts_with("Entity ID:") &&
                      !trimmed.contains("System A TOS:") &&
                      !trimmed.contains("System B TOS:") {
                // Also capture standalone lines that look like issue descriptions
                // (longer than 20 chars, not headers)
                if trimmed.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) ||
                   trimmed.contains("missing") ||
                   trimmed.contains("wrong") ||
                   trimmed.contains("duplicate") ||
                   trimmed.contains("extra") ||
                   trimmed.contains("incorrect") {
                    issues.push(trimmed.to_string());
                }
            }
        }
        
        if issues.is_empty() {
            // Fallback: Generate basic description with more context
            Ok(vec![format!(
                "TOS discrepancy of {:.2} detected for entity {}. System A value ({:.2}) differs from System B value ({:.2}). Analysis of table contributions suggests data quality issues in one or more source tables.",
                difference, loan_id, system_a_total, system_b_total
            )])
        } else {
            Ok(issues)
        }
    }
}

#[derive(Debug, Clone)]
pub struct RootCauseDetail {
    pub loan_id: String,
    pub system_a_value: f64,
    pub system_b_value: f64,
    pub difference: f64,
    pub table_contributions: HashMap<String, f64>,
    pub specific_issues: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DivergencePoint {
    pub step_index: usize,
    pub step_name_a: String,
    pub step_name_b: String,
    pub operation_a: String,
    pub operation_b: String,
    pub row_count_a: usize,
    pub row_count_b: usize,
    pub divergence_type: String,
    pub root_cause_details: Option<Vec<RootCauseDetail>>,
}

/// Result of drilldown analysis
#[derive(Debug, Clone)]
pub struct DrilldownResult {
    pub dimension: DrilldownDimension,
    pub comparison_data: DataFrame,
    pub insights: Vec<String>,
}

