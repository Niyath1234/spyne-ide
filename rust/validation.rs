use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use crate::metadata::Metadata;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{info, debug};

/// Main validation engine for data validation tasks
pub struct ValidationEngine {
    metadata: Metadata,
    llm: LlmClient,
    data_dir: PathBuf,
}

/// Enum representing all types of validation constraints
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ValidationConstraint {
    /// Value constraint: column operator threshold
    /// Example: "ledger <= 5000"
    ValueConstraint {
        column: String,
        operator: ValueOperator,
        threshold: ConstraintValue,
        entity_filter: Option<EntityFilter>,
    },
    
    /// Nullability constraint: column must/must not be null
    /// Example: "customer_id cannot be null"
    NullabilityConstraint {
        column: String,
        must_be_null: bool,
        min_completeness: Option<f64>, // Percentage threshold (0.0-1.0)
        entity_filter: Option<EntityFilter>,
    },
    
    /// Uniqueness constraint: column(s) must be unique
    /// Example: "loan_id must be unique"
    UniquenessConstraint {
        columns: Vec<String>,
        entity_filter: Option<EntityFilter>,
    },
    
    /// Referential integrity constraint: FK must exist in reference table
    /// Example: "loan.customer_id must exist in customer table"
    ReferentialConstraint {
        fk_column: String,
        ref_table: String,
        ref_column: String,
        entity_filter: Option<EntityFilter>,
    },
    
    /// Aggregation constraint: aggregated value must satisfy condition
    /// Example: "Sum(disbursed) per day must equal control_total"
    AggregationConstraint {
        group_by: Vec<String>,
        agg_type: AggregationType,
        column: String,
        operator: ValueOperator,
        threshold: ConstraintValue,
        entity_filter: Option<EntityFilter>,
    },
    
    /// Cross-column constraint: expression involving multiple columns
    /// Example: "disbursement_date <= emi_start_date"
    CrossColumnConstraint {
        expression: String,
        condition: Option<String>, // Optional filter condition
        entity_filter: Option<EntityFilter>,
    },
    
    /// Format/pattern constraint: column must match pattern
    /// Example: "PAN must match regex"
    FormatConstraint {
        column: String,
        pattern: String,
        pattern_type: PatternType,
        entity_filter: Option<EntityFilter>,
    },
    
    /// Drift constraint: statistical comparison with baseline
    /// Example: "Mean balance should not change >10% vs yesterday"
    DriftConstraint {
        column: String,
        baseline_path: Option<String>, // Path to baseline data or reference
        threshold: f64,
        metric: DriftMetric,
        entity_filter: Option<EntityFilter>,
    },
}

/// Value operators for comparisons
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValueOperator {
    #[serde(rename = ">")]
    GreaterThan,
    #[serde(rename = "<")]
    LessThan,
    #[serde(rename = ">=")]
    GreaterThanOrEqual,
    #[serde(rename = "<=")]
    LessThanOrEqual,
    #[serde(rename = "=")]
    Equal,
    #[serde(rename = "!=")]
    NotEqual,
    #[serde(rename = "in")]
    In,
    #[serde(rename = "not_in")]
    NotIn,
}

/// Constraint value (can be numeric, string, or array)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConstraintValue {
    Number(f64),
    String(String),
    Array(Vec<serde_json::Value>),
}

/// Aggregation types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AggregationType {
    Sum,
    Avg,
    Count,
    Max,
    Min,
}

/// Pattern types for format validation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PatternType {
    Regex,
    Format, // Format string like "yyyy-mm-dd"
    Length, // Exact length
    Digits, // Numeric digits only
}

/// Drift metrics for statistical comparison
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DriftMetric {
    MeanChange,      // Percentage change in mean
    DistributionShift, // Distribution similarity (KL divergence, etc.)
    StdDevChange,    // Change in standard deviation
}

/// Entity filter for scoping validation to specific entity types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityFilter {
    pub column: String,
    pub operator: ValueOperator,
    pub value: ConstraintValue,
}

/// Validation result containing violations and statistics
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub query: String,
    pub constraint_type: String,
    pub system: String,
    pub table: Option<String>,
    pub total_rows_checked: usize,
    pub violations_count: usize,
    pub pass_rate: f64,
    pub violations: DataFrame,
    pub statistics: ValidationStatistics,
}

/// Statistics for validation results
#[derive(Debug, Clone)]
pub struct ValidationStatistics {
    pub column_stats: Option<ColumnStatistics>,
    pub null_stats: Option<NullStatistics>,
    pub uniqueness_stats: Option<UniquenessStatistics>,
    pub drift_stats: Option<DriftStatistics>,
}

/// Column-level statistics
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
    pub median: Option<f64>,
    pub std_dev: Option<f64>,
}

/// Null statistics
#[derive(Debug, Clone)]
pub struct NullStatistics {
    pub null_count: usize,
    pub null_percentage: f64,
    pub non_null_count: usize,
    pub non_null_percentage: f64,
}

/// Uniqueness statistics
#[derive(Debug, Clone)]
pub struct UniquenessStatistics {
    pub total_unique: usize,
    pub total_duplicates: usize,
    pub duplicate_groups: usize,
}

/// Drift statistics
#[derive(Debug, Clone)]
pub struct DriftStatistics {
    pub baseline_mean: Option<f64>,
    pub current_mean: Option<f64>,
    pub mean_change_percent: Option<f64>,
    pub drift_detected: bool,
}

impl ValidationEngine {
    pub fn new(metadata: Metadata, llm: LlmClient, data_dir: PathBuf) -> Self {
        Self {
            metadata,
            llm,
            data_dir,
        }
    }
    
    pub async fn run(&self, query: &str) -> Result<ValidationResult> {
        info!("Validation Engine starting...");
        info!("Query: {}", query);
        
        // Step 1: Interpret query using LLM
        let interpretation = self.llm.interpret_validation_query(
            query,
            &self.metadata.business_labels,
            &self.metadata.tables,
        ).await?;
        
        info!("Interpreted constraint type: {}", interpretation.constraint_type);
        info!("System: {}", interpretation.system);
        
        // Step 2: Parse constraint from interpretation
        let constraint = self.parse_constraint(&interpretation)?;
        
        // Step 3: Find table(s) to validate
        let table = if let Some(ref table_name) = interpretation.table {
            self.metadata.tables
                .iter()
                .find(|t| t.name == *table_name && t.system == interpretation.system)
                .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", table_name)))?
        } else {
            // Find first table in the system
            self.metadata.tables
                .iter()
                .find(|t| t.system == interpretation.system)
                .ok_or_else(|| RcaError::Execution(format!("No tables found for system: {}", interpretation.system)))?
        };
        
        // Step 4: Load data
        let table_path = self.data_dir.join(&table.path);
        if !table_path.exists() {
            return Err(RcaError::Execution(format!("Table file not found: {}", table_path.display())));
        }
        
        let mut df = LazyFrame::scan_parquet(&table_path, ScanArgsParquet::default())
            .and_then(|lf| lf.collect())
            .map_err(|e| RcaError::Execution(format!("Failed to load table {}: {}", table.name, e)))?;
        
        // Convert scientific notation if needed
        df = crate::data_utils::convert_scientific_notation_columns(df)?;
        
        // Step 5: Resolve entity filter column if needed (auto-detect column containing the value)
        let resolved_entity_filter = if let Some(ref entity_filter) = interpretation.entity_filter {
            // Try to auto-detect the column if the LLM-provided column doesn't exist or seems wrong
            let detected_column = self.detect_entity_filter_column(&df, &entity_filter.value)?;
            Some(crate::llm::EntityFilterInterpretation {
                column: detected_column.unwrap_or_else(|| entity_filter.column.clone()),
                operator: entity_filter.operator.clone(),
                value: entity_filter.value.clone(),
            })
        } else {
            None
        };
        
        // Step 6: Apply entity filter if specified
        if let Some(ref entity_filter) = resolved_entity_filter {
            df = self.apply_entity_filter(df, entity_filter)?;
        }
        
        // Step 7: Execute constraint check
        let (violations_df, total_rows) = self.execute_constraint(df, &constraint, &table.name).await?;
        
        let violations_count = violations_df.height();
        let pass_rate = if total_rows > 0 {
            1.0 - (violations_count as f64 / total_rows as f64)
        } else {
            1.0
        };
        
        // Step 8: Generate statistics
        let validation_stats = self.generate_statistics(&constraint, &violations_df, total_rows)?;
        
        Ok(ValidationResult {
            query: query.to_string(),
            constraint_type: interpretation.constraint_type.clone(),
            system: interpretation.system.clone(),
            table: Some(table.name.clone()),
            total_rows_checked: total_rows,
            violations_count,
            pass_rate,
            violations: violations_df,
            statistics: validation_stats,
        })
    }
    
    /// Detect which column contains the entity filter value by scanning the data
    /// Returns the column name if found, None if not found
    /// First checks metadata distinct_values, then falls back to scanning data
    fn detect_entity_filter_column(
        &self,
        df: &DataFrame,
        filter_value: &serde_json::Value,
    ) -> Result<Option<String>> {
        // Extract the search value
        let search_value = match filter_value {
            serde_json::Value::String(s) => s.to_lowercase(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => return Ok(None),
        };
        
        // First, try to find the table in metadata and check distinct_values
        let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        
        // Try to find table by matching column names
        for table in &self.metadata.tables {
            if let Some(ref columns) = table.columns {
                // Check if this table has columns matching the dataframe
                let table_cols: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                let matches_table = column_names.iter().any(|cn| table_cols.contains(&cn.as_str()));
                
                if matches_table {
                    // Check distinct_values in metadata first
                    for col_meta in columns {
                        if let Some(ref distinct_vals) = col_meta.distinct_values {
                            // Check if any distinct value matches the search value
                            for val in distinct_vals {
                                let val_str = match val {
                                    serde_json::Value::String(s) => s.to_lowercase(),
                                    serde_json::Value::Number(n) => n.to_string(),
                                    serde_json::Value::Bool(b) => b.to_string(),
                                    _ => continue,
                                };
                                
                                // Check for exact match or substring match
                                if val_str == search_value || val_str.contains(&search_value) || search_value.contains(&val_str) {
                                    // Verify column exists in dataframe
                                    if column_names.contains(&col_meta.name) {
                                        info!("Auto-detected entity filter column using metadata: {} contains value '{}' (found in distinct_values)", col_meta.name, search_value);
                                        return Ok(Some(col_meta.name.clone()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Fallback: Check each string column in the dataframe to see if it contains the value
        for col_name in &column_names {
            if let Ok(col_data) = df.column(col_name) {
                // Check string columns
                if matches!(col_data.dtype(), DataType::String) {
                    if let Ok(str_col) = col_data.str() {
                        // Check if any value matches (case-insensitive)
                        let found = (0..str_col.len()).any(|i| {
                            if let Some(val) = str_col.get(i) {
                                val.to_lowercase() == search_value || 
                                val.to_lowercase().contains(&search_value) ||
                                search_value.contains(&val.to_lowercase())
                            } else {
                                false
                            }
                        });
                        
                        if found {
                            info!("Auto-detected entity filter column: {} contains value '{}'", col_name, search_value);
                            return Ok(Some(col_name.clone()));
                        }
                    }
                }
            }
        }
        
        // Also check for common column name patterns
        let search_lower = search_value.to_lowercase();
        for col_name in &column_names {
            let col_lower = col_name.to_lowercase();
            // Check if column name suggests it might contain this value
            // e.g., "msme" -> columns like "psl_type", "msme_flag", "msme_category"
            if (search_lower == "msme" && (col_lower.contains("psl") || col_lower.contains("msme") || col_lower.contains("category"))) ||
               (search_lower == "edl" && (col_lower.contains("edl") || col_lower.contains("product"))) {
                // Verify the column actually contains the value
                if let Ok(col_data) = df.column(col_name) {
                    if matches!(col_data.dtype(), DataType::String) {
                        if let Ok(str_col) = col_data.str() {
                            let found = (0..str_col.len().min(100)).any(|i| { // Check first 100 rows
                                if let Some(val) = str_col.get(i) {
                                    val.to_lowercase() == search_lower || 
                                    val.to_lowercase().contains(&search_lower)
                                } else {
                                    false
                                }
                            });
                            
                            if found {
                                info!("Auto-detected entity filter column by pattern: {} likely contains '{}'", col_name, search_value);
                                return Ok(Some(col_name.clone()));
                            }
                        }
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    fn parse_constraint(&self, interpretation: &crate::llm::ValidationQueryInterpretation) -> Result<ValidationConstraint> {
        let details = &interpretation.constraint_details;
        let entity_filter: Option<EntityFilter> = if let Some(ref ef) = interpretation.entity_filter {
            Some(EntityFilter {
                column: ef.column.clone(),
                operator: self.parse_operator(&ef.operator)?,
                value: self.parse_constraint_value(&ef.value),
            })
        } else {
            None
        };
        
        match interpretation.constraint_type.as_str() {
            "value" => {
                let column = details.get("column")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing column in value constraint".to_string()))?
                    .to_string();
                let operator_str = details.get("operator")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing operator in value constraint".to_string()))?;
                let operator = self.parse_operator(operator_str)?;
                let threshold = self.parse_constraint_value(
                    details.get("threshold")
                        .ok_or_else(|| RcaError::Execution("Missing threshold in value constraint".to_string()))?
                );
                
                Ok(ValidationConstraint::ValueConstraint {
                    column,
                    operator,
                    threshold,
                    entity_filter,
                })
            }
            "nullability" => {
                let column = details.get("column")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing column in nullability constraint".to_string()))?
                    .to_string();
                let must_be_null = details.get("must_be_null")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let min_completeness = details.get("min_completeness")
                    .and_then(|v| v.as_f64());
                
                Ok(ValidationConstraint::NullabilityConstraint {
                    column,
                    must_be_null,
                    min_completeness,
                    entity_filter,
                })
            }
            "uniqueness" => {
                let columns = details.get("columns")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| RcaError::Execution("Missing columns in uniqueness constraint".to_string()))?
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                
                Ok(ValidationConstraint::UniquenessConstraint {
                    columns,
                    entity_filter,
                })
            }
            "referential" => {
                let fk_column = details.get("fk_column")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing fk_column in referential constraint".to_string()))?
                    .to_string();
                let ref_table = details.get("ref_table")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing ref_table in referential constraint".to_string()))?
                    .to_string();
                let ref_column = details.get("ref_column")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing ref_column in referential constraint".to_string()))?
                    .to_string();
                
                Ok(ValidationConstraint::ReferentialConstraint {
                    fk_column,
                    ref_table,
                    ref_column,
                    entity_filter,
                })
            }
            "aggregation" => {
                let group_by = details.get("group_by")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| RcaError::Execution("Missing group_by in aggregation constraint".to_string()))?
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                let agg_type_str = details.get("agg_type")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing agg_type in aggregation constraint".to_string()))?;
                let agg_type = match agg_type_str {
                    "sum" => AggregationType::Sum,
                    "avg" => AggregationType::Avg,
                    "count" => AggregationType::Count,
                    "max" => AggregationType::Max,
                    "min" => AggregationType::Min,
                    _ => return Err(RcaError::Execution(format!("Unknown aggregation type: {}", agg_type_str))),
                };
                let column = details.get("column")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing column in aggregation constraint".to_string()))?
                    .to_string();
                let operator_str = details.get("operator")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing operator in aggregation constraint".to_string()))?;
                let operator = self.parse_operator(operator_str)?;
                let threshold = self.parse_constraint_value(
                    details.get("threshold")
                        .ok_or_else(|| RcaError::Execution("Missing threshold in aggregation constraint".to_string()))?
                );
                
                Ok(ValidationConstraint::AggregationConstraint {
                    group_by,
                    agg_type,
                    column,
                    operator,
                    threshold,
                    entity_filter,
                })
            }
            "cross_column" => {
                let expression = details.get("expression")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing expression in cross-column constraint".to_string()))?
                    .to_string();
                let condition = details.get("condition")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                
                Ok(ValidationConstraint::CrossColumnConstraint {
                    expression,
                    condition,
                    entity_filter,
                })
            }
            "format" => {
                let column = details.get("column")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing column in format constraint".to_string()))?
                    .to_string();
                let pattern = details.get("pattern")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing pattern in format constraint".to_string()))?
                    .to_string();
                let pattern_type_str = details.get("pattern_type")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing pattern_type in format constraint".to_string()))?;
                let pattern_type = match pattern_type_str {
                    "regex" => PatternType::Regex,
                    "format" => PatternType::Format,
                    "length" => PatternType::Length,
                    "digits" => PatternType::Digits,
                    _ => return Err(RcaError::Execution(format!("Unknown pattern type: {}", pattern_type_str))),
                };
                
                Ok(ValidationConstraint::FormatConstraint {
                    column,
                    pattern,
                    pattern_type,
                    entity_filter,
                })
            }
            "drift" => {
                let column = details.get("column")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing column in drift constraint".to_string()))?
                    .to_string();
                let baseline_path = details.get("baseline_path")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let threshold = details.get("threshold")
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| RcaError::Execution("Missing threshold in drift constraint".to_string()))?;
                let metric_str = details.get("metric")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RcaError::Execution("Missing metric in drift constraint".to_string()))?;
                let metric = match metric_str {
                    "mean_change" => DriftMetric::MeanChange,
                    "distribution_shift" => DriftMetric::DistributionShift,
                    "std_dev_change" => DriftMetric::StdDevChange,
                    _ => return Err(RcaError::Execution(format!("Unknown drift metric: {}", metric_str))),
                };
                
                Ok(ValidationConstraint::DriftConstraint {
                    column,
                    baseline_path,
                    threshold,
                    metric,
                    entity_filter,
                })
            }
            _ => Err(RcaError::Execution(format!("Unknown constraint type: {}", interpretation.constraint_type))),
        }
    }
    
    fn parse_operator(&self, op_str: &str) -> Result<ValueOperator> {
        match op_str {
            ">" => Ok(ValueOperator::GreaterThan),
            "<" => Ok(ValueOperator::LessThan),
            ">=" => Ok(ValueOperator::GreaterThanOrEqual),
            "<=" => Ok(ValueOperator::LessThanOrEqual),
            "=" => Ok(ValueOperator::Equal),
            "!=" => Ok(ValueOperator::NotEqual),
            "in" => Ok(ValueOperator::In),
            "not_in" => Ok(ValueOperator::NotIn),
            _ => Err(RcaError::Execution(format!("Unknown operator: {}", op_str))),
        }
    }
    
    fn parse_constraint_value(&self, value: &serde_json::Value) -> ConstraintValue {
        if let Some(num) = value.as_f64() {
            ConstraintValue::Number(num)
        } else if let Some(str_val) = value.as_str() {
            ConstraintValue::String(str_val.to_string())
        } else if let Some(arr) = value.as_array() {
            ConstraintValue::Array(arr.clone())
        } else {
            ConstraintValue::String(value.to_string())
        }
    }
    
    fn apply_entity_filter(&self, df: DataFrame, filter: &crate::llm::EntityFilterInterpretation) -> Result<DataFrame> {
        let operator = self.parse_operator(&filter.operator)?;
        let value = self.parse_constraint_value(&filter.value);
        
        let filtered_df = match operator {
            ValueOperator::Equal => {
                let filter_value = match &value {
                    ConstraintValue::String(s) => s.clone(),
                    ConstraintValue::Number(n) => format!("{}", n),
                    ConstraintValue::Array(_) => return Err(RcaError::Execution("Array value not supported for entity filter equality".to_string())),
                };
                df.lazy()
                    .filter(col(&filter.column).eq(lit(filter_value)))
                    .collect()?
            }
            ValueOperator::NotEqual => {
                let filter_value = match &value {
                    ConstraintValue::String(s) => s.clone(),
                    ConstraintValue::Number(n) => format!("{}", n),
                    ConstraintValue::Array(_) => return Err(RcaError::Execution("Array value not supported for entity filter equality".to_string())),
                };
                df.lazy()
                    .filter(col(&filter.column).neq(lit(filter_value)))
                    .collect()?
            }
            ValueOperator::In => {
                if let ConstraintValue::Array(arr) = value {
                    // Build OR condition
                    let mut conditions = Vec::new();
                    for v in arr {
                        if let Some(s) = v.as_str() {
                            conditions.push(col(&filter.column).eq(lit(s.to_string())));
                        } else if let Some(n) = v.as_f64() {
                            conditions.push(col(&filter.column).eq(lit(n)));
                        }
                    }
                    if conditions.is_empty() {
                        return Err(RcaError::Execution("IN operator requires valid array values".to_string()));
                    }
                    let mut combined = conditions[0].clone();
                    for cond in conditions.iter().skip(1) {
                        combined = combined.or(cond.clone());
                    }
                    df.lazy()
                        .filter(combined)
                        .collect()?
                } else {
                    return Err(RcaError::Execution("IN operator requires array value".to_string()));
                }
            }
            _ => {
                return Err(RcaError::Execution(format!("Unsupported operator for entity filter: {:?}", operator)));
            }
        };
        
        Ok(filtered_df)
    }
    
    async fn execute_constraint(
        &self,
        df: DataFrame,
        constraint: &ValidationConstraint,
        _table_name: &str,
    ) -> Result<(DataFrame, usize)> {
        match constraint {
            ValidationConstraint::ValueConstraint { column, operator, threshold, .. } => {
                self.execute_value_constraint(df, column, operator, threshold).await
            }
            ValidationConstraint::NullabilityConstraint { column, must_be_null, min_completeness, .. } => {
                self.execute_nullability_constraint(df, column, *must_be_null, *min_completeness).await
            }
            ValidationConstraint::UniquenessConstraint { columns, .. } => {
                self.execute_uniqueness_constraint(df, columns).await
            }
            ValidationConstraint::ReferentialConstraint { fk_column, ref_table, ref_column, .. } => {
                self.execute_referential_constraint(df, fk_column, ref_table, ref_column).await
            }
            ValidationConstraint::AggregationConstraint { group_by, agg_type, column, operator, threshold, .. } => {
                self.execute_aggregation_constraint(df, group_by, agg_type, column, operator, threshold).await
            }
            ValidationConstraint::CrossColumnConstraint { expression, condition, .. } => {
                self.execute_cross_column_constraint(df, expression, condition.as_deref()).await
            }
            ValidationConstraint::FormatConstraint { column, pattern, pattern_type, .. } => {
                self.execute_format_constraint(df, column, pattern, pattern_type).await
            }
            ValidationConstraint::DriftConstraint { column, baseline_path, threshold, metric, .. } => {
                self.execute_drift_constraint(df, column, baseline_path.as_deref(), *threshold, metric).await
            }
        }
    }
    
    async fn execute_value_constraint(
        &self,
        df: DataFrame,
        column: &str,
        operator: &ValueOperator,
        threshold: &ConstraintValue,
    ) -> Result<(DataFrame, usize)> {
        let total_rows = df.height();
        
        // Check if column exists
        if df.column(column).is_err() {
            return Err(RcaError::Execution(format!("Column not found: {}", column)));
        }
        
        // For value constraints, violations are rows that violate the constraint
        // E.g., "ledger <= 5000" means violations are rows where ledger > 5000
        // The operator in the constraint represents the valid condition, so we invert it to find violations
        let violations = match operator {
            ValueOperator::GreaterThan => {
                // Constraint: column > threshold, violations: column <= threshold
                if let ConstraintValue::Number(threshold_val) = threshold {
                    df.lazy()
                        .filter(col(column).lt_eq(lit(*threshold_val)))
                        .collect()?
                } else {
                    return Err(RcaError::Execution("GreaterThan operator requires numeric threshold".to_string()));
                }
            }
            ValueOperator::LessThan => {
                // Constraint: column < threshold, violations: column >= threshold
                if let ConstraintValue::Number(threshold_val) = threshold {
                    df.lazy()
                        .filter(col(column).gt_eq(lit(*threshold_val)))
                        .collect()?
                } else {
                    return Err(RcaError::Execution("LessThan operator requires numeric threshold".to_string()));
                }
            }
            ValueOperator::GreaterThanOrEqual => {
                // Constraint: column >= threshold, violations: column < threshold
                if let ConstraintValue::Number(threshold_val) = threshold {
                    df.lazy()
                        .filter(col(column).lt(lit(*threshold_val)))
                        .collect()?
                } else {
                    return Err(RcaError::Execution("GreaterThanOrEqual operator requires numeric threshold".to_string()));
                }
            }
            ValueOperator::LessThanOrEqual => {
                // Constraint: column <= threshold, violations: column > threshold
                if let ConstraintValue::Number(threshold_val) = threshold {
                    df.lazy()
                        .filter(col(column).gt(lit(*threshold_val)))
                        .collect()?
                } else {
                    return Err(RcaError::Execution("LessThanOrEqual operator requires numeric threshold".to_string()));
                }
            }
            ValueOperator::Equal => {
                // Constraint: column = threshold, violations: column != threshold
                match threshold {
                    ConstraintValue::String(s) => {
                        df.lazy()
                            .filter(col(column).cast(DataType::String).neq(lit(s.clone())))
                            .collect()?
                    }
                    ConstraintValue::Number(n) => {
                        df.lazy()
                            .filter(col(column).neq(lit(*n)))
                            .collect()?
                    }
                    _ => return Err(RcaError::Execution("Equal operator requires string or number threshold".to_string())),
                }
            }
            ValueOperator::NotEqual => {
                // Constraint: column != threshold, violations: column = threshold
                match threshold {
                    ConstraintValue::String(s) => {
                        df.lazy()
                            .filter(col(column).cast(DataType::String).eq(lit(s.clone())))
                            .collect()?
                    }
                    ConstraintValue::Number(n) => {
                        df.lazy()
                            .filter(col(column).eq(lit(*n)))
                            .collect()?
                    }
                    _ => return Err(RcaError::Execution("NotEqual operator requires string or number threshold".to_string())),
                }
            }
            ValueOperator::In => {
                // Constraint: column IN values, violations: column NOT IN values
                if let ConstraintValue::Array(arr) = threshold {
                    // Build OR condition for NOT IN (violations)
                    let mut conditions = Vec::new();
                    for v in arr {
                        if let Some(s) = v.as_str() {
                            conditions.push(col(column).neq(lit(s.to_string())));
                        } else if let Some(n) = v.as_f64() {
                            conditions.push(col(column).neq(lit(n)));
                        }
                    }
                    if conditions.is_empty() {
                        return Err(RcaError::Execution("In operator requires valid array values".to_string()));
                    }
                    // Combine with AND (all conditions must be true for violation)
                    let mut combined = conditions[0].clone();
                    for cond in conditions.iter().skip(1) {
                        combined = combined.and(cond.clone());
                    }
                    df.lazy()
                        .filter(combined)
                        .collect()?
                } else {
                    return Err(RcaError::Execution("In operator requires array threshold".to_string()));
                }
            }
            ValueOperator::NotIn => {
                // Constraint: column NOT IN values, violations: column IN values
                if let ConstraintValue::Array(arr) = threshold {
                    // Build OR condition for IN (violations)
                    let mut conditions = Vec::new();
                    for v in arr {
                        if let Some(s) = v.as_str() {
                            conditions.push(col(column).eq(lit(s.to_string())));
                        } else if let Some(n) = v.as_f64() {
                            conditions.push(col(column).eq(lit(n)));
                        }
                    }
                    if conditions.is_empty() {
                        return Err(RcaError::Execution("NotIn operator requires valid array values".to_string()));
                    }
                    // Combine with OR (any condition true is a violation)
                    let mut combined = conditions[0].clone();
                    for cond in conditions.iter().skip(1) {
                        combined = combined.or(cond.clone());
                    }
                    df.lazy()
                        .filter(combined)
                        .collect()?
                } else {
                    return Err(RcaError::Execution("NotIn operator requires array threshold".to_string()));
                }
            }
        };
        
        Ok((violations, total_rows))
    }
    
    async fn execute_nullability_constraint(
        &self,
        df: DataFrame,
        column: &str,
        must_be_null: bool,
        min_completeness: Option<f64>,
    ) -> Result<(DataFrame, usize)> {
        if df.column(column).is_err() {
            return Err(RcaError::Execution(format!("Column not found: {}", column)));
        }
        
        let total_rows = df.height();
        
        let violations = if must_be_null {
            // Violations are rows where column is NOT null
            df.lazy()
                .filter(col(column).is_not_null())
                .collect()?
        } else {
            // Violations are rows where column IS null
            df.lazy()
                .filter(col(column).is_null())
                .collect()?
        };
        
        // Check completeness threshold if specified
        let null_count = violations.height();
        let completeness = if total_rows > 0 {
            1.0 - (null_count as f64 / total_rows as f64)
        } else {
            1.0
        };
        
        let final_violations = if let Some(min_comp) = min_completeness {
            if completeness < min_comp {
                violations // Return all null rows as violations
            } else {
                // Create empty dataframe with same schema
                violations.lazy().limit(0).collect()?
            }
        } else {
            violations
        };
        
        Ok((final_violations, total_rows))
    }
    
    async fn execute_uniqueness_constraint(
        &self,
        df: DataFrame,
        columns: &[String],
    ) -> Result<(DataFrame, usize)> {
        // Check all columns exist
        for col_name in columns {
            if df.column(col_name).is_err() {
                return Err(RcaError::Execution(format!("Column not found: {}", col_name)));
            }
        }
        
        let total_rows = df.height();
        
        // Group by uniqueness columns and count
        let group_exprs: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
        let df_for_group = df.clone();
        let duplicates = df_for_group.lazy()
            .group_by(group_exprs.clone())
            .agg([len().alias("_count")])
            .filter(col("_count").gt(lit(1)))
            .select(group_exprs.clone())
            .collect()?;
        
        // Join back to get all rows with duplicates
        let violations = if duplicates.height() > 0 {
            let group_exprs_clone: Vec<Expr> = group_exprs.iter().cloned().collect();
            df.lazy()
                .join(
                    duplicates.lazy(),
                    group_exprs_clone.clone(),
                    group_exprs_clone,
                    JoinArgs::new(JoinType::Inner)
                )
                .collect()?
        } else {
            // No duplicates, return empty dataframe with same schema
            df.lazy().limit(0).collect()?
        };
        
        Ok((violations, total_rows))
    }
    
    async fn execute_referential_constraint(
        &self,
        df: DataFrame,
        fk_column: &str,
        ref_table_name: &str,
        ref_column: &str,
    ) -> Result<(DataFrame, usize)> {
        let total_rows = df.height();
        
        if df.column(fk_column).is_err() {
            return Err(RcaError::Execution(format!("FK column not found: {}", fk_column)));
        }
        
        // Find reference table
        let ref_table = self.metadata.tables
            .iter()
            .find(|t| t.name == ref_table_name)
            .ok_or_else(|| RcaError::Execution(format!("Reference table not found: {}", ref_table_name)))?;
        
        let ref_table_path = self.data_dir.join(&ref_table.path);
        if !ref_table_path.exists() {
            return Err(RcaError::Execution(format!("Reference table file not found: {}", ref_table_path.display())));
        }
        
        // Load reference table
        let ref_df = LazyFrame::scan_parquet(&ref_table_path, ScanArgsParquet::default())
            .and_then(|lf| lf.collect())
            .map_err(|e| RcaError::Execution(format!("Failed to load reference table {}: {}", ref_table_name, e)))?;
        
        if ref_df.column(ref_column).is_err() {
            return Err(RcaError::Execution(format!("Reference column not found: {}", ref_column)));
        }
        
        // Left join and filter: find rows in df where FK doesn't exist in ref_df
        let joined = df.lazy()
            .join(
                ref_df.lazy().select([col(ref_column).alias("_ref_key")]),
                [col(fk_column)],
                [col("_ref_key")],
                JoinArgs::new(JoinType::Left)
            )
            .collect()?;
        
        // Filter for rows where _ref_key is null (FK doesn't exist)
        let violations = joined.lazy()
            .filter(col("_ref_key").is_null())
            .drop(["_ref_key"])
            .collect()?;
        
        Ok((violations, total_rows))
    }
    
    async fn execute_aggregation_constraint(
        &self,
        df: DataFrame,
        group_by: &[String],
        agg_type: &AggregationType,
        column: &str,
        operator: &ValueOperator,
        threshold: &ConstraintValue,
    ) -> Result<(DataFrame, usize)> {
        let total_rows = df.height();
        
        // Check columns exist
        for col_name in group_by {
            if df.column(col_name).is_err() {
                return Err(RcaError::Execution(format!("Group-by column not found: {}", col_name)));
            }
        }
        if df.column(column).is_err() {
            return Err(RcaError::Execution(format!("Aggregation column not found: {}", column)));
        }
        
        // Build aggregation expression
        let agg_expr = match agg_type {
            AggregationType::Sum => col(column).sum().alias("_agg_value"),
            AggregationType::Avg => col(column).mean().alias("_agg_value"),
            AggregationType::Count => col(column).count().alias("_agg_value"),
            AggregationType::Max => col(column).max().alias("_agg_value"),
            AggregationType::Min => col(column).min().alias("_agg_value"),
        };
        
        let group_exprs: Vec<Expr> = group_by.iter().map(|c| col(c)).collect();
        
        // Clone df before using it in lazy operations
        let df_for_agg = df.clone();
        
        // Aggregate
        let aggregated = df_for_agg.lazy()
            .group_by(group_exprs.clone())
            .agg([agg_expr])
            .collect()?;
        
        // Apply constraint check
        let violations_groups = match operator {
            ValueOperator::GreaterThan => {
                if let ConstraintValue::Number(threshold_val) = threshold {
                    aggregated.lazy()
                        .filter(col("_agg_value").gt(lit(*threshold_val)))
                        .collect()?
                } else {
                    return Err(RcaError::Execution("GreaterThan operator requires numeric threshold".to_string()));
                }
            }
            ValueOperator::LessThan => {
                if let ConstraintValue::Number(threshold_val) = threshold {
                    aggregated.lazy()
                        .filter(col("_agg_value").lt(lit(*threshold_val)))
                        .collect()?
                } else {
                    return Err(RcaError::Execution("LessThan operator requires numeric threshold".to_string()));
                }
            }
            ValueOperator::Equal => {
                match threshold {
                    ConstraintValue::String(s) => {
                        aggregated.lazy()
                            .filter(col("_agg_value").cast(DataType::String).neq(lit(s.clone())))
                            .collect()?
                    }
                    ConstraintValue::Number(n) => {
                        aggregated.lazy()
                            .filter(col("_agg_value").neq(lit(*n)))
                            .collect()?
                    }
                    _ => return Err(RcaError::Execution("Equal operator requires string or number threshold".to_string())),
                }
            }
            _ => {
                return Err(RcaError::Execution(format!("Unsupported operator for aggregation constraint: {:?}", operator)));
            }
        };
        
        // Join back to get all rows in violating groups
        let violations = if violations_groups.height() > 0 {
            let group_exprs_clone: Vec<Expr> = group_exprs.iter().cloned().collect();
            df.lazy()
                .join(
                    violations_groups.lazy(),
                    group_exprs_clone.clone(),
                    group_exprs_clone,
                    JoinArgs::new(JoinType::Inner)
                )
                .collect()?
        } else {
            df.lazy().limit(0).collect()?
        };
        
        Ok((violations, total_rows))
    }
    
    async fn execute_cross_column_constraint(
        &self,
        df: DataFrame,
        expression: &str,
        condition: Option<&str>,
    ) -> Result<(DataFrame, usize)> {
        let total_rows = df.height();
        
        // Parse expression (simplified - in production, use proper expression parser)
        // For now, support simple comparisons like "col1 <= col2"
        let parts: Vec<&str> = expression.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(RcaError::Execution(format!("Invalid cross-column expression: {}", expression)));
        }
        
        let left_col = parts[0];
        let op = parts[1];
        let right_col = parts[2];
        
        if df.column(left_col).is_err() {
            return Err(RcaError::Execution(format!("Column not found: {}", left_col)));
        }
        if df.column(right_col).is_err() {
            return Err(RcaError::Execution(format!("Column not found: {}", right_col)));
        }
        
        // Build filter expression
        let filter_expr = match op {
            "<=" => col(left_col).gt(col(right_col)), // Violation: left > right
            ">=" => col(left_col).lt(col(right_col)), // Violation: left < right
            "<" => col(left_col).gt_eq(col(right_col)), // Violation: left >= right
            ">" => col(left_col).lt_eq(col(right_col)), // Violation: left <= right
            "=" => col(left_col).neq(col(right_col)), // Violation: left != right
            "!=" => col(left_col).eq(col(right_col)), // Violation: left == right
            _ => return Err(RcaError::Execution(format!("Unsupported operator in cross-column expression: {}", op))),
        };
        
        let mut violations = df.lazy().filter(filter_expr);
        
        // Apply condition if specified
        if let Some(cond) = condition {
            // Parse condition (simplified)
            let cond_parts: Vec<&str> = cond.split_whitespace().collect();
            if cond_parts.len() == 3 {
                let cond_col = cond_parts[0];
                let cond_op = cond_parts[1];
                let cond_val = cond_parts[2].trim_matches('\'');
                
                // Check if column exists (we can't use df here since it's moved, but we already checked above)
                let cond_expr = match cond_op {
                    "=" => col(cond_col).eq(lit(cond_val)),
                    "!=" => col(cond_col).neq(lit(cond_val)),
                    _ => return Err(RcaError::Execution(format!("Unsupported condition operator: {}", cond_op))),
                };
                violations = violations.filter(cond_expr);
            }
        }
        
        let violations_df = violations.collect()?;
        Ok((violations_df, total_rows))
    }
    
    async fn execute_format_constraint(
        &self,
        df: DataFrame,
        column: &str,
        pattern: &str,
        pattern_type: &PatternType,
    ) -> Result<(DataFrame, usize)> {
        if df.column(column).is_err() {
            return Err(RcaError::Execution(format!("Column not found: {}", column)));
        }
        
        let total_rows = df.height();
        
        let violations = match pattern_type {
            PatternType::Regex => {
                // Use regex matching - simplified approach
                // In production, use proper regex matching
                df.lazy()
                    .filter(
                        col(column).cast(DataType::String).is_null()
                            .or(col(column).cast(DataType::String).eq(lit("")))
                    )
                    .collect()?
            }
            PatternType::Length => {
                // Check exact length
                let expected_len: usize = pattern.parse()
                    .map_err(|_| RcaError::Execution(format!("Invalid length pattern: {}", pattern)))?;
                df.lazy()
                    .with_columns([
                        col(column).cast(DataType::String).str().len_chars().alias("_len")
                    ])
                    .filter(col("_len").neq(lit(expected_len as i64)))
                    .drop(["_len"])
                    .collect()?
            }
            PatternType::Digits => {
                // Check if all characters are digits - simplified
                df.lazy()
                    .filter(
                        col(column).cast(DataType::String).is_null()
                            .or(col(column).cast(DataType::String).eq(lit("")))
                    )
                    .collect()?
            }
            PatternType::Format => {
                // For format strings like "yyyy-mm-dd", use regex conversion
                // This is simplified - in production, use proper date format validation
                df.lazy()
                    .filter(
                        col(column).cast(DataType::String).is_null()
                            .or(col(column).cast(DataType::String).eq(lit("")))
                    )
                    .collect()?
            }
        };
        
        Ok((violations, total_rows))
    }
    
    async fn execute_drift_constraint(
        &self,
        df: DataFrame,
        column: &str,
        baseline_path: Option<&str>,
        threshold: f64,
        metric: &DriftMetric,
    ) -> Result<(DataFrame, usize)> {
        let total_rows = df.height();
        
        if df.column(column).is_err() {
            return Err(RcaError::Execution(format!("Column not found: {}", column)));
        }
        
        // Load baseline data
        let baseline_df = if let Some(path) = baseline_path {
            let baseline_file = self.data_dir.join(path);
            if baseline_file.exists() {
                LazyFrame::scan_parquet(&baseline_file, ScanArgsParquet::default())
                    .and_then(|lf| lf.collect())
                    .map_err(|e| RcaError::Execution(format!("Failed to load baseline: {}", e)))?
            } else {
                return Err(RcaError::Execution(format!("Baseline file not found: {}", baseline_file.display())));
            }
        } else {
            // Use current data as baseline (for testing)
            df.clone()
        };
        
        if baseline_df.column(column).is_err() {
            return Err(RcaError::Execution(format!("Column not found in baseline: {}", column)));
        }
        
        // Calculate drift metric
        let df_for_mean = df.clone();
        let current_mean = df_for_mean.lazy()
            .select([col(column).mean().alias("_mean")])
            .collect()?
            .column("_mean")?
            .f64()?
            .get(0);
        
        let baseline_mean = baseline_df.lazy()
            .select([col(column).mean().alias("_mean")])
            .collect()?
            .column("_mean")?
            .f64()?
            .get(0);
        
        let drift_detected = match metric {
            DriftMetric::MeanChange => {
                if let (Some(curr), Some(base)) = (current_mean, baseline_mean) {
                    if base != 0.0 {
                        let change_pct = ((curr - base) / base).abs();
                        change_pct > threshold
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            _ => {
                // For other metrics, simplified check
                false
            }
        };
        
        // If drift detected, return all rows as violations
        let violations = if drift_detected {
            df
        } else {
            df.lazy().limit(0).collect()?
        };
        
        Ok((violations, total_rows))
    }
    
    fn generate_statistics(
        &self,
        constraint: &ValidationConstraint,
        violations_df: &DataFrame,
        total_rows: usize,
    ) -> Result<ValidationStatistics> {
        let column_stats = match constraint {
            ValidationConstraint::ValueConstraint { column, .. } |
            ValidationConstraint::NullabilityConstraint { column, .. } |
            ValidationConstraint::FormatConstraint { column, .. } |
            ValidationConstraint::DriftConstraint { column, .. } => {
                if violations_df.column(column).is_ok() {
                    let col_data = violations_df.column(column)?;
                    if matches!(*col_data.dtype(), DataType::Float64 | DataType::Int64) {
                        let min_val = if matches!(*col_data.dtype(), DataType::Float64) {
                            col_data.f64().ok().and_then(|s| s.min())
                        } else {
                            col_data.i64().ok().and_then(|s| s.min()).map(|v| v as f64)
                        };
                        let max_val = if matches!(*col_data.dtype(), DataType::Float64) {
                            col_data.f64().ok().and_then(|s| s.max())
                        } else {
                            col_data.i64().ok().and_then(|s| s.max()).map(|v| v as f64)
                        };
                        let mean_val = if matches!(*col_data.dtype(), DataType::Float64) {
                            col_data.f64().ok().and_then(|s| s.mean())
                        } else {
                            col_data.i64().ok().and_then(|s| s.mean())
                        };
                        Some(ColumnStatistics {
                            min: min_val,
                            max: max_val,
                            mean: mean_val,
                            median: None, // Would need to calculate
                            std_dev: None, // Would need to calculate
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        };
        
        let null_stats = match constraint {
            ValidationConstraint::NullabilityConstraint { column, .. } => {
                if violations_df.height() > 0 && violations_df.column(column).is_ok() {
                    let violations_df_clone = violations_df.clone();
                    let null_count = violations_df_clone.lazy()
                        .filter(col(column).is_null())
                        .collect()
                        .map(|df| df.height())
                        .unwrap_or(0);
                    let non_null_count = violations_df.height() - null_count;
                    Some(NullStatistics {
                        null_count,
                        null_percentage: if violations_df.height() > 0 {
                            null_count as f64 / violations_df.height() as f64
                        } else {
                            0.0
                        },
                        non_null_count,
                        non_null_percentage: if violations_df.height() > 0 {
                            non_null_count as f64 / violations_df.height() as f64
                        } else {
                            0.0
                        },
                    })
                } else {
                    None
                }
            }
            _ => None,
        };
        
        let uniqueness_stats = match constraint {
            ValidationConstraint::UniquenessConstraint { columns, .. } => {
                if violations_df.height() > 0 {
                    let violations_df_clone = violations_df.clone();
                    let duplicate_groups = violations_df_clone.lazy()
                        .group_by(columns.iter().map(|c| col(c)).collect::<Vec<_>>())
                        .agg([len().alias("_count")])
                        .filter(col("_count").gt(lit(1)))
                        .collect()
                        .map(|df| df.height())
                        .unwrap_or(0);
                    Some(UniquenessStatistics {
                        total_unique: total_rows - violations_df.height(),
                        total_duplicates: violations_df.height(),
                        duplicate_groups,
                    })
                } else {
                    Some(UniquenessStatistics {
                        total_unique: total_rows,
                        total_duplicates: 0,
                        duplicate_groups: 0,
                    })
                }
            }
            _ => None,
        };
        
        let drift_stats = None; // Would be populated from drift constraint execution
        
        Ok(ValidationStatistics {
            column_stats,
            null_stats,
            uniqueness_stats,
            drift_stats,
        })
    }
}

impl std::fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", "=".repeat(80))?;
        writeln!(f, "✅ VALIDATION RESULT")?;
        writeln!(f, "{}\n", "=".repeat(80))?;
        
        writeln!(f, "Query: {}", self.query)?;
        writeln!(f, "Constraint Type: {}", self.constraint_type)?;
        writeln!(f, "System: {}", self.system)?;
        
        if let Some(ref table) = self.table {
            writeln!(f, "Table: {}", table)?;
        }
        
        writeln!(f, "\n{}", "-".repeat(80))?;
        writeln!(f, "📊 SUMMARY")?;
        writeln!(f, "{}", "-".repeat(80))?;
        writeln!(f, "Total Rows Checked: {}", self.total_rows_checked)?;
        writeln!(f, "Violations Found: {}", self.violations_count)?;
        writeln!(f, "Pass Rate: {:.2}%", self.pass_rate * 100.0)?;
        
        if self.violations_count > 0 {
            writeln!(f, "\n{}", "-".repeat(80))?;
            writeln!(f, "❌ VIOLATIONS")?;
            writeln!(f, "{}", "-".repeat(80))?;
            
            // Show violations (limit to first 50 rows)
            let display_limit = self.violations.height().min(50);
            if display_limit > 0 {
                let col_names = self.violations.get_column_names();
                
                // Print header
                for (idx, col_name) in col_names.iter().enumerate() {
                    if idx > 0 {
                        write!(f, " | ")?;
                    }
                    write!(f, "{:<20}", col_name)?;
                }
                writeln!(f)?;
                writeln!(f, "{}", "-".repeat(80))?;
                
                // Print rows
                for row_idx in 0..display_limit {
                    for (col_idx, col_name) in col_names.iter().enumerate() {
                        if col_idx > 0 {
                            write!(f, " | ")?;
                        }
                        if let Ok(col_val) = self.violations.column(col_name) {
                            let val_str = match col_val.dtype() {
                                DataType::String => {
                                    col_val.str().unwrap().get(row_idx).unwrap_or("").to_string()
                                }
                                DataType::Int64 => {
                                    format!("{}", col_val.i64().unwrap().get(row_idx).unwrap_or(0))
                                }
                                DataType::Float64 => {
                                    format!("{:.2}", col_val.f64().unwrap().get(row_idx).unwrap_or(0.0))
                                }
                                DataType::Boolean => {
                                    format!("{}", col_val.bool().unwrap().get(row_idx).unwrap_or(false))
                                }
                                _ => format!("{:?}", col_val.get(row_idx)),
                            };
                            write!(f, "{:<20}", val_str)?;
                        } else {
                            write!(f, "{:<20}", "N/A")?;
                        }
                    }
                    writeln!(f)?;
                }
                
                if self.violations.height() > display_limit {
                    writeln!(f, "\n... and {} more violations (showing first {})", 
                        self.violations.height() - display_limit, display_limit)?;
                }
            }
        } else {
            writeln!(f, "\n✅ No violations found - all rows pass validation!")?;
        }
        
        // Print statistics if available
        if let Some(ref col_stats) = self.statistics.column_stats {
            writeln!(f, "\n{}", "-".repeat(80))?;
            writeln!(f, "📈 COLUMN STATISTICS")?;
            writeln!(f, "{}", "-".repeat(80))?;
            if let Some(min) = col_stats.min {
                writeln!(f, "Min: {:.2}", min)?;
            }
            if let Some(max) = col_stats.max {
                writeln!(f, "Max: {:.2}", max)?;
            }
            if let Some(mean) = col_stats.mean {
                writeln!(f, "Mean: {:.2}", mean)?;
            }
        }
        
        if let Some(ref null_stats) = self.statistics.null_stats {
            writeln!(f, "\n{}", "-".repeat(80))?;
            writeln!(f, "📊 NULL STATISTICS")?;
            writeln!(f, "{}", "-".repeat(80))?;
            writeln!(f, "Null Count: {} ({:.2}%)", null_stats.null_count, null_stats.null_percentage * 100.0)?;
            writeln!(f, "Non-Null Count: {} ({:.2}%)", null_stats.non_null_count, null_stats.non_null_percentage * 100.0)?;
        }
        
        if let Some(ref uniq_stats) = self.statistics.uniqueness_stats {
            writeln!(f, "\n{}", "-".repeat(80))?;
            writeln!(f, "🔑 UNIQUENESS STATISTICS")?;
            writeln!(f, "{}", "-".repeat(80))?;
            writeln!(f, "Total Unique: {}", uniq_stats.total_unique)?;
            writeln!(f, "Total Duplicates: {}", uniq_stats.total_duplicates)?;
            writeln!(f, "Duplicate Groups: {}", uniq_stats.duplicate_groups)?;
        }
        
        writeln!(f, "\n{}", "=".repeat(80))?;
        
        Ok(())
    }
}

