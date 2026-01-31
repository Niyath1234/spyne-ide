//! SQL Compiler - Deterministic SQL generation from JSON intent
//! 
//! Takes a JSON intent specification and generates valid SQL using actual table/column metadata.
//! This ensures SQL is always correct and uses real schema information.

use crate::error::{RcaError, Result};
use crate::metadata::Metadata;
use crate::intent::SemanticSqlIntent;
use crate::intent::semantic_intent::DimensionIntent;
use crate::semantic::registry::SemanticRegistry;
use crate::compiler::join_resolver::JoinResolver;
use crate::compiler::join_planner::JoinPlanner;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// SQL Intent - JSON specification from LLM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlIntent {
    /// Tables to query (can be partial names, will be matched)
    pub tables: Vec<String>,
    
    /// Columns to select (can be partial names, will be matched)
    pub columns: Option<Vec<ColumnSpec>>,
    
    /// Aggregations to perform
    pub aggregations: Option<Vec<AggregationSpec>>,
    
    /// Filter conditions
    pub filters: Option<Vec<FilterSpec>>,
    
    /// Group by columns
    pub group_by: Option<Vec<String>>,
    
    /// Order by columns
    pub order_by: Option<Vec<OrderBySpec>>,
    
    /// Limit number of rows
    pub limit: Option<usize>,
    
    /// Joins between tables
    pub joins: Option<Vec<JoinSpec>>,
    
    /// Date/time constraints
    #[serde(default)]
    pub date_constraint: Option<DateConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSpec {
    /// Column name or pattern (e.g., "outstanding", "balance")
    pub name: String,
    
    /// Table name (optional, for disambiguation)
    pub table: Option<String>,
    
    /// Alias for the column
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationSpec {
    /// Aggregation function: "sum", "avg", "count", "min", "max"
    pub function: String,
    
    /// Column to aggregate
    pub column: String,
    
    /// Table name (optional)
    pub table: Option<String>,
    
    /// Alias for the result
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSpec {
    /// Column name
    pub column: String,
    
    /// Table name (optional)
    pub table: Option<String>,
    
    /// Operator: "=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "IS NULL", "IS NOT NULL"
    pub operator: String,
    
    /// Value (can be string, number, boolean, or array for IN)
    /// Optional for operators like "IS NULL" and "IS NOT NULL"
    #[serde(default)]
    pub value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBySpec {
    /// Column name
    pub column: String,
    
    /// Table name (optional)
    pub table: Option<String>,
    
    /// Direction: "ASC" or "DESC"
    pub direction: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinSpec {
    /// Left table name
    pub left_table: String,
    
    /// Right table name
    pub right_table: String,
    
    /// Join type: "INNER", "LEFT", "RIGHT", "FULL"
    pub join_type: Option<String>,
    
    /// Join condition (column pairs)
    pub condition: Vec<JoinCondition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    /// Left column name
    pub left_column: String,
    
    /// Right column name
    pub right_column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateConstraint {
    /// Column name for date filtering
    pub column: Option<String>,
    
    /// Date value or range (optional - empty objects are valid)
    #[serde(default)]
    pub value: Option<DateValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DateValue {
    /// Single date: "2024-12-31"
    Single(String),
    /// Date range: {"start": "2024-01-01", "end": "2024-12-31"}
    Range { start: String, end: String },
    /// Relative date: "end_of_year", "start_of_year", "today", "yesterday"
    Relative(String),
}

/// SQL Compiler - Deterministic SQL generation
pub struct SqlCompiler {
    metadata: Metadata,
}

impl SqlCompiler {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
    
    /// Compile SQL intent to actual SQL query
    pub fn compile(&self, intent: &SqlIntent) -> Result<String> {
        info!("🔧 Compiling SQL intent to query...");
        
        // Step 1: Resolve table names
        let tables = self.resolve_tables(&intent.tables)?;
        if tables.is_empty() {
            return Err(RcaError::Execution("No matching tables found".to_string()));
        }
        
        let main_table = &tables[0];
        
        // Step 2: Build SELECT clause
        let select_clause = self.build_select_clause(intent, main_table)?;
        
        // Step 3: Build FROM clause
        let from_clause = format!("FROM {}", main_table.name);
        
        // Step 4: Build JOIN clauses
        let join_clauses = self.build_join_clauses(intent, &tables)?;
        
        // Step 5: Build WHERE clause
        let where_clause = self.build_where_clause(intent, &tables)?;
        
        // Step 6: Build GROUP BY clause
        let group_by_clause = self.build_group_by_clause(intent, &tables)?;
        
        // Step 7: Build ORDER BY clause
        let order_by_clause = self.build_order_by_clause(intent, &tables)?;
        
        // Step 8: Build LIMIT clause
        let limit_clause = if let Some(limit) = intent.limit {
            format!("LIMIT {}", limit)
        } else {
            String::new()
        };
        
        // Combine all clauses
        let mut sql_parts = vec![select_clause, from_clause];
        sql_parts.extend(join_clauses);
        if !where_clause.is_empty() {
            sql_parts.push(where_clause);
        }
        if !group_by_clause.is_empty() {
            sql_parts.push(group_by_clause);
        }
        if !order_by_clause.is_empty() {
            sql_parts.push(order_by_clause);
        }
        if !limit_clause.is_empty() {
            sql_parts.push(limit_clause);
        }
        
        let sql = sql_parts.join(" ");
        info!("✅ Generated SQL: {}", sql);
        
        Ok(sql)
    }
    
    /// Resolve table names from intent (match partial names)
    fn resolve_tables(&self, table_names: &[String]) -> Result<Vec<&crate::metadata::Table>> {
        let mut resolved = Vec::new();
        let mut resolved_names = std::collections::HashSet::new();
        
        for table_name in table_names {
            // Try exact match first
            if let Some(table) = self.metadata.tables.iter().find(|t| t.name == *table_name) {
                if !resolved_names.contains(&table.name) {
                    resolved_names.insert(table.name.clone());
                    resolved.push(table);
                }
                continue;
            }
            
            // Try partial match (contains)
            if let Some(table) = self.metadata.tables.iter()
                .find(|t| (t.name.contains(table_name) || table_name.contains(&t.name)) && !resolved_names.contains(&t.name)) {
                resolved_names.insert(table.name.clone());
                resolved.push(table);
                continue;
            }
            
            // Try matching by entity or system
            if let Some(table) = self.metadata.tables.iter()
                .find(|t| (t.entity == *table_name || t.system == *table_name) && !resolved_names.contains(&t.name)) {
                resolved_names.insert(table.name.clone());
                resolved.push(table);
            }
        }
        
        Ok(resolved)
    }
    
    /// Build SELECT clause
    fn build_select_clause(&self, intent: &SqlIntent, main_table: &crate::metadata::Table) -> Result<String> {
        let mut select_parts = Vec::new();
        
        // Handle aggregations
        if let Some(ref aggregations) = intent.aggregations {
            for agg in aggregations {
                let column = self.resolve_column(&agg.column, &agg.table, main_table)?;
                let func = agg.function.to_uppercase();
                let alias = agg.alias.as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default();
                select_parts.push(format!("{}({}){}", func, column, alias));
            }
        }
        
        // Handle regular columns
        if let Some(ref columns) = intent.columns {
            for col_spec in columns {
                let column = self.resolve_column(&col_spec.name, &col_spec.table, main_table)?;
                let alias = col_spec.alias.as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default();
                select_parts.push(format!("{}{}", column, alias));
            }
        }
        
        // Default: SELECT * if nothing specified
        if select_parts.is_empty() {
            select_parts.push("*".to_string());
        }
        
        Ok(format!("SELECT {}", select_parts.join(", ")))
    }
    
    /// Resolve column name (match partial names)
    fn resolve_column(&self, column_name: &str, table_name: &Option<String>, default_table: &crate::metadata::Table) -> Result<String> {
        let table = if let Some(ref tname) = table_name {
            self.resolve_tables(&[tname.clone()])?
                .first()
                .ok_or_else(|| RcaError::Execution(format!("Table not found: {}", tname)))?
        } else {
            default_table
        };
        
        // Try exact match
        if let Some(ref columns) = table.columns {
            if let Some(col) = columns.iter().find(|c| c.name == *column_name) {
                return Ok(format!("{}.{}", table.name, col.name));
            }
            
            // Try partial match (contains, case-insensitive)
            if let Some(col) = columns.iter().find(|c| 
                c.name.to_lowercase().contains(&column_name.to_lowercase()) ||
                column_name.to_lowercase().contains(&c.name.to_lowercase())
            ) {
                return Ok(format!("{}.{}", table.name, col.name));
            }
        }
        
        // If not found, return as-is (might be an expression)
        Ok(format!("{}.{}", table.name, column_name))
    }
    
    /// Build WHERE clause
    fn build_where_clause(&self, intent: &SqlIntent, tables: &[&crate::metadata::Table]) -> Result<String> {
        let mut conditions = Vec::new();
        
        // Handle filters
        if let Some(ref filters) = intent.filters {
            for filter in filters {
                let column = self.resolve_column(&filter.column, &filter.table, tables[0])?;
                let condition = self.build_filter_condition(&column, &filter.operator, &filter.value)?;
                conditions.push(condition);
            }
        }
        
        // Handle date constraint
        if let Some(ref date_constraint) = intent.date_constraint {
            // Only process if value is present (skip empty date_constraint objects)
            if let Some(ref date_value) = date_constraint.value {
                let date_col = if let Some(ref col) = date_constraint.column {
                    self.resolve_column(col, &None, tables[0])?
                } else if let Some(ref time_col) = tables[0].time_column {
                    format!("{}.{}", tables[0].name, time_col)
                } else {
                    return Err(RcaError::Execution("No date column specified and table has no time_column".to_string()));
                };
                
                let date_condition = match date_value {
                    DateValue::Single(date) => format!("{} = '{}'", date_col, date),
                    DateValue::Range { start, end } => format!("{} >= '{}' AND {} <= '{}'", date_col, start, date_col, end),
                    DateValue::Relative(rel) => {
                        match rel.as_str() {
                            "end_of_year" => format!("{} = (SELECT MAX({}) FROM {})", date_col, date_col, tables[0].name),
                            "start_of_year" => format!("{} = (SELECT MIN({}) FROM {})", date_col, date_col, tables[0].name),
                            _ => format!("{} = CURRENT_DATE", date_col), // Default to today
                        }
                    }
                };
                conditions.push(date_condition);
            }
        }
        
        if conditions.is_empty() {
            return Ok(String::new());
        }
        
        Ok(format!("WHERE {}", conditions.join(" AND ")))
    }
    
    /// Build filter condition
    fn build_filter_condition(&self, column: &str, operator: &str, value: &Option<serde_json::Value>) -> Result<String> {
        let op = operator.to_uppercase();
        match op.as_str() {
            "IS NULL" => Ok(format!("{} IS NULL", column)),
            "IS NOT NULL" => Ok(format!("{} IS NOT NULL", column)),
            "=" | "!=" | ">" | "<" | ">=" | "<=" => {
                let val = value.as_ref().ok_or_else(|| {
                    RcaError::Execution(format!("Operator {} requires a value", operator))
                })?;
                let val_str = self.format_value(val);
                // For string equality, make case-insensitive
                if op == "=" && val.is_string() {
                    Ok(format!("UPPER({}) = UPPER({})", column, val_str))
                } else {
                    Ok(format!("{} {} {}", column, op, val_str))
                }
            }
            "IN" => {
                let val = value.as_ref().ok_or_else(|| {
                    RcaError::Execution("IN operator requires a value".to_string())
                })?;
                if let Some(arr) = val.as_array() {
                    let vals: Vec<String> = arr.iter()
                        .map(|v| self.format_value(v))
                        .collect();
                    Ok(format!("{} IN ({})", column, vals.join(", ")))
                } else {
                    Err(RcaError::Execution("IN operator requires an array value".to_string()))
                }
            }
            "NOT IN" => {
                let val = value.as_ref().ok_or_else(|| {
                    RcaError::Execution("NOT IN operator requires a value".to_string())
                })?;
                if let Some(arr) = val.as_array() {
                    let vals: Vec<String> = arr.iter()
                        .map(|v| self.format_value(v))
                        .collect();
                    Ok(format!("{} NOT IN ({})", column, vals.join(", ")))
                } else {
                    Err(RcaError::Execution("NOT IN operator requires an array value".to_string()))
                }
            }
            "LIKE" => {
                let val = value.as_ref().ok_or_else(|| {
                    RcaError::Execution("LIKE operator requires a value".to_string())
                })?;
                let val_str = self.format_value(val);
                // Make LIKE case-insensitive by default
                Ok(format!("UPPER({}) LIKE UPPER({})", column, val_str))
            }
            _ => Err(RcaError::Execution(format!("Unknown operator: {}", operator))),
        }
    }
    
    /// Build JOIN clauses
    fn build_join_clauses(&self, intent: &SqlIntent, tables: &[&crate::metadata::Table]) -> Result<Vec<String>> {
        let mut join_clauses = Vec::new();
        
        if let Some(ref joins) = intent.joins {
            for join in joins {
                let join_type = join.join_type.as_deref().unwrap_or("LEFT").to_uppercase();
                let conditions: Vec<String> = join.condition.iter()
                    .map(|c| format!("{}.{} = {}.{}", 
                        join.left_table, c.left_column,
                        join.right_table, c.right_column))
                    .collect();
                join_clauses.push(format!("{} JOIN {} ON {}", 
                    join_type, join.right_table, conditions.join(" AND ")));
            }
        }
        
        Ok(join_clauses)
    }
    
    /// Build GROUP BY clause
    fn build_group_by_clause(&self, intent: &SqlIntent, tables: &[&crate::metadata::Table]) -> Result<String> {
        if let Some(ref group_by) = intent.group_by {
            if group_by.is_empty() {
                return Ok(String::new());
            }
            let columns: Vec<String> = group_by.iter()
                .map(|col| self.resolve_column(col, &None, tables[0]))
                .collect::<Result<Vec<_>>>()?;
            if columns.is_empty() {
                Ok(String::new())
            } else {
                Ok(format!("GROUP BY {}", columns.join(", ")))
            }
        } else {
            Ok(String::new())
        }
    }
    
    /// Build ORDER BY clause
    fn build_order_by_clause(&self, intent: &SqlIntent, tables: &[&crate::metadata::Table]) -> Result<String> {
        if let Some(ref order_by) = intent.order_by {
            if order_by.is_empty() {
                return Ok(String::new());
            }
            let parts: Vec<String> = order_by.iter()
                .map(|spec| {
                    let col = self.resolve_column(&spec.column, &spec.table, tables[0])?;
                    let dir = spec.direction.as_deref().unwrap_or("ASC").to_uppercase();
                    Ok(format!("{} {}", col, dir))
                })
                .collect::<Result<Vec<_>>>()?;
            if parts.is_empty() {
                Ok(String::new())
            } else {
                // Check if we have GROUP BY - if so, ORDER BY must use aggregated columns or GROUP BY columns
                let has_group_by = intent.group_by.is_some() && 
                    intent.group_by.as_ref().map(|g| !g.is_empty()).unwrap_or(false);
                
                if has_group_by {
                    // For GROUP BY queries, ORDER BY should use aggregated columns or GROUP BY columns
                    // This is a simplified check - in production, you'd want more sophisticated validation
                    let order_by_cols: Vec<String> = order_by.iter()
                        .map(|s| s.column.clone())
                        .collect();
                    let group_by_cols: Vec<String> = intent.group_by.as_ref()
                        .map(|g| g.clone())
                        .unwrap_or_default();
                    
                    // Check if ORDER BY columns are in GROUP BY or are aggregations
                    let valid_order_by: Vec<String> = parts.iter()
                        .filter(|part| {
                            // Check if it's a GROUP BY column or an aggregation alias
                            let col_name = part.split_whitespace().next().unwrap_or("");
                            group_by_cols.iter().any(|g| col_name.contains(g)) ||
                            col_name.contains("SUM") || col_name.contains("AVG") || 
                            col_name.contains("COUNT") || col_name.contains("MAX") || 
                            col_name.contains("MIN")
                        })
                        .cloned()
                        .collect();
                    
                    if valid_order_by.is_empty() {
                        // Skip ORDER BY if it's invalid for GROUP BY
                        return Ok(String::new());
                    }
                    Ok(format!("ORDER BY {}", valid_order_by.join(", ")))
                } else {
                    Ok(format!("ORDER BY {}", parts.join(", ")))
                }
            }
        } else {
            Ok(String::new())
        }
    }

    /// Compile semantic SQL intent to actual SQL query
    pub fn compile_semantic(
        &self,
        intent: &SemanticSqlIntent,
        semantic_registry: Arc<dyn SemanticRegistry>,
    ) -> Result<String> {
        info!("🔧 Compiling semantic SQL intent to query...");

        // Get the first metric (for now, support single metric)
        let metric_name = intent.metrics.first()
            .ok_or_else(|| RcaError::Execution("No metrics specified".to_string()))?;

        let metric = semantic_registry.metric(metric_name)
            .ok_or_else(|| RcaError::Execution(format!("Metric '{}' not found", metric_name)))?;

        // Get dimension intents (with usage semantics)
        let dimension_intents = intent.get_dimension_intents();
        
        // Extract dimension names for join resolution
        let dimension_names: Vec<String> = dimension_intents.iter().map(|di| di.name.clone()).collect();

        // Resolve join edges (without join types - those will be determined by planner)
        let join_resolver = JoinResolver::new(Arc::clone(&semantic_registry));
        let join_edges = join_resolver.resolve_joins_for_intent(metric_name, &dimension_names)?;

        // Plan joins deterministically using dimension usage + metadata
        // Check if metric is additive (for fan-out protection)
        use crate::semantic::metric::Aggregation;
        let metric_is_additive = matches!(
            metric.aggregation(),
            Aggregation::Sum | 
            Aggregation::Count |
            Aggregation::CountDistinct
        );
        
        let mut join_plans = Vec::new();
        let mut join_clauses = Vec::new();
        
        // Map dimension intents by name for lookup
        let intent_map: std::collections::HashMap<_, _> = dimension_intents
            .iter()
            .map(|di| (di.name.as_str(), di))
            .collect();
        
        // Plan each join
        for join_edge in &join_edges {
            // Find which dimension uses this join
            // For now, match by to_table (in production, use more sophisticated matching)
            let matching_intent = dimension_intents
                .iter()
                .find(|di| {
                    if let Some(dim) = semantic_registry.dimension(&di.name) {
                        dim.base_table() == join_edge.to_table
                    } else {
                        false
                    }
                })
                .or_else(|| {
                    // Fallback: use first intent or default to Select
                    dimension_intents.first()
                });
            
            if let Some(intent) = matching_intent {
                match JoinPlanner::plan_join(intent, join_edge, metric_is_additive) {
                    Ok(plan) => {
                        join_plans.push(plan.clone());
                        
                        // Apply fan-out protection if needed
                        let join_sql = if let Some(ref protection) = plan.fan_out_protection {
                            match protection {
                                crate::compiler::join_planner::FanOutProtection::PreAggregate { subquery, .. } => {
                                    // Use pre-aggregated subquery
                                    format!("LEFT JOIN ({}) AS {}_agg ON {}", 
                                        subquery, 
                                        join_edge.to_table,
                                        join_edge.on
                                    )
                                }
                                crate::compiler::join_planner::FanOutProtection::DistinctMetric { .. } => {
                                    // Will apply DISTINCT to metric in SELECT
                                    format!("{} {} ON {}", 
                                        plan.join.join_type.as_sql(), 
                                        join_edge.to_table, 
                                        join_edge.on
                                    )
                                }
                                crate::compiler::join_planner::FanOutProtection::HardFail { reason } => {
                                    return Err(RcaError::Execution(format!(
                                        "Cannot safely join {} to {}: {}",
                                        join_edge.from_table, join_edge.to_table, reason
                                    )));
                                }
                            }
                        } else {
                            format!("{} {} ON {}", 
                                plan.join.join_type.as_sql(), 
                                join_edge.to_table, 
                                join_edge.on
                            )
                        };
                        
                        join_clauses.push(join_sql);
                        
                        info!("📋 Join plan: {}", plan.explanation);
                    }
                    Err(e) => {
                        return Err(RcaError::Execution(format!(
                            "Failed to plan join {} → {}: {}",
                            join_edge.from_table, join_edge.to_table, e
                        )));
                    }
                }
            } else {
                // No matching intent - use default (Select, optional)
                let default_intent = DimensionIntent::select("unknown".to_string());
                match JoinPlanner::plan_join(&default_intent, join_edge, metric_is_additive) {
                    Ok(plan) => {
                        join_clauses.push(format!("{} {} ON {}", 
                            plan.join.join_type.as_sql(), 
                            join_edge.to_table, 
                            join_edge.on
                        ));
                    }
                    Err(e) => {
                        return Err(RcaError::Execution(format!(
                            "Failed to plan default join {} → {}: {}",
                            join_edge.from_table, join_edge.to_table, e
                        )));
                    }
                }
            }
        }

        // Build SELECT clause with metric SQL expression and dimensions
        let mut select_parts = Vec::new();
        
        // Add dimensions to SELECT (only those with Select or Both usage)
        for dim_intent in &dimension_intents {
            if matches!(dim_intent.usage, crate::intent::semantic_intent::DimensionUsage::Select | 
                                      crate::intent::semantic_intent::DimensionUsage::Both) {
                if let Some(dimension) = semantic_registry.dimension(&dim_intent.name) {
                    let col = if let Some(sql_expr) = dimension.sql_expression() {
                        sql_expr.to_string()
                    } else {
                        format!("{}.{}", dimension.base_table(), dimension.column())
                    };
                    select_parts.push(col);
                }
            }
        }
        
        // Add metric expression (apply DISTINCT if needed for fan-out protection)
        let metric_expr = metric.sql_expression().to_string();
        let needs_distinct = join_plans.iter().any(|plan| {
            matches!(plan.fan_out_protection, 
                Some(crate::compiler::join_planner::FanOutProtection::DistinctMetric { .. }))
        });
        
        if needs_distinct {
            select_parts.push(format!("SUM(DISTINCT {})", metric_expr));
        } else {
            select_parts.push(metric_expr);
        }
        
        let select_clause = format!("SELECT {}", select_parts.join(", "));

        // Build FROM clause
        let from_clause = format!("FROM {}", metric.base_table());

        // Build WHERE clause from filters
        let mut where_parts = Vec::new();
        for filter in &intent.filters {
            if let Some(dimension) = semantic_registry.dimension(&filter.dimension) {
                // Use sql_expression if available, otherwise use table.column
                let mut column = if let Some(sql_expr) = dimension.sql_expression() {
                    sql_expr.to_string()
                } else {
                    format!("{}.{}", dimension.base_table(), dimension.column())
                };
                
                // Handle relative dates (e.g., "2_days_ago")
                if let Some(ref rel_date) = filter.relative_date {
                    match rel_date.as_str() {
                        "2_days_ago" => {
                            column = format!("date(dateadd(day, -2, current_date))");
                        }
                        "yesterday" => {
                            column = format!("date(dateadd(day, -1, current_date))");
                        }
                        "today" => {
                            column = "current_date".to_string();
                        }
                        "tomorrow" => {
                            column = format!("date(dateadd(day, 1, current_date))");
                        }
                        _ => {
                            // Try to parse pattern like "N_days_ago" or "N_days_from_now"
                            if rel_date.ends_with("_days_ago") {
                                if let Some(days_str) = rel_date.strip_suffix("_days_ago") {
                                    if let Ok(days) = days_str.parse::<i32>() {
                                        column = format!("date(dateadd(day, -{}, current_date))", days);
                                    }
                                }
                            } else if rel_date.ends_with("_days_from_now") {
                                if let Some(days_str) = rel_date.strip_suffix("_days_from_now") {
                                    if let Ok(days) = days_str.parse::<i32>() {
                                        column = format!("date(dateadd(day, {}, current_date))", days);
                                    }
                                }
                            }
                        }
                    }
                }
                
                let condition = match filter.operator.as_str() {
                    "IS NULL" => format!("{} IS NULL", column),
                    "IS NOT NULL" => format!("{} IS NOT NULL", column),
                    "NOT IN" | "not in" | "NOT_IN" => {
                        if let Some(ref value) = filter.value {
                            if let Some(arr) = value.as_array() {
                                let vals: Vec<String> = arr.iter()
                                    .map(|v| self.format_value(v))
                                    .collect();
                                format!("{} NOT IN ({})", column, vals.join(", "))
                            } else {
                                continue; // Skip if not an array
                            }
                        } else {
                            continue; // Skip filters without values
                        }
                    }
                    "IN" | "in" => {
                        if let Some(ref value) = filter.value {
                            if let Some(arr) = value.as_array() {
                                let vals: Vec<String> = arr.iter()
                                    .map(|v| self.format_value(v))
                                    .collect();
                                format!("{} IN ({})", column, vals.join(", "))
                            } else {
                                continue; // Skip if not an array
                            }
                        } else {
                            continue; // Skip filters without values
                        }
                    }
                    _ => {
                        if let Some(ref value) = filter.value {
                            format!("{} {} {}", column, filter.operator, self.format_value(value))
                        } else {
                            continue; // Skip filters without values
                        }
                    }
                };
                where_parts.push(condition);
            }
        }

        let where_clause = if !where_parts.is_empty() {
            format!("WHERE {}", where_parts.join(" AND "))
        } else {
            String::new()
        };

        // Build GROUP BY clause (only for dimensions used in SELECT)
        let group_by_clause = {
            let group_cols: Vec<String> = dimension_intents
                .iter()
                .filter(|di| matches!(di.usage, crate::intent::semantic_intent::DimensionUsage::Select | 
                                              crate::intent::semantic_intent::DimensionUsage::Both))
                .filter_map(|dim_intent| {
                    semantic_registry.dimension(&dim_intent.name).map(|d| {
                        // For GROUP BY, use sql_expression if available, otherwise use table.column
                        // Note: If sql_expression is used, it must match the SELECT expression
                        if let Some(sql_expr) = d.sql_expression() {
                            sql_expr.to_string()
                        } else {
                            format!("{}.{}", d.base_table(), d.column())
                        }
                    })
                })
                .collect();
            
            if !group_cols.is_empty() {
                format!("GROUP BY {}", group_cols.join(", "))
            } else {
                String::new()
            }
        };

        // Build LIMIT clause
        let limit_clause = intent.limit
            .map(|l| format!("LIMIT {}", l))
            .unwrap_or_default();

        // Combine all clauses
        let mut sql_parts = vec![select_clause, from_clause];
        sql_parts.extend(join_clauses);
        if !where_clause.is_empty() {
            sql_parts.push(where_clause);
        }
        if !group_by_clause.is_empty() {
            sql_parts.push(group_by_clause);
        }
        if !limit_clause.is_empty() {
            sql_parts.push(limit_clause);
        }

        let sql = sql_parts.join(" ");
        info!("✅ Generated SQL from semantic intent: {}", sql);

        Ok(sql)
    }

    fn format_value(&self, value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => format!("'{}'", s.replace("'", "''")),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Array(arr) => {
                let values: Vec<String> = arr.iter().map(|v| self.format_value(v)).collect();
                format!("({})", values.join(", "))
            }
            serde_json::Value::Null => "NULL".to_string(),
            _ => format!("'{}'", value.to_string().replace("'", "''")),
        }
    }
}

