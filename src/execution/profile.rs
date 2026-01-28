//! Query Profile - Extracts characteristics from SQL AST/intent
//! 
//! This module analyzes queries to determine their characteristics before execution.
//! Used by the ExecutionRouter to select the appropriate engine.

use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use sqlparser::ast::{Statement, Query, SetExpr, SelectItem, Expr, Function};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Query profile extracted from SQL AST or intent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryProfile {
    /// Query uses Common Table Expressions (WITH clauses)
    pub uses_ctes: bool,
    
    /// Query uses window functions
    pub uses_window_functions: bool,
    
    /// Query uses CASE expressions
    pub uses_case_expressions: bool,
    
    /// Number of joins in the query
    pub join_count: usize,
    
    /// Data sources involved (tables, catalogs, etc.)
    pub data_sources: Vec<DataSource>,
    
    /// Estimated scan size in GB (if available)
    pub estimated_scan_gb: Option<u64>,
    
    /// Requires federation (multiple physical backends)
    pub requires_federation: bool,
    
    /// Query complexity score (0-100)
    pub complexity_score: u8,
    
    /// Whether query is read-only
    pub is_read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataSource {
    /// Source name (table name, catalog name, etc.)
    pub name: String,
    
    /// Source type
    pub source_type: SourceType,
    
    /// Backend system (e.g., "postgresql", "s3", "hive")
    pub backend: String,
    
    /// Estimated row count (if available)
    pub estimated_rows: Option<u64>,
    
    /// Estimated size in GB (if available)
    pub estimated_size_gb: Option<f64>,
}

impl std::hash::Hash for DataSource {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.source_type.hash(state);
        self.backend.hash(state);
        self.estimated_rows.hash(state);
        // Skip estimated_size_gb for hashing since f64 doesn't implement Hash
    }
}

impl Eq for DataSource {} // Manual Eq implementation since f64 doesn't implement Eq

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SourceType {
    Table,
    View,
    Catalog,
    External,
}

impl QueryProfile {
    /// Create a new query profile
    pub fn new() -> Self {
        Self {
            uses_ctes: false,
            uses_window_functions: false,
            uses_case_expressions: false,
            join_count: 0,
            data_sources: Vec::new(),
            estimated_scan_gb: None,
            requires_federation: false,
            complexity_score: 0,
            is_read_only: true,
        }
    }
    
    /// Extract profile from SQL string using proper SQL parser
    /// 
    /// Uses sqlparser crate to parse SQL AST and extract characteristics.
    pub fn from_sql(sql: &str) -> Self {
        let dialect = GenericDialect {};
        let mut profile = Self::new();
        
        // Parse SQL using proper parser
        match Parser::parse_sql(&dialect, sql) {
            Ok(ast) => {
                profile.analyze_ast(&ast);
            }
            Err(e) => {
                // Fallback to heuristic if parsing fails
                tracing::warn!("SQL parsing failed: {}, using heuristic fallback", e);
                return Self::from_sql_heuristic(sql);
            }
        }
        
        profile
    }
    
    /// Fallback heuristic-based extraction (used when SQL parsing fails)
    fn from_sql_heuristic(sql: &str) -> Self {
        let sql_upper = sql.to_uppercase();
        
        let uses_ctes = sql_upper.contains("WITH ") && sql_upper.contains(" AS (");
        let uses_window_functions = sql_upper.contains("OVER (") || 
            sql_upper.contains("ROW_NUMBER()") ||
            sql_upper.contains("RANK()") ||
            sql_upper.contains("DENSE_RANK()");
        let uses_case_expressions = sql_upper.contains("CASE ") && sql_upper.contains("WHEN ");
        
        // Count JOIN keywords
        let join_count = sql_upper.matches(" JOIN ").count() + 
            sql_upper.matches(" INNER JOIN ").count() +
            sql_upper.matches(" LEFT JOIN ").count() +
            sql_upper.matches(" RIGHT JOIN ").count() +
            sql_upper.matches(" FULL JOIN ").count();
        
        // Extract table names from FROM and JOIN clauses
        let mut data_sources = Vec::new();
        let from_idx = sql_upper.find(" FROM ");
        if let Some(idx) = from_idx {
            let after_from = &sql_upper[idx + 6..];
            let table_name = after_from
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches('`');
            
            if !table_name.is_empty() {
                data_sources.push(DataSource {
                    name: table_name.to_string(),
                    source_type: SourceType::Table,
                    backend: "local".to_string(),
                    estimated_rows: None,
                    estimated_size_gb: None,
                });
            }
        }
        
        // Calculate complexity score
        let mut complexity = 0;
        if uses_ctes { complexity += 10; }
        if uses_window_functions { complexity += 15; }
        if uses_case_expressions { complexity += 5; }
        complexity += (join_count * 5).min(30);
        if sql_upper.contains("GROUP BY") { complexity += 10; }
        if sql_upper.contains("HAVING") { complexity += 5; }
        if sql_upper.contains("UNION") { complexity += 10; }
        if sql_upper.contains("SUBQUERY") || sql_upper.contains("(SELECT") { complexity += 10; }
        
        let complexity_score = complexity.min(100) as u8;
        
        // Check if read-only
        let is_read_only = !sql_upper.contains("INSERT") &&
            !sql_upper.contains("UPDATE") &&
            !sql_upper.contains("DELETE") &&
            !sql_upper.contains("CREATE") &&
            !sql_upper.contains("DROP") &&
            !sql_upper.contains("ALTER");
        
        Self {
            uses_ctes,
            uses_window_functions,
            uses_case_expressions,
            join_count,
            data_sources,
            estimated_scan_gb: None,
            requires_federation: false,
            complexity_score,
            is_read_only,
        }
    }
    
    /// Analyze SQL AST to extract query characteristics
    fn analyze_ast(&mut self, ast: &[Statement]) {
        for stmt in ast {
            match stmt {
                Statement::Query(query) => {
                    self.analyze_query(query);
                }
                Statement::Insert { .. } |
                Statement::Update { .. } |
                Statement::Delete { .. } |
                Statement::CreateTable { .. } |
                Statement::Drop { .. } |
                Statement::AlterTable { .. } => {
                    self.is_read_only = false;
                }
                _ => {}
            }
        }
    }
    
    /// Analyze a Query AST node
    fn analyze_query(&mut self, query: &Query) {
        // Check for CTEs
        if query.with.is_some() {
            self.uses_ctes = true;
        }
        
        // Analyze the main query body
        // Note: query.body is Box<SetExpr>, so we need to dereference
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                // Check for window functions in SELECT items
                for item in &select.projection {
                    self.analyze_select_item(item);
                }
                
                // Count joins
                if !select.from.is_empty() {
                    self.join_count = select.from.len() - 1; // Number of joins = tables - 1
                    
                    // Extract table names
                    if let Some(first_table) = select.from.first() {
                        self.extract_table_from_table_factor(&first_table.relation);
                    }
                    for table_with_joins in &select.from {
                        for join in &table_with_joins.joins {
                            self.extract_table_from_table_factor(&join.relation);
                        }
                    }
                }
                
                // Check for CASE expressions in WHERE clause
                if let Some(where_clause) = &select.selection {
                    self.analyze_expr_for_case(where_clause);
                }
                
                // Check for GROUP BY, HAVING
                match &select.group_by {
                    sqlparser::ast::GroupByExpr::All => {
                        self.complexity_score = (self.complexity_score + 10).min(100);
                    }
                    sqlparser::ast::GroupByExpr::Expressions(exprs) => {
                        if !exprs.is_empty() {
                            self.complexity_score = (self.complexity_score + 10).min(100);
                        }
                    }
                }
                if select.having.is_some() {
                    self.complexity_score = (self.complexity_score + 5).min(100);
                }
            }
            SetExpr::Query(query) => {
                // Recursively analyze nested query
                self.analyze_query(query);
                self.complexity_score = (self.complexity_score + 10).min(100);
            }
            SetExpr::SetOperation { op, .. } => {
                // Handle UNION, EXCEPT, INTERSECT operations
                match op {
                    sqlparser::ast::SetOperator::Union => {
                        self.complexity_score = (self.complexity_score + 10).min(100);
                    }
                    sqlparser::ast::SetOperator::Except => {
                        self.complexity_score = (self.complexity_score + 10).min(100);
                    }
                    sqlparser::ast::SetOperator::Intersect => {
                        self.complexity_score = (self.complexity_score + 10).min(100);
                    }
                }
            }
            _ => {}
        }
        
        // Update complexity based on findings
        if self.uses_ctes {
            self.complexity_score = (self.complexity_score + 10).min(100);
        }
        if self.uses_window_functions {
            self.complexity_score = (self.complexity_score + 15).min(100);
        }
        if self.uses_case_expressions {
            self.complexity_score = (self.complexity_score + 5).min(100);
        }
        self.complexity_score = (self.complexity_score as usize + (self.join_count * 5).min(30)).min(100) as u8;
    }
    
    /// Analyze SELECT item for window functions
    fn analyze_select_item(&mut self, item: &SelectItem) {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                self.analyze_expr_for_window(expr);
                self.analyze_expr_for_case(expr);
            }
            _ => {}
        }
    }
    
    /// Analyze expression for window functions (recursive)
    fn analyze_expr_for_window(&mut self, expr: &Expr) {
        match expr {
            Expr::Function(func) => {
                // Check if function has OVER clause (window function)
                if func.over.is_some() {
                    self.uses_window_functions = true;
                }
                // Recursively check function arguments
                for arg in &func.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) | 
                        sqlparser::ast::FunctionArg::Named { arg: sqlparser::ast::FunctionArgExpr::Expr(expr), .. } => {
                            self.analyze_expr_for_window(expr);
                        }
                        _ => {}
                    }
                }
            }
            Expr::Case { operand, conditions, results, else_result } => {
                if let Some(op) = operand {
                    self.analyze_expr_for_window(op);
                }
                for (cond, res) in conditions.iter().zip(results.iter()) {
                    self.analyze_expr_for_window(cond);
                    self.analyze_expr_for_window(res);
                }
                if let Some(else_expr) = else_result {
                    self.analyze_expr_for_window(else_expr);
                }
            }
            Expr::BinaryOp { left, right, .. } |
            Expr::IsDistinctFrom(left, right) |
            Expr::IsNotDistinctFrom(left, right) => {
                self.analyze_expr_for_window(left);
                self.analyze_expr_for_window(right);
            }
            Expr::UnaryOp { expr, .. } |
            Expr::IsNull(expr) |
            Expr::IsNotNull(expr) |
            Expr::IsTrue(expr) |
            Expr::IsNotTrue(expr) |
            Expr::IsFalse(expr) |
            Expr::IsNotFalse(expr) |
            Expr::IsUnknown(expr) |
            Expr::IsNotUnknown(expr) => {
                self.analyze_expr_for_window(expr);
            }
            Expr::Cast { expr, .. } => {
                self.analyze_expr_for_window(expr);
            }
            Expr::TypedString { .. } => {
                // TypedString doesn't have expr, it's a literal value
                // No window function analysis needed for literals
            }
            Expr::AtTimeZone { timestamp, .. } => {
                self.analyze_expr_for_window(timestamp);
            }
            Expr::Subquery(query) => {
                self.analyze_query(query);
            }
            Expr::InList { expr, list, .. } => {
                self.analyze_expr_for_window(expr);
                for e in list {
                    self.analyze_expr_for_window(e);
                }
            }
            Expr::InSubquery { expr, .. } => {
                self.analyze_expr_for_window(expr);
            }
            Expr::Between { expr, low, high, .. } => {
                self.analyze_expr_for_window(expr);
                self.analyze_expr_for_window(low);
                self.analyze_expr_for_window(high);
            }
            Expr::JsonAccess { left, right, .. } => {
                self.analyze_expr_for_window(left);
                self.analyze_expr_for_window(right);
            }
            Expr::CompositeAccess { expr, .. } => {
                self.analyze_expr_for_window(expr);
            }
            _ => {
                // Other expression types don't contain nested expressions we need to check
            }
        }
    }
    
    /// Analyze expression for CASE expressions (recursive)
    fn analyze_expr_for_case(&mut self, expr: &Expr) {
        match expr {
            Expr::Case { operand, conditions, results, else_result } => {
                self.uses_case_expressions = true;
                // Recursively check nested expressions
                if let Some(op) = operand {
                    self.analyze_expr_for_case(op);
                }
                for (cond, res) in conditions.iter().zip(results.iter()) {
                    self.analyze_expr_for_case(cond);
                    self.analyze_expr_for_case(res);
                }
                if let Some(else_expr) = else_result {
                    self.analyze_expr_for_case(else_expr);
                }
            }
            Expr::Function(func) => {
                for arg in &func.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) | 
                        sqlparser::ast::FunctionArg::Named { arg: sqlparser::ast::FunctionArgExpr::Expr(expr), .. } => {
                            self.analyze_expr_for_case(expr);
                        }
                        _ => {}
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } |
            Expr::IsDistinctFrom(left, right) |
            Expr::IsNotDistinctFrom(left, right) => {
                self.analyze_expr_for_case(left);
                self.analyze_expr_for_case(right);
            }
            Expr::UnaryOp { expr, .. } |
            Expr::IsNull(expr) |
            Expr::IsNotNull(expr) |
            Expr::IsTrue(expr) |
            Expr::IsNotTrue(expr) |
            Expr::IsFalse(expr) |
            Expr::IsNotFalse(expr) |
            Expr::IsUnknown(expr) |
            Expr::IsNotUnknown(expr) => {
                self.analyze_expr_for_case(expr);
            }
            Expr::Cast { expr, .. } => {
                self.analyze_expr_for_case(expr);
            }
            Expr::TypedString { .. } => {
                // TypedString doesn't have expr, it's a literal value
                // No case expression analysis needed for literals
            }
            Expr::AtTimeZone { timestamp, .. } => {
                self.analyze_expr_for_case(timestamp);
            }
            Expr::Subquery(query) => {
                self.analyze_query(query);
            }
            Expr::InList { expr, list, .. } => {
                self.analyze_expr_for_case(expr);
                for e in list {
                    self.analyze_expr_for_case(e);
                }
            }
            Expr::InSubquery { expr, .. } => {
                self.analyze_expr_for_case(expr);
            }
            Expr::Between { expr, low, high, .. } => {
                self.analyze_expr_for_case(expr);
                self.analyze_expr_for_case(low);
                self.analyze_expr_for_case(high);
            }
            Expr::JsonAccess { left, right, .. } => {
                self.analyze_expr_for_case(left);
                self.analyze_expr_for_case(right);
            }
            Expr::CompositeAccess { expr, .. } => {
                self.analyze_expr_for_case(expr);
            }
            _ => {
                // Other expression types don't contain CASE expressions
            }
        }
    }
    
    /// Extract table name from TableFactor
    fn extract_table_from_table_factor(&mut self, factor: &sqlparser::ast::TableFactor) {
        match factor {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                self.data_sources.push(DataSource {
                    name: table_name,
                    source_type: SourceType::Table,
                    backend: "local".to_string(), // Will be enhanced with metadata
                    estimated_rows: None,
                    estimated_size_gb: None,
                });
            }
            sqlparser::ast::TableFactor::Derived { .. } => {
                self.complexity_score = (self.complexity_score + 10).min(100);
            }
            _ => {}
        }
    }
    
    /// Extract profile from intent specification
    pub fn from_intent(intent: &crate::intent_compiler::IntentSpec) -> Self {
        let mut profile = Self::new();
        
        // Check for CTEs (would be in joins or tables)
        profile.uses_ctes = intent.tables.len() > 1 && !intent.joins.is_empty();
        
        // Window functions would be in aggregations (not directly in IntentSpec)
        // Would need to check rules/metrics
        
        // Join count
        profile.join_count = intent.joins.len();
        
        // Data sources from systems/tables
        for system in &intent.systems {
            profile.data_sources.push(DataSource {
                name: system.clone(),
                source_type: SourceType::Table,
                backend: "local".to_string(),
                estimated_rows: None,
                estimated_size_gb: None,
            });
        }
        
        for table in &intent.tables {
            profile.data_sources.push(DataSource {
                name: table.clone(),
                source_type: SourceType::Table,
                backend: "local".to_string(),
                estimated_rows: None,
                estimated_size_gb: None,
            });
        }
        
        // Check if federation is required (multiple systems)
        profile.requires_federation = intent.systems.len() > 1;
        
        // Calculate complexity
        let mut complexity = 0;
        if profile.uses_ctes { complexity += 10; }
        complexity += (profile.join_count * 5).min(30);
        if !intent.constraints.is_empty() { complexity += 5; }
        if intent.time_scope.is_some() { complexity += 5; }
        if !intent.grain.is_empty() { complexity += 10; }
        
        profile.complexity_score = complexity.min(100) as u8;
        
        profile
    }
    
    /// Calculate estimated scan size from data sources
    pub fn calculate_scan_size(&mut self) {
        let total_gb: f64 = self.data_sources.iter()
            .filter_map(|ds| ds.estimated_size_gb)
            .sum();
        
        if total_gb > 0.0 {
            self.estimated_scan_gb = Some(total_gb as u64);
        }
    }
    
    /// Check if federation is required based on backends
    pub fn check_federation(&mut self) {
        let backends: HashSet<String> = self.data_sources.iter()
            .map(|ds| ds.backend.clone())
            .collect();
        
        self.requires_federation = backends.len() > 1;
    }
}

impl Default for QueryProfile {
    fn default() -> Self {
        Self::new()
    }
}

