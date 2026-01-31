use crate::error::Result;
use crate::metadata::{Metadata, Table};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use regex::Regex;

/// Intelligent rule parser that interprets natural language formulas
/// and automatically matches columns from tables
pub struct IntelligentRuleParser {
    metadata: Metadata,
    data_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ColumnMatch {
    pub column_name: String,
    pub table_name: String,
    pub description: Option<String>,
    pub confidence: f64,
    pub match_reason: String,
}

#[derive(Debug, Clone)]
pub struct ParsedFormula {
    pub formula: String,
    pub variables: Vec<String>,
    pub column_matches: HashMap<String, ColumnMatch>,
    pub needs_clarification: bool,
    pub clarification_questions: Vec<ClarificationQuestion>,
}

#[derive(Debug, Clone)]
pub struct ClarificationQuestion {
    pub variable: String,
    pub options: Vec<ColumnOption>,
    pub question: String,
}

#[derive(Debug, Clone)]
pub struct ColumnOption {
    pub column_name: String,
    pub table_name: String,
    pub description: Option<String>,
    pub confidence: f64,
}

impl IntelligentRuleParser {
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self { metadata, data_dir }
    }

    /// Parse a natural language formula like "TOS = A+B-C for all loans"
    /// Returns parsed formula with column matches or clarification questions
    /// This gathers ALL columns from ALL tables in the specified system individually
    pub async fn parse_formula(
        &self,
        formula_text: &str,
        system: &str,
        metric: &str,
        target_entity: &str,
    ) -> Result<ParsedFormula> {
        println!("\n🔍 Intelligent Rule Parser");
        println!("   Parsing formula: \"{}\"", formula_text);
        println!("   System: {}", system);
        println!("   Metric: {}", metric);
        println!("   Target Entity: {}", target_entity);
        
        // Extract formula expression (e.g., "A+B-C" from "TOS = A+B-C")
        let formula_expr = self.extract_formula_expression(formula_text)?;
        println!("   Extracted expression: {}", formula_expr);
        
        // Extract variables (A, B, C)
        let variables = self.extract_variables(&formula_expr)?;
        println!("   Found variables: {:?}", variables);
        
        // Get all tables for this system - gather individually
        let system_tables: Vec<&Table> = self.metadata.tables
            .iter()
            .filter(|t| t.system == system)
            .collect();
        
        println!("   Found {} tables in system '{}'", system_tables.len(), system);
        
        // Get all columns from all tables in this system individually
        // This ensures we gather columns from khatabook and TB separately
        let mut all_columns: Vec<ColumnInfo> = Vec::new();
        for table in &system_tables {
            println!("   Gathering columns from table: {}", table.name);
            let columns = self.get_table_columns(table).await?;
            println!("     Found {} columns", columns.len());
            for (col_name, col_desc) in columns {
                all_columns.push(ColumnInfo {
                    name: col_name.clone(),
                    table: table.name.clone(),
                    description: col_desc.clone(),
                    entity: table.entity.clone(),
                });
                if let Some(ref desc) = col_desc {
                    println!("     - {}: {}", col_name, desc);
                } else {
                    println!("     - {}", col_name);
                }
            }
        }
        
        println!("   Total columns gathered: {}", all_columns.len());
        
        // Match variables to columns - find best matches for A, B, C
        let mut column_matches = HashMap::new();
        let mut clarification_questions = Vec::new();
        
        for variable in &variables {
            println!("\n   Matching variable '{}'...", variable);
            let matches = self.find_column_matches(variable, &all_columns, target_entity)?;
            
            if matches.is_empty() {
                // No match found - ask for clarification with ALL columns
                println!("     ❌ No matches found for '{}'", variable);
                clarification_questions.push(ClarificationQuestion {
                    variable: variable.clone(),
                    options: self.get_all_column_options(&all_columns, target_entity),
                    question: format!("Which column represents '{}'? No automatic matches found. Please select from available columns:", variable),
                });
            } else if matches.len() == 1 {
                // Single best match - use it automatically
                let best_match = &matches[0];
                println!("     ✅ Single match found: {} (table: {}, confidence: {:.2}%)", 
                    best_match.column_name, 
                    best_match.table_name,
                    best_match.confidence * 100.0
                );
                column_matches.insert(variable.clone(), best_match.clone());
            } else {
                // Multiple matches - show top matches and ask for clarification
                println!("     ⚠️  Found {} possible matches (showing top 5):", matches.len());
                let top_matches: Vec<_> = matches.iter().take(5).collect();
                for (idx, m) in top_matches.iter().enumerate() {
                    println!("       {}: {} (table: {}, confidence: {:.2}%)", 
                        idx + 1,
                        m.column_name, 
                        m.table_name,
                        m.confidence * 100.0
                    );
                    if let Some(ref desc) = m.description {
                        println!("          Description: {}", desc);
                    }
                }
                clarification_questions.push(ClarificationQuestion {
                    variable: variable.clone(),
                    options: matches.iter().take(10).map(|m| ColumnOption {
                        column_name: m.column_name.clone(),
                        table_name: m.table_name.clone(),
                        description: m.description.clone(),
                        confidence: m.confidence,
                    }).collect(),
                    question: format!("Which column represents '{}'? Found {} possible matches (showing top 10):", variable, matches.len()),
                });
            }
        }
        
        // Build final formula by replacing variables with column names
        let mut final_formula = formula_expr.clone();
        for (var, col_match) in &column_matches {
            // Replace variable with table.column or just column if unambiguous
            let replacement = if self.is_column_unambiguous(&col_match.column_name, &all_columns) {
                col_match.column_name.clone()
            } else {
                format!("{}.{}", col_match.table_name, col_match.column_name)
            };
            final_formula = final_formula.replace(var, &replacement);
        }
        
        Ok(ParsedFormula {
            formula: final_formula,
            variables: variables.clone(),
            column_matches,
            needs_clarification: !clarification_questions.is_empty(),
            clarification_questions,
        })
    }

    /// Extract formula expression from natural language text
    /// Handles patterns like:
    /// - "TOS = A+B-C" -> "A+B-C"
    /// - "TOS or total outstanding is A+B-C" -> "A+B-C"
    /// - "A+B-C" -> "A+B-C"
    /// - "total outstanding is A+B-C for all loans" -> "A+B-C"
    fn extract_formula_expression(&self, text: &str) -> Result<String> {
        let text = text.trim();
        
        // Pattern 1: Look for "is" followed by formula (e.g., "TOS is A+B-C")
        let re_is = Regex::new(r"(?i)(?:is|equals?|=\s*)\s*([A-Z]\s*[+\-*/]\s*[A-Z0-9_+\-*/()\s]+)").unwrap();
        if let Some(captures) = re_is.captures(text) {
            return Ok(captures.get(1).unwrap().as_str().trim().to_string());
        }
        
        // Pattern 2: Look for "=" followed by formula
        let re_equals = Regex::new(r"=\s*([A-Z]\s*[+\-*/]\s*[A-Z0-9_+\-*/()\s]+)").unwrap();
        if let Some(captures) = re_equals.captures(text) {
            return Ok(captures.get(1).unwrap().as_str().trim().to_string());
        }
        
        // Pattern 3: Look for formula pattern directly (A+B-C, A + B - C, etc.)
        let re_formula = Regex::new(r"([A-Z]\s*[+\-*/]\s*[A-Z0-9_+\-*/()\s]+)").unwrap();
        if let Some(captures) = re_formula.captures(text) {
            let formula = captures.get(1).unwrap().as_str().trim();
            // Check if it looks like a formula (has operators)
            if formula.contains('+') || formula.contains('-') || formula.contains('*') || formula.contains('/') {
                return Ok(formula.to_string());
            }
        }
        
        // Pattern 4: Split by common separators and find formula-like parts
        let separators = ["is", "equals", "=", "for", "where"];
        for sep in &separators {
            if let Some(pos) = text.to_lowercase().find(sep) {
                let after_sep = &text[pos + sep.len()..];
                // Look for formula pattern after separator
                let re_after = Regex::new(r"([A-Z]\s*[+\-*/]\s*[A-Z0-9_+\-*/()\s]+)").unwrap();
                if let Some(captures) = re_after.captures(after_sep) {
                    let formula = captures.get(1).unwrap().as_str().trim();
                    if formula.contains('+') || formula.contains('-') || formula.contains('*') || formula.contains('/') {
                        return Ok(formula.to_string());
                    }
                }
            }
        }
        
        // Fallback: return the text as-is (might be just the formula)
        Ok(text.to_string())
    }

    /// Extract variables from formula expression
    /// "A+B-C" -> ["A", "B", "C"]
    /// Handles spaces: "A + B - C" -> ["A", "B", "C"]
    fn extract_variables(&self, formula: &str) -> Result<Vec<String>> {
        // Match single uppercase letters that are variables (not part of function names)
        // Exclude common function names like SUM, AVG, COUNT, etc.
        let re = Regex::new(r"\b([A-Z])\b").unwrap();
        let mut variables = HashSet::new();
        // Note: We don't exclude any letters - all uppercase letters can be variables
        
        for cap in re.captures_iter(formula) {
            let var = cap.get(1).unwrap().as_str();
            // Only add if it's not part of a function name context
            // Simple heuristic: if surrounded by operators or spaces, it's likely a variable
            let var_pos = cap.get(0).unwrap().start();
            if var_pos > 0 && var_pos < formula.len() - 1 {
                let before = formula.chars().nth(var_pos - 1).unwrap_or(' ');
                let after = formula.chars().nth(var_pos + 1).unwrap_or(' ');
                // Variable if surrounded by operators, spaces, or parentheses
                if "+-*/() ".contains(before) || "+-*/() ".contains(after) {
                    variables.insert(var.to_string());
                }
            } else {
                // At start or end, likely a variable
                variables.insert(var.to_string());
            }
        }
        
        // Sort for consistent output
        let mut result: Vec<String> = variables.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// Get columns from a table by reading the parquet file or metadata
    /// First tries to read from table.columns metadata, then falls back to parquet file,
    /// then falls back to entity attributes
    async fn get_table_columns(&self, table: &Table) -> Result<Vec<(String, Option<String>)>> {
        use std::fs::File;
        
        // PRIORITY 1: Use column metadata from table.columns if available
        if let Some(ref columns) = table.columns {
            let mut result = Vec::new();
            for col_meta in columns {
                result.push((col_meta.name.clone(), col_meta.description.clone()));
            }
            if !result.is_empty() {
                return Ok(result);
            }
        }
        
        // PRIORITY 2: Try to read parquet file to get columns
        let file_path = self.data_dir.join(&table.path);
        if file_path.exists() {
            if let Ok(file) = File::open(&file_path) {
                if let Ok(df) = ParquetReader::new(file).finish() {
                    let mut columns = Vec::new();
                    for col_name in df.get_column_names() {
                        // Try to get description from table metadata if available
                        let description = self.get_column_description(table, col_name);
                        columns.push((col_name.to_string(), description));
                    }
                    if !columns.is_empty() {
                        return Ok(columns);
                    }
                }
            }
        }
        
        // PRIORITY 3: Fallback to entity attributes
        if let Some(entity) = self.metadata.entities.iter().find(|e| e.id == table.entity) {
            let mut columns = Vec::new();
            for attr in &entity.attributes {
                let description = self.get_column_description(table, attr);
                columns.push((attr.clone(), description));
            }
            if !columns.is_empty() {
                return Ok(columns);
            }
        }
        
        // If nothing found, return empty (will be populated when data is available)
        Ok(Vec::new())
    }

    /// Get column description from table metadata or entity attributes
    fn get_column_description(&self, table: &Table, column_name: &str) -> Option<String> {
        // Check if table has column metadata (enhanced structure)
        if let Some(ref columns) = table.columns {
            for col in columns {
                if col.name == column_name {
                    return col.description.clone();
                }
            }
        }
        None
    }

    /// Find best column matches for a variable
    fn find_column_matches(
        &self,
        variable: &str,
        all_columns: &[ColumnInfo],
        target_entity: &str,
    ) -> Result<Vec<ColumnMatch>> {
        let mut matches = Vec::new();
        
        for col_info in all_columns {
            let score = self.score_column_match(variable, col_info, target_entity);
            if score > 0.3 {  // Threshold for potential matches
                matches.push(ColumnMatch {
                    column_name: col_info.name.clone(),
                    table_name: col_info.table.clone(),
                    description: col_info.description.clone(),
                    confidence: score,
                    match_reason: self.generate_match_reason(variable, col_info, score),
                });
            }
        }
        
        // Sort by confidence (highest first)
        matches.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());
        
        Ok(matches)
    }

    /// Score how well a column matches a variable
    /// Prioritizes column names (primary) and uses descriptions as hints (secondary)
    fn score_column_match(
        &self,
        variable: &str,
        col_info: &ColumnInfo,
        target_entity: &str,
    ) -> f64 {
        let mut score: f64 = 0.0;
        let var_lower = variable.to_lowercase();
        let col_lower = col_info.name.to_lowercase();
        
        // PRIORITY 1: Column name matching (PRIMARY CHOICE)
        // Exact name match (highest priority - 0.9 points)
        if col_lower == var_lower {
            score += 0.9;
            return score.min(1.0_f64); // Early return for exact match
        }
        
        // Column name contains variable (high priority - 0.7 points)
        if col_lower.contains(&var_lower) {
            score += 0.7;
        }
        
        // Variable contains column name (medium-high priority - 0.6 points)
        if var_lower.contains(&col_lower) {
            score += 0.6;
        }
        
        // Partial word match in column name (medium priority - 0.5 points)
        let col_words: Vec<&str> = col_lower.split(&['_', '-', ' '][..]).collect();
        for word in &col_words {
            if word.contains(&var_lower) || var_lower.contains(word) {
                score += 0.5;
                break;
            }
        }
        
        // PRIORITY 2: Description matching (HINT - only if name doesn't match well)
        // Only use description if name score is low (< 0.5)
        if score < 0.5 {
            if let Some(ref desc) = col_info.description {
                let desc_lower = desc.to_lowercase();
                // Description contains variable (hint - 0.4 points)
                if desc_lower.contains(&var_lower) {
                    score += 0.4;
                }
                // Variable words appear in description (hint - 0.3 points)
                let desc_words: Vec<&str> = desc_lower.split(&[' ', '-', ',', '.'][..]).collect();
                for word in &desc_words {
                    if word == &var_lower || var_lower.contains(word) {
                        score += 0.3;
                        break;
                    }
                }
            }
        }
        
        // PRIORITY 3: Entity relevance (bonus - 0.2 points)
        // Prefer columns from target entity
        if col_info.entity == target_entity {
            score += 0.2;
        }
        
        // PRIORITY 4: Common business patterns (bonus - 0.3 points)
        // Amount/Value patterns for variable A
        if (var_lower == "a" || var_lower == "amount") && 
           (col_lower.contains("amount") || col_lower.contains("value") || 
            col_lower.contains("principal") || col_lower.contains("emi")) {
            score += 0.3;
        }
        
        // Balance/Outstanding patterns for variable B
        if (var_lower == "b" || var_lower == "balance") && 
           (col_lower.contains("balance") || col_lower.contains("outstanding") ||
            col_lower.contains("penalty") || col_lower.contains("interest")) {
            score += 0.3;
        }
        
        // Count/Transaction patterns for variable C
        if (var_lower == "c" || var_lower == "count" || var_lower == "credit") && 
           (col_lower.contains("count") || col_lower.contains("number") ||
            col_lower.contains("transaction") || col_lower.contains("payment")) {
            score += 0.3;
        }
        
        // Deduction patterns for variable C (subtraction)
        if var_lower == "c" && 
           (col_lower.contains("deduct") || col_lower.contains("paid") ||
            col_lower.contains("transaction") || col_lower.contains("payment")) {
            score += 0.3;
        }
        
        // Cap at 1.0
        score.min(1.0_f64)
    }

    fn generate_match_reason(&self, variable: &str, col_info: &ColumnInfo, score: f64) -> String {
        if score > 0.8 {
            format!("Exact match: column '{}' matches variable '{}'", col_info.name, variable)
        } else if score > 0.5 {
            format!("Partial match: column '{}' partially matches variable '{}'", col_info.name, variable)
        } else if col_info.description.is_some() {
            format!("Description match: column '{}' description matches variable '{}'", col_info.name, variable)
        } else {
            format!("Fuzzy match: column '{}' may match variable '{}'", col_info.name, variable)
        }
    }

    fn is_column_unambiguous(&self, column_name: &str, all_columns: &[ColumnInfo]) -> bool {
        all_columns.iter()
            .filter(|c| c.name == column_name)
            .count() == 1
    }

    fn get_all_column_options(&self, all_columns: &[ColumnInfo], target_entity: &str) -> Vec<ColumnOption> {
        // Sort columns: prefer columns from target entity first, then alphabetically
        let mut options: Vec<ColumnOption> = all_columns.iter()
            .map(|c| {
                let mut confidence = 0.5; // Default confidence for manual selection
                // Boost confidence slightly for target entity columns
                if c.entity == target_entity {
                    confidence = 0.6;
                }
                ColumnOption {
                    column_name: c.name.clone(),
                    table_name: c.table.clone(),
                    description: c.description.clone(),
                    confidence,
                }
            })
            .collect();
        
        // Sort by: target entity first, then by column name
        options.sort_by(|a, b| {
            let a_is_target = all_columns.iter()
                .find(|c| c.name == a.column_name && c.table == a.table_name)
                .map(|c| c.entity == target_entity)
                .unwrap_or(false);
            let b_is_target = all_columns.iter()
                .find(|c| c.name == b.column_name && c.table == b.table_name)
                .map(|c| c.entity == target_entity)
                .unwrap_or(false);
            
            match (a_is_target, b_is_target) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => a.column_name.cmp(&b.column_name),
            }
        });
        
        options
    }
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    table: String,
    description: Option<String>,
    entity: String,
}

