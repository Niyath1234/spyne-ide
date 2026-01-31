//! SQL Completeness Validation
//! 
//! Validates that generated SQL includes all required tables.

use crate::error::Result;
use std::collections::HashSet;

pub struct ValidationResult {
    pub is_complete: bool,
    pub missing_tables: Vec<String>,
}

pub struct SqlValidator;

impl SqlValidator {
    pub fn validate_completeness(
        &self,
        sql: &str,
        required_tables: &[String],
    ) -> Result<ValidationResult> {
        // Extract table names from SQL using simple parsing
        // This is a simplified approach - for production, use sqlparser
        let sql_tables = self.extract_tables_from_sql(sql)?;

        // Check if all required tables are present
        let sql_tables_set: HashSet<String> = sql_tables.iter()
            .map(|t| t.to_lowercase())
            .collect();

        let missing: Vec<String> = required_tables
            .iter()
            .filter(|req| {
                let req_lower = req.to_lowercase();
                // Check exact match or if table name contains required entity
                !sql_tables_set.contains(&req_lower) &&
                !sql_tables.iter().any(|sql_table| {
                    sql_table.to_lowercase().contains(&req_lower) ||
                    req_lower.contains(&sql_table.to_lowercase())
                })
            })
            .cloned()
            .collect();

        Ok(ValidationResult {
            is_complete: missing.is_empty(),
            missing_tables: missing,
        })
    }

    fn extract_tables_from_sql(&self, sql: &str) -> Result<Vec<String>> {
        let sql_upper = sql.to_uppercase();
        let mut tables = Vec::new();

        // Simple regex-free extraction
        // Look for FROM and JOIN clauses
        let from_patterns = vec!["FROM", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"];
        
        let words: Vec<&str> = sql_upper.split_whitespace().collect();
        let mut i = 0;
        
        while i < words.len() {
            // Check for FROM or JOIN keywords
            if from_patterns.iter().any(|pattern| words[i].contains(pattern)) {
                // Next token(s) should be table name
                if i + 1 < words.len() {
                    let mut table_name = String::new();
                    let mut j = i + 1;
                    
                    // Collect table name (may be schema.table format)
                    while j < words.len() {
                        let word = words[j];
                        
                        // Stop at keywords that indicate end of table name
                        if word == "ON" || word == "WHERE" || word == "GROUP" || 
                           word == "ORDER" || word == "HAVING" || word == "LIMIT" ||
                           from_patterns.iter().any(|p| word.contains(p)) {
                            break;
                        }
                        
                        if !table_name.is_empty() {
                            table_name.push(' ');
                        }
                        table_name.push_str(word);
                        
                        // If we hit a comma or semicolon, we might have multiple tables
                        if word.ends_with(',') || word.ends_with(';') {
                            let clean_name = table_name.trim_end_matches(',').trim_end_matches(';').trim().to_string();
                            if !clean_name.is_empty() && !clean_name.chars().all(|c| c.is_uppercase() && c.is_alphabetic()) {
                                // Extract actual table name (last part if schema.table format)
                                let parts: Vec<&str> = clean_name.split('.').collect();
                                let final_name = parts.last().map(|s| s.to_string()).unwrap_or(clean_name);
                                if !final_name.is_empty() {
                                    tables.push(final_name);
                                }
                            }
                            table_name.clear();
                        }
                        
                        j += 1;
                    }
                    
                    // Add remaining table name
                    if !table_name.is_empty() {
                        let clean_name = table_name.trim().to_string();
                        if !clean_name.chars().all(|c| c.is_uppercase() && c.is_alphabetic()) {
                            let parts: Vec<&str> = clean_name.split('.').collect();
                            let final_name = parts.last().map(|s| s.to_string()).unwrap_or(clean_name);
                            if !final_name.is_empty() {
                                tables.push(final_name);
                            }
                        }
                    }
                    
                    i = j;
                    continue;
                }
            }
            i += 1;
        }

        // Also try to extract from original SQL (case-sensitive) for better accuracy
        // Look for table patterns in original SQL
        let original_words: Vec<&str> = sql.split_whitespace().collect();
        let mut i = 0;
        
        while i < original_words.len() {
            let word_upper = original_words[i].to_uppercase();
            if from_patterns.iter().any(|pattern| word_upper.contains(pattern)) {
                if i + 1 < original_words.len() {
                    let next_word = original_words[i + 1];
                    // Remove schema prefix if present (e.g., "schema.table" -> "table")
                    let table_name = if next_word.contains('.') {
                        next_word.split('.').last().unwrap_or(next_word).to_string()
                    } else {
                        next_word.to_string()
                    };
                    
                    // Remove trailing punctuation
                    let clean_name = table_name.trim_end_matches(',').trim_end_matches(';').trim().to_string();
                    if !clean_name.is_empty() && !tables.contains(&clean_name) {
                        tables.push(clean_name);
                    }
                }
            }
            i += 1;
        }

        Ok(tables)
    }
}

