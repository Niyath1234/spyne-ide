use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KnowledgeHints {
    #[serde(default)]
    pub terms: HashMap<String, TermHint>,
    #[serde(default)]
    pub tables: HashMap<String, TableHint>,
    #[serde(default)]
    pub relationships: HashMap<String, RelationshipHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TermHint {
    #[serde(default)]
    pub definition: Option<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub business_meaning: Option<String>,
    #[serde(default)]
    pub related_tables: Vec<String>,
    #[serde(default)]
    pub data_type: Option<String>,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub filters: Vec<String>,
    #[serde(default)]
    pub valid_values: Vec<String>,
    #[serde(default)]
    pub use_cases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableHint {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub contains: Vec<String>,
    #[serde(default)]
    pub key_fields: Vec<String>,
    #[serde(default)]
    pub time_field: Option<String>,
    #[serde(default)]
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RelationshipHint {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub join_key: Option<String>,
    #[serde(default)]
    pub relationship: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TermHit {
    pub term: String,
    pub matched: String,
    pub hint: TermHint,
}

impl KnowledgeHints {
    pub fn load(path: impl AsRef<Path>) -> Result<Option<Self>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(path)
            .map_err(|e| RcaError::Metadata(format!("Failed to read {}: {}", path.display(), e)))?;
        let parsed: KnowledgeHints = serde_json::from_str(&content)
            .map_err(|e| RcaError::Metadata(format!("Failed to parse {}: {}", path.display(), e)))?;
        Ok(Some(parsed))
    }

    pub fn find_term_hits(&self, text: &str) -> Vec<TermHit> {
        let text_lower = text.to_lowercase();
        let mut hits = Vec::new();
        let mut seen_terms = HashSet::new();

        for (term, hint) in &self.terms {
            let term_lower = term.to_lowercase();
            let mut matched = None;
            if contains_word(&text_lower, &term_lower) {
                matched = Some(term.clone());
            } else {
                for alias in &hint.aliases {
                    let alias_lower = alias.to_lowercase();
                    if contains_word(&text_lower, &alias_lower) {
                        matched = Some(alias.clone());
                        break;
                    }
                }
            }

            if let Some(matched_term) = matched {
                if seen_terms.insert(term.clone()) {
                    hits.push(TermHit {
                        term: term.clone(),
                        matched: matched_term,
                        hint: hint.clone(),
                    });
                }
            }
        }

        hits
    }

    pub fn find_table_mentions(&self, text: &str) -> Vec<String> {
        let text_lower = text.to_lowercase();
        let mut tables = Vec::new();
        for table in self.tables.keys() {
            let table_lower = table.to_lowercase();
            if contains_word(&text_lower, &table_lower) {
                tables.push(table.clone());
            }
        }
        tables
    }
}

fn contains_word(text: &str, word: &str) -> bool {
    if word.is_empty() {
        return false;
    }
    if text == word {
        return true;
    }
    if text.starts_with(word) {
        return boundary_at(text, word.len());
    }
    if text.ends_with(word) {
        let pos = text.len().saturating_sub(word.len());
        return boundary_at(text, pos);
    }
    if let Some(pos) = text.find(word) {
        let start_ok = boundary_at(text, pos);
        let end_ok = boundary_at(text, pos + word.len());
        return start_ok && end_ok;
    }
    false
}

fn boundary_at(text: &str, idx: usize) -> bool {
    if idx == 0 || idx >= text.len() {
        return true;
    }
    if let Some(ch) = text.chars().nth(idx) {
        !ch.is_alphanumeric() && ch != '_'
    } else {
        true
    }
}





