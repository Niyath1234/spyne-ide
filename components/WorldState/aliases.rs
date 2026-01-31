//! Table Alias Registry - Maps human-friendly names to table IDs
//! 
//! Enables natural language queries to resolve "loan pool" → "adh433" (Assets table)
//! Supports org-wide, team, and user-scoped aliases with learning capability.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Alias scope - determines who can see/use this alias
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AliasScope {
    /// Organization-wide alias (everyone can use)
    Org,
    /// Team-scoped alias (only team members)
    Team(String),
    /// User-scoped alias (only this user)
    User(String),
}

/// Table alias entry - maps human phrases to table IDs
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableAlias {
    /// The human-friendly phrase (e.g., "loan pool", "assets")
    pub phrase: String,
    
    /// The actual table ID this maps to (e.g., "adh433")
    pub table_id: String,
    
    /// Display name for the table (e.g., "Assets")
    pub display_name: String,
    
    /// Scope of this alias
    pub scope: AliasScope,
    
    /// Confidence score (0.0-1.0) - how confident we are in this mapping
    pub confidence: f64,
    
    /// Usage count - how many times this alias has been used
    pub usage_count: u64,
    
    /// Timestamp when alias was created
    pub created_at: u64,
    
    /// Timestamp when alias was last used
    pub last_used_at: u64,
}

impl TableAlias {
    pub fn new(
        phrase: String,
        table_id: String,
        display_name: String,
        scope: AliasScope,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            phrase: phrase.to_lowercase(), // Normalize to lowercase
            table_id,
            display_name,
            scope,
            confidence: 1.0, // User-confirmed aliases start at 1.0
            usage_count: 0,
            created_at: now,
            last_used_at: now,
        }
    }
    
    /// Record usage of this alias
    pub fn record_usage(&mut self) {
        self.usage_count += 1;
        self.last_used_at = Self::now_timestamp();
    }
    
    fn now_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Table Alias Registry - stores all table aliases
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableAliasRegistry {
    /// Phrase → Vec<TableAlias> (multiple aliases possible for same phrase in different scopes)
    aliases: HashMap<String, Vec<TableAlias>>,
    
    /// Table ID → display name cache
    display_names: HashMap<String, String>,
}

impl TableAliasRegistry {
    pub fn new() -> Self {
        Self {
            aliases: HashMap::new(),
            display_names: HashMap::new(),
        }
    }
    
    /// Register a new alias
    pub fn register_alias(&mut self, alias: TableAlias) {
        let phrase = alias.phrase.clone();
        self.aliases
            .entry(phrase)
            .or_insert_with(Vec::new)
            .push(alias.clone());
        
        // Update display name cache
        self.display_names.insert(alias.table_id.clone(), alias.display_name.clone());
    }
    
    /// Find aliases for a phrase, filtered by scope
    /// Returns all matching aliases sorted by confidence and usage
    pub fn find_aliases(&self, phrase: &str, scope_filter: Option<&AliasScope>) -> Vec<&TableAlias> {
        let normalized = phrase.to_lowercase();
        
        if let Some(aliases) = self.aliases.get(&normalized) {
            let mut results: Vec<&TableAlias> = aliases.iter()
                .filter(|a| {
                    // Filter by scope if provided
                    if let Some(filter) = scope_filter {
                        match (filter, &a.scope) {
                            (AliasScope::Org, AliasScope::Org) => true,
                            (AliasScope::Team(t1), AliasScope::Team(t2)) => t1 == t2,
                            (AliasScope::User(u1), AliasScope::User(u2)) => u1 == u2,
                            (AliasScope::Org, _) => true, // Org can see all
                            _ => false,
                        }
                    } else {
                        true
                    }
                })
                .collect();
            
            // Sort by confidence (desc) then usage_count (desc)
            results.sort_by(|a, b| {
                b.confidence.partial_cmp(&a.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(b.usage_count.cmp(&a.usage_count))
            });
            
            results
        } else {
            Vec::new()
        }
    }
    
    /// Get best alias for a phrase (highest confidence + usage)
    pub fn get_best_alias(&self, phrase: &str, scope_filter: Option<&AliasScope>) -> Option<&TableAlias> {
        self.find_aliases(phrase, scope_filter).first().copied()
    }
    
    /// Record usage of an alias
    pub fn record_usage(&mut self, phrase: &str, table_id: &str) {
        let normalized = phrase.to_lowercase();
        if let Some(aliases) = self.aliases.get_mut(&normalized) {
            for alias in aliases.iter_mut() {
                if alias.table_id == table_id {
                    alias.record_usage();
                    break;
                }
            }
        }
    }
    
    /// Get display name for a table ID
    pub fn get_display_name(&self, table_id: &str) -> Option<&String> {
        self.display_names.get(table_id)
    }
    
    /// Check if phrase has any aliases
    pub fn has_alias(&self, phrase: &str) -> bool {
        self.aliases.contains_key(&phrase.to_lowercase())
    }
    
    /// List all aliases for a table
    pub fn list_aliases_for_table(&self, table_id: &str) -> Vec<&TableAlias> {
        self.aliases.values()
            .flatten()
            .filter(|a| a.table_id == table_id)
            .collect()
    }
    
    /// Remove old/unused aliases (cleanup)
    pub fn prune_unused(&mut self, days_unused: u64, min_usage: u64) {
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(days_unused * 86400);
        
        for aliases in self.aliases.values_mut() {
            aliases.retain(|a| {
                a.last_used_at > cutoff_time || a.usage_count >= min_usage
            });
        }
        
        // Remove empty entries
        self.aliases.retain(|_, v| !v.is_empty());
    }
    
    /// List all aliases (for UI/admin purposes)
    pub fn list_all_aliases(&self) -> Vec<&TableAlias> {
        self.aliases.values()
            .flatten()
            .collect()
    }
}

impl Default for TableAliasRegistry {
    fn default() -> Self {
        Self::new()
    }
}








