//! Business Concepts - Semantic metadata definitions

use super::types::{ConceptType, ConceptUsage};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A business concept definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BusinessConcept {
    /// Unique concept ID
    pub concept_id: String,
    
    /// Concept name (e.g., "TOS", "Khatabook", "loans table")
    pub name: String,
    
    /// Type of concept
    pub concept_type: ConceptType,
    
    /// Definition/description
    pub definition: String,
    
    /// Related tables (for table/entity concepts)
    pub related_tables: Vec<String>,
    
    /// Related columns (for column semantics)
    pub related_columns: Vec<String>,
    
    /// Component breakdown (e.g., "TOS = principal + interest - writeoff")
    pub components: Vec<String>,
    
    /// SQL expression if applicable
    pub sql_expression: Option<String>,
    
    /// Tags for categorization
    pub tags: Vec<String>,
    
    /// Vector embedding (for similarity search)
    pub embedding: Option<Vec<f32>>,
    
    /// Created by (admin username)
    pub created_by: Option<String>,
    
    /// Created timestamp
    pub created_at: u64,
    
    /// Updated timestamp
    pub updated_at: u64,
}

impl BusinessConcept {
    pub fn new(
        concept_id: String,
        name: String,
        concept_type: ConceptType,
        definition: String,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            concept_id,
            name,
            concept_type,
            definition,
            related_tables: Vec::new(),
            related_columns: Vec::new(),
            components: Vec::new(),
            sql_expression: None,
            tags: Vec::new(),
            embedding: None,
            created_by: None,
            created_at: now,
            updated_at: now,
        }
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
    
    /// Generate a text representation for embedding
    pub fn to_embedding_text(&self) -> String {
        let mut text = format!("{}: {}\n", self.name, self.definition);
        
        if !self.related_tables.is_empty() {
            text.push_str(&format!("Related tables: {}\n", self.related_tables.join(", ")));
        }
        
        if !self.related_columns.is_empty() {
            text.push_str(&format!("Related columns: {}\n", self.related_columns.join(", ")));
        }
        
        if !self.components.is_empty() {
            text.push_str(&format!("Components: {}\n", self.components.join(", ")));
        }
        
        if let Some(ref sql) = self.sql_expression {
            text.push_str(&format!("SQL: {}\n", sql));
        }
        
        if !self.tags.is_empty() {
            text.push_str(&format!("Tags: {}\n", self.tags.join(", ")));
        }
        
        text
    }
}

/// Knowledge Base - Registry of business concepts
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KnowledgeBase {
    /// Concept ID → Concept
    concepts: HashMap<String, BusinessConcept>,
    
    /// Name → Concept IDs (for fast lookup)
    name_index: HashMap<String, Vec<String>>,
    
    /// Type → Concept IDs
    type_index: HashMap<ConceptType, Vec<String>>,
    
    /// Table name → Concept IDs
    table_index: HashMap<String, Vec<String>>,
    
    /// Tag → Concept IDs (for fast lookup by tags)
    #[serde(default)]
    tag_index: HashMap<String, Vec<String>>,
    
    /// Component name → Concept IDs (for fast lookup by components)
    #[serde(default)]
    component_index: HashMap<String, Vec<String>>,
    
    /// User (created_by) → Concept IDs (for fast lookup by user)
    #[serde(default)]
    user_index: HashMap<String, Vec<String>>,
    
    /// Pending table mappings: Concept name → Vec<Table names>
    /// Stores mappings that were mentioned in contracts but the concept doesn't exist yet
    /// These will be synced when the concept is added later
    #[serde(default)]
    pending_table_mappings: HashMap<String, Vec<String>>,
}

impl KnowledgeBase {
    pub fn new() -> Self {
        Self {
            concepts: HashMap::new(),
            name_index: HashMap::new(),
            type_index: HashMap::new(),
            table_index: HashMap::new(),
            tag_index: HashMap::new(),
            component_index: HashMap::new(),
            user_index: HashMap::new(),
            pending_table_mappings: HashMap::new(),
        }
    }
    
    /// Add or update a concept
    pub fn add_concept(&mut self, mut concept: BusinessConcept) {
        let concept_id = concept.concept_id.clone();
        let name = concept.name.clone();
        let concept_type = concept.concept_type.clone();
        
        // Remove old indexes if updating existing concept
        if self.concepts.contains_key(&concept_id) {
            self._remove_from_indexes(&concept_id);
        }
        
        // Sync pending mappings: If there are pending table mappings for this concept name,
        // add them to related_tables now
        if let Some(pending_tables) = self.pending_table_mappings.remove(&name) {
            for table in &pending_tables {
                if !concept.related_tables.contains(table) {
                    concept.related_tables.push(table.clone());
                }
            }
        }
        
        // Update indexes
        self.name_index
            .entry(name.clone())
            .or_insert_with(Vec::new)
            .push(concept_id.clone());
        
        self.type_index
            .entry(concept_type)
            .or_insert_with(Vec::new)
            .push(concept_id.clone());
        
        // Index by related tables
        for table in &concept.related_tables {
            self.table_index
                .entry(table.clone())
                .or_insert_with(Vec::new)
                .push(concept_id.clone());
        }
        
        // Index by tags
        for tag in &concept.tags {
            self.tag_index
                .entry(tag.clone())
                .or_insert_with(Vec::new)
                .push(concept_id.clone());
        }
        
        // Index by components
        for component in &concept.components {
            self.component_index
                .entry(component.clone())
                .or_insert_with(Vec::new)
                .push(concept_id.clone());
        }
        
        // Index by user (created_by)
        if let Some(ref user) = concept.created_by {
            self.user_index
                .entry(user.clone())
                .or_insert_with(Vec::new)
                .push(concept_id.clone());
        }
        
        // Store concept
        self.concepts.insert(concept_id, concept);
    }
    
    /// Internal helper to remove concept from all indexes
    fn _remove_from_indexes(&mut self, concept_id: &str) {
        if let Some(concept) = self.concepts.get(concept_id) {
            // Remove from name index
            if let Some(ids) = self.name_index.get_mut(&concept.name) {
                ids.retain(|id| id != concept_id);
                if ids.is_empty() {
                    self.name_index.remove(&concept.name);
                }
            }
            
            // Remove from type index
            if let Some(ids) = self.type_index.get_mut(&concept.concept_type) {
                ids.retain(|id| id != concept_id);
                if ids.is_empty() {
                    self.type_index.remove(&concept.concept_type);
                }
            }
            
            // Remove from table index
            for table in &concept.related_tables {
                if let Some(ids) = self.table_index.get_mut(table) {
                    ids.retain(|id| id != concept_id);
                    if ids.is_empty() {
                        self.table_index.remove(table);
                    }
                }
            }
            
            // Remove from tag index
            for tag in &concept.tags {
                if let Some(ids) = self.tag_index.get_mut(tag) {
                    ids.retain(|id| id != concept_id);
                    if ids.is_empty() {
                        self.tag_index.remove(tag);
                    }
                }
            }
            
            // Remove from component index
            for component in &concept.components {
                if let Some(ids) = self.component_index.get_mut(component) {
                    ids.retain(|id| id != concept_id);
                    if ids.is_empty() {
                        self.component_index.remove(component);
                    }
                }
            }
            
            // Remove from user index
            if let Some(ref user) = concept.created_by {
                if let Some(ids) = self.user_index.get_mut(user) {
                    ids.retain(|id| id != concept_id);
                    if ids.is_empty() {
                        self.user_index.remove(user);
                    }
                }
            }
        }
    }
    
    /// Get concept by ID
    pub fn get_concept(&self, concept_id: &str) -> Option<&BusinessConcept> {
        self.concepts.get(concept_id)
    }
    
    /// Get mutable concept by ID
    pub fn get_concept_mut(&mut self, concept_id: &str) -> Option<&mut BusinessConcept> {
        self.concepts.get_mut(concept_id)
    }
    
    /// Find business concept names mentioned in text
    /// Returns concept names that appear in the text (for pending mapping)
    pub fn find_mentioned_concepts(&self, text: &str) -> Vec<String> {
        let text_lower = text.to_lowercase();
        let mut mentioned = Vec::new();
        
        // Check all Entity and Table concepts (these are the ones that map to tables)
        let entity_concepts = self.get_by_type(&ConceptType::Entity);
        let table_concepts = self.get_by_type(&ConceptType::Table);
        
        for concept in entity_concepts.iter().chain(table_concepts.iter()) {
            let concept_name_lower = concept.name.to_lowercase();
            // Check if concept name appears in text (word boundary aware)
            if Self::contains_word(&text_lower, &concept_name_lower) {
                mentioned.push(concept.name.clone());
            }
        }
        
        mentioned
    }
    
    /// Check if text contains a word (with word boundaries)
    fn contains_word(text: &str, word: &str) -> bool {
        if word.len() > text.len() {
            return false;
        }
        
        // Exact match
        if text == word {
            return true;
        }
        
        // Simple word boundary check (without regex dependency)
        // Check if word appears at start, end, or with non-alphanumeric boundaries
        if text.starts_with(word) {
            if word.len() == text.len() {
                return true;
            }
            if let Some(ch) = text.chars().nth(word.len()) {
                if !ch.is_alphanumeric() && ch != '_' {
                    return true;
                }
            }
        }
        
        if text.ends_with(word) {
            if word.len() == text.len() {
                return true;
            }
            let pos = text.len().saturating_sub(word.len() + 1);
            if pos < text.len() {
                if let Some(ch) = text.chars().nth(pos) {
                    if !ch.is_alphanumeric() && ch != '_' {
                        return true;
                    }
                }
            }
        }
        
        // Check for word in middle with boundaries
        if let Some(pos) = text.find(word) {
            if pos > 0 {
                if let Some(prev_ch) = text.chars().nth(pos.saturating_sub(1)) {
                    if !prev_ch.is_alphanumeric() && prev_ch != '_' {
                        let end_pos = pos + word.len();
                        if end_pos < text.len() {
                            if let Some(next_ch) = text.chars().nth(end_pos) {
                                if !next_ch.is_alphanumeric() && next_ch != '_' {
                                    return true;
                                }
                            }
                        } else {
                            return true; // Word at end
                        }
                    }
                }
            }
        }
        
        false
    }
    
    /// Add a pending table mapping for a concept that doesn't exist yet
    /// When the concept is added later, these tables will be automatically added to related_tables
    pub fn add_pending_table_mapping(&mut self, concept_name: &str, table_name: String) {
        let concept_name_lower = concept_name.to_lowercase();
        self.pending_table_mappings
            .entry(concept_name_lower)
            .or_insert_with(Vec::new)
            .push(table_name);
    }
    
    /// Get pending table mappings for a concept name
    pub fn get_pending_mappings(&self, concept_name: &str) -> Vec<String> {
        let concept_name_lower = concept_name.to_lowercase();
        self.pending_table_mappings
            .get(&concept_name_lower)
            .cloned()
            .unwrap_or_default()
    }
    
    /// Search concepts by name (fuzzy)
    pub fn search_by_name(&self, query: &str) -> Vec<&BusinessConcept> {
        let query_lower = query.to_lowercase();
        self.concepts
            .values()
            .filter(|c| c.name.to_lowercase().contains(&query_lower))
            .collect()
    }
    
    /// Search concepts by name with optional table filter (universe reduction)
    pub fn search_by_name_with_universe(&self, query: &str, tables_filter: Option<&[String]>) -> Vec<&BusinessConcept> {
        let query_lower = query.to_lowercase();
        
        // If table filter provided, first get concepts that reference those tables
        let candidate_ids: Option<std::collections::HashSet<String>> = if let Some(tables) = tables_filter {
            if tables.is_empty() {
                None // Empty filter means no universe reduction
            } else {
                let mut ids = std::collections::HashSet::new();
                for table in tables {
                    if let Some(concept_ids) = self.table_index.get(table) {
                        for id in concept_ids {
                            ids.insert(id.clone());
                        }
                    }
                }
                if ids.is_empty() {
                    return Vec::new(); // No concepts match tables filter
                }
                Some(ids)
            }
        } else {
            None
        };
        
        // Search within candidate set (or all concepts if no filter)
        self.concepts
            .values()
            .filter(|c| {
                // Check if in candidate set (if universe reduction is active)
                if let Some(ref candidate_set) = candidate_ids {
                    if !candidate_set.contains(&c.concept_id) {
                        return false;
                    }
                }
                // Check name match
                c.name.to_lowercase().contains(&query_lower)
            })
            .collect()
    }
    
    /// Fuzzy search by name using Levenshtein distance
    pub fn fuzzy_search_name(&self, query: &str, max_distance: usize) -> Vec<&BusinessConcept> {
        let query_lower = query.to_lowercase();
        self.concepts
            .values()
            .filter(|c| {
                let name_lower = c.name.to_lowercase();
                Self::levenshtein_distance(&name_lower, &query_lower) <= max_distance
            })
            .collect()
    }
    
    /// Calculate Levenshtein distance between two strings
    fn levenshtein_distance(s1: &str, s2: &str) -> usize {
        let s1_chars: Vec<char> = s1.chars().collect();
        let s2_chars: Vec<char> = s2.chars().collect();
        let n = s1_chars.len();
        let m = s2_chars.len();
        
        if n == 0 {
            return m;
        }
        if m == 0 {
            return n;
        }
        
        let mut dp = vec![vec![0; m + 1]; n + 1];
        
        for i in 0..=n {
            dp[i][0] = i;
        }
        for j in 0..=m {
            dp[0][j] = j;
        }
        
        for i in 1..=n {
            for j in 1..=m {
                let cost = if s1_chars[i - 1] == s2_chars[j - 1] { 0 } else { 1 };
                dp[i][j] = (dp[i - 1][j] + 1)
                    .min(dp[i][j - 1] + 1)
                    .min(dp[i - 1][j - 1] + cost);
            }
        }
        
        dp[n][m]
    }
    
    /// Get concepts by tags
    pub fn get_by_tags(&self, tags: &[String]) -> Vec<&BusinessConcept> {
        let mut concept_ids = std::collections::HashSet::new();
        
        for tag in tags {
            if let Some(ids) = self.tag_index.get(tag) {
                for id in ids {
                    concept_ids.insert(id.clone());
                }
            }
        }
        
        concept_ids
            .iter()
            .filter_map(|id| self.concepts.get(id))
            .collect()
    }
    
    /// Get concepts by component name
    pub fn get_by_component(&self, component: &str) -> Vec<&BusinessConcept> {
        self.component_index
            .get(component)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.concepts.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Get concepts by user (created_by)
    pub fn get_by_user(&self, user: &str) -> Vec<&BusinessConcept> {
        self.user_index
            .get(user)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.concepts.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Search with universe reduction (filter by tables first, then search)
    pub fn search_with_universe(
        &self,
        query: &str,
        tables: &[String],
        concept_type: Option<&ConceptType>,
    ) -> Vec<&BusinessConcept> {
        // First, get concepts that reference any of the provided tables
        let mut candidate_ids = std::collections::HashSet::new();
        
        if tables.is_empty() {
            // No table filter - use all concepts
            candidate_ids = self.concepts.keys().cloned().collect();
        } else {
            for table in tables {
                if let Some(ids) = self.table_index.get(table) {
                    for id in ids {
                        candidate_ids.insert(id.clone());
                    }
                }
            }
        }
        
        if candidate_ids.is_empty() {
            return Vec::new();
        }
        
        // Filter by concept type if provided
        let filtered_by_type: std::collections::HashSet<String> = if let Some(ct) = concept_type {
            self.type_index
                .get(ct)
                .map(|ids| ids.iter().cloned().collect())
                .unwrap_or_default()
        } else {
            candidate_ids.clone()
        };
        
        // Intersect: concepts that match both tables and type
        candidate_ids.retain(|id| filtered_by_type.contains(id));
        
        // Now search by query within candidate set
        let query_lower = query.to_lowercase();
        candidate_ids
            .iter()
            .filter_map(|id| self.concepts.get(id))
            .filter(|c| {
                c.name.to_lowercase().contains(&query_lower)
                    || c.definition.to_lowercase().contains(&query_lower)
            })
            .collect()
    }
    
    /// Get concepts by type
    pub fn get_by_type(&self, concept_type: &ConceptType) -> Vec<&BusinessConcept> {
        self.type_index
            .get(concept_type)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.concepts.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Get concepts related to a table
    pub fn get_table_concepts(&self, table_name: &str) -> Vec<&BusinessConcept> {
        self.table_index
            .get(table_name)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.concepts.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// List all concepts
    pub fn list_all(&self) -> Vec<&BusinessConcept> {
        self.concepts.values().collect()
    }
    
    /// Delete a concept
    pub fn delete_concept(&mut self, concept_id: &str) -> bool {
        if self.concepts.contains_key(concept_id) {
            self._remove_from_indexes(concept_id);
            self.concepts.remove(concept_id);
            true
        } else {
            false
        }
    }
    
    /// Find concepts by business term (O(1) lookup via name_index and tag_index)
    /// 
    /// Searches both name_index and tag_index for the term
    /// Returns all matching BusinessConcept objects
    pub fn find_concepts_by_term(&self, term: &str) -> Vec<BusinessConcept> {
        let mut concept_ids = Vec::new();
        let term_lower = term.to_lowercase();
        
        // Search name_index (exact match, case-insensitive)
        for (name, ids) in &self.name_index {
            if name.to_lowercase() == term_lower {
                concept_ids.extend(ids.clone());
            }
        }
        
        // Search tag_index (exact match, case-insensitive)
        for (tag, ids) in &self.tag_index {
            if tag.to_lowercase() == term_lower {
                concept_ids.extend(ids.clone());
            }
        }
        
        // Deduplicate and get concepts
        concept_ids.sort();
        concept_ids.dedup();
        
        concept_ids.into_iter()
            .filter_map(|id| self.concepts.get(&id).cloned())
            .collect()
    }
    
    /// Get tables associated with a business term (O(1) via indexes)
    /// 
    /// Finds concepts matching the term, then extracts related_tables
    /// Returns all unique table names
    pub fn get_tables_for_term(&self, term: &str) -> Vec<String> {
        let concepts = self.find_concepts_by_term(term);
        let mut tables = std::collections::HashSet::new();
        
        for concept in concepts {
            tables.extend(concept.related_tables.clone());
        }
        
        tables.into_iter().collect()
    }
    
    /// Fuzzy match a term against concept names and tags
    /// 
    /// Uses Jaro-Winkler similarity to find close matches
    /// Returns terms sorted by similarity (threshold: 0.0-1.0, typically 0.6-0.8)
    pub fn fuzzy_match_term(&self, term: &str, threshold: f64) -> Vec<String> {
        let term_lower = term.to_lowercase();
        let mut matches: Vec<(String, f64)> = Vec::new();
        
        // Match against names
        for name in self.name_index.keys() {
            let name_lower = name.to_lowercase();
            let similarity = strsim::jaro_winkler(&term_lower, &name_lower);
            if similarity >= threshold {
                matches.push((name.clone(), similarity));
            }
        }
        
        // Match against tags
        for tag in self.tag_index.keys() {
            let tag_lower = tag.to_lowercase();
            let similarity = strsim::jaro_winkler(&term_lower, &tag_lower);
            if similarity >= threshold {
                matches.push((tag.clone(), similarity));
            }
        }
        
        // Sort by similarity descending
        matches.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // Deduplicate and take top 5
        let mut seen = std::collections::HashSet::new();
        matches.into_iter()
            .filter(|(term, _)| seen.insert(term.clone()))
            .take(5)
            .map(|(term, _)| term)
            .collect()
    }
}

impl Default for KnowledgeBase {
    fn default() -> Self {
        Self::new()
    }
}

