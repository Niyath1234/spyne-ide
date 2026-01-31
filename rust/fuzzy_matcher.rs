use crate::error::{RcaError, Result};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use strsim::jaro_winkler;

/// Fuzzy matcher for entity keys with name variations
pub struct FuzzyMatcher {
    /// Similarity threshold (0.0-1.0) for considering two strings as matches
    pub similarity_threshold: f64,
    /// Whether to normalize strings before comparison
    pub normalize: bool,
}

impl Default for FuzzyMatcher {
    fn default() -> Self {
        Self {
            similarity_threshold: 0.85, // 85% similarity threshold
            normalize: true,
        }
    }
}

impl FuzzyMatcher {
    pub fn new(threshold: f64) -> Self {
        Self {
            similarity_threshold: threshold,
            normalize: true,
        }
    }

    /// Normalize a string for fuzzy matching
    /// - Removes common titles (Mr, Mrs, Ms, Dr, etc.)
    /// - Normalizes whitespace
    /// - Converts to lowercase
    /// - Removes punctuation
    pub fn normalize_string(&self, s: &str) -> String {
        if !self.normalize {
            return s.to_lowercase();
        }

        let mut normalized = s.to_lowercase();
        
        // Remove punctuation FIRST (so "Mr." becomes "mr")
        normalized = normalized
            .chars()
            .filter(|c| c.is_alphanumeric() || c.is_whitespace())
            .collect();
        
        // Normalize whitespace (multiple spaces to single space)
        normalized = regex::Regex::new(r"\s+")
            .unwrap()
            .replace_all(&normalized, " ")
            .to_string();
        
        // Trim
        normalized = normalized.trim().to_string();
        
        // Remove common titles (after punctuation removal so "Mr." -> "mr" matches)
        let titles = [
            "mr", "mrs", "ms", "miss", "dr", "prof", "professor",
            "sir", "madam", "lord", "lady",
        ];
        for title in &titles {
            let pattern = format!(r"^{}\s+", title);
            normalized = regex::Regex::new(&pattern)
                .unwrap()
                .replace(&normalized, "")
                .to_string();
        }
        
        // Normalize whitespace again after title removal
        normalized = regex::Regex::new(r"\s+")
            .unwrap()
            .replace_all(&normalized, " ")
            .to_string();
        
        normalized.trim().to_string()
    }

    /// Calculate similarity between two strings
    /// Returns a score between 0.0 and 1.0 (higher = more similar)
    pub fn similarity(&self, s1: &str, s2: &str) -> f64 {
        let norm1 = self.normalize_string(s1);
        let norm2 = self.normalize_string(s2);
        
        // Use Jaro-Winkler for better handling of name variations
        // Falls back to Levenshtein if strings are very different
        let jw_score = jaro_winkler(&norm1, &norm2);
        
        // Also check if one is a substring of the other (for cases like "Radhika" vs "Radhika Apte")
        let is_substring = norm1.contains(&norm2) || norm2.contains(&norm1);
        let substring_bonus = if is_substring && (norm1.len() > 0 && norm2.len() > 0) {
            // Bonus for substring matches, but penalize if length difference is too large
            let len_diff = (norm1.len() as f64 - norm2.len() as f64).abs();
            let max_len = norm1.len().max(norm2.len()) as f64;
            if max_len > 0.0 {
                (1.0 - (len_diff / max_len)) * 0.1 // Up to 10% bonus
            } else {
                0.0
            }
        } else {
            0.0
        };
        
        (jw_score + substring_bonus).min(1.0)
    }

    /// Check if two strings are similar enough to be considered a match
    pub fn is_match(&self, s1: &str, s2: &str) -> bool {
        self.similarity(s1, s2) >= self.similarity_threshold
    }

    /// Find best matching key from a set of keys
    /// Returns (matched_key, similarity_score) if match found, None otherwise
    pub fn find_best_match(
        &self,
        target_key: &[String],
        candidate_keys: &HashSet<Vec<String>>,
        grain_columns: &[String],
    ) -> Option<(Vec<String>, f64)> {
        if target_key.len() != grain_columns.len() {
            return None;
        }

        let mut best_match: Option<(Vec<String>, f64)> = None;
        let mut best_score = 0.0;

        for candidate in candidate_keys {
            if candidate.len() != grain_columns.len() {
                continue;
            }

            // Calculate overall similarity for multi-column keys
            let mut total_similarity = 0.0;
            let mut matched_columns = 0;

            for (idx, grain_col) in grain_columns.iter().enumerate() {
                let target_val = &target_key[idx];
                let candidate_val = &candidate[idx];

                // Check if this column should use fuzzy matching (string columns)
                // For now, assume all columns are strings - can be enhanced to check data types
                let similarity = self.similarity(target_val, candidate_val);
                
                if similarity >= self.similarity_threshold {
                    total_similarity += similarity;
                    matched_columns += 1;
                } else {
                    // If any column doesn't match, this candidate is not a match
                    break;
                }
            }

            if matched_columns == grain_columns.len() {
                let avg_similarity = total_similarity / matched_columns as f64;
                if avg_similarity > best_score {
                    best_score = avg_similarity;
                    best_match = Some((candidate.clone(), avg_similarity));
                }
            }
        }

        best_match
    }

    /// Perform fuzzy population diff - finds matches even with name variations
    pub fn fuzzy_population_diff(
        &self,
        df_a: &DataFrame,
        df_b: &DataFrame,
        grain: &[String],
    ) -> Result<FuzzyPopulationDiff> {
        println!("    Performing fuzzy matching for grain columns: {:?}", grain);
        
        // Extract keys from both dataframes
        let keys_a: HashSet<Vec<String>> = self.extract_keys(df_a, grain)?;
        let keys_b: HashSet<Vec<String>> = self.extract_keys(df_b, grain)?;
        
        // First, find exact matches
        let exact_matches: Vec<Vec<String>> = keys_a.intersection(&keys_b).cloned().collect();
        println!("    Found {} exact matches", exact_matches.len());
        
        // Find keys in A that don't have exact matches in B
        let unmatched_a: Vec<Vec<String>> = keys_a
            .difference(&keys_b)
            .cloned()
            .collect();
        
        // Find keys in B that don't have exact matches in A
        let unmatched_b: Vec<Vec<String>> = keys_b
            .difference(&keys_a)
            .cloned()
            .collect();
        
        println!("    Attempting fuzzy matching for {} unmatched keys in A", unmatched_a.len());
        println!("    Attempting fuzzy matching for {} unmatched keys in B", unmatched_b.len());
        
        // Try to find fuzzy matches
        let mut fuzzy_matches: Vec<FuzzyMatch> = Vec::new();
        let mut matched_a: HashSet<Vec<String>> = HashSet::new();
        let mut matched_b: HashSet<Vec<String>> = HashSet::new();
        
        // For each unmatched key in A, try to find best match in unmatched B
        for key_a in &unmatched_a {
            if let Some((matched_key_b, score)) = self.find_best_match(
                key_a,
                &unmatched_b.iter().cloned().collect(),
                grain,
            ) {
                fuzzy_matches.push(FuzzyMatch {
                    key_a: key_a.clone(),
                    key_b: matched_key_b.clone(),
                    similarity: score,
                });
                matched_a.insert(key_a.clone());
                matched_b.insert(matched_key_b);
            }
        }
        
        println!("    Found {} fuzzy matches", fuzzy_matches.len());
        
        // Remaining unmatched keys
        let missing_in_b: Vec<Vec<String>> = unmatched_a
            .iter()
            .filter(|k| !matched_a.contains(*k))
            .cloned()
            .collect();
        
        let extra_in_b: Vec<Vec<String>> = unmatched_b
            .iter()
            .filter(|k| !matched_b.contains(*k))
            .cloned()
            .collect();
        
        let common_count = exact_matches.len() + fuzzy_matches.len();
        
        Ok(FuzzyPopulationDiff {
            exact_matches,
            fuzzy_matches,
            missing_in_b,
            extra_in_b,
            common_count,
        })
    }

    fn extract_keys(&self, df: &DataFrame, grain: &[String]) -> Result<HashSet<Vec<String>>> {
        let mut keys = HashSet::new();
        
        for row_idx in 0..df.height() {
            let mut key = Vec::new();
            let mut has_null = false;
            
            for col_name in grain {
                let col_val = df.column(col_name)?;
                let val_str = match col_val.dtype() {
                    DataType::String => {
                        match col_val.str().unwrap().get(row_idx) {
                            Some(s) => s.to_string(),
                            None => { has_null = true; "NULL".to_string() }
                        }
                    }
                    DataType::Int64 => {
                        match col_val.i64().unwrap().get(row_idx) {
                            Some(v) => v.to_string(),
                            None => { has_null = true; "NULL".to_string() }
                        }
                    }
                    DataType::Float64 => {
                        match col_val.f64().unwrap().get(row_idx) {
                            Some(v) => v.to_string(),
                            None => { has_null = true; "NULL".to_string() }
                        }
                    }
                    _ => format!("{:?}", col_val.get(row_idx)),
                };
                key.push(val_str);
            }
            
            // Skip rows with null keys (they can't be matched anyway)
            if !has_null {
                keys.insert(key);
            }
        }
        
        Ok(keys)
    }
}

#[derive(Debug, Clone)]
pub struct FuzzyMatch {
    pub key_a: Vec<String>,
    pub key_b: Vec<String>,
    pub similarity: f64,
}

#[derive(Debug, Clone)]
pub struct FuzzyPopulationDiff {
    pub exact_matches: Vec<Vec<String>>,
    pub fuzzy_matches: Vec<FuzzyMatch>,
    pub missing_in_b: Vec<Vec<String>>,
    pub extra_in_b: Vec<Vec<String>>,
    pub common_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_string() {
        let matcher = FuzzyMatcher::default();
        
        assert_eq!(
            matcher.normalize_string("Ms Radhika apte"),
            "radhika apte"
        );
        assert_eq!(
            matcher.normalize_string("Radika APte"),
            "radika apte"
        );
        assert_eq!(
            matcher.normalize_string("Mr. John   Doe"),
            "john doe"
        );
    }

    #[test]
    fn test_similarity() {
        let matcher = FuzzyMatcher::default();
        
        let s1 = "Ms Radhika apte";
        let s2 = "Radika APte";
        let similarity = matcher.similarity(s1, s2);
        
        println!("Similarity between '{}' and '{}': {:.2}%", s1, s2, similarity * 100.0);
        assert!(similarity > 0.7); // Should be reasonably similar
    }

    #[test]
    fn test_is_match() {
        let matcher = FuzzyMatcher::new(0.85);
        
        assert!(matcher.is_match("Radhika Apte", "Radhika apte"));
        assert!(matcher.is_match("Ms Radhika apte", "Radhika APte"));
        assert!(!matcher.is_match("John Doe", "Jane Smith"));
    }
}

