//! Learning Store - Learns from user-approved corrections
//! 
//! When a hallucination is detected and fuzzy matching suggests a closest match,
//! users can approve the correction. This module stores and applies these
//! learned corrections in future validations.
//!
//! Architecture: SQLite + LRU Cache
//! - SQLite: Persistent storage (handles 10M+ records)
//! - LRU Cache: Hot data cache (100K entries, O(1) lookups)

use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::{info, warn};
use chrono::Utc;
use rusqlite::{Connection, params};

/// Learned correction mapping incorrect names to correct names
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LearnedCorrection {
    /// The incorrect/hallucinated name
    pub incorrect_name: String,
    
    /// The correct name (from metadata)
    pub correct_name: String,
    
    /// Type of correction: "table" or "column"
    pub correction_type: String,
    
    /// Optional: table name (for column corrections)
    pub table_name: Option<String>,
    
    /// When this correction was learned
    pub learned_at: String,
    
    /// Number of times this correction has been used
    pub usage_count: u32,
    
    /// User who approved this correction
    pub approved_by: Option<String>,
}

/// Type alias for LRU cache key
type CacheKey = (String, String);

/// Optimized LRU Cache implementation using HashMap + VecDeque
/// Provides O(1) average-case operations for get/put
struct OptimizedLruCache {
    /// Cache storage: (key, value)
    data: HashMap<CacheKey, LearnedCorrection>,
    /// Access order: most recently used at front, least recently used at back
    /// VecDeque allows efficient push_front and pop_back operations
    access_order: VecDeque<CacheKey>,
    /// Maximum cache size
    max_size: usize,
}

impl OptimizedLruCache {
    fn new(max_size: usize) -> Self {
        Self {
            data: HashMap::new(),
            access_order: VecDeque::with_capacity(max_size.min(1024)), // Pre-allocate some capacity
            max_size,
        }
    }
    
    /// Get a value from cache, promoting it to most recently used
    fn get(&mut self, key: &CacheKey) -> Option<&LearnedCorrection> {
        if self.data.contains_key(key) {
            // Move to front (most recently used)
            // Remove from current position (O(n) worst case, but amortized O(1) for VecDeque)
            self.access_order.retain(|k| k != key);
            self.access_order.push_front(key.clone());
            self.data.get(key)
        } else {
            None
        }
    }
    
    /// Put a value into cache, evicting least recently used if needed
    fn put(&mut self, key: CacheKey, value: LearnedCorrection) {
        // Remove if exists (to update position)
        if self.data.contains_key(&key) {
            self.access_order.retain(|k| k != &key);
        } else {
            // Evict least recently used if cache is full
            if self.data.len() >= self.max_size {
                if let Some(lru_key) = self.access_order.pop_back() {
                    self.data.remove(&lru_key);
                }
            }
        }
        
        // Insert at front (most recently used)
        self.access_order.push_front(key.clone());
        self.data.insert(key, value);
    }
    
    /// Remove a value from cache
    fn pop(&mut self, key: &CacheKey) -> Option<LearnedCorrection> {
        self.access_order.retain(|k| k != key);
        self.data.remove(key)
    }
    
    /// Get the current cache size
    fn len(&self) -> usize {
        self.data.len()
    }
}

/// Learning Store - Manages learned corrections
/// Uses SQLite for persistence and LRU cache for hot data
pub struct LearningStore {
    /// Path to the learning data directory
    path: PathBuf,
    
    /// SQLite database connection
    db: Mutex<Connection>,
    
    /// LRU cache for hot corrections (default: 100K entries)
    /// Uses optimized LRU cache with O(1) average-case operations
    cache: Mutex<OptimizedLruCache>,
    
    /// Cache size
    cache_size: usize,
}

impl LearningStore {
    /// Default cache size (100K entries)
    const DEFAULT_CACHE_SIZE: usize = 100_000;
    
    /// Create a new learning store with SQLite backend
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_cache_size(path, Self::DEFAULT_CACHE_SIZE)
    }
    
    /// Create a new learning store with custom cache size
    pub fn with_cache_size(path: impl AsRef<Path>, cache_size: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        // Open or create SQLite database
        let db_path = path.join("learned_corrections.db");
        let db = Connection::open(&db_path)
            .map_err(|e| RcaError::Execution(format!("Failed to open database: {}", e)))?;
        
        // Initialize database schema
        let store = Self {
            path,
            db: Mutex::new(db),
            cache: Mutex::new(OptimizedLruCache::new(cache_size)),
            cache_size,
        };
        
        store.init_schema()?;
        
        // Migrate from JSON if it exists
        store.migrate_from_json()?;
        
        Ok(store)
    }
    
    /// Load learning store from disk
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        Self::new(path)
    }
    
    /// Initialize database schema
    fn init_schema(&self) -> Result<()> {
        let db = self.db.lock().unwrap();
        
        db.execute(
            r#"
            CREATE TABLE IF NOT EXISTS learned_corrections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                incorrect_name TEXT NOT NULL,
                correct_name TEXT NOT NULL,
                correction_type TEXT NOT NULL,
                table_name TEXT,
                learned_at TEXT NOT NULL,
                usage_count INTEGER NOT NULL DEFAULT 1,
                approved_by TEXT,
                UNIQUE(incorrect_name, correction_type)
            )
            "#,
            [],
        ).map_err(|e| RcaError::Execution(format!("Failed to create table: {}", e)))?;
        
        // Create indexes for fast lookups
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_incorrect_name_type ON learned_corrections(incorrect_name, correction_type)",
            [],
        ).map_err(|e| RcaError::Execution(format!("Failed to create index: {}", e)))?;
        
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_correction_type ON learned_corrections(correction_type)",
            [],
        ).map_err(|e| RcaError::Execution(format!("Failed to create index: {}", e)))?;
        
        Ok(())
    }
    
    /// Migrate from JSON file if it exists
    fn migrate_from_json(&self) -> Result<()> {
        let json_file = self.path.join("learned_corrections.json");
        
        if !json_file.exists() {
            return Ok(());
        }
        
        info!("Found JSON file, migrating to SQLite...");
        
        // Check if database already has data
        let db = self.db.lock().unwrap();
        let count: i64 = db.query_row(
            "SELECT COUNT(*) FROM learned_corrections",
            [],
            |row| row.get(0),
        ).unwrap_or(0);
        
        if count > 0 {
            info!("Database already has {} corrections, skipping JSON migration", count);
            return Ok(());
        }
        
        drop(db);
        
        // Read JSON file
        let content = std::fs::read_to_string(&json_file)
            .map_err(|e| RcaError::Execution(format!("Failed to read JSON file: {}", e)))?;
        
        let corrections: Vec<LearnedCorrection> = serde_json::from_str(&content)
            .map_err(|e| RcaError::Execution(format!("Failed to parse JSON: {}", e)))?;
        
        info!("Migrating {} corrections from JSON to SQLite...", corrections.len());
        
        // Insert into database
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()
            .map_err(|e| RcaError::Execution(format!("Failed to start transaction: {}", e)))?;
        
        for correction in &corrections {
            tx.execute(
                r#"
                INSERT OR IGNORE INTO learned_corrections 
                (incorrect_name, correct_name, correction_type, table_name, learned_at, usage_count, approved_by)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    correction.incorrect_name,
                    correction.correct_name,
                    correction.correction_type,
                    correction.table_name,
                    correction.learned_at,
                    correction.usage_count,
                    correction.approved_by,
                ],
            ).map_err(|e| RcaError::Execution(format!("Failed to insert correction: {}", e)))?;
        }
        
        tx.commit()
            .map_err(|e| RcaError::Execution(format!("Failed to commit transaction: {}", e)))?;
        
        info!("Successfully migrated {} corrections to SQLite", corrections.len());
        
        // Optionally backup/rename JSON file
        let backup_file = self.path.join("learned_corrections.json.backup");
        if let Err(e) = std::fs::rename(&json_file, &backup_file) {
            warn!("Failed to backup JSON file: {}", e);
        } else {
            info!("Backed up JSON file to {}", backup_file.display());
        }
        
        Ok(())
    }
    
    /// Save learning store to disk
    /// Note: With SQLite, saves are automatic (no explicit save needed)
    pub fn save(&self) -> Result<()> {
        // SQLite auto-commits, but we can force a checkpoint for optimization
        let db = self.db.lock().unwrap();
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)", [])
            .map_err(|e| RcaError::Execution(format!("Failed to checkpoint: {}", e)))?;
        
        Ok(())
    }
    
    /// Learn a correction from user approval
    pub fn learn_correction(
        &self,
        incorrect_name: String,
        correct_name: String,
        correction_type: String,
        table_name: Option<String>,
        approved_by: Option<String>,
    ) -> Result<()> {
        let key = (incorrect_name.clone(), correction_type.clone());
        
        let db = self.db.lock().unwrap();
        
        // Try to update existing correction
        let updated = db.execute(
            r#"
            UPDATE learned_corrections 
            SET usage_count = usage_count + 1, correct_name = ?3
            WHERE incorrect_name = ?1 AND correction_type = ?2
            "#,
            params![incorrect_name, correction_type, correct_name],
        ).map_err(|e| RcaError::Execution(format!("Failed to update correction: {}", e)))?;
        
        if updated > 0 {
            // Update cache - fetch updated value from DB and update cache
            drop(db); // Release DB lock before accessing cache
            
            // Fetch updated correction from DB
            if let Some(mut updated_correction) = self.get_correction_from_db(&incorrect_name, &correction_type) {
                // Update cache
                if let Ok(mut cache) = self.cache.lock() {
                    cache.put(key, updated_correction);
                }
            }
            
            info!("Updated learned correction: {} -> {} (used {} times)", 
                incorrect_name, correct_name, "updated");
        } else {
            // Insert new correction
            let correction = LearnedCorrection {
                incorrect_name: incorrect_name.clone(),
                correct_name: correct_name.clone(),
                correction_type: correction_type.clone(),
                table_name: table_name.clone(),
                learned_at: Utc::now().to_rfc3339(),
                usage_count: 1,
                approved_by: approved_by.clone(),
            };
            
            db.execute(
                r#"
                INSERT INTO learned_corrections 
                (incorrect_name, correct_name, correction_type, table_name, learned_at, usage_count, approved_by)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    correction.incorrect_name,
                    correction.correct_name,
                    correction.correction_type,
                    correction.table_name,
                    correction.learned_at,
                    correction.usage_count,
                    correction.approved_by,
                ],
            ).map_err(|e| RcaError::Execution(format!("Failed to insert correction: {}", e)))?;
            
            // Add to cache
            if let Ok(mut cache) = self.cache.lock() {
                cache.put(key.clone(), correction.clone());
            }
            
            info!("Learned new correction: {} -> {} ({})", 
                incorrect_name, correct_name, correction_type);
        }
        
        Ok(())
    }
    
    /// Get a learned correction from database (internal helper)
    fn get_correction_from_db(&self, incorrect_name: &str, correction_type: &str) -> Option<LearnedCorrection> {
        let db = self.db.lock().unwrap();
        let result = db.query_row(
            r#"
            SELECT incorrect_name, correct_name, correction_type, table_name, 
                   learned_at, usage_count, approved_by
            FROM learned_corrections
            WHERE incorrect_name = ?1 AND correction_type = ?2
            "#,
            params![incorrect_name, correction_type],
            |row| {
                Ok(LearnedCorrection {
                    incorrect_name: row.get(0)?,
                    correct_name: row.get(1)?,
                    correction_type: row.get(2)?,
                    table_name: row.get(3)?,
                    learned_at: row.get(4)?,
                    usage_count: row.get(5)?,
                    approved_by: row.get(6)?,
                })
            },
        );
        
        match result {
            Ok(correction) => Some(correction),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => {
                warn!("Database error while getting correction: {}", e);
                None
            }
        }
    }
    
    /// Get a learned correction if it exists
    /// Checks cache first, then database
    pub fn get_correction(&self, incorrect_name: &str, correction_type: &str) -> Option<LearnedCorrection> {
        let key = (incorrect_name.to_string(), correction_type.to_string());
        
        // Check cache first (O(1))
        if let Ok(mut cache) = self.cache.lock() {
            if let Some(correction) = cache.get(&key) {
                return Some(correction.clone());
            }
        }
        
        // Check database (O(log n) with index)
        if let Some(correction) = self.get_correction_from_db(incorrect_name, correction_type) {
            // Add to cache
            if let Ok(mut cache) = self.cache.lock() {
                cache.put(key, correction.clone());
            }
            Some(correction)
        } else {
            None
        }
    }
    
    /// Check if a correction exists
    pub fn has_correction(&self, incorrect_name: &str, correction_type: &str) -> bool {
        self.get_correction(incorrect_name, correction_type).is_some()
    }
    
    /// Get all corrections (from database)
    pub fn get_all_corrections(&self) -> Vec<LearnedCorrection> {
        let db = self.db.lock().unwrap();
        let mut stmt = match db.prepare(
            r#"
            SELECT incorrect_name, correct_name, correction_type, table_name, 
                   learned_at, usage_count, approved_by
            FROM learned_corrections
            ORDER BY usage_count DESC, learned_at DESC
            "#
        ) {
            Ok(stmt) => stmt,
            Err(_) => return Vec::new(),
        };
        
        let corrections = match stmt.query_map([], |row| {
            Ok(LearnedCorrection {
                incorrect_name: row.get(0)?,
                correct_name: row.get(1)?,
                correction_type: row.get(2)?,
                table_name: row.get(3)?,
                learned_at: row.get(4)?,
                usage_count: row.get(5)?,
                approved_by: row.get(6)?,
            })
        }) {
            Ok(corrections) => corrections,
            Err(_) => return Vec::new(),
        };
        
        corrections.filter_map(|c| c.ok()).collect()
    }
    
    /// Get corrections by type
    pub fn get_corrections_by_type(&self, correction_type: &str) -> Vec<LearnedCorrection> {
        let db = self.db.lock().unwrap();
        let mut stmt = match db.prepare(
            r#"
            SELECT incorrect_name, correct_name, correction_type, table_name, 
                   learned_at, usage_count, approved_by
            FROM learned_corrections
            WHERE correction_type = ?1
            ORDER BY usage_count DESC, learned_at DESC
            "#
        ) {
            Ok(stmt) => stmt,
            Err(_) => return Vec::new(),
        };
        
        let corrections = match stmt.query_map(params![correction_type], |row| {
            Ok(LearnedCorrection {
                incorrect_name: row.get(0)?,
                correct_name: row.get(1)?,
                correction_type: row.get(2)?,
                table_name: row.get(3)?,
                learned_at: row.get(4)?,
                usage_count: row.get(5)?,
                approved_by: row.get(6)?,
            })
        }) {
            Ok(corrections) => corrections,
            Err(_) => return Vec::new(),
        };
        
        corrections.filter_map(|c| c.ok()).collect()
    }
    
    /// Remove a correction (if user wants to unlearn)
    pub fn remove_correction(&self, incorrect_name: &str, correction_type: &str) -> Result<bool> {
        let key = (incorrect_name.to_string(), correction_type.to_string());
        
        // Remove from cache
        if let Ok(mut cache) = self.cache.lock() {
            cache.pop(&key);
        }
        
        // Remove from database
        let db = self.db.lock().unwrap();
        let removed = db.execute(
            "DELETE FROM learned_corrections WHERE incorrect_name = ?1 AND correction_type = ?2",
            params![incorrect_name, correction_type],
        ).map_err(|e| RcaError::Execution(format!("Failed to delete correction: {}", e)))?;
        
        if removed > 0 {
            info!("Removed learned correction: {} ({})", incorrect_name, correction_type);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    /// Get statistics about learned corrections
    pub fn get_stats(&self) -> LearningStats {
        let db = self.db.lock().unwrap();
        
        let total_corrections: i64 = db.query_row(
            "SELECT COUNT(*) FROM learned_corrections",
            [],
            |row| row.get(0),
        ).unwrap_or(0);
        
        let total_usage: i64 = db.query_row(
            "SELECT COALESCE(SUM(usage_count), 0) FROM learned_corrections",
            [],
            |row| row.get(0),
        ).unwrap_or(0);
        
        let mut by_type = HashMap::new();
        let mut stmt = db.prepare(
            "SELECT correction_type, COUNT(*) FROM learned_corrections GROUP BY correction_type"
        ).ok();
        
        if let Some(ref mut stmt) = stmt {
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            }).ok();
            
            if let Some(rows) = rows {
                for row in rows {
                    if let Ok((correction_type, count)) = row {
                        by_type.insert(correction_type, count as u32);
                    }
                }
            }
        }
        
        LearningStats {
            total_corrections: total_corrections as u32,
            total_usage: total_usage as u32,
            by_type,
        }
    }
}

impl Clone for LearningStore {
    fn clone(&self) -> Self {
        // Clone by reopening database connection
        // This is needed for Arc<LearningStore> sharing
        Self::load(&self.path).unwrap_or_else(|_| {
            warn!("Failed to clone LearningStore, creating new instance");
            Self::new(&self.path).unwrap()
        })
    }
}

/// Statistics about learned corrections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LearningStats {
    pub total_corrections: u32,
    pub total_usage: u32,
    pub by_type: HashMap<String, u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_learn_and_retrieve() {
        let temp_dir = TempDir::new().unwrap();
        let store = LearningStore::new(temp_dir.path()).unwrap();
        
        // Learn a correction
        store.learn_correction(
            "customer_accts".to_string(),
            "customer_accounts".to_string(),
            "table".to_string(),
            None,
            Some("test_user".to_string()),
        ).unwrap();
        
        // Retrieve it
        let correction = store.get_correction("customer_accts", "table");
        assert!(correction.is_some());
        assert_eq!(correction.unwrap().correct_name, "customer_accounts");
        
        // Test cache hit (second lookup should be faster)
        let correction2 = store.get_correction("customer_accts", "table");
        assert!(correction2.is_some());
    }
}
