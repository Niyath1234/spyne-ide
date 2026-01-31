//! Schema Registry - Table schemas with versioning

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Schema version identifier
pub type SchemaVersion = u64;

/// Column information with dual-name architecture
/// 
/// The system maintains two names per column:
/// - `canonical_name`: Normalized, convention-based name used internally for joins, SQL generation, etc.
///   Examples: "id", "customer_id", "order_id"
/// - `user_facing_name`: Original or business-friendly name that users see and query with.
///   Examples: "user", "customer", "order_number"
/// 
/// This enables:
/// - Deterministic join discovery (system uses canonical names)
/// - User-friendly queries (users use business names)
/// - Schema evolution (canonical names stay stable while user-facing names can change)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Canonical/internal name (normalized, convention-based)
    /// Used for: join discovery, SQL generation, query planning
    /// Examples: "id", "customer_id", "order_id"
    pub canonical_name: String,
    
    /// User-facing/display name (original or business-friendly)
    /// Used for: UI display, natural language queries, user input
    /// Examples: "user", "customer", "order_number"
    pub user_facing_name: String,
    
    /// Synonyms (alternative user-facing names that map to this column)
    /// Examples: ["cust", "user"] for a column with canonical_name="customer_id"
    #[serde(default)]
    pub synonyms: Vec<String>,
    
    /// Data type (Arrow-compatible string representation)
    pub data_type: String,
    
    /// Is nullable
    pub nullable: bool,
    
    /// Semantic tags (e.g., "dimension/user", "fact/amount", "time/event")
    pub semantic_tags: Vec<String>,
    
    /// Optional description
    pub description: Option<String>,
    
    /// Source API endpoint that populates this column (for API-based join discovery)
    /// If two tables have columns from the same API endpoint, they can join
    #[serde(default)]
    pub source_api_endpoint: Option<String>,
    
    /// @deprecated: Legacy field for backward compatibility
    /// Always equals `canonical_name`. Use `canonical_name` or `user_facing_name` directly.
    #[serde(default = "default_name_from_canonical")]
    pub name: String,
}

// Helper for deserialization - ensures name is set from canonical_name
fn default_name_from_canonical() -> String {
    String::new()
}

impl ColumnInfo {
    /// Create a new ColumnInfo with both canonical and user-facing names
    pub fn new(canonical_name: String, user_facing_name: String) -> Self {
        let canonical = canonical_name.clone();
        Self {
            canonical_name: canonical.clone(),
            user_facing_name,
            synonyms: Vec::new(),
            data_type: String::new(),
            nullable: true,
            semantic_tags: Vec::new(),
            description: None,
            source_api_endpoint: None,
            name: canonical, // Backward compatibility
        }
    }
    
    /// Create with same name for both (backward compatibility)
    pub fn with_single_name(name: String) -> Self {
        Self {
            canonical_name: name.clone(),
            user_facing_name: name.clone(),
            synonyms: Vec::new(),
            data_type: String::new(),
            nullable: true,
            semantic_tags: Vec::new(),
            description: None,
            source_api_endpoint: None,
            name: name.clone(), // Backward compatibility
        }
    }
    
    /// Get the canonical name (for internal use)
    pub fn canonical(&self) -> &str {
        &self.canonical_name
    }
    
    /// Get the user-facing name (for display)
    pub fn user_facing(&self) -> &str {
        &self.user_facing_name
    }
    
    /// Check if a name matches this column (canonical, user-facing, or synonym)
    pub fn matches_name(&self, name: &str) -> bool {
        self.canonical_name == name
            || self.user_facing_name == name
            || self.synonyms.iter().any(|s| s == name)
    }
    
    /// Resolve a user-facing name or synonym to canonical name
    pub fn resolve_to_canonical(&self, name: &str) -> Option<&str> {
        if self.matches_name(name) {
            Some(&self.canonical_name)
        } else {
            None
        }
    }
}


impl Hash for ColumnInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash canonical name (deterministic identity)
        self.canonical_name.hash(state);
        self.data_type.hash(state);
        self.nullable.hash(state);
        // Semantic tags sorted for deterministic hashing
        let mut tags = self.semantic_tags.clone();
        tags.sort();
        tags.hash(state);
    }
}

// Note: Serialization/Deserialization handled by derive macros
// The `name` field will be set from `canonical_name` during construction
// For backward compatibility with old JSON that only has "name", we'll need to handle that in ingestion

/// Table schema with versioning
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableSchema {
    /// Schema name (None defaults to "main" for backward compatibility)
    #[serde(default)]
    pub schema_name: Option<String>,
    
    /// Table name
    pub table_name: String,
    
    /// Current schema version
    pub version: SchemaVersion,
    
    /// Columns in this table
    pub columns: Vec<ColumnInfo>,
    
    /// Child tables (for array columns that were split)
    pub child_tables: Vec<String>, // Names of child tables
    
    /// Timestamp when schema was created
    pub created_at: u64,
    
    /// Timestamp when schema was last updated
    pub updated_at: u64,
}

impl Hash for TableSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash schema name (use "main" if None for consistency)
        self.schema_name.as_ref().unwrap_or(&"main".to_string()).hash(state);
        self.table_name.hash(state);
        self.version.hash(state);
        // Hash columns deterministically
        for col in &self.columns {
            col.hash(state);
        }
        // Hash child tables
        let mut child_tables = self.child_tables.clone();
        child_tables.sort();
        child_tables.hash(state);
    }
}

impl TableSchema {
    pub fn new(table_name: String) -> Self {
        Self::new_with_schema(None, table_name)
    }
    
    /// Create a new table schema with optional schema name
    pub fn new_with_schema(schema_name: Option<String>, table_name: String) -> Self {
        let now = Self::now_timestamp();
        Self {
            schema_name,
            table_name,
            version: 1,
            columns: Vec::new(),
            child_tables: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Get the effective schema name (defaults to "main" if None)
    pub fn effective_schema_name(&self) -> String {
        self.schema_name.clone().unwrap_or_else(|| "main".to_string())
    }
    
    /// Get fully qualified table name (schema.table)
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.effective_schema_name(), self.table_name)
    }
    
    /// Add a column (creates new version)
    pub fn add_column(&mut self, column: ColumnInfo) -> SchemaVersion {
        self.columns.push(column);
        self.version += 1;
        self.updated_at = Self::now_timestamp();
        self.version
    }
    
    /// Get column by canonical name
    pub fn get_column_by_canonical(&self, canonical_name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.canonical_name == canonical_name)
    }
    
    /// Get column by user-facing name or synonym
    pub fn get_column_by_user_facing(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.matches_name(name))
    }
    
    /// Resolve a user-facing name or synonym to canonical name
    pub fn resolve_to_canonical(&self, name: &str) -> Option<String> {
        self.columns.iter()
            .find_map(|c| c.resolve_to_canonical(name))
            .map(|s| s.to_string())
    }
    
    /// Get column by name (tries canonical first, then user-facing/synonyms)
    /// @deprecated: Use get_column_by_canonical or get_column_by_user_facing
    #[deprecated(note = "Use get_column_by_canonical or get_column_by_user_facing")]
    pub fn get_column(&self, name: &str) -> Option<&ColumnInfo> {
        self.get_column_by_canonical(name)
            .or_else(|| self.get_column_by_user_facing(name))
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Schema Registry - stores all table schemas
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaRegistry {
    /// (schema_name, table_name) → TableSchema
    /// Schema name defaults to "main" if None
    tables: HashMap<(String, String), TableSchema>,
    
    /// Schema name → list of table names (for fast schema lookups)
    schema_index: HashMap<String, Vec<String>>,
}

impl Hash for SchemaRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash tables in sorted order for deterministic hashing
        let mut keys: Vec<_> = self.tables.keys().collect();
        keys.sort();
        for key in keys {
            key.hash(state);
            if let Some(schema) = self.tables.get(key) {
                schema.hash(state);
            }
        }
    }
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            schema_index: HashMap::new(),
        }
    }
    
    /// Register or update a table schema
    pub fn register_table(&mut self, schema: TableSchema) {
        let schema_name = schema.effective_schema_name();
        let table_name = schema.table_name.clone();
        let key = (schema_name.clone(), table_name.clone());
        
        // Remove old entry if it exists (in case schema changed)
        if let Some(old_schema) = self.tables.get(&key) {
            let old_schema_name = old_schema.effective_schema_name();
            if old_schema_name != schema_name {
                // Schema changed, remove from old schema index
                if let Some(tables) = self.schema_index.get_mut(&old_schema_name) {
                    tables.retain(|t| t != &table_name);
                }
            }
        }
        
        // Insert/update table
        self.tables.insert(key.clone(), schema);
        
        // Update schema index (avoid duplicates)
        let schema_tables = self.schema_index
            .entry(schema_name)
            .or_insert_with(Vec::new);
        if !schema_tables.contains(&table_name) {
            schema_tables.push(table_name);
        }
    }
    
    /// Register table with explicit schema name
    pub fn register_table_with_schema(&mut self, schema_name: Option<String>, schema: TableSchema) {
        let mut schema = schema;
        schema.schema_name = schema_name;
        self.register_table(schema);
    }
    
    /// Get table schema (searches in "main" schema if not qualified)
    pub fn get_table(&self, table_name: &str) -> Option<&TableSchema> {
        self.get_table_by_schema(None, table_name)
    }
    
    /// Get table schema by schema and table name
    pub fn get_table_by_schema(&self, schema_name: Option<&str>, table_name: &str) -> Option<&TableSchema> {
        let schema = schema_name.unwrap_or("main");
        self.tables.get(&(schema.to_string(), table_name.to_string()))
    }
    
    /// Get mutable table schema (searches in "main" schema if not qualified)
    pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut TableSchema> {
        self.get_table_mut_by_schema(None, table_name)
    }
    
    /// Get mutable table schema by schema and table name
    pub fn get_table_mut_by_schema(&mut self, schema_name: Option<&str>, table_name: &str) -> Option<&mut TableSchema> {
        let schema = schema_name.unwrap_or("main");
        self.tables.get_mut(&(schema.to_string(), table_name.to_string()))
    }
    
    /// List all table names (returns qualified names: schema.table)
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter()
            .map(|((schema, table), _)| format!("{}.{}", schema, table))
            .collect()
    }
    
    /// List tables in a specific schema
    pub fn list_tables_in_schema(&self, schema_name: &str) -> Vec<String> {
        self.schema_index
            .get(schema_name)
            .cloned()
            .unwrap_or_default()
    }
    
    /// List all schema names
    pub fn list_schemas(&self) -> Vec<String> {
        let mut schemas: Vec<String> = self.schema_index.keys().cloned().collect();
        schemas.sort();
        schemas
    }
    
    /// Check if table exists (searches in "main" schema if not qualified)
    pub fn has_table(&self, table_name: &str) -> bool {
        self.has_table_by_schema(None, table_name)
    }
    
    /// Get all tables (for serialization)
    pub(crate) fn tables(&self) -> &HashMap<(String, String), TableSchema> {
        &self.tables
    }
    
    /// Get schema index (for serialization)
    pub(crate) fn schema_index(&self) -> &HashMap<String, Vec<String>> {
        &self.schema_index
    }
    
    /// Set schema index (for deserialization)
    pub(crate) fn set_schema_index(&mut self, index: HashMap<String, Vec<String>>) {
        self.schema_index = index;
    }
    
    /// Check if table exists in specific schema
    pub fn has_table_by_schema(&self, schema_name: Option<&str>, table_name: &str) -> bool {
        let schema = schema_name.unwrap_or("main");
        self.tables.contains_key(&(schema.to_string(), table_name.to_string()))
    }
    
    /// Remove a table from the registry (searches in "main" schema if not qualified)
    pub fn remove_table(&mut self, table_name: &str) -> bool {
        self.remove_table_by_schema(None, table_name)
    }
    
    /// Remove a table from the registry by schema and table name
    pub fn remove_table_by_schema(&mut self, schema_name: Option<&str>, table_name: &str) -> bool {
        let schema = schema_name.unwrap_or("main").to_string();
        let table = table_name.to_string();
        
        if self.tables.remove(&(schema.clone(), table.clone())).is_some() {
            // Update schema index
            if let Some(tables) = self.schema_index.get_mut(&schema) {
                tables.retain(|t| t != &table);
            }
            true
        } else {
            false
        }
    }
    
    /// Migrate a table to a new schema
    pub fn migrate_table_to_schema(&mut self, old_schema: Option<&str>, table_name: &str, new_schema: Option<String>) -> Result<(), String> {
        let old_schema_name = old_schema.unwrap_or("main");
        let new_schema_name = new_schema.as_ref().map(|s| s.as_str()).unwrap_or("main");
        
        if old_schema_name == new_schema_name {
            return Ok(()); // No change needed
        }
        
        // Get the table schema
        let mut schema = self.tables.remove(&(old_schema_name.to_string(), table_name.to_string()))
            .ok_or_else(|| format!("Table {} not found in schema {}", table_name, old_schema_name))?;
        
        // Update schema name
        schema.schema_name = new_schema.clone();
        
        // Remove from old schema index
        if let Some(tables) = self.schema_index.get_mut(old_schema_name) {
            tables.retain(|t| t != table_name);
        }
        
        // Insert into new location
        let new_key = (new_schema_name.to_string(), table_name.to_string());
        if self.tables.contains_key(&new_key) {
            return Err(format!("Table {} already exists in schema {}", table_name, new_schema_name));
        }
        
        self.tables.insert(new_key.clone(), schema);
        
        // Update new schema index
        self.schema_index
            .entry(new_schema_name.to_string())
            .or_insert_with(Vec::new)
            .push(table_name.to_string());
        
        Ok(())
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

