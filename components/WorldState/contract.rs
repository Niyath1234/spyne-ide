//! Contract Registry - API endpoint to table mappings
//! 
//! Analysts upload contracts that define:
//! - API endpoint → table mappings
//! - API column → table column mappings
//! - Primary key definitions
//! - Business rules and descriptions

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Reference to a column in the data pool
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolColumnReference {
    /// Unique pool column ID (format: "{endpoint}::{column_name}")
    pub pool_column_id: String,
    
    /// Which endpoint provides this column
    pub source_endpoint: String,
    
    /// Original API column name
    pub api_column: String,
}

impl PoolColumnReference {
    pub fn new(pool_column_id: String, source_endpoint: String, api_column: String) -> Self {
        Self {
            pool_column_id,
            source_endpoint,
            api_column,
        }
    }
    
    /// Create a pool column reference from endpoint and column name
    pub fn from_endpoint_and_column(endpoint: &str, column_name: &str) -> Self {
        let pool_column_id = format!("{}::{}", endpoint, column_name);
        Self {
            pool_column_id,
            source_endpoint: endpoint.to_string(),
            api_column: column_name.to_string(),
        }
    }
}

/// Column mapping: API column name → table column name
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnMapping {
    /// API column name (as received from endpoint)
    /// If empty, this is a computed column (no API source)
    pub api_column: String,
    
    /// Table column name (as stored in database)
    pub table_column: String,
    
    /// Is this column part of primary key?
    pub is_primary_key: bool,
    
    /// Column description
    pub description: Option<String>,
    
    /// Computed column expression (SQL expression that calculates the value)
    /// If set, this column is computed from other columns, not from API
    /// Example: "CONCAT(first_name, ' ', last_name)" or "price * quantity"
    pub computed_expression: Option<String>,
    
    /// Data type for computed columns (if not inferred from expression)
    pub computed_data_type: Option<String>,
}

/// Table contract - defines how API endpoint maps to table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableContract {
    /// API endpoint name (e.g., "/api/v1/orders")
    /// DEPRECATED: For backward compatibility. Use source_columns and preferred_endpoints instead.
    /// If present, will be migrated to preferred_endpoints on load.
    #[serde(default)]
    pub endpoint: Option<String>,
    
    /// Schema name (None defaults to "main" for backward compatibility)
    #[serde(default)]
    pub schema_name: Option<String>,
    
    /// Table name
    pub table_name: String,
    
    /// References to columns in the data pool (NEW: pool-based approach)
    /// If empty, falls back to legacy endpoint-based approach
    #[serde(default)]
    pub source_columns: Vec<PoolColumnReference>,
    
    /// Preferred endpoints for this contract (suggested, but not required)
    /// Used when user doesn't specify endpoints during ingestion
    #[serde(default)]
    pub preferred_endpoints: Vec<String>,
    
    /// Column mappings: pool column → table column
    /// For legacy contracts: api_column → table_column
    /// For pool-based contracts: pool_column_id → table_column
    pub column_mappings: Vec<ColumnMapping>,
    
    /// Primary key columns (from table_column names)
    pub primary_key: Vec<String>,
    
    /// Table description
    pub description: Option<String>,
    
    /// Business rules
    #[serde(default)]
    pub business_rules: Vec<String>,
    
    /// Created by analyst
    pub created_by: Option<String>,
    
    /// Created timestamp
    #[serde(default = "TableContract::now_timestamp")]
    pub created_at: u64,
    
    /// Updated timestamp
    #[serde(default = "TableContract::now_timestamp")]
    pub updated_at: u64,
}

impl TableContract {
    /// Create a new contract (legacy: single endpoint)
    pub fn new(
        endpoint: String,
        table_name: String,
        column_mappings: Vec<ColumnMapping>,
        primary_key: Vec<String>,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            endpoint: Some(endpoint.clone()),
            table_name,
            schema_name: None,
            source_columns: Vec::new(),
            preferred_endpoints: vec![endpoint],
            column_mappings,
            primary_key,
            description: None,
            business_rules: Vec::new(),
            created_by: None,
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Create a new pool-based contract (multiple sources)
    pub fn new_pool_based(
        table_name: String,
        source_columns: Vec<PoolColumnReference>,
        column_mappings: Vec<ColumnMapping>,
        primary_key: Vec<String>,
        preferred_endpoints: Vec<String>,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            endpoint: None,
            table_name,
            schema_name: None,
            source_columns,
            preferred_endpoints,
            column_mappings,
            primary_key,
            description: None,
            business_rules: Vec::new(),
            created_by: None,
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Check if this is a legacy contract (has endpoint, no source_columns)
    pub fn is_legacy(&self) -> bool {
        self.endpoint.is_some() && self.source_columns.is_empty()
    }
    
    /// Check if this is a pool-based contract
    pub fn is_pool_based(&self) -> bool {
        !self.source_columns.is_empty()
    }
    
    /// Get the endpoint (for backward compatibility)
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
    
    /// Migrate legacy contract to pool-based format
    pub fn migrate_to_pool_based(&mut self, endpoint: &str) {
        if self.is_legacy() {
            // Convert column mappings to pool references
            self.source_columns = self.column_mappings.iter()
                .filter(|m| !m.api_column.is_empty()) // Skip computed columns
                .map(|m| PoolColumnReference::from_endpoint_and_column(endpoint, &m.api_column))
                .collect();
            
            // Set preferred endpoints
            if self.preferred_endpoints.is_empty() {
                self.preferred_endpoints = vec![endpoint.to_string()];
            }
            
            // Clear legacy endpoint
            self.endpoint = None;
        }
    }
    
    /// Get table column name for an API column (legacy) or pool column ID (pool-based)
    pub fn get_table_column(&self, api_column_or_pool_id: &str) -> Option<&str> {
        self.column_mappings
            .iter()
            .find(|m| {
                if self.is_pool_based() {
                    // For pool-based: match by pool_column_id in source_columns
                    self.source_columns.iter()
                        .any(|sc| sc.pool_column_id == api_column_or_pool_id && 
                             m.api_column == sc.api_column)
                } else {
                    // For legacy: match by api_column
                    m.api_column == api_column_or_pool_id
                }
            })
            .map(|m| m.table_column.as_str())
    }
    
    /// Get API column name or pool column ID for a table column
    pub fn get_api_column(&self, table_column: &str) -> Option<String> {
        self.column_mappings
            .iter()
            .find(|m| m.table_column == table_column)
            .map(|m| {
                if self.is_pool_based() {
                    // Return pool_column_id
                    self.source_columns.iter()
                        .find(|sc| sc.api_column == m.api_column)
                        .map(|sc| sc.pool_column_id.clone())
                        .unwrap_or_else(|| m.api_column.clone())
                } else {
                    // Return api_column
                    m.api_column.clone()
                }
            })
    }
    
    /// Get pool column reference for a table column
    pub fn get_pool_column_ref(&self, table_column: &str) -> Option<&PoolColumnReference> {
        if !self.is_pool_based() {
            return None;
        }
        
        // Find the column mapping
        let mapping = self.column_mappings.iter()
            .find(|m| m.table_column == table_column)?;
        
        // Find the corresponding pool column reference
        self.source_columns.iter()
            .find(|sc| sc.api_column == mapping.api_column)
    }
    
    /// Check if a table column is part of primary key
    pub fn is_primary_key_column(&self, table_column: &str) -> bool {
        self.primary_key.contains(&table_column.to_string())
    }
    
    /// Get current timestamp (used for serde defaults)
    pub(crate) fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Contract Registry - stores all API endpoint to table contracts
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContractRegistry {
    /// Endpoint → Contract (one endpoint can map to multiple tables)
    endpoint_contracts: HashMap<String, Vec<TableContract>>,
    
    /// Table name → Contract (for fast lookup)
    table_contracts: HashMap<String, TableContract>,
}

impl ContractRegistry {
    pub fn new() -> Self {
        Self {
            endpoint_contracts: HashMap::new(),
            table_contracts: HashMap::new(),
        }
    }
    
    /// Migrate all legacy contracts to pool-based format
    /// This should be called once during system upgrade
    pub fn migrate_all_legacy_contracts(&mut self) {
        let legacy_contracts: Vec<(String, TableContract)> = self.table_contracts.iter()
            .filter(|(_, contract)| contract.is_legacy())
            .map(|(name, contract)| (name.clone(), contract.clone()))
            .collect();
        
        let count = legacy_contracts.len();
        
        for (table_name, mut contract) in legacy_contracts {
            if let Some(endpoint) = contract.endpoint.clone() {
                eprintln!("Migrating legacy contract for table '{}' from endpoint '{}'", table_name, endpoint);
                contract.migrate_to_pool_based(&endpoint);
                
                // Re-register the migrated contract
                self.register_contract(contract);
            }
        }
        
        eprintln!("Migrated {} legacy contracts to pool-based format", count);
    }
    
    /// Register a contract (uploaded by analyst)
    pub fn register_contract(&mut self, mut contract: TableContract) {
        let table_name = contract.table_name.clone();
        
        // Migrate legacy contracts to pool-based format
        if contract.is_legacy() {
            if let Some(endpoint) = contract.endpoint.clone() {
                contract.migrate_to_pool_based(&endpoint);
            }
        }
        
        // Index by preferred endpoints (for backward compatibility, also index by legacy endpoint)
        let endpoints_to_index: Vec<String> = if !contract.preferred_endpoints.is_empty() {
            contract.preferred_endpoints.clone()
        } else if let Some(ref endpoint) = contract.endpoint {
            vec![endpoint.clone()]
        } else {
            Vec::new()
        };
        
        // Add to endpoint index (for each preferred endpoint)
        for endpoint in &endpoints_to_index {
            self.endpoint_contracts
                .entry(endpoint.clone())
                .or_insert_with(Vec::new)
                .push(contract.clone());
        }
        
        // Add to table index
        self.table_contracts.insert(table_name, contract);
    }
    
    /// Get contract for a table
    pub fn get_table_contract(&self, table_name: &str) -> Option<&TableContract> {
        self.table_contracts.get(table_name)
    }
    
    /// Get all contracts for an endpoint
    pub fn get_endpoint_contracts(&self, endpoint: &str) -> Vec<&TableContract> {
        self.endpoint_contracts
            .get(endpoint)
            .map(|contracts| contracts.iter().collect())
            .unwrap_or_default()
    }
    
    /// List all contracts (for API)
    pub fn list_all_contracts(&self) -> Vec<&TableContract> {
        self.table_contracts.values().collect()
    }
    
    /// Remove contract for a table
    pub fn remove_table_contract(&mut self, table_name: &str) -> bool {
        if let Some(contract) = self.table_contracts.remove(table_name) {
            // Remove from endpoint index (check all preferred endpoints)
            let endpoints_to_check: Vec<String> = if !contract.preferred_endpoints.is_empty() {
                contract.preferred_endpoints.clone()
            } else if let Some(ref endpoint) = contract.endpoint {
                vec![endpoint.clone()]
            } else {
                Vec::new()
            };
            
            for endpoint in endpoints_to_check {
                if let Some(contracts) = self.endpoint_contracts.get_mut(&endpoint) {
                    contracts.retain(|c| c.table_name != table_name);
                    if contracts.is_empty() {
                        self.endpoint_contracts.remove(&endpoint);
                    }
                }
            }
            true
        } else {
            false
        }
    }
    
    /// Find tables from same endpoint with matching primary keys
    /// Returns pairs of (table1, table2, matching_pk_columns)
    pub fn find_matching_tables(&self, endpoint: &str, table_name: &str) -> Vec<(String, Vec<String>)> {
        let mut matches = Vec::new();
        
        eprintln!("DEBUG find_matching_tables: Looking for matches for table '{}' from endpoint '{}'", table_name, endpoint);
        
        // Get contract for this table
        let this_contract = match self.get_table_contract(table_name) {
            Some(c) => {
                eprintln!("DEBUG: Found contract for table '{}' with PK: {:?}", table_name, c.primary_key);
                c
            },
            None => {
                eprintln!("DEBUG: No contract found for table '{}'", table_name);
                return matches;
            }
        };
        
        // Get all contracts for same endpoint
        let endpoint_contracts = self.get_endpoint_contracts(endpoint);
        eprintln!("DEBUG: Found {} contracts for endpoint '{}'", endpoint_contracts.len(), endpoint);
        
        for other_contract in endpoint_contracts {
            eprintln!("DEBUG: Checking contract for table '{}' with PK: {:?}", other_contract.table_name, other_contract.primary_key);
            
            // Skip self
            if other_contract.table_name == table_name {
                eprintln!("DEBUG: Skipping self (same table name)");
                continue;
            }
            
            // Check if primary keys match
            if this_contract.primary_key == other_contract.primary_key {
                // Exact PK match - 100% join
                eprintln!("DEBUG: ✅ EXACT PK MATCH! {} PK {:?} == {} PK {:?}", 
                    table_name, this_contract.primary_key, 
                    other_contract.table_name, other_contract.primary_key);
                matches.push((
                    other_contract.table_name.clone(),
                    this_contract.primary_key.clone(),
                ));
            } else {
                // Check for partial/composite key matches
                let common_pk: Vec<String> = this_contract.primary_key
                    .iter()
                    .filter(|pk_col| other_contract.primary_key.contains(pk_col))
                    .cloned()
                    .collect();
                
                if !common_pk.is_empty() {
                    eprintln!("DEBUG: ✅ PARTIAL PK MATCH! Common keys: {:?}", common_pk);
                    matches.push((
                        other_contract.table_name.clone(),
                        common_pk,
                    ));
                } else {
                    eprintln!("DEBUG: ❌ No PK match between {} and {}", table_name, other_contract.table_name);
                }
            }
        }
        
        eprintln!("DEBUG find_matching_tables: Returning {} matches", matches.len());
        matches
    }
    
    /// Check if two tables from same endpoint can join (matching PK)
    pub fn can_join(&self, table1: &str, table2: &str) -> Option<Vec<String>> {
        let contract1 = self.get_table_contract(table1)?;
        let contract2 = self.get_table_contract(table2)?;
        
        // Must share at least one preferred endpoint (or legacy endpoint)
        let endpoints1: Vec<&str> = if !contract1.preferred_endpoints.is_empty() {
            contract1.preferred_endpoints.iter().map(|s| s.as_str()).collect()
        } else if let Some(ref e) = contract1.endpoint {
            vec![e.as_str()]
        } else {
            return None;
        };
        
        let endpoints2: Vec<&str> = if !contract2.preferred_endpoints.is_empty() {
            contract2.preferred_endpoints.iter().map(|s| s.as_str()).collect()
        } else if let Some(ref e) = contract2.endpoint {
            vec![e.as_str()]
        } else {
            return None;
        };
        
        // Check if they share any endpoint
        let share_endpoint = endpoints1.iter().any(|e1| endpoints2.contains(e1));
        if !share_endpoint {
            return None;
        }
        
        // Check if primary keys match
        if contract1.primary_key == contract2.primary_key {
            // Exact match - can join on all PK columns
            Some(contract1.primary_key.clone())
        } else {
            // Check for partial match
            let common_pk: Vec<String> = contract1.primary_key
                .iter()
                .filter(|pk_col| contract2.primary_key.contains(pk_col))
                .cloned()
                .collect();
            
            if !common_pk.is_empty() {
                Some(common_pk)
            } else {
                None
            }
        }
    }
}

impl Default for ContractRegistry {
    fn default() -> Self {
        Self::new()
    }
}

