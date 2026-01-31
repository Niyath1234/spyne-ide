//! Contract Registry Module - Direct WorldState Integration
//! 
//! A modular contract registration system that directly connects to WorldState,
//! implementing contract registration and schema management.

use crate::world_state::contract::{TableContract, ContractRegistry};
use crate::world_state::schema::SchemaRegistry;
use anyhow::Result;

/// Contract Registry Module - Modular component for registering contracts directly into WorldState
pub struct ContractRegistryModule {
    /// Contract registry
    contract_registry: ContractRegistry,
    
    /// Schema registry (optional, for name normalization)
    schema_registry: Option<SchemaRegistry>,
}

impl ContractRegistryModule {
    /// Create a new contract registry module
    pub fn new() -> Self {
        Self {
            contract_registry: ContractRegistry::new(),
            schema_registry: None,
        }
    }
    
    /// Create with schema registry
    pub fn with_schema_registry(mut self, schema_registry: SchemaRegistry) -> Self {
        self.schema_registry = Some(schema_registry);
        self
    }
    
    /// Register a contract in WorldState
    /// 
    /// This method:
    /// 1. Registers contract in the contract registry
    /// 2. Creates/updates table schema in WorldState based on contract
    /// 3. Registers primary keys
    pub fn register_contract_to_worldstate(
        &mut self,
        world_state: &mut crate::world_state::WorldState,
        contract: TableContract,
    ) -> Result<ContractRegistrationResult> {
        // Step 1: Register contract in registry
        self.contract_registry.register_contract(contract.clone());
        
        // Step 2: Update WorldState contract registry
        world_state.contract_registry.register_contract(contract.clone());
        
        // Step 3: Create/update table schema in WorldState
        use crate::ingestion::table_builder::TableBuilder;
        let table_builder = TableBuilder::new();
        
        let table_schema = table_builder.build_table_schema_from_contract(
            &contract,
            &world_state.schema_registry,
            &[], // No payloads available at contract registration time
        )?;
        
        // Register schema
        world_state.schema_registry.register_table(table_schema.clone());
        
        // Step 4: Register primary keys
        if !contract.primary_key.is_empty() {
            use crate::world_state::keys::{PrimaryKey, TableKeys, DedupeStrategy};
            let pk = PrimaryKey {
                columns: contract.primary_key.clone(),
                is_synthetic: false,
            };
            let mut table_keys = TableKeys::default();
            table_keys.primary_key = Some(pk);
            table_keys.dedupe_strategy = DedupeStrategy::AppendOnly;
            world_state.key_registry.register_table_keys(contract.table_name.clone(), table_keys);
        }
        
        world_state.bump_version();
        
        Ok(ContractRegistrationResult {
            table_name: contract.table_name.clone(),
            contract_registered: true,
        })
    }
    
    /// Get contract registry reference
    pub fn contract_registry(&self) -> &ContractRegistry {
        &self.contract_registry
    }
    
    /// Get mutable contract registry reference
    pub fn contract_registry_mut(&mut self) -> &mut ContractRegistry {
        &mut self.contract_registry
    }
}

/// Result of contract registration
#[derive(Clone, Debug)]
pub struct ContractRegistrationResult {
    /// Table name
    pub table_name: String,
    
    /// Whether contract was successfully registered
    pub contract_registered: bool,
}

impl Default for ContractRegistryModule {
    fn default() -> Self {
        Self::new()
    }
}

