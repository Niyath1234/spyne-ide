//! Table Builder - Creates/updates tables from inferred schemas
//! Adapted for RCA-ENGINE: Works with WorldState and metadata instead of SQL engine

use crate::world_state::schema::{TableSchema, ColumnInfo, SchemaRegistry};
use crate::world_state::keys::{TableKeys, PrimaryKey, DedupeStrategy};
use crate::ingestion::schema_inference::{InferredSchema, InferredColumn, SchemaEvolution};
use anyhow::Result;

/// Table Builder - Converts inferred schemas to actual table schemas in WorldState
pub struct TableBuilder {
    pub auto_infer_keys: bool,
    pub default_dedupe: DedupeStrategy,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            auto_infer_keys: true,
            default_dedupe: DedupeStrategy::AppendOnly,
        }
    }
    
    /// Build table schema from contract (contract-first approach)
    pub fn build_table_schema_from_contract(
        &self,
        contract: &crate::world_state::contract::TableContract,
        _schema_registry: &SchemaRegistry,
        payloads: &[serde_json::Value],
    ) -> Result<TableSchema> {
        let mut schema = TableSchema::new_with_schema(
            contract.schema_name.clone(),
            contract.table_name.clone()
        );
        
        for mapping in &contract.column_mappings {
            if mapping.computed_expression.is_some() {
                let data_type = mapping.computed_data_type.clone().unwrap_or_else(|| "VARCHAR".to_string());
                let table_column_name = mapping.table_column.clone();
                
                let canonical_name = table_column_name.clone(); // Simplified for RCA-ENGINE
                
                let col = ColumnInfo {
                    canonical_name: canonical_name.clone(),
                    user_facing_name: table_column_name.clone(),
                    synonyms: Vec::new(),
                    data_type,
                    nullable: true,
                    semantic_tags: vec!["computed".to_string()],
                    description: mapping.description.clone().or_else(|| {
                        Some(format!("Computed: {}", mapping.computed_expression.as_ref().unwrap()))
                    }),
                    source_api_endpoint: None,
                    name: canonical_name.clone(),
                };
                schema.add_column(col);
                continue;
            }
            
            if mapping.api_column.is_empty() {
                continue;
            }
            
            let (data_type, nullable, source_endpoint) = if contract.is_pool_based() {
                if let Some(pool_ref) = contract.source_columns.iter()
                    .find(|sc| sc.api_column == mapping.api_column) {
                    let inferred_type = if payloads.is_empty() {
                        "VARCHAR".to_string()
                    } else {
                        self.infer_column_type_from_payloads(&mapping.api_column, payloads)
                    };
                    let inferred_nullable = if payloads.is_empty() {
                        true
                    } else {
                        self.infer_nullable_from_payloads(&mapping.api_column, payloads)
                    };
                    (inferred_type, inferred_nullable, Some(pool_ref.source_endpoint.clone()))
                } else {
                    let inferred_type = if payloads.is_empty() {
                        "VARCHAR".to_string()
                    } else {
                        self.infer_column_type_from_payloads(&mapping.api_column, payloads)
                    };
                    let inferred_nullable = if payloads.is_empty() {
                        true
                    } else {
                        self.infer_nullable_from_payloads(&mapping.api_column, payloads)
                    };
                    (inferred_type, inferred_nullable, None)
                }
            } else {
                let inferred_type = if payloads.is_empty() {
                    "VARCHAR".to_string()
                } else {
                    self.infer_column_type_from_payloads(&mapping.api_column, payloads)
                };
                let inferred_nullable = if payloads.is_empty() {
                    true
                } else {
                    self.infer_nullable_from_payloads(&mapping.api_column, payloads)
                };
                let endpoint = contract.endpoint().map(|s| s.to_string());
                (inferred_type, inferred_nullable, endpoint)
            };
            
            let table_column_name = mapping.table_column.clone();
            let canonical_name = table_column_name.clone(); // Simplified
            
            let col = ColumnInfo {
                canonical_name: canonical_name.clone(),
                user_facing_name: table_column_name.clone(),
                synonyms: Vec::new(),
                data_type,
                nullable,
                semantic_tags: Vec::new(),
                description: mapping.description.clone(),
                source_api_endpoint: source_endpoint,
                name: canonical_name.clone(),
            };
            schema.add_column(col);
        }
        
        Ok(schema)
    }
    
    fn infer_column_type_from_payloads(&self, api_column: &str, payloads: &[serde_json::Value]) -> String {
        use serde_json::Value;
        
        for payload in payloads {
            if let Some(value) = self.get_value_from_payload(payload, api_column) {
                match value {
                    Value::Null => continue,
                    Value::Bool(_) => return "BOOLEAN".to_string(),
                    Value::Number(n) => {
                        if n.is_f64() {
                            return "FLOAT64".to_string();
                        } else {
                            return "INT64".to_string();
                        }
                    }
                    Value::String(_) => return "VARCHAR".to_string(),
                    Value::Array(_) => return "ARRAY".to_string(),
                    Value::Object(_) => return "JSON".to_string(),
                }
            }
        }
        
        "VARCHAR".to_string()
    }
    
    fn infer_nullable_from_payloads(&self, api_column: &str, payloads: &[serde_json::Value]) -> bool {
        for payload in payloads {
            if let Some(value) = self.get_value_from_payload(payload, api_column) {
                if matches!(value, serde_json::Value::Null) {
                    return true;
                }
            }
        }
        false
    }
    
    fn get_value_from_payload<'a>(&self, payload: &'a serde_json::Value, api_column: &str) -> Option<&'a serde_json::Value> {
        let mut current = payload;
        for part in api_column.split('.') {
            match current {
                serde_json::Value::Object(map) => {
                    current = map.get(part)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }
    
    pub fn build_table_schema(
        &self,
        inferred: &InferredSchema,
        schema_registry: &SchemaRegistry,
        source_api_endpoint: Option<&str>,
    ) -> TableSchema {
        self.build_table_schema_with_schema(inferred, schema_registry, source_api_endpoint, None)
    }
    
    pub fn build_table_schema_with_schema(
        &self,
        inferred: &InferredSchema,
        _schema_registry: &SchemaRegistry,
        source_api_endpoint: Option<&str>,
        schema_name: Option<String>,
    ) -> TableSchema {
        let mut schema = TableSchema::new_with_schema(schema_name, inferred.table_name.clone());
        
        for inferred_col in &inferred.columns {
            let user_facing_name = inferred_col.name.clone();
            let canonical_name = user_facing_name.clone(); // Simplified for RCA-ENGINE
            
            let col = ColumnInfo {
                canonical_name: canonical_name.clone(),
                user_facing_name: user_facing_name.clone(),
                synonyms: Vec::new(),
                data_type: inferred_col.data_type.clone(),
                nullable: inferred_col.nullable,
                semantic_tags: self.infer_semantic_tags(inferred_col),
                description: None,
                source_api_endpoint: source_api_endpoint.map(|s| s.to_string()),
                name: canonical_name.clone(),
            };
            schema.add_column(col);
        }
        
        for child_table in &inferred.child_tables {
            schema.child_tables.push(child_table.table_name.clone());
        }
        
        schema
    }
    
    fn infer_semantic_tags(&self, col: &InferredColumn) -> Vec<String> {
        let mut tags = Vec::new();
        let name_lower = col.name.to_lowercase();
        
        if name_lower.contains("time") || name_lower.contains("date") || name_lower.contains("timestamp") {
            tags.push("time/event".to_string());
        }
        
        if name_lower.ends_with("_id") || name_lower == "id" {
            tags.push("key/natural".to_string());
        }
        
        if name_lower.contains("amount") || name_lower.contains("price") || name_lower.contains("cost") {
            tags.push("fact/amount".to_string());
        }
        
        if name_lower.contains("user") || name_lower.contains("customer") {
            tags.push("dimension/user".to_string());
        }
        
        tags
    }
    
    pub fn infer_primary_key(&self, schema: &TableSchema) -> Option<PrimaryKey> {
        if !self.auto_infer_keys {
            return None;
        }
        
        for col in &schema.columns {
            let canonical_lower = col.canonical_name.to_lowercase();
            
            if canonical_lower == "id" {
                return Some(PrimaryKey {
                    columns: vec![col.canonical_name.clone()],
                    is_synthetic: false,
                });
            }
            
            if canonical_lower == format!("{}_id", schema.table_name.to_lowercase()) {
                return Some(PrimaryKey {
                    columns: vec![col.canonical_name.clone()],
                    is_synthetic: false,
                });
            }
        }
        
        None
    }
    
    /// Register table schema in WorldState (RCA-ENGINE specific)
    pub fn register_table_schema(
        &self,
        world_state: &mut crate::world_state::WorldState,
        schema: TableSchema,
        contract: Option<&crate::world_state::contract::TableContract>,
    ) -> Result<()> {
        world_state.schema_registry.register_table(schema.clone());
        
        if let Some(contract) = contract {
            if !contract.primary_key.is_empty() {
                let pk = PrimaryKey {
                    columns: contract.primary_key.clone(),
                    is_synthetic: false,
                };
                let mut table_keys = TableKeys::default();
                table_keys.primary_key = Some(pk);
                table_keys.dedupe_strategy = self.default_dedupe.clone();
                world_state.key_registry.register_table_keys(contract.table_name.clone(), table_keys);
            }
        } else if let Some(pk) = self.infer_primary_key(&schema) {
            let mut table_keys = TableKeys::default();
            table_keys.primary_key = Some(pk);
            table_keys.dedupe_strategy = self.default_dedupe.clone();
            world_state.key_registry.register_table_keys(schema.table_name.clone(), table_keys);
        }
        
        world_state.bump_version();
        Ok(())
    }
    
    pub fn apply_evolution(
        &self,
        world_state: &mut crate::world_state::WorldState,
        table_name: &str,
        evolution: &SchemaEvolution,
    ) -> Result<()> {
        match evolution {
            SchemaEvolution::NoChange => {}
            SchemaEvolution::AddColumns { columns, .. } => {
                if let Some(table_schema) = world_state.schema_registry.get_table_mut(table_name) {
                    for col in columns {
                        table_schema.add_column(col.clone());
                    }
                }
                world_state.bump_version();
            }
            SchemaEvolution::BreakingChange { reason, .. } => {
                return Err(anyhow::anyhow!(
                    "Breaking schema change detected for {}: {}",
                    table_name, reason
                ));
            }
        }
        
        Ok(())
    }
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

