//! Ingestion Orchestrator - Main ingestion coordinator
//! Adapted for RCA-ENGINE: Works with WorldState and Polars instead of SQL engine

use crate::ingestion::{
    IngestionConnector, IngestionResult, IngestionStatus,
    SchemaInference, InferredSchema, SchemaEvolution,
    TableBuilder,
    IngestionLineage,
};
use crate::world_state::WorldState;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;
use polars::prelude::*;
use std::path::PathBuf;

/// Ingestion Orchestrator - Coordinates the entire ingestion process
pub struct IngestionOrchestrator {
    schema_inference: SchemaInference,
    table_builder: TableBuilder,
}

/// Options controlling what gets registered into WorldState
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestionOptions {
    pub register_worldstate: bool,
    pub save_worldstate: bool,
    pub collect_stats: bool,
    pub track_lineage: bool,
    pub use_batch_inserts: bool,
    pub batch_size: usize,
    pub max_parallel_threads: Option<usize>,
    #[serde(default)]
    pub skip_join_creation: bool,
}

impl Default for IngestionOptions {
    fn default() -> Self {
        Self {
            register_worldstate: true,
            save_worldstate: true,
            collect_stats: true,
            track_lineage: true,
            use_batch_inserts: true,
            batch_size: 1000,
            max_parallel_threads: None,
            skip_join_creation: false,
        }
    }
}

impl IngestionOrchestrator {
    pub fn new() -> Self {
        Self {
            schema_inference: SchemaInference::new(),
            table_builder: TableBuilder::new(),
        }
    }
    
    /// Ingest data from a connector into WorldState and write to parquet
    pub fn ingest(
        &self,
        world_state: &mut WorldState,
        data_dir: &PathBuf,
        mut connector: Box<dyn IngestionConnector>,
        table_name: Option<String>,
    ) -> Result<IngestionResult> {
        self.ingest_with_options(world_state, data_dir, connector, table_name, IngestionOptions::default())
    }
    
    /// Ingest data with explicit options
    pub fn ingest_with_options(
        &self,
        world_state: &mut WorldState,
        data_dir: &PathBuf,
        mut connector: Box<dyn IngestionConnector>,
        table_name: Option<String>,
        options: IngestionOptions,
    ) -> Result<IngestionResult> {
        let run_id = Uuid::new_v4().to_string();
        let source_id = connector.source_id().to_string();
        
        // Register source if not already registered
        if options.register_worldstate && options.track_lineage {
            if world_state.lineage_registry.get_source(&source_id).is_none() {
                IngestionLineage::register_source(
                    world_state,
                    source_id.clone(),
                    connector.source_type().to_string(),
                    connector.source_uri().map(|s| s.to_string()),
                );
                world_state.bump_version();
            }
        }
        
        // Start ingestion run
        let _run = if options.register_worldstate && options.track_lineage {
            Some(IngestionLineage::start_run(world_state, &source_id, run_id.clone()))
        } else {
            None
        };
        
        // Determine table name
        let table_name = table_name.unwrap_or_else(|| format!("table_{}", source_id));
        
        let mut all_payloads = Vec::new();
        let mut checkpoint = None;
        let mut has_more = true;
        
        // Fetch all batches
        while has_more {
            let result = connector.fetch(checkpoint)
                .context("Failed to fetch from connector")?;
            
            all_payloads.extend(result.payloads);
            checkpoint = Some(result.checkpoint);
            has_more = result.has_more;
        }
        
        if all_payloads.is_empty() {
            return Ok(IngestionResult {
                records_ingested: 0,
                tables_affected: Vec::new(),
                schema_versions: Default::default(),
                child_tables_created: Vec::new(),
                run_id: run_id.clone(),
                status: IngestionStatus::Success,
                error: None,
            });
        }
        
        // Check for contract
        let contract = {
            if options.register_worldstate {
                world_state.contract_registry.get_table_contract(&table_name).cloned()
            } else {
                None
            }
        };
        
        // Infer schema
        let inferred_schema = self.schema_inference.infer_schema(&table_name, &all_payloads);
        
        // Get existing schema
        let existing_schema = {
            if options.register_worldstate {
                world_state.schema_registry.get_table(&table_name).cloned()
            } else {
                None
            }
        };
        
        // Determine schema evolution
        let evolution = if let Some(ref contract) = contract {
            let contract_schema = self.table_builder.build_table_schema_from_contract(
                contract,
                &world_state.schema_registry,
                &all_payloads,
            )?;
            
            use crate::ingestion::schema_inference::InferredSchema;
            let contract_inferred = InferredSchema {
                table_name: contract_schema.table_name.clone(),
                columns: contract_schema.columns.iter().map(|col| {
                    crate::ingestion::schema_inference::InferredColumn {
                        name: col.canonical_name.clone(),
                        data_type: col.data_type.clone(),
                        nullable: col.nullable,
                        is_array: false,
                        sample_values: Vec::new(),
                    }
                }).collect(),
                child_tables: Vec::new(),
            };
            
            self.schema_inference.compare_schema(&contract_inferred, existing_schema.as_ref())
        } else {
            self.schema_inference.compare_schema(&inferred_schema, existing_schema.as_ref())
        };
        
        // Create or update table schema
        let tables_affected = vec![table_name.clone()];
        let mut schema_versions = std::collections::HashMap::new();
        
        match evolution {
            SchemaEvolution::NoChange if existing_schema.is_none() => {
                // New table - create schema
                let table_schema = if let Some(ref contract) = contract {
                    self.table_builder.build_table_schema_from_contract(
                        contract,
                        &world_state.schema_registry,
                        &all_payloads,
                    )?
                } else {
                    self.table_builder.build_table_schema(
                        &inferred_schema,
                        &world_state.schema_registry,
                        connector.source_uri(),
                    )
                };
                
                if options.register_worldstate {
                    self.table_builder.register_table_schema(
                        world_state,
                        table_schema,
                        contract.as_ref(),
                    )?;
                }
                
                schema_versions.insert(table_name.clone(), 1);
            }
            SchemaEvolution::AddColumns { new_version, .. } => {
                if existing_schema.is_none() {
                    // Create new table
                    let table_schema = if let Some(ref contract) = contract {
                        self.table_builder.build_table_schema_from_contract(
                            contract,
                            &world_state.schema_registry,
                            &all_payloads,
                        )?
                    } else {
                        self.table_builder.build_table_schema(
                            &inferred_schema,
                            &world_state.schema_registry,
                            connector.source_uri(),
                        )
                    };
                    
                    if options.register_worldstate {
                        self.table_builder.register_table_schema(
                            world_state,
                            table_schema,
                            contract.as_ref(),
                        )?;
                    }
                    schema_versions.insert(table_name.clone(), 1);
                } else {
                    // Apply evolution
                    if options.register_worldstate {
                        self.table_builder.apply_evolution(world_state, &table_name, &evolution)?;
                    }
                    schema_versions.insert(table_name.clone(), new_version);
                }
            }
            SchemaEvolution::NoChange => {
                let version = existing_schema.map(|s| s.version).unwrap_or(1);
                schema_versions.insert(table_name.clone(), version);
            }
            SchemaEvolution::BreakingChange { reason, .. } => {
                return Err(anyhow::anyhow!("Breaking schema change: {}", reason));
            }
        }
        
        // Write data to parquet file using Polars
        self.write_data_to_parquet(data_dir, &table_name, &all_payloads, &inferred_schema)?;
        
        // Update lineage
        if options.register_worldstate && options.track_lineage {
            let version = schema_versions.get(&table_name).copied().unwrap_or(1);
            IngestionLineage::register_table_lineage(
                world_state,
                table_name.clone(),
                &source_id,
                version,
                run_id.clone(),
            );
            IngestionLineage::complete_run(world_state, &run_id, all_payloads.len() as u64)
                .context("Failed to complete run")?;
            world_state.bump_version();
        }
        
        Ok(IngestionResult {
            records_ingested: all_payloads.len() as u64,
            tables_affected,
            schema_versions,
            child_tables_created: Vec::new(),
            run_id,
            status: IngestionStatus::Success,
            error: None,
        })
    }
    
    /// Write data to parquet file using Polars
    fn write_data_to_parquet(
        &self,
        data_dir: &PathBuf,
        table_name: &str,
        payloads: &[Value],
        _schema: &InferredSchema,
    ) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        
        // Write JSON to temporary file first, then read with Polars
        let temp_json = data_dir.join(format!("{}_temp.json", table_name));
        {
            let json_lines: Vec<String> = payloads.iter()
                .map(|p| serde_json::to_string(p).unwrap_or_default())
                .collect();
            let json_str = json_lines.join("\n");
            std::fs::write(&temp_json, json_str)
                .context("Failed to write temporary JSON file")?;
        }
        
        // Read JSON into Polars DataFrame
        let df = LazyJsonLineReader::new(&temp_json)
            .finish()
            .context("Failed to read JSON into DataFrame")?
            .collect()
            .context("Failed to collect DataFrame")?;
        
        // Write to parquet
        let output_path = data_dir.join(format!("{}.parquet", table_name));
        let mut file = std::fs::File::create(&output_path)
            .context(format!("Failed to create parquet file: {}", output_path.display()))?;
        
        ParquetWriter::new(&mut file)
            .finish(&mut df.clone())
            .context("Failed to write parquet file")?;
        
        // Clean up temporary JSON file
        let _ = std::fs::remove_file(&temp_json);
        
        Ok(())
    }
}

impl Default for IngestionOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

