use crate::error::{RcaError, Result};
use crate::metadata::{Metadata, Table};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    pub system_id: String,
    pub label: String,
    #[serde(default)]
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSummary {
    pub table_id: String,
    pub name: String,
    pub system: String,
    pub entity: String,
    pub path: String,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub time_column: Option<String>,
    #[serde(default)]
    pub column_names: Vec<String>,
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Clone)]
pub struct CatalogIndex {
    pub metadata_dir: PathBuf,
    pub data_dir: PathBuf,
    pub metadata: Metadata,
    pub systems: Vec<SystemInfo>,
    pub tables: Vec<TableSummary>,
    pub tables_by_system: HashMap<String, Vec<TableSummary>>,
    pub tables_by_name: HashMap<String, TableSummary>,
}

impl CatalogIndex {
    pub fn load(metadata_dir: impl AsRef<Path>, data_dir: impl AsRef<Path>) -> Result<Self> {
        let metadata_dir = metadata_dir.as_ref().to_path_buf();
        let data_dir = data_dir.as_ref().to_path_buf();

        let metadata = Metadata::load(&metadata_dir)?;

        let systems = metadata
            .business_labels
            .systems
            .iter()
            .map(|s| SystemInfo {
                system_id: s.system_id.clone(),
                label: s.label.clone(),
                aliases: s.aliases.clone(),
            })
            .collect::<Vec<_>>();

        let mut tables = Vec::new();
        for t in &metadata.tables {
            tables.push(Self::table_to_summary(t));
        }

        // If business labels didn’t include systems, still infer from tables.
        let mut seen: HashSet<String> = systems.iter().map(|s| s.system_id.clone()).collect();
        let mut systems = systems;
        for sys in metadata.tables.iter().map(|t| t.system.clone()) {
            if seen.insert(sys.clone()) {
                systems.push(SystemInfo {
                    system_id: sys.clone(),
                    label: sys.clone(),
                    aliases: vec![],
                });
            }
        }

        let mut tables_by_system: HashMap<String, Vec<TableSummary>> = HashMap::new();
        let mut tables_by_name: HashMap<String, TableSummary> = HashMap::new();
        for ts in &tables {
            tables_by_system
                .entry(ts.system.clone())
                .or_default()
                .push(ts.clone());
            tables_by_name.insert(ts.name.clone(), ts.clone());
            tables_by_name.insert(ts.table_id.clone(), ts.clone());
        }
        for v in tables_by_system.values_mut() {
            v.sort_by(|a, b| a.name.cmp(&b.name));
        }

        Ok(Self {
            metadata_dir,
            data_dir,
            metadata,
            systems,
            tables,
            tables_by_system,
            tables_by_name,
        })
    }

    fn table_to_summary(t: &Table) -> TableSummary {
        TableSummary {
            table_id: t.name.clone(),
            name: t.name.clone(),
            system: t.system.clone(),
            entity: t.entity.clone(),
            path: t.path.clone(),
            primary_key: t.primary_key.clone(),
            time_column: t.time_column.clone(),
            column_names: t.columns
                .as_ref()
                .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
                .unwrap_or_default(),
            labels: t.labels.clone().unwrap_or_default(),
        }
    }

    pub fn resolve_table(&self, table_id_or_name: &str) -> Result<TableSummary> {
        self.tables_by_name
            .get(table_id_or_name)
            .cloned()
            .ok_or_else(|| {
                RcaError::Execution(format!("Unknown table '{}'", table_id_or_name))
            })
    }
}


