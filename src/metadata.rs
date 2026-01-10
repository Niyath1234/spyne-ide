use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: String,
    pub name: String,
    pub description: String,
    pub grain: Vec<String>,
    pub attributes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub entity: String,
    pub primary_key: Vec<String>,
    pub time_column: String,
    pub system: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub id: String,
    pub name: String,
    pub description: String,
    pub grain: Vec<String>,
    pub precision: u32,
    pub null_policy: String,
    pub unit: String,
    #[serde(default)]
    pub versions: Vec<MetricVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricVersion {
    pub version: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessLabel {
    pub systems: Vec<SystemLabel>,
    pub metrics: Vec<MetricLabel>,
    pub reconciliation_types: Vec<ReconciliationTypeLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemLabel {
    pub label: String,
    pub aliases: Vec<String>,
    pub system_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricLabel {
    pub label: String,
    pub aliases: Vec<String>,
    pub metric_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationTypeLabel {
    pub label: String,
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Rule {
    pub id: String,
    pub name: String,
    pub system: String,
    pub metric: String,
    pub grain: Vec<String>,
    pub pipeline: Vec<PipelineOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "op")]
pub enum PipelineOp {
    #[serde(rename = "scan")]
    Scan { table: String },
    #[serde(rename = "join")]
    Join { table: String, on: Vec<String>, #[serde(rename = "type")] join_type: String },
    #[serde(rename = "filter")]
    Filter { expr: String },
    #[serde(rename = "derive")]
    Derive { expr: String, r#as: String },
    #[serde(rename = "group")]
    Group { by: Vec<String>, agg: HashMap<String, String> },
    #[serde(rename = "select")]
    Select { columns: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lineage {
    pub edges: Vec<LineageEdge>,
    pub possible_joins: Vec<PossibleJoin>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    pub from: String,
    pub to: String,
    pub keys: HashMap<String, String>,
    pub relationship: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PossibleJoin {
    pub tables: Vec<String>,
    pub keys: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRules {
    pub as_of_rules: Vec<AsOfRule>,
    pub lateness_rules: Vec<LatenessRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsOfRule {
    pub table: String,
    pub as_of_column: String,
    pub default: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatenessRule {
    pub table: String,
    pub max_lateness_days: u32,
    pub action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub canonical_keys: Vec<CanonicalKey>,
    pub key_mappings: Vec<KeyMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalKey {
    pub entity: String,
    pub canonical: String,
    pub alternates: Vec<KeyAlternate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyAlternate {
    pub system: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMapping {
    pub from_system: String,
    pub to_system: String,
    pub from_key: String,
    pub to_key: String,
    pub mapping_table: String,
    pub confidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exceptions {
    pub exceptions: Vec<Exception>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exception {
    pub id: String,
    pub description: String,
    pub condition: ExceptionCondition,
    pub applies_to: Vec<String>,
    #[serde(default)]
    pub override_field: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionCondition {
    pub table: String,
    pub filter: String,
}

pub struct Metadata {
    pub entities: Vec<Entity>,
    pub tables: Vec<Table>,
    pub metrics: Vec<Metric>,
    pub business_labels: BusinessLabel,
    pub rules: Vec<Rule>,
    pub lineage: Lineage,
    pub time_rules: TimeRules,
    pub identity: Identity,
    pub exceptions: Exceptions,
    
    // Indexes for fast lookup
    pub tables_by_name: HashMap<String, Table>,
    pub rules_by_id: HashMap<String, Rule>,
    pub rules_by_system_metric: HashMap<(String, String), Vec<Rule>>,
    pub metrics_by_id: HashMap<String, Metric>,
}

impl Metadata {
    pub fn load(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        
        let entities: Vec<Entity> = Self::load_json(dir.join("entities.json"))?;
        let tables: Vec<Table> = Self::load_json(dir.join("tables.json"))?;
        let metrics: Vec<Metric> = Self::load_json(dir.join("metrics.json"))?;
        let business_labels: BusinessLabel = Self::load_json(dir.join("business_labels.json"))?;
        let rules: Vec<Rule> = Self::load_json(dir.join("rules.json"))?;
        let lineage: Lineage = Self::load_json(dir.join("lineage.json"))?;
        let time_rules: TimeRules = Self::load_json(dir.join("time.json"))?;
        let identity: Identity = Self::load_json(dir.join("identity.json"))?;
        let exceptions: Exceptions = Self::load_json(dir.join("exceptions.json"))?;
        
        // Build indexes
        let tables_by_name: HashMap<_, _> = tables.iter()
            .map(|t| (t.name.clone(), t.clone()))
            .collect();
        
        let rules_by_id: HashMap<_, _> = rules.iter()
            .map(|r| (r.id.clone(), r.clone()))
            .collect();
        
        let mut rules_by_system_metric = HashMap::new();
        for rule in &rules {
            rules_by_system_metric
                .entry((rule.system.clone(), rule.metric.clone()))
                .or_insert_with(Vec::new)
                .push(rule.clone());
        }
        
        let metrics_by_id: HashMap<_, _> = metrics.iter()
            .map(|m| (m.id.clone(), m.clone()))
            .collect();
        
        Ok(Metadata {
            entities,
            tables,
            metrics,
            business_labels,
            rules,
            lineage,
            time_rules,
            identity,
            exceptions,
            tables_by_name,
            rules_by_id,
            rules_by_system_metric,
            metrics_by_id,
        })
    }
    
    fn load_json<T: for<'de> Deserialize<'de>>(path: PathBuf) -> Result<T> {
        let content = std::fs::read_to_string(&path)
            .map_err(|e| RcaError::Metadata(format!("Failed to read {}: {}", path.display(), e)))?;
        serde_json::from_str(&content)
            .map_err(|e| RcaError::Metadata(format!("Failed to parse {}: {}", path.display(), e)))
    }
    
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables_by_name.get(name)
    }
    
    pub fn get_rule(&self, id: &str) -> Option<&Rule> {
        self.rules_by_id.get(id)
    }
    
    pub fn get_rules_for_system_metric(&self, system: &str, metric: &str) -> Vec<Rule> {
        self.rules_by_system_metric
            .get(&(system.to_string(), metric.to_string()))
            .cloned()
            .unwrap_or_default()
    }
    
    pub fn get_metric(&self, id: &str) -> Option<&Metric> {
        self.metrics_by_id.get(id)
    }
}

