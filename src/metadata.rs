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
#[serde(untagged)]
pub enum BusinessLabelItem {
    System(SystemLabel),
    Metric(MetricLabel),
    ReconciliationType(ReconciliationTypeLabel),
}

// For backward compatibility, also support the old object format
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BusinessLabel {
    Array(Vec<BusinessLabelItem>),
    Object(BusinessLabelObject),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessLabelObject {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rule {
    pub id: String,
    pub system: String,
    pub metric: String,
    pub target_entity: String,
    pub target_grain: Vec<String>,
    pub computation: ComputationDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComputationDefinition {
    pub description: String,
    pub source_entities: Vec<String>,
    pub attributes_needed: HashMap<String, Vec<String>>,
    pub formula: String,
    pub aggregation_grain: Vec<String>,
}

impl std::hash::Hash for Rule {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.system.hash(state);
        self.metric.hash(state);
        self.target_entity.hash(state);
        self.target_grain.hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

impl std::hash::Hash for PipelineOp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            PipelineOp::Scan { table } => {
                "scan".hash(state);
                table.hash(state);
            }
            PipelineOp::Join { table, on, join_type } => {
                "join".hash(state);
                table.hash(state);
                on.hash(state);
                join_type.hash(state);
            }
            PipelineOp::Filter { expr } => {
                "filter".hash(state);
                expr.hash(state);
            }
            PipelineOp::Derive { expr, r#as } => {
                "derive".hash(state);
                expr.hash(state);
                r#as.hash(state);
            }
            PipelineOp::Group { by, agg } => {
                "group".hash(state);
                by.hash(state);
                // For HashMap, convert to sorted Vec for consistent hashing
                let mut agg_vec: Vec<_> = agg.iter().collect();
                agg_vec.sort();
                agg_vec.hash(state);
            }
            PipelineOp::Select { columns } => {
                "select".hash(state);
                columns.hash(state);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Lineage {
    Array(Vec<LineageItem>),
    Object(LineageObject),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageObject {
    pub edges: Vec<LineageEdge>,
    pub possible_joins: Vec<PossibleJoin>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum LineageItem {
    #[serde(rename = "edge")]
    Edge(LineageEdge),
    #[serde(rename = "possible_join")]
    PossibleJoin(PossibleJoin),
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

#[derive(Clone)]
pub struct Metadata {
    pub entities: Vec<Entity>,
    pub tables: Vec<Table>,
    pub metrics: Vec<Metric>,
    pub business_labels: BusinessLabelObject,
    pub rules: Vec<Rule>,
    pub lineage: LineageObject,
    pub time_rules: TimeRules,
    pub identity: Identity,
    pub exceptions: Exceptions,
    
    // Indexes for fast lookup
    pub tables_by_name: HashMap<String, Table>,
    pub tables_by_entity: HashMap<String, Vec<Table>>,
    pub tables_by_system: HashMap<String, Vec<Table>>,
    pub rules_by_id: HashMap<String, Rule>,
    pub rules_by_system_metric: HashMap<(String, String), Vec<Rule>>,
    pub metrics_by_id: HashMap<String, Metric>,
    pub entities_by_id: HashMap<String, Entity>,
}

impl Metadata {
    pub fn load(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        
        let entities: Vec<Entity> = Self::load_json(dir.join("entities.json"))?;
        let tables_obj: serde_json::Value = Self::load_json(dir.join("tables.json"))?;
        let tables: Vec<Table> = if tables_obj.get("tables").is_some() {
            serde_json::from_value(tables_obj["tables"].clone())?
        } else {
            serde_json::from_value(tables_obj)?
        };
        
        let metrics: Vec<Metric> = Self::load_json(dir.join("metrics.json"))?;
        let business_labels_raw: BusinessLabel = Self::load_json(dir.join("business_labels.json"))?;
        let business_labels = Self::normalize_business_labels(business_labels_raw)?;
        let rules: Vec<Rule> = Self::load_json(dir.join("rules.json"))?;
        let lineage_raw: Lineage = Self::load_json(dir.join("lineage.json"))?;
        let lineage = Self::normalize_lineage(lineage_raw)?;
        let time_rules: TimeRules = Self::load_json(dir.join("time.json"))?;
        let identity: Identity = Self::load_json(dir.join("identity.json"))?;
        let exceptions: Exceptions = Self::load_json(dir.join("exceptions.json"))?;
        
        // Build indexes
        let tables_by_name: HashMap<_, _> = tables.iter()
            .map(|t| (t.name.clone(), t.clone()))
            .collect();
        
        let mut tables_by_entity: HashMap<_, _> = HashMap::new();
        for table in &tables {
            tables_by_entity
                .entry(table.entity.clone())
                .or_insert_with(Vec::new)
                .push(table.clone());
        }
        
        let mut tables_by_system: HashMap<_, _> = HashMap::new();
        for table in &tables {
            tables_by_system
                .entry(table.system.clone())
                .or_insert_with(Vec::new)
                .push(table.clone());
        }
        
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
        
        let entities_by_id: HashMap<_, _> = entities.iter()
            .map(|e| (e.id.clone(), e.clone()))
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
            tables_by_entity,
            tables_by_system,
            rules_by_id,
            rules_by_system_metric,
            metrics_by_id,
            entities_by_id,
        })
    }
    
    fn load_json<T: for<'de> Deserialize<'de>>(path: PathBuf) -> Result<T> {
        let content = std::fs::read_to_string(&path)
            .map_err(|e| RcaError::Metadata(format!("Failed to read {}: {}", path.display(), e)))?;
        serde_json::from_str(&content)
            .map_err(|e| RcaError::Metadata(format!("Failed to parse {}: {}", path.display(), e)))
    }
    
    fn normalize_business_labels(labels: BusinessLabel) -> Result<BusinessLabelObject> {
        match labels {
            BusinessLabel::Object(obj) => Ok(obj),
            BusinessLabel::Array(items) => {
                let mut systems = Vec::new();
                let mut metrics = Vec::new();
                let mut recon_types = Vec::new();
                
                for item in items {
                    match item {
                        BusinessLabelItem::System(s) => systems.push(s),
                        BusinessLabelItem::Metric(m) => metrics.push(m),
                        BusinessLabelItem::ReconciliationType(r) => recon_types.push(r),
                    }
                }
                
                Ok(BusinessLabelObject {
                    systems,
                    metrics,
                    reconciliation_types: recon_types,
                })
            }
        }
    }
    
    fn normalize_lineage(lineage: Lineage) -> Result<LineageObject> {
        match lineage {
            Lineage::Object(obj) => Ok(obj),
            Lineage::Array(items) => {
                let mut edges = Vec::new();
                let mut possible_joins = Vec::new();
                
                for item in items {
                    match item {
                        LineageItem::Edge(e) => edges.push(e),
                        LineageItem::PossibleJoin(pj) => possible_joins.push(pj),
                    }
                }
                
                Ok(LineageObject { edges, possible_joins })
            }
        }
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

