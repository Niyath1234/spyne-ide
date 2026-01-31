use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing;

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
    #[serde(default)]
    pub time_column: Option<String>,
    pub system: String,
    pub path: String,
    #[serde(default)]
    pub columns: Option<Vec<ColumnMetadata>>,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub data_type: Option<String>,
    #[serde(default)]
    pub distinct_values: Option<Vec<serde_json::Value>>,
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
    #[serde(default)]
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComputationDefinition {
    pub description: String,
    pub source_entities: Vec<String>,
    pub attributes_needed: HashMap<String, Vec<String>>,
    pub formula: String,
    pub aggregation_grain: Vec<String>,
    #[serde(default)]
    pub filter_conditions: Option<HashMap<String, String>>,
    #[serde(default)]
    pub source_table: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
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
#[serde(untagged)]
pub enum Identity {
    Array(Vec<IdentityItem>),
    Object(IdentityObject),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityObject {
    pub canonical_keys: Vec<CanonicalKey>,
    pub key_mappings: Vec<KeyMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum IdentityItem {
    #[serde(rename = "canonical_key")]
    CanonicalKey(CanonicalKey),
    #[serde(rename = "key_mapping")]
    KeyMapping(KeyMapping),
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
#[serde(untagged)]
pub enum Exceptions {
    Array(Vec<Exception>),
    Object(ExceptionsObject),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionsObject {
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
    pub identity: IdentityObject,
    pub exceptions: ExceptionsObject,
    
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
    /// Load metadata from PostgreSQL if USE_POSTGRES=true, otherwise from JSON files
    pub async fn load_auto(dir: impl AsRef<Path>) -> Result<Self> {
        use std::env;
        
        if env::var("USE_POSTGRES").unwrap_or_default() == "true" {
            Self::load_from_db().await
        } else {
            Self::load(dir)
        }
    }
    
    /// Load metadata from PostgreSQL
    pub async fn load_from_db() -> Result<Self> {
        use std::env;
        use crate::db::{init_pool, MetadataRepository};
        
        let database_url = env::var("DATABASE_URL")
            .map_err(|_| RcaError::Metadata("DATABASE_URL not set. Set USE_POSTGRES=true and DATABASE_URL in .env".to_string()))?;
        
        let pool = init_pool(&database_url).await
            .map_err(|e| RcaError::Database(format!("Failed to connect to database: {}", e)))?;
        
        let repo = MetadataRepository::new(pool);
        repo.load_all().await
    }
    
    /// Load metadata from JSON files (original method)
    pub fn load(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        
        // REQUIRED: rules.json must be provided (business-defined)
        let rules: Vec<Rule> = Self::load_json(dir.join("rules.json"))?;
        
        // REQUIRED: exceptions.json must be provided (business-defined)
        let exceptions_raw: Exceptions = Self::load_json(dir.join("exceptions.json"))?;
        let exceptions_obj = Self::normalize_exceptions(exceptions_raw)?;
        
        // OPTIONAL: tables.json - try to load, will be needed for other auto-generation
        let tables: Vec<Table> = match Self::load_json::<serde_json::Value>(dir.join("tables.json")) {
            Ok(tables_obj) => {
                if tables_obj.get("tables").is_some() {
                    serde_json::from_value(tables_obj["tables"].clone())?
                } else {
                    serde_json::from_value(tables_obj)?
                }
            }
            Err(_) => {
                // If tables.json doesn't exist, we can't auto-generate other files
                return Err(RcaError::Metadata("tables.json is required for auto-generation".to_string()));
            }
        };
        
        // OPTIONAL: entities.json - auto-generate if missing
        let entities: Vec<Entity> = match Self::load_json(dir.join("entities.json")) {
            Ok(e) => e,
            Err(_) => {
                tracing::info!("entities.json not found, auto-generating from tables");
                Self::auto_generate_entities(&tables)?
            }
        };
        
        // OPTIONAL: metrics.json - auto-generate if missing
        let metrics: Vec<Metric> = match Self::load_json(dir.join("metrics.json")) {
            Ok(m) => m,
            Err(_) => {
                tracing::info!("metrics.json not found, auto-generating from rules");
                Self::auto_generate_metrics(&rules)?
            }
        };
        
        // OPTIONAL: business_labels.json - auto-generate if missing
        let business_labels_raw: BusinessLabel = match Self::load_json(dir.join("business_labels.json")) {
            Ok(bl) => bl,
            Err(_) => {
                tracing::info!("business_labels.json not found, auto-generating from rules and tables");
                Self::auto_generate_business_labels(&rules, &tables, &metrics)?
            }
        };
        let business_labels = Self::normalize_business_labels(business_labels_raw)?;
        
        // OPTIONAL: lineage.json - auto-generate if missing
        let lineage_raw: Lineage = match Self::load_json(dir.join("lineage.json")) {
            Ok(l) => l,
            Err(_) => {
                tracing::info!("lineage.json not found, auto-generating from tables");
                Self::auto_generate_lineage(&tables)?
            }
        };
        let lineage = Self::normalize_lineage(lineage_raw)?;
        
        // OPTIONAL: time.json - auto-generate if missing
        let time_rules: TimeRules = match Self::load_json(dir.join("time.json")) {
            Ok(tr) => tr,
            Err(_) => {
                tracing::info!("time.json not found, auto-generating from tables");
                Self::auto_generate_time_rules(&tables)?
            }
        };
        
        // OPTIONAL: identity.json - auto-generate if missing
        let identity_raw: Identity = match Self::load_json(dir.join("identity.json")) {
            Ok(id) => id,
            Err(_) => {
                tracing::info!("identity.json not found, auto-generating from entities");
                Self::auto_generate_identity(&entities)?
            }
        };
        let identity_obj = Self::normalize_identity(identity_raw)?;
        
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
            identity: identity_obj,
            exceptions: exceptions_obj,
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
    
    /// Auto-generate entities.json from tables
    fn auto_generate_entities(tables: &[Table]) -> Result<Vec<Entity>> {
        use std::collections::HashMap;
        
        let mut entities_map: HashMap<String, Entity> = HashMap::new();
        
        for table in tables {
            let entity_id = &table.entity;
            
            // Get or create entity
            let entity = entities_map.entry(entity_id.clone()).or_insert_with(|| {
                Entity {
                    id: entity_id.clone(),
                    name: entity_id.clone().chars().next().unwrap().to_uppercase().collect::<String>() 
                        + &entity_id[1..],
                    description: format!("{} entity", entity_id),
                    grain: table.primary_key.clone(),
                    attributes: Vec::new(),
                }
            });
            
            // Merge grain (union of all primary keys for this entity)
            for pk in &table.primary_key {
                if !entity.grain.contains(pk) {
                    entity.grain.push(pk.clone());
                }
            }
            
            // Collect attributes from table columns
            if let Some(ref columns) = table.columns {
                for col in columns {
                    if !entity.attributes.contains(&col.name) {
                        entity.attributes.push(col.name.clone());
                    }
                }
            }
        }
        
        Ok(entities_map.into_values().collect())
    }
    
    /// Auto-generate metrics.json from rules
    fn auto_generate_metrics(rules: &[Rule]) -> Result<Vec<Metric>> {
        use std::collections::HashMap;
        
        let mut metrics_map: HashMap<String, Metric> = HashMap::new();
        
        for rule in rules {
            let metric_id = &rule.metric;
            
            // Get or create metric
            let metric = metrics_map.entry(metric_id.clone()).or_insert_with(|| {
                Metric {
                    id: metric_id.clone(),
                    name: metric_id.clone().to_uppercase(),
                    description: format!("{} metric", metric_id),
                    grain: rule.target_grain.clone(),
                    precision: 2, // Default precision
                    null_policy: "zero".to_string(), // Default null policy
                    unit: "currency".to_string(), // Default unit
                    versions: Vec::new(),
                }
            });
            
            // Merge grain (union of all target_grain for this metric)
            for grain_col in &rule.target_grain {
                if !metric.grain.contains(grain_col) {
                    metric.grain.push(grain_col.clone());
                }
            }
        }
        
        Ok(metrics_map.into_values().collect())
    }
    
    /// Auto-generate business_labels.json from rules, tables, and metrics
    fn auto_generate_business_labels(
        rules: &[Rule],
        tables: &[Table],
        metrics: &[Metric],
    ) -> Result<BusinessLabel> {
        use std::collections::HashSet;
        
        // Extract unique systems from rules and tables
        let mut systems_set: HashSet<String> = HashSet::new();
        for rule in rules {
            systems_set.insert(rule.system.clone());
        }
        for table in tables {
            systems_set.insert(table.system.clone());
        }
        
        // Extract unique metrics from rules
        let mut metrics_set: HashSet<String> = HashSet::new();
        for rule in rules {
            metrics_set.insert(rule.metric.clone());
        }
        for metric in metrics {
            metrics_set.insert(metric.id.clone());
        }
        
        // Create system labels
        let systems: Vec<SystemLabel> = systems_set.into_iter().map(|system_id| {
            SystemLabel {
                label: system_id.clone().chars().next().unwrap().to_uppercase().collect::<String>()
                    + &system_id[1..],
                aliases: vec![system_id.clone(), system_id.to_lowercase()],
                system_id,
            }
        }).collect();
        
        // Create metric labels
        let metric_labels: Vec<MetricLabel> = metrics_set.into_iter().map(|metric_id| {
            MetricLabel {
                label: metric_id.clone().to_uppercase(),
                aliases: vec![metric_id.clone(), metric_id.to_lowercase()],
                metric_id,
            }
        }).collect();
        
        // Default reconciliation types
        let reconciliation_types = vec![
            ReconciliationTypeLabel {
                label: "reconciliation".to_string(),
                aliases: vec!["recon".to_string(), "reconcile".to_string(), "compare".to_string(), "match".to_string()],
            },
            ReconciliationTypeLabel {
                label: "as of".to_string(),
                aliases: vec!["as-of".to_string(), "asof".to_string(), "as of date".to_string(), "snapshot".to_string()],
            },
        ];
        
        Ok(BusinessLabel::Object(BusinessLabelObject {
            systems,
            metrics: metric_labels,
            reconciliation_types,
        }))
    }
    
    /// Auto-generate lineage.json from tables
    fn auto_generate_lineage(tables: &[Table]) -> Result<Lineage> {
        use std::collections::HashMap;
        
        let mut edges = Vec::new();
        
        // Group tables by entity
        let mut tables_by_entity: HashMap<String, Vec<&Table>> = HashMap::new();
        for table in tables {
            tables_by_entity
                .entry(table.entity.clone())
                .or_insert_with(Vec::new)
                .push(table);
        }
        
        // For each entity, create edges between tables with matching primary keys
        for (entity, entity_tables) in &tables_by_entity {
            if entity_tables.len() < 2 {
                continue;
            }
            
            // Find common primary keys
            let mut common_keys: Option<Vec<String>> = None;
            for table in entity_tables {
                if let Some(ref keys) = common_keys {
                    let new_keys: Vec<String> = keys.iter()
                        .filter(|k| table.primary_key.contains(k))
                        .cloned()
                        .collect();
                    common_keys = Some(new_keys);
                } else {
                    common_keys = Some(table.primary_key.clone());
                }
            }
            
            if let Some(ref keys) = common_keys {
                if !keys.is_empty() {
                    // Create edges between all pairs of tables for this entity
                    for i in 0..entity_tables.len() {
                        for j in (i + 1)..entity_tables.len() {
                            let from_table = entity_tables[i];
                            let to_table = entity_tables[j];
                            
                            let mut key_map = HashMap::new();
                            for key in keys {
                                key_map.insert(key.clone(), key.clone());
                            }
                            
                            edges.push(LineageEdge {
                                from: from_table.name.clone(),
                                to: to_table.name.clone(),
                                keys: key_map,
                                relationship: "join".to_string(),
                            });
                        }
                    }
                }
            }
        }
        
        Ok(Lineage::Object(LineageObject {
            edges,
            possible_joins: Vec::new(),
        }))
    }
    
    /// Auto-generate time.json from tables
    fn auto_generate_time_rules(tables: &[Table]) -> Result<TimeRules> {
        let mut as_of_rules = Vec::new();
        
        for table in tables {
            if let Some(ref time_col) = table.time_column {
                if !time_col.is_empty() {
                    // Check if time_column matches common patterns
                    if time_col.contains("date") || time_col.contains("time") || time_col.contains("timestamp") {
                        as_of_rules.push(AsOfRule {
                            table: table.name.clone(),
                            as_of_column: time_col.clone(),
                            default: "2025-12-31".to_string(), // Default date
                        });
                    }
                }
            }
        }
        
        Ok(TimeRules {
            as_of_rules,
            lateness_rules: Vec::new(),
        })
    }
    
    /// Auto-generate identity.json from entities
    fn auto_generate_identity(entities: &[Entity]) -> Result<Identity> {
        let canonical_keys: Vec<CanonicalKey> = entities.iter().map(|entity| {
            CanonicalKey {
                entity: entity.id.clone(),
                canonical: entity.grain.first().unwrap_or(&entity.id).clone(),
                alternates: Vec::new(),
            }
        }).collect();
        
        Ok(Identity::Object(IdentityObject {
            canonical_keys,
            key_mappings: Vec::new(),
        }))
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
    
    fn normalize_identity(identity: Identity) -> Result<IdentityObject> {
        match identity {
            Identity::Object(obj) => Ok(obj),
            Identity::Array(items) => {
                let mut canonical_keys = Vec::new();
                let mut key_mappings = Vec::new();
                
                for item in items {
                    match item {
                        IdentityItem::CanonicalKey(ck) => canonical_keys.push(ck),
                        IdentityItem::KeyMapping(km) => key_mappings.push(km),
                    }
                }
                
                Ok(IdentityObject { canonical_keys, key_mappings })
            }
        }
    }
    
    fn normalize_exceptions(exceptions: Exceptions) -> Result<ExceptionsObject> {
        match exceptions {
            Exceptions::Object(obj) => Ok(obj),
            Exceptions::Array(items) => {
                Ok(ExceptionsObject { exceptions: items })
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
        // Step 1: Try exact match first (explicit rules take precedence)
        let exact_match = self.rules_by_system_metric
            .get(&(system.to_string(), metric.to_string()))
            .cloned()
            .unwrap_or_default();
        
        if !exact_match.is_empty() {
            return exact_match;
        }
        
        // Step 2: Try case-insensitive match for explicit rules
        let system_lower = system.to_lowercase();
        let metric_lower = metric.to_lowercase();
        
        let case_insensitive_match: Vec<Rule> = self.rules
            .iter()
            .filter(|r| {
                r.system.to_lowercase() == system_lower && 
                r.metric.to_lowercase() == metric_lower
            })
            .cloned()
            .collect();
        
        if !case_insensitive_match.is_empty() {
            return case_insensitive_match;
        }
        
        // Step 3: Auto-infer rule from metadata (only for simple cases - direct column access)
        // Rules should only be needed for complex business logic (joins, transformations, etc.)
        self.auto_infer_rule(system, metric)
    }
    
    /// Auto-infer a simple rule from metadata when no explicit rule exists.
    /// This handles the common case where a metric is just a direct column in a table.
    /// Complex cases (joins, transformations) should still use explicit rules.
    fn auto_infer_rule(&self, system: &str, metric: &str) -> Vec<Rule> {
        use std::collections::HashMap;
        
        // Find tables in this system that have a column matching the metric name
        let system_tables: Vec<&Table> = self.tables
            .iter()
            .filter(|t| t.system.to_lowercase() == system.to_lowercase())
            .collect();
        
        for table in system_tables {
            // Check if table has a column matching the metric name
            let has_metric_column = table.columns.as_ref().map_or(false, |cols| {
                cols.iter().any(|c| {
                    c.name.to_lowercase() == metric.to_lowercase() ||
                    c.name.to_lowercase().replace("_", "") == metric.to_lowercase().replace("_", "")
                })
            });
            
            if has_metric_column {
                // Found a table with matching column - create auto-inferred rule
                let metric_col = table.columns.as_ref().and_then(|cols| {
                    cols.iter().find(|c| {
                        c.name.to_lowercase() == metric.to_lowercase() ||
                        c.name.to_lowercase().replace("_", "") == metric.to_lowercase().replace("_", "")
                    })
                });
                
                if let Some(col) = metric_col {
                    let rule_id = format!("{}_system_{}_metric_{}_auto", system, metric, table.name);
                    
                    let mut attributes_needed = HashMap::new();
                    attributes_needed.insert(table.entity.clone(), vec![col.name.clone()]);
                    
                    // Use table's primary key as target grain (grain is typically same as primary key)
                    let target_grain = table.primary_key.clone();
                    
                    // Add grain columns to attributes needed
                    for grain_col in &target_grain {
                        if !attributes_needed.get(&table.entity).unwrap().contains(grain_col) {
                            attributes_needed.get_mut(&table.entity).unwrap().push(grain_col.clone());
                        }
                    }
                    
                    let auto_rule = Rule {
                        id: rule_id,
                        system: system.to_string(),
                        metric: metric.to_string(),
                        target_entity: table.entity.clone(),
                        target_grain: target_grain.clone(),
                        computation: ComputationDefinition {
                            description: format!("Auto-inferred: {} column from {} table", metric, table.name),
                            source_entities: vec![table.entity.clone()],
                            attributes_needed,
                            formula: col.name.clone(),
                            aggregation_grain: target_grain,
                            filter_conditions: None,
                            source_table: Some(table.name.clone()),
                            note: Some("Auto-inferred rule - no explicit rule needed for simple column access".to_string()),
                        },
                        labels: Some(vec!["auto_inferred".to_string(), system.to_string()]),
                    };
                    
                    return vec![auto_rule];
                }
            }
        }
        
        // No matching column found - return empty (user needs to define explicit rule)
        Vec::new()
    }
    
    pub fn get_metric(&self, id: &str) -> Option<&Metric> {
        self.metrics_by_id.get(id)
    }
    
    /// Populate distinct values for columns in a table from data file
    /// Only stores distinct values if the distinct count is < 50
    pub fn populate_distinct_values(
        &mut self,
        table_name: &str,
        data_dir: impl AsRef<Path>,
    ) -> Result<()> {
        use polars::prelude::*;
        
        // Get table path first (need to clone table to avoid borrow issues)
        let table_path = {
            let table = self.tables_by_name.get(table_name)
                .ok_or_else(|| RcaError::Metadata(format!("Table not found: {}", table_name)))?;
            data_dir.as_ref().join(&table.path)
        };
        
        if !table_path.exists() {
            return Err(RcaError::Metadata(format!("Table file not found: {}", table_path.display())));
        }
        
        // Load the data file
        let df = LazyFrame::scan_parquet(&table_path, ScanArgsParquet::default())
            .and_then(|lf| lf.collect())
            .map_err(|e| RcaError::Metadata(format!("Failed to load table {}: {}", table_name, e)))?;
        
        // Get mutable reference to table
        let table = self.tables_by_name.get_mut(table_name)
            .ok_or_else(|| RcaError::Metadata(format!("Table not found: {}", table_name)))?;
        
        // Initialize columns metadata if it doesn't exist
        if table.columns.is_none() {
            let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
            table.columns = Some(
                column_names.into_iter()
                    .map(|name| ColumnMetadata {
                        name,
                        description: None,
                        data_type: None,
                        distinct_values: None,
                    })
                    .collect()
            );
        }
        
        // Update distinct values for each column
        if let Some(ref mut columns) = table.columns {
            for col_meta in columns.iter_mut() {
                if df.column(&col_meta.name).is_ok() {
                    // Get distinct values
                    let distinct_df = df.clone()
                        .lazy()
                        .select([col(&col_meta.name).unique()])
                        .collect()
                        .map_err(|e| RcaError::Metadata(format!("Failed to get distinct values for column {}: {}", col_meta.name, e)))?;
                    
                    let distinct_count = distinct_df.height();
                    
                    // Only store if distinct count < 50
                    if distinct_count < 50 {
                        let mut distinct_vals = Vec::new();
                        let col_series = distinct_df.column(&col_meta.name)
                            .map_err(|e| RcaError::Metadata(format!("Failed to get column {}: {}", col_meta.name, e)))?;
                        
                        for i in 0..distinct_count {
                            let val = match col_series.dtype() {
                                DataType::String => {
                                    if let Ok(s) = col_series.str() {
                                        s.get(i).map(|v| serde_json::Value::String(v.to_string()))
                                    } else {
                                        None
                                    }
                                }
                                DataType::Int64 => {
                                    if let Ok(n) = col_series.i64() {
                                        n.get(i).map(|v| serde_json::Value::Number(serde_json::Number::from(v)))
                                    } else {
                                        None
                                    }
                                }
                                DataType::Float64 => {
                                    if let Ok(n) = col_series.f64() {
                                        n.get(i).and_then(|v| {
                                            serde_json::Number::from_f64(v).map(serde_json::Value::Number)
                                        })
                                    } else {
                                        None
                                    }
                                }
                                DataType::Boolean => {
                                    if let Ok(b) = col_series.bool() {
                                        b.get(i).map(|v| serde_json::Value::Bool(v))
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            };
                            
                            if let Some(val) = val {
                                distinct_vals.push(val);
                            }
                        }
                        
                        col_meta.distinct_values = Some(distinct_vals);
                    }
                }
            }
        }
        
        // Also update the tables vector to keep it in sync
        if let Some(table_in_vec) = self.tables.iter_mut().find(|t| t.name == table_name) {
            *table_in_vec = table.clone();
        }
        
        Ok(())
    }
}

