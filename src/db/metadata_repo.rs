//! Metadata repository for PostgreSQL operations

use crate::metadata::*;
use crate::error::{RcaError, Result};
use sqlx::PgPool;
use std::collections::HashMap;

pub struct MetadataRepository {
    pool: PgPool,
}

impl MetadataRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    
    /// Load all metadata from PostgreSQL
    pub async fn load_all(&self) -> Result<Metadata> {
        // Load all components
        let entities = self.load_entities().await?;
        let tables = self.load_tables().await?;
        let metrics = self.load_metrics().await?;
        let rules = self.load_rules().await?;
        let lineage_edges = self.load_lineage_edges().await?;
        let business_labels = self.load_business_labels().await?;
        let time_rules = self.load_time_rules().await?;
        let identity_mappings = self.load_identity_mappings().await?;
        let exceptions_list = self.load_exceptions().await?;
        
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
        
        // Create lineage object
        let lineage = LineageObject {
            edges: lineage_edges,
            possible_joins: Vec::new(),
        };
        
        // Create identity object
        let identity = IdentityObject {
            canonical_keys: identity_mappings.iter().map(|im| CanonicalKey {
                entity: im.entity_id.clone().unwrap_or_default(),
                canonical: im.canonical_key.clone().unwrap_or_default(),
                alternates: Vec::new(),
            }).collect(),
            key_mappings: Vec::new(),
        };
        
        // Create exceptions object
        let exceptions = ExceptionsObject {
            exceptions: exceptions_list,
        };
        
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
    
    async fn load_entities(&self) -> Result<Vec<Entity>> {
        let rows = sqlx::query!(
            r#"
            SELECT id, name, description, grain, attributes
            FROM entities
            ORDER BY id
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RcaError::Database(format!("Failed to load entities: {}", e)))?;
        
        Ok(rows.into_iter().map(|row| Entity {
            id: row.id,
            name: row.name,
            description: row.description,
            grain: row.grain.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
            attributes: row.attributes.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
        }).collect())
    }
    
    async fn load_tables(&self) -> Result<Vec<Table>> {
        let rows = sqlx::query!(
            r#"
            SELECT name, entity_id, primary_key, time_column, system, path, columns, labels
            FROM tables
            ORDER BY name
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RcaError::Database(format!("Failed to load tables: {}", e)))?;
        
        Ok(rows.into_iter().map(|row| Table {
            name: row.name,
            entity: row.entity_id.unwrap_or_default(),
            primary_key: row.primary_key.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
            time_column: row.time_column.unwrap_or_default(),
            system: row.system,
            path: row.path.unwrap_or_default(),
            columns: row.columns.and_then(|v| serde_json::from_value(v).ok()),
            labels: row.labels.and_then(|v| serde_json::from_value(v).ok()),
        }).collect())
    }
    
    async fn load_metrics(&self) -> Result<Vec<Metric>> {
        let rows = sqlx::query!(
            r#"
            SELECT id, name, description, grain, precision, null_policy, unit, versions
            FROM metrics
            ORDER BY id
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RcaError::Database(format!("Failed to load metrics: {}", e)))?;
        
        Ok(rows.into_iter().map(|row| Metric {
            id: row.id,
            name: row.name,
            description: row.description,
            grain: row.grain.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
            precision: row.precision.unwrap_or(2) as u32,
            null_policy: row.null_policy.unwrap_or_else(|| "zero".to_string()),
            unit: row.unit.unwrap_or_else(|| "currency".to_string()),
            versions: row.versions.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
        }).collect())
    }
    
    async fn load_rules(&self) -> Result<Vec<Rule>> {
        let rows = sqlx::query!(
            r#"
            SELECT id, system, metric_id, target_entity_id, target_grain,
                   description, formula, source_entities, aggregation_grain,
                   filter_conditions, source_table, note, labels
            FROM rules
            ORDER BY id
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RcaError::Database(format!("Failed to load rules: {}", e)))?;
        
        Ok(rows.into_iter().map(|row| {
            let attributes_needed: HashMap<String, Vec<String>> = HashMap::new();
            
            Rule {
                id: row.id,
                system: row.system,
                metric: row.metric_id.unwrap_or_default(),
                target_entity: row.target_entity_id.unwrap_or_default(),
                target_grain: row.target_grain.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
                computation: ComputationDefinition {
                    description: row.description.unwrap_or_default(),
                    source_entities: row.source_entities.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
                    attributes_needed,
                    formula: row.formula.unwrap_or_default(),
                    aggregation_grain: row.aggregation_grain.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
                    filter_conditions: row.filter_conditions.and_then(|v| serde_json::from_value(v).ok()),
                    source_table: row.source_table,
                    note: row.note,
                },
                labels: row.labels.and_then(|v| serde_json::from_value(v).ok()),
            }
        }).collect())
    }
    
    async fn load_lineage_edges(&self) -> Result<Vec<LineageEdge>> {
        let rows = sqlx::query!(
            r#"
            SELECT from_table, to_table, keys, relationship
            FROM lineage_edges
            ORDER BY id
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RcaError::Database(format!("Failed to load lineage edges: {}", e)))?;
        
        Ok(rows.into_iter().map(|row| LineageEdge {
            from: row.from_table.unwrap_or_default(),
            to: row.to_table.unwrap_or_default(),
            keys: row.keys.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
            relationship: row.relationship.unwrap_or_else(|| "join".to_string()),
        }).collect())
    }
    
    async fn load_business_labels(&self) -> Result<BusinessLabelObject> {
        let rows = sqlx::query!(
            r#"
            SELECT label_type, label, aliases, system_id, metric_id
            FROM business_labels
            ORDER BY id
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RcaError::Database(format!("Failed to load business labels: {}", e)))?;
        
        let mut systems = Vec::new();
        let mut metrics = Vec::new();
        let mut reconciliation_types = Vec::new();
        
        for row in rows {
            let aliases: Vec<String> = row.aliases
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default();
            
            match row.label_type.as_str() {
                "system" => systems.push(SystemLabel {
                    label: row.label,
                    aliases,
                    system_id: row.system_id.unwrap_or_default(),
                }),
                "metric" => metrics.push(MetricLabel {
                    label: row.label,
                    aliases,
                    metric_id: row.metric_id.unwrap_or_default(),
                }),
                "reconciliation_type" => reconciliation_types.push(ReconciliationTypeLabel {
                    label: row.label,
                    aliases,
                }),
                _ => {}
            }
        }
        
        Ok(BusinessLabelObject {
            systems,
            metrics,
            reconciliation_types,
        })
    }
    
    async fn load_time_rules(&self) -> Result<TimeRules> {
        let rows = sqlx::query!(
            r#"
            SELECT table_name, rule_type, as_of_column, default_value, max_lateness_days, action
            FROM time_rules
            ORDER BY id
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RcaError::Database(format!("Failed to load time rules: {}", e)))?;
        
        let mut as_of_rules = Vec::new();
        let mut lateness_rules = Vec::new();
        
        for row in rows {
            match row.rule_type.as_str() {
                "as_of" => as_of_rules.push(AsOfRule {
                    table: row.table_name.unwrap_or_default(),
                    as_of_column: row.as_of_column.unwrap_or_default(),
                    default: row.default_value.unwrap_or_default(),
                }),
                "lateness" => lateness_rules.push(LatenessRule {
                    table: row.table_name.unwrap_or_default(),
                    max_lateness_days: row.max_lateness_days.unwrap_or(0) as u32,
                    action: row.action.unwrap_or_default(),
                }),
                _ => {}
            }
        }
        
        Ok(TimeRules {
            as_of_rules,
            lateness_rules,
        })
    }
    
    async fn load_identity_mappings(&self) -> Result<Vec<IdentityMapping>> {
        #[derive(Debug)]
        struct IdentityMapping {
            entity_id: Option<String>,
            canonical_key: Option<String>,
        }
        
        let rows = sqlx::query_as!(
            IdentityMapping,
            r#"
            SELECT entity_id, canonical_key
            FROM identity_mappings
            ORDER BY id
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RcaError::Database(format!("Failed to load identity mappings: {}", e)))?;
        
        Ok(rows)
    }
    
    async fn load_exceptions(&self) -> Result<Vec<Exception>> {
        let rows = sqlx::query!(
            r#"
            SELECT id, description, table_name, filter_condition, applies_to, override_fields
            FROM exceptions
            ORDER BY id
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| RcaError::Database(format!("Failed to load exceptions: {}", e)))?;
        
        Ok(rows.into_iter().map(|row| Exception {
            id: row.id,
            description: row.description.unwrap_or_default(),
            condition: ExceptionCondition {
                table: row.table_name.unwrap_or_default(),
                filter: row.filter_condition.unwrap_or_default(),
            },
            applies_to: row.applies_to.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
            override_field: row.override_fields.and_then(|v| serde_json::from_value(v).ok()),
        }).collect())
    }
    
    /// Insert all metadata into PostgreSQL (for migration)
    pub async fn insert_all(&self, metadata: &Metadata) -> Result<()> {
        // Insert in order respecting foreign keys
        self.insert_entities(&metadata.entities).await?;
        self.insert_metrics(&metadata.metrics).await?;
        self.insert_tables(&metadata.tables).await?;
        self.insert_rules(&metadata.rules).await?;
        self.insert_lineage_edges(&metadata.lineage.edges).await?;
        self.insert_business_labels(&metadata.business_labels).await?;
        self.insert_time_rules(&metadata.time_rules).await?;
        self.insert_exceptions(&metadata.exceptions.exceptions).await?;
        
        Ok(())
    }
    
    async fn insert_entities(&self, entities: &[Entity]) -> Result<()> {
        for entity in entities {
            let grain = serde_json::to_value(&entity.grain)
                .map_err(|e| RcaError::Database(format!("Failed to serialize grain: {}", e)))?;
            let attributes = serde_json::to_value(&entity.attributes)
                .map_err(|e| RcaError::Database(format!("Failed to serialize attributes: {}", e)))?;
            
            sqlx::query!(
                r#"
                INSERT INTO entities (id, name, description, grain, attributes)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    grain = EXCLUDED.grain,
                    attributes = EXCLUDED.attributes,
                    updated_at = NOW()
                "#,
                entity.id,
                entity.name,
                entity.description,
                grain,
                attributes
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert entity {}: {}", entity.id, e)))?;
        }
        Ok(())
    }
    
    async fn insert_metrics(&self, metrics: &[Metric]) -> Result<()> {
        for metric in metrics {
            let grain = serde_json::to_value(&metric.grain)
                .map_err(|e| RcaError::Database(format!("Failed to serialize grain: {}", e)))?;
            let versions = serde_json::to_value(&metric.versions)
                .map_err(|e| RcaError::Database(format!("Failed to serialize versions: {}", e)))?;
            
            sqlx::query!(
                r#"
                INSERT INTO metrics (id, name, description, grain, precision, null_policy, unit, versions)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    grain = EXCLUDED.grain,
                    precision = EXCLUDED.precision,
                    null_policy = EXCLUDED.null_policy,
                    unit = EXCLUDED.unit,
                    versions = EXCLUDED.versions,
                    updated_at = NOW()
                "#,
                metric.id,
                metric.name,
                metric.description,
                grain,
                metric.precision as i32,
                metric.null_policy,
                metric.unit,
                versions
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert metric {}: {}", metric.id, e)))?;
        }
        Ok(())
    }
    
    async fn insert_tables(&self, tables: &[Table]) -> Result<()> {
        for table in tables {
            let primary_key = serde_json::to_value(&table.primary_key)
                .map_err(|e| RcaError::Database(format!("Failed to serialize primary_key: {}", e)))?;
            let columns = table.columns.as_ref()
                .map(|c| serde_json::to_value(c))
                .transpose()
                .map_err(|e| RcaError::Database(format!("Failed to serialize columns: {}", e)))?;
            let labels = table.labels.as_ref()
                .map(|l| serde_json::to_value(l))
                .transpose()
                .map_err(|e| RcaError::Database(format!("Failed to serialize labels: {}", e)))?;
            
            sqlx::query!(
                r#"
                INSERT INTO tables (name, entity_id, primary_key, time_column, system, path, columns, labels)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (name) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    primary_key = EXCLUDED.primary_key,
                    time_column = EXCLUDED.time_column,
                    system = EXCLUDED.system,
                    path = EXCLUDED.path,
                    columns = EXCLUDED.columns,
                    labels = EXCLUDED.labels,
                    updated_at = NOW()
                "#,
                table.name,
                table.entity,
                primary_key,
                table.time_column,
                table.system,
                table.path,
                columns,
                labels
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert table {}: {}", table.name, e)))?;
        }
        Ok(())
    }
    
    async fn insert_rules(&self, rules: &[Rule]) -> Result<()> {
        for rule in rules {
            let target_grain = serde_json::to_value(&rule.target_grain)
                .map_err(|e| RcaError::Database(format!("Failed to serialize target_grain: {}", e)))?;
            let source_entities = serde_json::to_value(&rule.computation.source_entities)
                .map_err(|e| RcaError::Database(format!("Failed to serialize source_entities: {}", e)))?;
            let aggregation_grain = serde_json::to_value(&rule.computation.aggregation_grain)
                .map_err(|e| RcaError::Database(format!("Failed to serialize aggregation_grain: {}", e)))?;
            let filter_conditions = rule.computation.filter_conditions.as_ref()
                .map(|fc| serde_json::to_value(fc))
                .transpose()
                .map_err(|e| RcaError::Database(format!("Failed to serialize filter_conditions: {}", e)))?;
            let labels = rule.labels.as_ref()
                .map(|l| serde_json::to_value(l))
                .transpose()
                .map_err(|e| RcaError::Database(format!("Failed to serialize labels: {}", e)))?;
            
            sqlx::query!(
                r#"
                INSERT INTO rules (id, system, metric_id, target_entity_id, target_grain,
                                 description, formula, source_entities, aggregation_grain,
                                 filter_conditions, source_table, note, labels)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (id) DO UPDATE SET
                    system = EXCLUDED.system,
                    metric_id = EXCLUDED.metric_id,
                    target_entity_id = EXCLUDED.target_entity_id,
                    target_grain = EXCLUDED.target_grain,
                    description = EXCLUDED.description,
                    formula = EXCLUDED.formula,
                    source_entities = EXCLUDED.source_entities,
                    aggregation_grain = EXCLUDED.aggregation_grain,
                    filter_conditions = EXCLUDED.filter_conditions,
                    source_table = EXCLUDED.source_table,
                    note = EXCLUDED.note,
                    labels = EXCLUDED.labels,
                    updated_at = NOW()
                "#,
                rule.id,
                rule.system,
                rule.metric,
                rule.target_entity,
                target_grain,
                rule.computation.description,
                rule.computation.formula,
                source_entities,
                aggregation_grain,
                filter_conditions,
                rule.computation.source_table,
                rule.computation.note,
                labels
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert rule {}: {}", rule.id, e)))?;
        }
        Ok(())
    }
    
    async fn insert_lineage_edges(&self, edges: &[LineageEdge]) -> Result<()> {
        // Clear existing edges first
        sqlx::query!("DELETE FROM lineage_edges")
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to clear lineage edges: {}", e)))?;
        
        for edge in edges {
            let keys = serde_json::to_value(&edge.keys)
                .map_err(|e| RcaError::Database(format!("Failed to serialize keys: {}", e)))?;
            
            sqlx::query!(
                r#"
                INSERT INTO lineage_edges (from_table, to_table, keys, relationship)
                VALUES ($1, $2, $3, $4)
                "#,
                edge.from,
                edge.to,
                keys,
                edge.relationship
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert lineage edge: {}", e)))?;
        }
        Ok(())
    }
    
    async fn insert_business_labels(&self, labels: &BusinessLabelObject) -> Result<()> {
        // Clear existing labels first
        sqlx::query!("DELETE FROM business_labels")
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to clear business labels: {}", e)))?;
        
        for system in &labels.systems {
            let aliases = serde_json::to_value(&system.aliases)
                .map_err(|e| RcaError::Database(format!("Failed to serialize aliases: {}", e)))?;
            
            sqlx::query!(
                r#"
                INSERT INTO business_labels (label_type, label, aliases, system_id)
                VALUES ('system', $1, $2, $3)
                "#,
                system.label,
                aliases,
                system.system_id
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert system label: {}", e)))?;
        }
        
        for metric in &labels.metrics {
            let aliases = serde_json::to_value(&metric.aliases)
                .map_err(|e| RcaError::Database(format!("Failed to serialize aliases: {}", e)))?;
            
            sqlx::query!(
                r#"
                INSERT INTO business_labels (label_type, label, aliases, metric_id)
                VALUES ('metric', $1, $2, $3)
                "#,
                metric.label,
                aliases,
                metric.metric_id
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert metric label: {}", e)))?;
        }
        
        for recon_type in &labels.reconciliation_types {
            let aliases = serde_json::to_value(&recon_type.aliases)
                .map_err(|e| RcaError::Database(format!("Failed to serialize aliases: {}", e)))?;
            
            sqlx::query!(
                r#"
                INSERT INTO business_labels (label_type, label, aliases)
                VALUES ('reconciliation_type', $1, $2)
                "#,
                recon_type.label,
                aliases
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert reconciliation type label: {}", e)))?;
        }
        
        Ok(())
    }
    
    async fn insert_time_rules(&self, time_rules: &TimeRules) -> Result<()> {
        // Clear existing time rules first
        sqlx::query!("DELETE FROM time_rules")
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to clear time rules: {}", e)))?;
        
        for rule in &time_rules.as_of_rules {
            sqlx::query!(
                r#"
                INSERT INTO time_rules (table_name, rule_type, as_of_column, default_value)
                VALUES ($1, 'as_of', $2, $3)
                "#,
                rule.table,
                rule.as_of_column,
                rule.default
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert as_of rule: {}", e)))?;
        }
        
        for rule in &time_rules.lateness_rules {
            sqlx::query!(
                r#"
                INSERT INTO time_rules (table_name, rule_type, max_lateness_days, action)
                VALUES ($1, 'lateness', $2, $3)
                "#,
                rule.table,
                rule.max_lateness_days as i32,
                rule.action
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert lateness rule: {}", e)))?;
        }
        
        Ok(())
    }
    
    async fn insert_exceptions(&self, exceptions: &[Exception]) -> Result<()> {
        for exception in exceptions {
            let applies_to = serde_json::to_value(&exception.applies_to)
                .map_err(|e| RcaError::Database(format!("Failed to serialize applies_to: {}", e)))?;
            let override_fields = exception.override_field.as_ref()
                .map(|of| serde_json::to_value(of))
                .transpose()
                .map_err(|e| RcaError::Database(format!("Failed to serialize override_fields: {}", e)))?;
            
            sqlx::query!(
                r#"
                INSERT INTO exceptions (id, description, table_name, filter_condition, applies_to, override_fields)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO UPDATE SET
                    description = EXCLUDED.description,
                    table_name = EXCLUDED.table_name,
                    filter_condition = EXCLUDED.filter_condition,
                    applies_to = EXCLUDED.applies_to,
                    override_fields = EXCLUDED.override_fields
                "#,
                exception.id,
                exception.description,
                exception.condition.table,
                exception.condition.filter,
                applies_to,
                override_fields
            )
            .execute(&self.pool)
            .await
            .map_err(|e| RcaError::Database(format!("Failed to insert exception {}: {}", exception.id, e)))?;
        }
        Ok(())
    }
}

