//! Tests for semantic completeness gate

#[cfg(test)]
mod tests {
    use crate::semantic_completeness::*;
    use crate::metadata::{Metadata, Table, ColumnMetadata};
    use std::collections::HashMap;

    fn create_test_metadata() -> Metadata {
        // Create minimal test metadata
        let tables = vec![
            Table {
                name: "loan_master_b".to_string(),
                system: "system_b".to_string(),
                entity: "loan".to_string(),
                primary_key: vec!["loan_id".to_string()],
                path: "data/loan_master_b.csv".to_string(),
                columns: Some(vec![
                    ColumnMetadata {
                        name: "loan_id".to_string(),
                        data_type: Some("varchar".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                    ColumnMetadata {
                        name: "loan_amount".to_string(),
                        data_type: Some("decimal".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                ]),
                time_column: None,
                labels: None,
            },
            Table {
                name: "loan_disbursements_b".to_string(),
                system: "system_b".to_string(),
                entity: "disbursement".to_string(),
                primary_key: vec!["disbursement_id".to_string()],
                path: "data/loan_disbursements_b.csv".to_string(),
                columns: Some(vec![
                    ColumnMetadata {
                        name: "disbursement_id".to_string(),
                        data_type: Some("varchar".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                    ColumnMetadata {
                        name: "loan_id".to_string(),
                        data_type: Some("varchar".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                    ColumnMetadata {
                        name: "disbursement_date".to_string(),
                        data_type: Some("date".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                    ColumnMetadata {
                        name: "disbursed_amount".to_string(),
                        data_type: Some("decimal".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                ]),
                time_column: None,
                labels: None,
            },
            Table {
                name: "customer_accounts_a".to_string(),
                system: "system_a".to_string(),
                entity: "customer_account".to_string(),
                primary_key: vec!["account_id".to_string()],
                path: "data/customer_accounts_a.csv".to_string(),
                columns: Some(vec![
                    ColumnMetadata {
                        name: "account_id".to_string(),
                        data_type: Some("varchar".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                    ColumnMetadata {
                        name: "customer_id".to_string(),
                        data_type: Some("varchar".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                ]),
                time_column: None,
                labels: None,
            },
            Table {
                name: "customer_transactions_a".to_string(),
                system: "system_a".to_string(),
                entity: "transaction".to_string(),
                primary_key: vec!["transaction_id".to_string()],
                path: "data/customer_transactions_a.csv".to_string(),
                columns: Some(vec![
                    ColumnMetadata {
                        name: "transaction_id".to_string(),
                        data_type: Some("varchar".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                    ColumnMetadata {
                        name: "account_id".to_string(),
                        data_type: Some("varchar".to_string()),
                        description: None,
                        distinct_values: None,
                    },
                ]),
                time_column: None,
                labels: None,
            },
        ];

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

        // Create minimal metadata structure
        Metadata {
            entities: vec![],
            tables,
            metrics: vec![],
            business_labels: crate::metadata::BusinessLabelObject {
                systems: vec![],
                metrics: vec![],
                reconciliation_types: vec![],
            },
            rules: vec![],
            lineage: crate::metadata::LineageObject {
                edges: vec![],
            },
            time_rules: crate::metadata::TimeRules {
                rules: vec![],
            },
            identity: crate::metadata::IdentityObject {
                mappings: vec![],
            },
            exceptions: crate::metadata::ExceptionsObject {
                conditions: vec![],
            },
            tables_by_name,
            tables_by_entity,
            tables_by_system,
            rules_by_id: HashMap::new(),
            rules_by_system_metric: HashMap::new(),
            metrics_by_id: HashMap::new(),
            entities_by_id: HashMap::new(),
        }
    }

    #[test]
    fn test_sql_validator_extract_tables_simple() {
        let validator = SqlValidator;
        let sql = "SELECT * FROM loan_master_b";
        let tables = validator.extract_tables_from_sql(sql).unwrap();
        assert!(tables.contains(&"loan_master_b".to_string()));
    }

    #[test]
    fn test_sql_validator_extract_tables_with_join() {
        let validator = SqlValidator;
        let sql = "SELECT * FROM loan_master_b lm JOIN loan_disbursements_b ld ON lm.loan_id = ld.loan_id";
        let tables = validator.extract_tables_from_sql(sql).unwrap();
        assert!(tables.contains(&"loan_master_b".to_string()));
        assert!(tables.contains(&"loan_disbursements_b".to_string()));
    }

    #[test]
    fn test_sql_validator_extract_tables_with_schema() {
        let validator = SqlValidator;
        let sql = "SELECT * FROM schema.loan_master_b";
        let tables = validator.extract_tables_from_sql(sql).unwrap();
        assert!(tables.contains(&"loan_master_b".to_string()));
    }

    #[test]
    fn test_sql_validator_validate_completeness_complete() {
        let validator = SqlValidator;
        let sql = "SELECT * FROM loan_master_b JOIN loan_disbursements_b ON loan_master_b.loan_id = loan_disbursements_b.loan_id";
        let required = vec!["loan_master_b".to_string(), "loan_disbursements_b".to_string()];
        let result = validator.validate_completeness(sql, &required).unwrap();
        assert!(result.is_complete);
        assert!(result.missing_tables.is_empty());
    }

    #[test]
    fn test_sql_validator_validate_completeness_incomplete() {
        let validator = SqlValidator;
        let sql = "SELECT * FROM loan_disbursements_b";
        let required = vec!["loan_master_b".to_string(), "loan_disbursements_b".to_string()];
        let result = validator.validate_completeness(sql, &required).unwrap();
        assert!(!result.is_complete);
        assert!(result.missing_tables.contains(&"loan_master_b".to_string()));
    }

    #[test]
    fn test_entity_mapper_map_loan_entity() {
        let metadata = create_test_metadata();
        let mapper = EntityMapper::new(None, metadata);
        let entities = RequiredEntitySet {
            anchor_entities: vec!["loan".to_string()],
            attribute_entities: vec![],
            relationship_entities: vec![],
        };
        let tables = mapper.map_entities_to_tables(&entities).unwrap();
        assert!(tables.contains(&"loan_master_b".to_string()));
    }

    #[test]
    fn test_entity_mapper_map_multiple_entities() {
        let metadata = create_test_metadata();
        let mapper = EntityMapper::new(None, metadata);
        let entities = RequiredEntitySet {
            anchor_entities: vec!["loan".to_string()],
            attribute_entities: vec!["disbursement".to_string()],
            relationship_entities: vec![],
        };
        let tables = mapper.map_entities_to_tables(&entities).unwrap();
        assert!(tables.contains(&"loan_master_b".to_string()));
        assert!(tables.contains(&"loan_disbursements_b".to_string()));
    }

    #[test]
    fn test_entity_mapper_map_relationship_entity() {
        let metadata = create_test_metadata();
        let mapper = EntityMapper::new(None, metadata);
        let entities = RequiredEntitySet {
            anchor_entities: vec!["transaction".to_string()],
            attribute_entities: vec![],
            relationship_entities: vec!["customer_account".to_string()],
        };
        let tables = mapper.map_entities_to_tables(&entities).unwrap();
        assert!(tables.contains(&"customer_transactions_a".to_string()));
        assert!(tables.contains(&"customer_accounts_a".to_string()));
    }
}

