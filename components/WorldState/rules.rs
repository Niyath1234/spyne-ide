//! Join Rule Registry - Authoritative join relationships

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Join rule state (approval workflow)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RuleState {
    /// Proposed (inference suggested this, not yet approved)
    Proposed,
    
    /// Approved (authoritative, can be used in queries)
    Approved,
    
    /// Blocked (explicitly disallowed)
    Blocked,
    
    /// Deprecated (was approved, now deprecated)
    Deprecated,
}

/// Join rule - authoritative join relationship
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinRule {
    /// Unique rule ID
    pub id: String,
    
    /// Left table name
    pub left_table: String,
    
    /// Left key column(s)
    pub left_key: Vec<String>,
    
    /// Right table name
    pub right_table: String,
    
    /// Right key column(s)
    pub right_key: Vec<String>,
    
    /// Join type (inner, left, etc.)
    pub join_type: String,
    
    /// Expected cardinality (1:1, 1:N, N:M)
    pub cardinality: String,
    
    /// Rule state
    pub state: RuleState,
    
    /// Confidence score (0.0-1.0) if inferred
    pub confidence: Option<f64>,
    
    /// Justification/reason for this rule
    pub justification: Option<String>,
    
    /// Timestamp when rule was created
    pub created_at: u64,
    
    /// Timestamp when rule was last updated
    pub updated_at: u64,
}

impl Hash for JoinRule {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.left_table.hash(state);
        let mut left_key = self.left_key.clone();
        left_key.sort();
        left_key.hash(state);
        self.right_table.hash(state);
        let mut right_key = self.right_key.clone();
        right_key.sort();
        right_key.hash(state);
        self.join_type.hash(state);
        self.cardinality.hash(state);
        self.state.hash(state);
    }
}

impl JoinRule {
    pub fn new(
        id: String,
        left_table: String,
        left_key: Vec<String>,
        right_table: String,
        right_key: Vec<String>,
        join_type: String,
        cardinality: String,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            id,
            left_table,
            left_key,
            right_table,
            right_key,
            join_type,
            cardinality,
            state: RuleState::Proposed,
            confidence: None,
            justification: None,
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Approve this rule
    pub fn approve(&mut self) {
        self.state = RuleState::Approved;
        self.updated_at = Self::now_timestamp();
    }
    
    /// Check if rule is approved (can be used in queries)
    pub fn is_approved(&self) -> bool {
        matches!(self.state, RuleState::Approved)
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Join Rule Registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinRuleRegistry {
    /// Rule ID → JoinRule
    rules: HashMap<String, JoinRule>,
    
    /// Table pair → Rule IDs (for fast lookup)
    table_pairs: HashMap<String, Vec<String>>,
}

impl Hash for JoinRuleRegistry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Only hash approved rules for deterministic hashing
        let mut approved_rules: Vec<_> = self.rules
            .values()
            .filter(|r| r.is_approved())
            .collect();
        approved_rules.sort_by_key(|r| &r.id);
        for rule in approved_rules {
            rule.hash(state);
        }
    }
}

impl JoinRuleRegistry {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
            table_pairs: HashMap::new(),
        }
    }

    fn table_pair_key(left: &str, right: &str) -> String {
        // Deterministic string key for JSON serialization.
        format!("{}::{}", left, right)
    }
    
    /// Register a join rule
    pub fn register_rule(&mut self, rule: JoinRule) {
        let id = rule.id.clone();
        let left = rule.left_table.clone();
        let right = rule.right_table.clone();
        
        // Store rule
        self.rules.insert(id.clone(), rule);
        
        // Index by table pair (both directions)
        self.table_pairs
            .entry(Self::table_pair_key(&left, &right))
            .or_insert_with(Vec::new)
            .push(id.clone());
        self.table_pairs
            .entry(Self::table_pair_key(&right, &left))
            .or_insert_with(Vec::new)
            .push(id);
    }
    
    /// Get rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<&JoinRule> {
        self.rules.get(rule_id)
    }
    
    /// Get approved rules between two tables
    pub fn get_approved_rules(&self, left_table: &str, right_table: &str) -> Vec<&JoinRule> {
        self.table_pairs
            .get(&Self::table_pair_key(left_table, right_table))
            .iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.rules.get(id))
            .filter(|r| r.is_approved())
            .collect()
    }
    
    /// List all approved rules
    pub fn list_approved_rules(&self) -> Vec<&JoinRule> {
        self.rules.values().filter(|r| r.is_approved()).collect()
    }
    
    /// List all rules (including proposed)
    pub fn list_all_rules(&self) -> Vec<&JoinRule> {
        self.rules.values().collect()
    }
    
    /// Remove all rules involving a table (both as left or right table)
    pub fn remove_rules_for_table(&mut self, table_name: &str) -> usize {
        let mut removed_count = 0;
        
        // Find all rule IDs that involve this table
        let rule_ids_to_remove: Vec<String> = self.rules
            .iter()
            .filter(|(_, rule)| rule.left_table == table_name || rule.right_table == table_name)
            .map(|(id, _)| id.clone())
            .collect();
        
        // Remove rules
        for rule_id in &rule_ids_to_remove {
            if let Some(rule) = self.rules.remove(rule_id) {
                // Remove from table_pairs index
                let left = rule.left_table;
                let right = rule.right_table;
                let pair1 = Self::table_pair_key(&left, &right);
                let pair2 = Self::table_pair_key(&right, &left);
                
                if let Some(ids) = self.table_pairs.get_mut(&pair1) {
                    ids.retain(|id| id != rule_id);
                    if ids.is_empty() {
                        self.table_pairs.remove(&pair1);
                    }
                }
                if let Some(ids) = self.table_pairs.get_mut(&pair2) {
                    ids.retain(|id| id != rule_id);
                    if ids.is_empty() {
                        self.table_pairs.remove(&pair2);
                    }
                }
                
                removed_count += 1;
            }
        }
        
        removed_count
    }
}

impl Default for JoinRuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter rule - business rule that applies default filters to tables
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterRule {
    /// Unique rule ID
    pub id: String,
    
    /// Table name this rule applies to
    pub table_name: String,
    
    /// Column name to filter
    pub column: String,
    
    /// Operator: "eq", "ne", "gt", "gte", "lt", "lte", "in", "like", "between"
    pub operator: String,
    
    /// Value to filter by (JSON value: string, number, array, etc.)
    pub value: serde_json::Value,

    /// Optional lookup definition for cross-table rules (e.g., NOT IN (SELECT ...)).
    /// When present, `column` is interpreted as the left-side column on `table_name`.
    /// The lookup identifies the right-side table/column.
    #[serde(default)]
    pub lookup: Option<LookupFilter>,
    
    /// Whether this rule is mandatory (always apply) or optional
    pub mandatory: bool,
    
    /// Rule state (Approved/Blocked/Deprecated)
    pub state: RuleState,
    
    /// Justification/reason for this rule
    pub justification: Option<String>,
    
    /// Natural language description of the rule (e.g., "exclude writeoffs", "only active records")
    /// This will be parsed by LLM to extract structured rule components
    pub description: Option<String>,
    
    /// Timestamp when rule was created
    pub created_at: u64,
    
    /// Timestamp when rule was last updated
    pub updated_at: u64,
}

impl FilterRule {
    pub fn new(
        id: String,
        table_name: String,
        column: String,
        operator: String,
        value: serde_json::Value,
        mandatory: bool,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            id,
            table_name,
            column,
            operator,
            value,
            lookup: None,
            mandatory,
            state: RuleState::Proposed,
            justification: None,
            description: None,
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Approve this rule
    pub fn approve(&mut self) {
        self.state = RuleState::Approved;
        self.updated_at = Self::now_timestamp();
    }
    
    /// Check if rule is approved (can be used in queries)
    pub fn is_approved(&self) -> bool {
        matches!(self.state, RuleState::Approved)
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Lookup filter used for cross-table business rules.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LookupFilter {
    /// Lookup table name (e.g., "writeoff_users")
    pub table_name: String,
    /// Lookup column name (e.g., "user_id")
    pub column: String,
}

/// Filter Rule Registry - stores business filter rules
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterRuleRegistry {
    /// Rule ID → FilterRule
    rules: HashMap<String, FilterRule>,
    
    /// Table name → Rule IDs (for fast lookup)
    table_rules: HashMap<String, Vec<String>>,
}

impl FilterRuleRegistry {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
            table_rules: HashMap::new(),
        }
    }
    
    /// Register a filter rule
    pub fn register_rule(&mut self, rule: FilterRule) {
        let id = rule.id.clone();
        let table = rule.table_name.clone();
        
        // Store rule
        self.rules.insert(id.clone(), rule);
        
        // Index by table
        self.table_rules
            .entry(table)
            .or_insert_with(Vec::new)
            .push(id);
    }
    
    /// Get rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<&FilterRule> {
        self.rules.get(rule_id)
    }
    
    /// Get approved mandatory rules for a table
    pub fn get_mandatory_rules(&self, table_name: &str) -> Vec<&FilterRule> {
        self.table_rules
            .get(table_name)
            .iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.rules.get(id))
            .filter(|r| r.is_approved() && r.mandatory)
            .collect()
    }
    
    /// Get all approved rules for a table (mandatory + optional)
    pub fn get_approved_rules(&self, table_name: &str) -> Vec<&FilterRule> {
        self.table_rules
            .get(table_name)
            .iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.rules.get(id))
            .filter(|r| matches!(r.state, RuleState::Approved))
            .collect()
    }
    
    /// Remove all rules for a table
    pub fn remove_rules_for_table(&mut self, table_name: &str) -> usize {
        let mut removed_count = 0;
        
        // Get all rule IDs for this table
        if let Some(rule_ids) = self.table_rules.remove(table_name) {
            // Remove rules
            for rule_id in rule_ids {
                if self.rules.remove(&rule_id).is_some() {
                    removed_count += 1;
                }
            }
        }
        
        removed_count
    }
    
    /// List all approved rules
    pub fn list_approved_rules(&self) -> Vec<&FilterRule> {
        self.rules.values()
            .filter(|r| matches!(r.state, RuleState::Approved))
            .collect()
    }
}

impl Default for FilterRuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Calculated Metric Rule - defines computed metrics from multiple tables/columns
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CalculatedMetricRule {
    /// Unique rule ID
    pub id: String,
    
    /// Metric name (e.g., "revenue", "profit_margin", "total_cost")
    pub metric_name: String,
    
    /// Natural language description of the calculation
    /// e.g., "revenue is amount from orders table times volume from order_items table"
    pub description: String,
    
    /// Parsed formula (JSON structure representing the calculation)
    /// Format: {"operation": "multiply", "left": {"table": "orders", "column": "amount"}, "right": {"table": "order_items", "column": "volume"}}
    pub formula: Option<serde_json::Value>,
    
    /// Output data type (e.g., "decimal", "integer", "money")
    pub output_type: String,
    
    /// Tables involved in this calculation
    pub involved_tables: Vec<String>,
    
    /// Rule state (Approved/Blocked/Deprecated)
    pub state: RuleState,
    
    /// Justification/reason for this rule
    pub justification: Option<String>,
    
    /// Timestamp when rule was created
    pub created_at: u64,
    
    /// Timestamp when rule was last updated
    pub updated_at: u64,
}

impl CalculatedMetricRule {
    pub fn new(
        id: String,
        metric_name: String,
        description: String,
        output_type: String,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            id,
            metric_name,
            description,
            formula: None,
            output_type,
            involved_tables: Vec::new(),
            state: RuleState::Proposed,
            justification: None,
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Approve this rule
    pub fn approve(&mut self) {
        self.state = RuleState::Approved;
        self.updated_at = Self::now_timestamp();
    }
    
    /// Check if rule is approved (can be used in queries)
    pub fn is_approved(&self) -> bool {
        matches!(self.state, RuleState::Approved)
    }
    
    fn now_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

/// Calculated Metric Rule Registry - stores business metric calculation rules
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CalculatedMetricRuleRegistry {
    /// Rule ID → CalculatedMetricRule
    rules: HashMap<String, CalculatedMetricRule>,
    
    /// Metric name → Rule ID (for fast lookup)
    metric_index: HashMap<String, String>,
}

impl CalculatedMetricRuleRegistry {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
            metric_index: HashMap::new(),
        }
    }
    
    /// Register a calculated metric rule
    pub fn register_rule(&mut self, rule: CalculatedMetricRule) {
        let id = rule.id.clone();
        let metric_name = rule.metric_name.clone();
        
        // Store rule
        self.rules.insert(id.clone(), rule);
        
        // Index by metric name
        self.metric_index.insert(metric_name, id);
    }
    
    /// Get rule by ID
    pub fn get_rule(&self, rule_id: &str) -> Option<&CalculatedMetricRule> {
        self.rules.get(rule_id)
    }
    
    /// Get rule by metric name
    pub fn get_rule_by_metric(&self, metric_name: &str) -> Option<&CalculatedMetricRule> {
        self.metric_index.get(metric_name)
            .and_then(|id| self.rules.get(id))
    }
    
    /// Get all approved rules
    pub fn list_approved_rules(&self) -> Vec<&CalculatedMetricRule> {
        self.rules.values()
            .filter(|r| matches!(r.state, RuleState::Approved))
            .collect()
    }
    
    /// Get rules involving a specific table
    pub fn get_rules_for_table(&self, table_name: &str) -> Vec<&CalculatedMetricRule> {
        self.rules.values()
            .filter(|r| r.is_approved() && r.involved_tables.contains(&table_name.to_string()))
            .collect()
    }
    
    /// Remove all rules involving a specific table
    pub fn remove_rules_for_table(&mut self, table_name: &str) -> usize {
        let mut removed_count = 0;
        
        // Find all rule IDs that involve this table
        let rule_ids_to_remove: Vec<String> = self.rules
            .iter()
            .filter(|(_, rule)| rule.involved_tables.contains(&table_name.to_string()))
            .map(|(id, _)| id.clone())
            .collect();
        
        // Remove rules and update metric_index
        for rule_id in &rule_ids_to_remove {
            if let Some(rule) = self.rules.remove(rule_id) {
                // Remove from metric_index
                self.metric_index.remove(&rule.metric_name);
                removed_count += 1;
            }
        }
        
        removed_count
    }
}

impl Default for CalculatedMetricRuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

