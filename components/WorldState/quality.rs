//! Data Quality - ingestion-time validation rules + stored reports
//!
//! MVP goal:
//! - NotNull and AllowedValues rules
//! - Evaluate rules on ingestion payloads (JSON records)
//! - Persist reports in WorldState for auditability

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::rules::RuleState;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataQualitySeverity {
    Warn,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataQualityRuleKind {
    NotNull { column: String },
    AllowedValues { column: String, values: Vec<serde_json::Value> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataQualityRule {
    pub id: String,
    pub table_name: String,
    pub kind: DataQualityRuleKind,
    pub severity: DataQualitySeverity,
    /// If true and severity=Error, ingestion should fail when violations exist.
    pub enforce: bool,
    pub state: RuleState,
    pub justification: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

impl DataQualityRule {
    pub fn new(
        id: String,
        table_name: String,
        kind: DataQualityRuleKind,
        severity: DataQualitySeverity,
        enforce: bool,
    ) -> Self {
        let now = Self::now_timestamp();
        Self {
            id,
            table_name,
            kind,
            severity,
            enforce,
            state: RuleState::Proposed,
            justification: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn approve(&mut self) {
        self.state = RuleState::Approved;
        self.updated_at = Self::now_timestamp();
    }

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataQualityRuleRegistry {
    rules: HashMap<String, DataQualityRule>,
    table_rules: HashMap<String, Vec<String>>,
}

impl DataQualityRuleRegistry {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
            table_rules: HashMap::new(),
        }
    }

    pub fn register_rule(&mut self, rule: DataQualityRule) {
        let id = rule.id.clone();
        let table = rule.table_name.clone();
        self.rules.insert(id.clone(), rule);
        self.table_rules.entry(table).or_insert_with(Vec::new).push(id);
    }

    pub fn get_rule(&self, rule_id: &str) -> Option<&DataQualityRule> {
        self.rules.get(rule_id)
    }

    pub fn list_approved_rules_for_table(&self, table_name: &str) -> Vec<&DataQualityRule> {
        self.table_rules
            .get(table_name)
            .iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.rules.get(id))
            .filter(|r| r.is_approved())
            .collect()
    }

    pub fn list_all_rules(&self) -> Vec<&DataQualityRule> {
        self.rules.values().collect()
    }
}

impl Default for DataQualityRuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataQualityRuleViolationSummary {
    pub rule_id: String,
    pub severity: DataQualitySeverity,
    pub failed_rows: u64,
    /// Small sample of failing values (stringified) for quick debugging.
    pub sample_values: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataQualityReport {
    pub report_id: String,
    pub table_name: String,
    pub run_id: String,
    pub total_rows: u64,
    pub violations: Vec<DataQualityRuleViolationSummary>,
    pub created_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataQualityReportRegistry {
    /// Table name -> list of reports (append-only, keep last N)
    reports: HashMap<String, Vec<DataQualityReport>>,
    /// Max reports kept per table
    max_reports_per_table: usize,
}

impl DataQualityReportRegistry {
    pub fn new() -> Self {
        Self {
            reports: HashMap::new(),
            max_reports_per_table: 50,
        }
    }

    pub fn add_report(&mut self, report: DataQualityReport) {
        let entry = self.reports.entry(report.table_name.clone()).or_insert_with(Vec::new);
        entry.push(report);
        if entry.len() > self.max_reports_per_table {
            let drain_n = entry.len() - self.max_reports_per_table;
            entry.drain(0..drain_n);
        }
    }

    pub fn list_reports_for_table(&self, table_name: &str) -> Vec<&DataQualityReport> {
        self.reports
            .get(table_name)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    pub fn list_latest_reports(&self) -> Vec<&DataQualityReport> {
        let mut out = Vec::new();
        for (_table, reports) in &self.reports {
            if let Some(last) = reports.last() {
                out.push(last);
            }
        }
        out
    }
}

impl Default for DataQualityReportRegistry {
    fn default() -> Self {
        Self::new()
    }
}


