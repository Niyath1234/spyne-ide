//! Comprehensive Root Cause Classification System
//! 
//! Provides detailed taxonomy of root causes with confidence scoring,
//! evidence tracking, and human-readable explanations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Comprehensive root cause types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RootCauseType {
    // Data Quality Issues
    /// Missing data in one or both systems
    MissingData,
    /// Invalid data (out of range, wrong format, violates constraints)
    InvalidData,
    /// Duplicate records causing incorrect aggregations
    DuplicateData,
    /// Stale data (not updated, outdated)
    StaleData,
    /// Data type mismatches
    DataTypeMismatch,
    
    // Logic Mismatches
    /// Different calculation formulas
    FormulaDifference,
    /// Different aggregation methods (SUM vs AVG, etc.)
    AggregationDifference,
    /// Different filter conditions
    FilterDifference,
    /// Different business rules applied
    BusinessRuleDifference,
    /// Different null handling (NULL vs 0, etc.)
    NullHandlingDifference,
    
    // Timing Issues
    /// Different cutoff times
    CutoffTimeDifference,
    /// Time zone mismatches
    TimezoneMismatch,
    /// Different as-of dates
    AsOfDateDifference,
    /// Different lateness handling
    LatenessHandlingDifference,
    /// Temporal misalignment (data from different time periods)
    TemporalMisalignment,
    
    // Grain Mismatches
    /// Different granularities (customer vs loan level)
    GrainMismatch,
    /// Aggregation errors (incorrect grouping)
    AggregationError,
    /// Disaggregation errors (splitting incorrectly)
    DisaggregationError,
    /// Missing grain columns
    MissingGrainColumns,
    
    // Identity Resolution Issues
    /// Different identifiers for same entity
    IdentifierMismatch,
    /// Missing identity mappings
    MissingMapping,
    /// Incorrect identity mappings
    IncorrectMapping,
    /// Identity resolution failures
    IdentityResolutionFailure,
    
    // System Issues
    /// Data not loaded into system
    DataNotLoaded,
    /// Processing failures
    ProcessingFailure,
    /// Configuration errors
    ConfigurationError,
    /// System downtime/availability issues
    SystemUnavailable,
    
    // Join/Relationship Issues
    /// Missing join relationships
    MissingJoin,
    /// Incorrect join keys
    IncorrectJoinKeys,
    /// Join cardinality mismatches (1:1 vs 1:many)
    JoinCardinalityMismatch,
    
    // Unknown/Other
    /// Root cause not yet identified
    Unknown,
    /// Multiple root causes (composite)
    MultipleCauses,
}

impl RootCauseType {
    /// Get human-readable name
    pub fn display_name(&self) -> &'static str {
        match self {
            RootCauseType::MissingData => "Missing Data",
            RootCauseType::InvalidData => "Invalid Data",
            RootCauseType::DuplicateData => "Duplicate Data",
            RootCauseType::StaleData => "Stale Data",
            RootCauseType::DataTypeMismatch => "Data Type Mismatch",
            RootCauseType::FormulaDifference => "Formula Difference",
            RootCauseType::AggregationDifference => "Aggregation Method Difference",
            RootCauseType::FilterDifference => "Filter Condition Difference",
            RootCauseType::BusinessRuleDifference => "Business Rule Difference",
            RootCauseType::NullHandlingDifference => "Null Handling Difference",
            RootCauseType::CutoffTimeDifference => "Cutoff Time Difference",
            RootCauseType::TimezoneMismatch => "Timezone Mismatch",
            RootCauseType::AsOfDateDifference => "As-of Date Difference",
            RootCauseType::LatenessHandlingDifference => "Lateness Handling Difference",
            RootCauseType::TemporalMisalignment => "Temporal Misalignment",
            RootCauseType::GrainMismatch => "Grain Mismatch",
            RootCauseType::AggregationError => "Aggregation Error",
            RootCauseType::DisaggregationError => "Disaggregation Error",
            RootCauseType::MissingGrainColumns => "Missing Grain Columns",
            RootCauseType::IdentifierMismatch => "Identifier Mismatch",
            RootCauseType::MissingMapping => "Missing Identity Mapping",
            RootCauseType::IncorrectMapping => "Incorrect Identity Mapping",
            RootCauseType::IdentityResolutionFailure => "Identity Resolution Failure",
            RootCauseType::DataNotLoaded => "Data Not Loaded",
            RootCauseType::ProcessingFailure => "Processing Failure",
            RootCauseType::ConfigurationError => "Configuration Error",
            RootCauseType::SystemUnavailable => "System Unavailable",
            RootCauseType::MissingJoin => "Missing Join Relationship",
            RootCauseType::IncorrectJoinKeys => "Incorrect Join Keys",
            RootCauseType::JoinCardinalityMismatch => "Join Cardinality Mismatch",
            RootCauseType::Unknown => "Unknown Root Cause",
            RootCauseType::MultipleCauses => "Multiple Root Causes",
        }
    }
    
    /// Get category
    pub fn category(&self) -> RootCauseCategory {
        match self {
            RootCauseType::MissingData
            | RootCauseType::InvalidData
            | RootCauseType::DuplicateData
            | RootCauseType::StaleData
            | RootCauseType::DataTypeMismatch => RootCauseCategory::DataQuality,
            
            RootCauseType::FormulaDifference
            | RootCauseType::AggregationDifference
            | RootCauseType::FilterDifference
            | RootCauseType::BusinessRuleDifference
            | RootCauseType::NullHandlingDifference => RootCauseCategory::LogicMismatch,
            
            RootCauseType::CutoffTimeDifference
            | RootCauseType::TimezoneMismatch
            | RootCauseType::AsOfDateDifference
            | RootCauseType::LatenessHandlingDifference
            | RootCauseType::TemporalMisalignment => RootCauseCategory::Timing,
            
            RootCauseType::GrainMismatch
            | RootCauseType::AggregationError
            | RootCauseType::DisaggregationError
            | RootCauseType::MissingGrainColumns => RootCauseCategory::Grain,
            
            RootCauseType::IdentifierMismatch
            | RootCauseType::MissingMapping
            | RootCauseType::IncorrectMapping
            | RootCauseType::IdentityResolutionFailure => RootCauseCategory::Identity,
            
            RootCauseType::DataNotLoaded
            | RootCauseType::ProcessingFailure
            | RootCauseType::ConfigurationError
            | RootCauseType::SystemUnavailable => RootCauseCategory::System,
            
            RootCauseType::MissingJoin
            | RootCauseType::IncorrectJoinKeys
            | RootCauseType::JoinCardinalityMismatch => RootCauseCategory::Relationship,
            
            RootCauseType::Unknown | RootCauseType::MultipleCauses => RootCauseCategory::Other,
        }
    }
    
    /// Get severity level
    pub fn severity(&self) -> Severity {
        match self {
            RootCauseType::MissingData
            | RootCauseType::DataNotLoaded
            | RootCauseType::SystemUnavailable => Severity::Critical,
            
            RootCauseType::InvalidData
            | RootCauseType::FormulaDifference
            | RootCauseType::ProcessingFailure
            | RootCauseType::ConfigurationError => Severity::High,
            
            RootCauseType::DuplicateData
            | RootCauseType::AggregationDifference
            | RootCauseType::FilterDifference
            | RootCauseType::GrainMismatch => Severity::Medium,
            
            RootCauseType::StaleData
            | RootCauseType::NullHandlingDifference
            | RootCauseType::TimezoneMismatch => Severity::Low,
            
            _ => Severity::Medium,
        }
    }
}

/// Root cause categories
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RootCauseCategory {
    DataQuality,
    LogicMismatch,
    Timing,
    Grain,
    Identity,
    System,
    Relationship,
    Other,
}

impl RootCauseCategory {
    pub fn display_name(&self) -> &'static str {
        match self {
            RootCauseCategory::DataQuality => "Data Quality Issues",
            RootCauseCategory::LogicMismatch => "Logic Mismatches",
            RootCauseCategory::Timing => "Timing Issues",
            RootCauseCategory::Grain => "Grain Mismatches",
            RootCauseCategory::Identity => "Identity Resolution Issues",
            RootCauseCategory::System => "System Issues",
            RootCauseCategory::Relationship => "Join/Relationship Issues",
            RootCauseCategory::Other => "Other",
        }
    }
}

/// Severity levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

impl Severity {
    pub fn display_name(&self) -> &'static str {
        match self {
            Severity::Low => "Low",
            Severity::Medium => "Medium",
            Severity::High => "High",
            Severity::Critical => "Critical",
        }
    }
    
    pub fn emoji(&self) -> &'static str {
        match self {
            Severity::Low => "🟢",
            Severity::Medium => "🟡",
            Severity::High => "🟠",
            Severity::Critical => "🔴",
        }
    }
}

/// Comprehensive root cause classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootCauseClassification {
    /// Primary root cause type
    pub root_cause_type: RootCauseType,
    
    /// Subtype or specific variant
    pub subtype: String,
    
    /// Human-readable description
    pub description: String,
    
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    
    /// Confidence interval (lower, upper)
    pub confidence_interval: Option<(f64, f64)>,
    
    /// Evidence supporting this classification
    pub evidence: Vec<EvidenceItem>,
    
    /// Affected entities/rows count
    pub affected_count: usize,
    
    /// Percentage of total mismatches this explains
    pub explanation_percentage: f64,
    
    /// Severity
    pub severity: Severity,
    
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Evidence item supporting root cause
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceItem {
    /// Type of evidence
    pub evidence_type: EvidenceType,
    
    /// Description of evidence
    pub description: String,
    
    /// Specific values/data points
    pub data_points: HashMap<String, String>,
    
    /// Strength of evidence (0.0 to 1.0)
    pub strength: f64,
}

/// Types of evidence
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceType {
    /// Formula comparison
    FormulaComparison,
    /// Data sample
    DataSample,
    /// Rule comparison
    RuleComparison,
    /// Metadata comparison
    MetadataComparison,
    /// Statistical analysis
    StatisticalAnalysis,
    /// User confirmation
    UserConfirmation,
    /// System log
    SystemLog,
    /// Other
    Other,
}

/// Root cause classifier
pub struct RootCauseClassifier;

impl RootCauseClassifier {
    /// Classify root causes from comparison results
    pub fn classify(
        &self,
        comparison: &crate::diff::ComparisonResult,
        system_a: &str,
        system_b: &str,
        metadata: &crate::metadata::Metadata,
    ) -> Vec<RootCauseClassification> {
        let mut classifications = Vec::new();
        
        // 1. Check for missing data
        if !comparison.population_diff.missing_in_b.is_empty() {
            classifications.push(self.classify_missing_data(
                &comparison.population_diff.missing_in_b,
                system_a,
                system_b,
            ));
        }
        
        if !comparison.population_diff.extra_in_b.is_empty() {
            classifications.push(self.classify_extra_data(
                &comparison.population_diff.extra_in_b,
                system_a,
                system_b,
            ));
        }
        
        // 2. Check for value mismatches
        if comparison.data_diff.mismatches > 0 {
            classifications.extend(self.classify_value_mismatches(
                comparison,
                system_a,
                system_b,
                metadata,
            ));
        }
        
        // 3. Rank by confidence and impact
        classifications.sort_by(|a, b| {
            (b.confidence * b.explanation_percentage)
                .partial_cmp(&(a.confidence * a.explanation_percentage))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        
        classifications
    }
    
    fn classify_missing_data(
        &self,
        missing_entities: &[Vec<String>],
        system_a: &str,
        system_b: &str,
    ) -> RootCauseClassification {
        let count = missing_entities.len();
        RootCauseClassification {
            root_cause_type: RootCauseType::MissingData,
            subtype: format!("Missing in {}", system_b),
            description: format!(
                "{} entities exist in {} but are missing in {}",
                count, system_a, system_b
            ),
            confidence: 0.95, // High confidence for missing data
            confidence_interval: Some((0.90, 1.0)),
            evidence: vec![EvidenceItem {
                evidence_type: EvidenceType::DataSample,
                description: format!("{} entities found in {} but not in {}", count, system_a, system_b),
                data_points: HashMap::from([
                    ("missing_count".to_string(), count.to_string()),
                    ("system_a".to_string(), system_a.to_string()),
                    ("system_b".to_string(), system_b.to_string()),
                ]),
                strength: 0.95,
            }],
            affected_count: count,
            explanation_percentage: if count > 0 { 1.0 } else { 0.0 },
            severity: Severity::High,
            metadata: HashMap::new(),
        }
    }
    
    fn classify_extra_data(
        &self,
        extra_entities: &[Vec<String>],
        system_a: &str,
        system_b: &str,
    ) -> RootCauseClassification {
        let count = extra_entities.len();
        RootCauseClassification {
            root_cause_type: RootCauseType::MissingData,
            subtype: format!("Extra in {}", system_b),
            description: format!(
                "{} entities exist in {} but are missing in {}",
                count, system_b, system_a
            ),
            confidence: 0.95,
            confidence_interval: Some((0.90, 1.0)),
            evidence: vec![EvidenceItem {
                evidence_type: EvidenceType::DataSample,
                description: format!("{} entities found in {} but not in {}", count, system_b, system_a),
                data_points: HashMap::from([
                    ("extra_count".to_string(), count.to_string()),
                    ("system_a".to_string(), system_a.to_string()),
                    ("system_b".to_string(), system_b.to_string()),
                ]),
                strength: 0.95,
            }],
            affected_count: count,
            explanation_percentage: if count > 0 { 1.0 } else { 0.0 },
            severity: Severity::High,
            metadata: HashMap::new(),
        }
    }
    
    fn classify_value_mismatches(
        &self,
        comparison: &crate::diff::ComparisonResult,
        system_a: &str,
        system_b: &str,
        metadata: &crate::metadata::Metadata,
    ) -> Vec<RootCauseClassification> {
        let mut classifications = Vec::new();
        
        // Check for formula differences
        let rule_a = metadata.rules.iter().find(|r| r.system == system_a);
        let rule_b = metadata.rules.iter().find(|r| r.system == system_b);
        
        if let (Some(rule_a), Some(rule_b)) = (rule_a, rule_b) {
            if rule_a.computation.formula != rule_b.computation.formula {
                classifications.push(RootCauseClassification {
                    root_cause_type: RootCauseType::FormulaDifference,
                    subtype: "Different calculation formulas".to_string(),
                    description: format!(
                        "System {} uses formula: {}\nSystem {} uses formula: {}",
                        system_a, rule_a.computation.formula,
                        system_b, rule_b.computation.formula
                    ),
                    confidence: 0.85,
                    confidence_interval: Some((0.75, 0.95)),
                    evidence: vec![
                        EvidenceItem {
                            evidence_type: EvidenceType::FormulaComparison,
                            description: format!("Formula in {}: {}", system_a, rule_a.computation.formula),
                            data_points: HashMap::from([
                                ("system_a_formula".to_string(), rule_a.computation.formula.clone()),
                            ]),
                            strength: 0.9,
                        },
                        EvidenceItem {
                            evidence_type: EvidenceType::FormulaComparison,
                            description: format!("Formula in {}: {}", system_b, rule_b.computation.formula),
                            data_points: HashMap::from([
                                ("system_b_formula".to_string(), rule_b.computation.formula.clone()),
                            ]),
                            strength: 0.9,
                        },
                    ],
                    affected_count: comparison.data_diff.mismatches,
                    explanation_percentage: 0.8, // Assume 80% of mismatches due to formula
                    severity: Severity::High,
                    metadata: HashMap::from([
                        ("system_a_rule_id".to_string(), rule_a.id.clone()),
                        ("system_b_rule_id".to_string(), rule_b.id.clone()),
                    ]),
                });
            }
            
            // Check for filter differences
            if rule_a.computation.filter_conditions != rule_b.computation.filter_conditions {
                classifications.push(RootCauseClassification {
                    root_cause_type: RootCauseType::FilterDifference,
                    subtype: "Different filter conditions".to_string(),
                    description: format!(
                        "System {} and {} apply different filters, causing different populations",
                        system_a, system_b
                    ),
                    confidence: 0.80,
                    confidence_interval: Some((0.70, 0.90)),
                    evidence: vec![EvidenceItem {
                        evidence_type: EvidenceType::RuleComparison,
                        description: "Filter conditions differ between systems".to_string(),
                        data_points: HashMap::from([
                            ("system_a_filters".to_string(), 
                             serde_json::to_string(&rule_a.computation.filter_conditions).unwrap_or_default()),
                            ("system_b_filters".to_string(),
                             serde_json::to_string(&rule_b.computation.filter_conditions).unwrap_or_default()),
                        ]),
                        strength: 0.85,
                    }],
                    affected_count: comparison.data_diff.mismatches,
                    explanation_percentage: 0.6,
                    severity: Severity::Medium,
                    metadata: HashMap::new(),
                });
            }
        }
        
        classifications
    }
}

/// Human-readable explanation generator
pub struct ExplanationGenerator;

impl ExplanationGenerator {
    /// Generate human-readable explanation for a root cause
    pub fn generate_explanation(
        &self,
        classification: &RootCauseClassification,
        system_a: &str,
        system_b: &str,
        metric: &str,
    ) -> String {
        let confidence_pct = (classification.confidence * 100.0) as u32;
        let severity_emoji = classification.severity.emoji();
        
        format!(
            "{}\n\n**Root Cause**: {} ({})\n**Confidence**: {}%\n**Severity**: {} {}\n\n**Explanation**:\n{}\n\n**Impact**:\n- Affects {} entities\n- Explains {:.1}% of mismatches\n\n**Evidence**:\n{}",
            severity_emoji,
            classification.root_cause_type.display_name(),
            classification.subtype,
            confidence_pct,
            severity_emoji,
            classification.severity.display_name(),
            classification.description,
            classification.affected_count,
            classification.explanation_percentage * 100.0,
            self.format_evidence(&classification.evidence)
        )
    }
    
    fn format_evidence(&self, evidence: &[EvidenceItem]) -> String {
        evidence.iter()
            .map(|e| {
                format!(
                    "- **{}**: {} (strength: {:.0}%)",
                    match e.evidence_type {
                        EvidenceType::FormulaComparison => "Formula Comparison",
                        EvidenceType::DataSample => "Data Sample",
                        EvidenceType::RuleComparison => "Rule Comparison",
                        EvidenceType::MetadataComparison => "Metadata Comparison",
                        EvidenceType::StatisticalAnalysis => "Statistical Analysis",
                        EvidenceType::UserConfirmation => "User Confirmation",
                        EvidenceType::SystemLog => "System Log",
                        EvidenceType::Other => "Other",
                    },
                    e.description,
                    e.strength * 100.0
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

