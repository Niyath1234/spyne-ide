//! Join Validator - Validate join proposals

use serde::{Deserialize, Serialize};

/// Join validation result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinValidationResult {
    pub is_valid: bool,
    pub reason: Option<String>,
}

/// Join Validator
pub struct JoinValidator;

impl JoinValidator {
    pub fn new() -> Self {
        Self
    }
    
    pub fn validate_join(&self, _from_table: &str, _to_table: &str, _keys: &[(String, String)]) -> JoinValidationResult {
        JoinValidationResult {
            is_valid: true,
            reason: None,
        }
    }
}

impl Default for JoinValidator {
    fn default() -> Self {
        Self::new()
    }
}

