//! Security Policy Definitions

use serde::{Deserialize, Serialize};

/// User role for access control
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Finance,
    Admin,
    Analyst,
    Public,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Finance => "finance",
            Role::Admin => "admin",
            Role::Analyst => "analyst",
            Role::Public => "public",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "finance" => Role::Finance,
            "admin" => Role::Admin,
            "analyst" => Role::Analyst,
            _ => Role::Public,
        }
    }
}

/// User context for authorization
#[derive(Debug, Clone)]
pub struct UserContext {
    pub user_id: String,
    pub role: Role,
}

impl UserContext {
    pub fn new(user_id: String, role: Role) -> Self {
        Self { user_id, role }
    }

    pub fn public() -> Self {
        Self {
            user_id: "public".to_string(),
            role: Role::Public,
        }
    }
}





