//! Access Control
//! 
//! Metric-level access control and authorization.

use crate::error::{RcaError, Result};
use crate::security::policy::{Role, UserContext};
use crate::semantic::metric::MetricDefinition;
use std::collections::HashSet;

/// Access controller
pub struct AccessController {
    user_context: UserContext,
}

impl AccessController {
    pub fn new(user_context: UserContext) -> Self {
        Self { user_context }
    }

    /// Authorize access to a metric
    pub fn authorize_metric(&self, metric: &dyn MetricDefinition) -> Result<()> {
        let policy = metric.policy();
        let allowed_roles: HashSet<String> = policy
            .allowed_roles
            .iter()
            .map(|r| r.to_lowercase())
            .collect();

        let user_role_str = self.user_context.role.as_str().to_lowercase();

        if !allowed_roles.contains(&user_role_str) {
            return Err(RcaError::Execution(format!(
                "Unauthorized access to metric '{}'. Required roles: {:?}, User role: {}",
                metric.name(),
                policy.allowed_roles,
                user_role_str
            )));
        }

        Ok(())
    }

    /// Check if user has required role
    pub fn has_role(&self, role: &Role) -> bool {
        &self.user_context.role == role
    }
}





