//! User Management
//! 
//! User management and role assignment for production access control.

use crate::error::{RcaError, Result};
use crate::security::policy::{Role, UserContext};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// User information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub role: Role,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_active: Option<chrono::DateTime<chrono::Utc>>,
    pub metadata: HashMap<String, String>,
}

impl User {
    pub fn new(user_id: String, email: String, name: String, role: Role) -> Self {
        Self {
            user_id,
            email,
            name,
            role,
            created_at: chrono::Utc::now(),
            last_active: None,
            metadata: HashMap::new(),
        }
    }

    pub fn to_user_context(&self) -> UserContext {
        UserContext::new(self.user_id.clone(), self.role.clone())
    }
}

/// User manager for production access control
pub struct UserManager {
    users: Arc<RwLock<HashMap<String, User>>>,
    email_to_user_id: Arc<RwLock<HashMap<String, String>>>,
}

impl UserManager {
    pub fn new() -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            email_to_user_id: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new user
    pub fn create_user(
        &self,
        user_id: String,
        email: String,
        name: String,
        role: Role,
    ) -> Result<User> {
        let mut users = self.users.write().map_err(|e| {
            RcaError::Execution(format!("Failed to acquire write lock: {}", e))
        })?;

        if users.contains_key(&user_id) {
            return Err(RcaError::Execution(format!("User '{}' already exists", user_id)));
        }

        let mut email_map = self.email_to_user_id.write().map_err(|e| {
            RcaError::Execution(format!("Failed to acquire write lock: {}", e))
        })?;

        if email_map.contains_key(&email) {
            return Err(RcaError::Execution(format!("Email '{}' already registered", email)));
        }

        let user = User::new(user_id.clone(), email.clone(), name, role);
        users.insert(user_id.clone(), user.clone());
        email_map.insert(email, user_id);

        Ok(user)
    }

    /// Get user by ID
    pub fn get_user(&self, user_id: &str) -> Result<User> {
        let users = self.users.read().map_err(|e| {
            RcaError::Execution(format!("Failed to acquire read lock: {}", e))
        })?;

        users.get(user_id)
            .cloned()
            .ok_or_else(|| RcaError::Execution(format!("User '{}' not found", user_id)))
    }

    /// Get user by email
    pub fn get_user_by_email(&self, email: &str) -> Result<User> {
        let email_map = self.email_to_user_id.read().map_err(|e| {
            RcaError::Execution(format!("Failed to acquire read lock: {}", e))
        })?;

        let user_id = email_map.get(email)
            .ok_or_else(|| RcaError::Execution(format!("User with email '{}' not found", email)))?;

        self.get_user(user_id)
    }

    /// Update user role
    pub fn update_user_role(&self, user_id: &str, new_role: Role) -> Result<User> {
        let mut users = self.users.write().map_err(|e| {
            RcaError::Execution(format!("Failed to acquire write lock: {}", e))
        })?;

        let user = users.get_mut(user_id)
            .ok_or_else(|| RcaError::Execution(format!("User '{}' not found", user_id)))?;

        user.role = new_role.clone();
        Ok(user.clone())
    }

    /// Update user last active timestamp
    pub fn update_last_active(&self, user_id: &str) -> Result<()> {
        let mut users = self.users.write().map_err(|e| {
            RcaError::Execution(format!("Failed to acquire write lock: {}", e))
        })?;

        if let Some(user) = users.get_mut(user_id) {
            user.last_active = Some(chrono::Utc::now());
        }

        Ok(())
    }

    /// Delete user
    pub fn delete_user(&self, user_id: &str) -> Result<()> {
        let mut users = self.users.write().map_err(|e| {
            RcaError::Execution(format!("Failed to acquire write lock: {}", e))
        })?;

        if let Some(user) = users.remove(user_id) {
            let mut email_map = self.email_to_user_id.write().map_err(|e| {
                RcaError::Execution(format!("Failed to acquire write lock: {}", e))
            })?;
            email_map.remove(&user.email);
        }

        Ok(())
    }

    /// List all users
    pub fn list_users(&self) -> Result<Vec<User>> {
        let users = self.users.read().map_err(|e| {
            RcaError::Execution(format!("Failed to acquire read lock: {}", e))
        })?;

        Ok(users.values().cloned().collect())
    }

    /// List users by role
    pub fn list_users_by_role(&self, role: &Role) -> Result<Vec<User>> {
        let users = self.users.read().map_err(|e| {
            RcaError::Execution(format!("Failed to acquire read lock: {}", e))
        })?;

        Ok(users.values()
            .filter(|u| &u.role == role)
            .cloned()
            .collect())
    }

    /// Get user context for a user ID
    pub fn get_user_context(&self, user_id: &str) -> Result<UserContext> {
        let user = self.get_user(user_id)?;
        Ok(user.to_user_context())
    }

    /// Initialize with default admin user
    pub fn initialize_with_defaults(&self) -> Result<()> {
        // Create default admin user if it doesn't exist
        if self.get_user("admin").is_err() {
            self.create_user(
                "admin".to_string(),
                "admin@example.com".to_string(),
                "System Administrator".to_string(),
                Role::Admin,
            )?;
        }

        Ok(())
    }
}

impl Default for UserManager {
    fn default() -> Self {
        Self::new()
    }
}





