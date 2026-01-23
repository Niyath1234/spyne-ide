//! User Management API
//! 
//! REST API endpoints for user management and access control.

use crate::error::{RcaError, Result};
use crate::security::policy::Role;
use crate::security::user_manager::{User, UserManager};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// User creation request
#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub role: String,
}

/// User update request
#[derive(Debug, Deserialize)]
pub struct UpdateUserRoleRequest {
    pub role: String,
}

/// User response
#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub role: String,
    pub created_at: String,
    pub last_active: Option<String>,
}

impl From<User> for UserResponse {
    fn from(user: User) -> Self {
        Self {
            user_id: user.user_id,
            email: user.email,
            name: user.name,
            role: user.role.as_str().to_string(),
            created_at: user.created_at.to_rfc3339(),
            last_active: user.last_active.map(|d| d.to_rfc3339()),
        }
    }
}

/// User API handler
pub struct UserApi {
    user_manager: Arc<UserManager>,
}

impl UserApi {
    pub fn new(user_manager: Arc<UserManager>) -> Self {
        Self { user_manager }
    }

    /// Create a new user
    pub fn create_user(&self, request: CreateUserRequest) -> Result<UserResponse> {
        let role = Role::from_str(&request.role);

        let user = self.user_manager.create_user(
            request.user_id,
            request.email,
            request.name,
            role,
        )?;

        Ok(user.into())
    }

    /// Get user by ID
    pub fn get_user(&self, user_id: &str) -> Result<UserResponse> {
        let user = self.user_manager.get_user(user_id)?;
        Ok(user.into())
    }

    /// Get user by email
    pub fn get_user_by_email(&self, email: &str) -> Result<UserResponse> {
        let user = self.user_manager.get_user_by_email(email)?;
        Ok(user.into())
    }

    /// Update user role
    pub fn update_user_role(&self, user_id: &str, request: UpdateUserRoleRequest) -> Result<UserResponse> {
        let role = Role::from_str(&request.role);

        let user = self.user_manager.update_user_role(user_id, role)?;
        Ok(user.into())
    }

    /// Delete user
    pub fn delete_user(&self, user_id: &str) -> Result<()> {
        self.user_manager.delete_user(user_id)
    }

    /// List all users
    pub fn list_users(&self) -> Result<Vec<UserResponse>> {
        let users = self.user_manager.list_users()?;
        Ok(users.into_iter().map(|u| u.into()).collect())
    }

    /// List users by role
    pub fn list_users_by_role(&self, role_str: &str) -> Result<Vec<UserResponse>> {
        let role = Role::from_str(role_str);

        let users = self.user_manager.list_users_by_role(&role)?;
        Ok(users.into_iter().map(|u| u.into()).collect())
    }

    /// Get user context for authentication
    pub fn get_user_context(&self, user_id: &str) -> Result<crate::security::policy::UserContext> {
        self.user_manager.get_user_context(user_id)
    }
}

