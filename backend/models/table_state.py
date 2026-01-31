"""
Table State Model

Implements the table lifecycle states from EXECUTION_PLAN.md:
- READ_ONLY: External DB tables
- SHADOW: Ingestion-ready, isolated
- ACTIVE: Canonical, user-facing
- DEPRECATED: Legacy, safe fallback
"""

from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


class TableState(str, Enum):
    """Table lifecycle states."""
    READ_ONLY = "READ_ONLY"
    SHADOW = "SHADOW"
    ACTIVE = "ACTIVE"
    DEPRECATED = "DEPRECATED"


class SystemMode(str, Enum):
    """System operation modes."""
    READ_ONLY = "READ_ONLY"  # Default: safest for Day 1 users
    INGESTION_READY = "INGESTION_READY"  # Admin-enabled


class UserRole(str, Enum):
    """User roles with hard boundaries."""
    VIEWER = "VIEWER"
    ANALYST = "ANALYST"
    ENGINEER = "ENGINEER"
    ADMIN = "ADMIN"


@dataclass
class TableStateInfo:
    """Table state information."""
    name: str
    state: TableState
    version: str
    owner: Optional[str] = None
    supersedes: Optional[str] = None
    deprecated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = asdict(self)
        if self.deprecated_at:
            result['deprecated_at'] = self.deprecated_at.isoformat()
        if self.created_at:
            result['created_at'] = self.created_at.isoformat()
        if self.updated_at:
            result['updated_at'] = self.updated_at.isoformat()
        return result


class TableStateManager:
    """Manages table state transitions and validation."""

    # Valid state transitions
    VALID_TRANSITIONS = {
        TableState.SHADOW: [TableState.ACTIVE],
        TableState.ACTIVE: [TableState.DEPRECATED],
        TableState.DEPRECATED: [TableState.ACTIVE],  # Restore
        TableState.READ_ONLY: [],  # No transitions allowed
    }

    # State properties
    STATE_PROPERTIES = {
        TableState.READ_ONLY: {
            "queryable": True,
            "writable": False,
            "visible": True,
            "auto_joined": True,
        },
        TableState.SHADOW: {
            "queryable": False,  # Default: not queryable
            "writable": True,
            "visible": False,  # Admin-only
            "auto_joined": False,
        },
        TableState.ACTIVE: {
            "queryable": True,
            "writable": True,
            "visible": True,
            "auto_joined": True,
        },
        TableState.DEPRECATED: {
            "queryable": True,  # Opt-in
            "writable": False,
            "visible": False,  # Hidden
            "auto_joined": False,
        },
    }

    @classmethod
    def can_transition(cls, from_state: TableState, to_state: TableState) -> bool:
        """Check if state transition is allowed."""
        allowed = cls.VALID_TRANSITIONS.get(from_state, [])
        return to_state in allowed

    @classmethod
    def get_state_properties(cls, state: TableState) -> Dict[str, bool]:
        """Get properties for a state."""
        return cls.STATE_PROPERTIES.get(state, {})

    @classmethod
    def validate_transition(
        cls,
        from_state: TableState,
        to_state: TableState,
        role: UserRole
    ) -> tuple:
        """
        Validate state transition with role check.
        
        Returns:
            (is_valid, error_message)
        """
        # Check if transition is allowed
        if not cls.can_transition(from_state, to_state):
            return False, f"Invalid transition from {from_state} to {to_state}"

        # Promotion requires Admin
        if from_state == TableState.SHADOW and to_state == TableState.ACTIVE:
            if role != UserRole.ADMIN:
                return False, "Promotion requires ADMIN role"

        # Deprecation requires Admin
        if to_state == TableState.DEPRECATED:
            if role != UserRole.ADMIN:
                return False, "Deprecation requires ADMIN role"

        # Restore requires Admin
        if from_state == TableState.DEPRECATED and to_state == TableState.ACTIVE:
            if role != UserRole.ADMIN:
                return False, "Restore requires ADMIN role"

        return True, None


class RolePermissions:
    """Role-based permissions matrix."""

    PERMISSIONS = {
        UserRole.VIEWER: {
            "can_query": True,
            "can_create_contracts": False,
            "can_ingest": False,
            "can_promote": False,
            "can_deprecate": False,
            "can_view_shadow": False,
        },
        UserRole.ANALYST: {
            "can_query": True,
            "can_create_contracts": False,
            "can_ingest": False,
            "can_promote": False,
            "can_deprecate": False,
            "can_view_shadow": False,
        },
        UserRole.ENGINEER: {
            "can_query": True,
            "can_create_contracts": True,
            "can_ingest": True,
            "can_promote": False,  # Admin-only
            "can_deprecate": False,  # Admin-only
            "can_view_shadow": True,
        },
        UserRole.ADMIN: {
            "can_query": True,
            "can_create_contracts": True,
            "can_ingest": True,
            "can_promote": True,
            "can_deprecate": True,
            "can_view_shadow": True,
        },
    }

    @classmethod
    def can(cls, role: UserRole, permission: str) -> bool:
        """Check if role has permission."""
        return cls.PERMISSIONS.get(role, {}).get(permission, False)

    @classmethod
    def require(cls, role: UserRole, permission: str) -> None:
        """Require permission or raise exception."""
        if not cls.can(role, permission):
            raise PermissionError(
                f"Role {role} does not have permission: {permission}"
            )

