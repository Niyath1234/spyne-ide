"""
Drift Detection Engine

Implements metadata drift detection from EXECUTION_PLAN.md Section 5.
"""

from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DriftSeverity(str, Enum):
    """Drift severity levels."""
    COMPATIBLE = "COMPATIBLE"  # Safe, auto-apply
    WARNING = "WARNING"  # Needs review, allow override
    BREAKING = "BREAKING"  # Blocks promotion, require new version


class ChangeType(str, Enum):
    """Types of schema changes."""
    ADD = "ADD"
    REMOVE = "REMOVE"
    RENAME = "RENAME"
    TYPE_CHANGE = "TYPE_CHANGE"


@dataclass
class ColumnInfo:
    """Column information."""
    name: str
    data_type: str
    nullable: bool
    default_value: Optional[str] = None


@dataclass
class SchemaChange:
    """Represents a schema change."""
    change_type: ChangeType
    column: Optional[str] = None
    old_column: Optional[str] = None
    new_column: Optional[str] = None
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    severity: DriftSeverity = DriftSeverity.COMPATIBLE


@dataclass
class DriftReport:
    """Drift detection report."""
    changes: List[SchemaChange]
    severity: DriftSeverity
    from_version: str
    to_version: str


class DriftDetectionEngine:
    """Detects schema drift between versions."""

    @staticmethod
    def detect_drift(
        current_schema: Dict[str, Any],
        new_schema: Dict[str, Any]
    ) -> DriftReport:
        """
        Detect drift between current and new schema.
        
        Args:
            current_schema: Current schema with columns
            new_schema: New schema with columns
            
        Returns:
            DriftReport with detected changes
        """
        changes = []
        
        current_columns = {
            col['name']: ColumnInfo(
                name=col['name'],
                data_type=col.get('data_type', 'unknown'),
                nullable=col.get('nullable', True),
                default_value=col.get('default_value')
            )
            for col in current_schema.get('columns', [])
        }
        
        new_columns = {
            col['name']: ColumnInfo(
                name=col['name'],
                data_type=col.get('data_type', 'unknown'),
                nullable=col.get('nullable', True),
                default_value=col.get('default_value')
            )
            for col in new_schema.get('columns', [])
        }
        
        current_col_names = set(current_columns.keys())
        new_col_names = set(new_columns.keys())
        
        # Detect additions
        for col_name in new_col_names - current_col_names:
            new_col = new_columns[col_name]
            severity = DriftSeverity.COMPATIBLE if new_col.nullable else DriftSeverity.WARNING
            changes.append(SchemaChange(
                change_type=ChangeType.ADD,
                column=col_name,
                severity=severity
            ))
        
        # Detect removals
        for col_name in current_col_names - new_col_names:
            changes.append(SchemaChange(
                change_type=ChangeType.REMOVE,
                column=col_name,
                severity=DriftSeverity.BREAKING
            ))
        
        # Detect renames (heuristic: same type, similar name)
        for old_col_name in current_col_names & new_col_names:
            old_col = current_columns[old_col_name]
            new_col = new_columns[old_col_name]
            
            # Check type changes
            if old_col.data_type != new_col.data_type:
                severity = DriftDetectionEngine._get_type_change_severity(
                    old_col.data_type, new_col.data_type
                )
                changes.append(SchemaChange(
                    change_type=ChangeType.TYPE_CHANGE,
                    column=old_col_name,
                    old_type=old_col.data_type,
                    new_type=new_col.data_type,
                    severity=severity
                ))
        
        # Detect renames (check if column disappeared and similar one appeared)
        removed_cols = current_col_names - new_col_names
        added_cols = new_col_names - current_col_names
        
        for removed_col in removed_cols:
            for added_col in added_cols:
                if DriftDetectionEngine._could_be_rename(removed_col, added_col):
                    changes.append(SchemaChange(
                        change_type=ChangeType.RENAME,
                        old_column=removed_col,
                        new_column=added_col,
                        severity=DriftSeverity.BREAKING
                    ))
                    # Remove from ADD/REMOVE lists
                    changes = [c for c in changes if not (
                        c.change_type == ChangeType.ADD and c.column == added_col
                    )]
                    changes = [c for c in changes if not (
                        c.change_type == ChangeType.REMOVE and c.column == removed_col
                    )]
        
        # Determine overall severity
        severity = DriftDetectionEngine._get_overall_severity(changes)
        
        return DriftReport(
            changes=changes,
            severity=severity,
            from_version=current_schema.get('version', 'v1'),
            to_version=new_schema.get('version', 'v2')
        )

    @staticmethod
    def _get_type_change_severity(old_type: str, new_type: str) -> DriftSeverity:
        """Determine severity of type change."""
        # Type widening (safe)
        widening_map = {
            'INT': ['BIGINT'],
            'VARCHAR': ['TEXT'],
            'FLOAT': ['DOUBLE'],
        }
        
        for base_type, wider_types in widening_map.items():
            if old_type.upper() == base_type and new_type.upper() in wider_types:
                return DriftSeverity.COMPATIBLE
        
        # Type narrowing (warning)
        narrowing_map = {
            'BIGINT': ['INT'],
            'TEXT': ['VARCHAR'],
            'DOUBLE': ['FLOAT'],
        }
        
        for base_type, narrower_types in narrowing_map.items():
            if old_type.upper() == base_type and new_type.upper() in narrower_types:
                return DriftSeverity.WARNING
        
        # Other type changes (breaking)
        return DriftSeverity.BREAKING

    @staticmethod
    def _could_be_rename(old_name: str, new_name: str) -> bool:
        """Heuristic to detect potential renames."""
        # Simple similarity check (could be improved with fuzzy matching)
        old_lower = old_name.lower()
        new_lower = new_name.lower()
        
        # Exact match (shouldn't happen, but check anyway)
        if old_lower == new_lower:
            return False
        
        # Check if one contains the other
        if old_lower in new_lower or new_lower in old_lower:
            return True
        
        # Check common patterns
        common_patterns = [
            ('id', 'identifier'),
            ('name', 'title'),
            ('desc', 'description'),
            ('created', 'created_at'),
            ('updated', 'updated_at'),
        ]
        
        for pattern_old, pattern_new in common_patterns:
            if pattern_old in old_lower and pattern_new in new_lower:
                return True
            if pattern_new in old_lower and pattern_old in new_lower:
                return True
        
        return False

    @staticmethod
    def _get_overall_severity(changes: List[SchemaChange]) -> DriftSeverity:
        """Get overall severity from list of changes."""
        if not changes:
            return DriftSeverity.COMPATIBLE
        
        severities = [c.severity for c in changes]
        
        if DriftSeverity.BREAKING in severities:
            return DriftSeverity.BREAKING
        elif DriftSeverity.WARNING in severities:
            return DriftSeverity.WARNING
        else:
            return DriftSeverity.COMPATIBLE

    @staticmethod
    def is_compatible_change(change: SchemaChange) -> bool:
        """Check if change is compatible (can be auto-applied)."""
        return change.severity == DriftSeverity.COMPATIBLE

    @staticmethod
    def requires_new_version(change: SchemaChange) -> bool:
        """Check if change requires new version."""
        return change.severity == DriftSeverity.BREAKING

