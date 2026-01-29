"""
Business rules models.

These models represent business rules in a structured format,
derived from natural language input.
"""

from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class RuleCategory(str, Enum):
    """Categories of business rules."""

    DATA_QUALITY = "data_quality"
    DATA_COMPLETENESS = "data_completeness"
    DATA_TRANSFORMATION = "data_transformation"
    DATA_CONSISTENCY = "data_consistency"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    AGGREGATION = "aggregation"
    BUSINESS_LOGIC = "business_logic"
    DATA_FORMAT = "data_format"
    DEDUPLICATION = "deduplication"
    NULL_HANDLING = "null_handling"
    DATE_HANDLING = "date_handling"
    NUMERIC_HANDLING = "numeric_handling"
    STRING_HANDLING = "string_handling"
    CUSTOM = "custom"


class RulePriority(str, Enum):
    """Priority levels for business rules."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class BusinessRule(BaseModel):
    """Represents a single business rule for validation."""

    id: str = Field(..., description="Unique rule identifier")
    name: str = Field(..., description="Rule name")
    description: str = Field(..., description="Original natural language description")
    category: RuleCategory = Field(
        default=RuleCategory.CUSTOM, description="Rule category"
    )
    priority: RulePriority = Field(
        default=RulePriority.MEDIUM, description="Rule priority"
    )

    # Source context
    source_tables: list[str] = Field(
        default_factory=list, description="Source tables involved"
    )
    source_columns: list[str] = Field(
        default_factory=list, description="Source columns involved"
    )

    # Target context
    target_tables: list[str] = Field(
        default_factory=list, description="Target tables involved"
    )
    target_columns: list[str] = Field(
        default_factory=list, description="Target columns involved"
    )

    # Transformation details
    transformation_logic: str | None = Field(
        None, description="Extracted transformation logic"
    )
    expected_behavior: str | None = Field(
        None, description="Expected behavior/outcome"
    )

    # Validation parameters
    tolerance: float | None = Field(
        None, description="Tolerance for numeric comparisons"
    )
    case_sensitive: bool = Field(
        default=False, description="Whether string comparisons are case-sensitive"
    )
    allow_null: bool = Field(
        default=True, description="Whether NULL values are allowed"
    )

    # Metadata
    tags: list[str] = Field(default_factory=list, description="Rule tags")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )

    def to_prompt_context(self) -> str:
        """Generate context for LLM prompt."""
        lines = [
            f"Rule ID: {self.id}",
            f"Name: {self.name}",
            f"Description: {self.description}",
            f"Category: {self.category.value}",
            f"Priority: {self.priority.value}",
        ]

        if self.source_tables:
            lines.append(f"Source Tables: {', '.join(self.source_tables)}")
        if self.source_columns:
            lines.append(f"Source Columns: {', '.join(self.source_columns)}")
        if self.target_tables:
            lines.append(f"Target Tables: {', '.join(self.target_tables)}")
        if self.target_columns:
            lines.append(f"Target Columns: {', '.join(self.target_columns)}")
        if self.transformation_logic:
            lines.append(f"Transformation: {self.transformation_logic}")
        if self.expected_behavior:
            lines.append(f"Expected: {self.expected_behavior}")

        return "\n".join(lines)


class BusinessRuleSet(BaseModel):
    """Collection of business rules for validation."""

    id: str = Field(..., description="Rule set identifier")
    name: str = Field(..., description="Rule set name")
    description: str | None = Field(None, description="Rule set description")
    rules: list[BusinessRule] = Field(default_factory=list, description="Business rules")
    created_at: str = Field(..., description="Creation timestamp")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    def get_rules_by_category(self, category: RuleCategory) -> list[BusinessRule]:
        """Get rules filtered by category."""
        return [r for r in self.rules if r.category == category]

    def get_rules_by_priority(self, priority: RulePriority) -> list[BusinessRule]:
        """Get rules filtered by priority."""
        return [r for r in self.rules if r.priority == priority]

    def get_critical_rules(self) -> list[BusinessRule]:
        """Get all critical priority rules."""
        return self.get_rules_by_priority(RulePriority.CRITICAL)

    def to_summary(self) -> dict[str, Any]:
        """Generate summary statistics."""
        category_counts = {}
        priority_counts = {}

        for rule in self.rules:
            category_counts[rule.category.value] = (
                category_counts.get(rule.category.value, 0) + 1
            )
            priority_counts[rule.priority.value] = (
                priority_counts.get(rule.priority.value, 0) + 1
            )

        return {
            "total_rules": len(self.rules),
            "by_category": category_counts,
            "by_priority": priority_counts,
        }
