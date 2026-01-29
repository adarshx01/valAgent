"""
Helper utilities for ETL Validator.

Provides common utility functions used across the application.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any
import uuid


def generate_uuid() -> str:
    """Generate a unique identifier."""
    return str(uuid.uuid4())


def generate_short_id() -> str:
    """Generate a short unique identifier."""
    return uuid.uuid4().hex[:12]


def get_utc_now() -> datetime:
    """Get current UTC timestamp."""
    return datetime.now(timezone.utc)


def get_timestamp_str() -> str:
    """Get current timestamp as ISO format string."""
    return get_utc_now().isoformat()


def hash_content(content: str) -> str:
    """Generate SHA256 hash of content."""
    return hashlib.sha256(content.encode()).hexdigest()


def sanitize_sql(sql: str) -> str:
    """
    Basic SQL sanitization - removes comments and normalizes whitespace.
    Note: This is not a security measure, just for logging/display.
    """
    # Remove SQL comments
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    # Normalize whitespace
    sql = ' '.join(sql.split())
    return sql.strip()


def truncate_string(s: str, max_length: int = 100, suffix: str = "...") -> str:
    """Truncate string to max length with suffix."""
    if len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix


def safe_json_dumps(obj: Any, default: str = "{}") -> str:
    """Safely serialize object to JSON."""
    try:
        return json.dumps(obj, default=str, ensure_ascii=False)
    except Exception:
        return default


def parse_table_reference(reference: str) -> tuple[str, str]:
    """
    Parse a table reference into schema and table name.
    
    Args:
        reference: Table reference like 'schema.table' or 'table'
        
    Returns:
        Tuple of (schema, table_name)
    """
    parts = reference.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    return "public", parts[0]


def format_row_count(count: int) -> str:
    """Format large row counts for display."""
    if count >= 1_000_000_000:
        return f"{count / 1_000_000_000:.2f}B"
    elif count >= 1_000_000:
        return f"{count / 1_000_000:.2f}M"
    elif count >= 1_000:
        return f"{count / 1_000:.2f}K"
    return str(count)


def compare_values(
    source_value: Any,
    target_value: Any,
    tolerance: float | None = None,
) -> bool:
    """
    Compare two values with optional tolerance for numeric values.
    
    Args:
        source_value: Value from source database
        target_value: Value from target database
        tolerance: Tolerance for numeric comparison (e.g., 0.01 for 1%)
        
    Returns:
        True if values match within tolerance
    """
    # Handle None cases
    if source_value is None and target_value is None:
        return True
    if source_value is None or target_value is None:
        return False

    # Numeric comparison with tolerance
    if tolerance is not None and isinstance(source_value, (int, float)) and isinstance(target_value, (int, float)):
        if source_value == 0 and target_value == 0:
            return True
        if source_value == 0:
            return abs(target_value) <= tolerance
        return abs((source_value - target_value) / source_value) <= tolerance

    # String comparison (case-insensitive)
    if isinstance(source_value, str) and isinstance(target_value, str):
        return source_value.strip().lower() == target_value.strip().lower()

    # Direct comparison
    return source_value == target_value


def extract_table_names_from_sql(sql: str) -> list[str]:
    """
    Extract table names from SQL query.
    
    This is a basic implementation - may not catch all cases.
    """
    # Common patterns for table references
    patterns = [
        r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
        r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
        r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
        r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
    ]
    
    tables = set()
    for pattern in patterns:
        matches = re.findall(pattern, sql, re.IGNORECASE)
        tables.update(matches)
    
    return list(tables)


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def chunk_list(lst: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def deep_merge(base: dict, override: dict) -> dict:
    """Deep merge two dictionaries."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
