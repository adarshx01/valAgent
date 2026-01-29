"""
Custom JSON encoder for handling special types.
"""

import json
from datetime import datetime, date
from decimal import Decimal
from typing import Any


class CustomJSONEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal, datetime, and other special types."""
    
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            # Convert Decimal to float for JSON serialization
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if hasattr(obj, 'model_dump'):
            # Handle Pydantic models
            return obj.model_dump()
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        return super().default(obj)


def json_dumps(obj: Any, **kwargs) -> str:
    """Serialize object to JSON string with custom encoder."""
    return json.dumps(obj, cls=CustomJSONEncoder, **kwargs)


def safe_json_serialize(obj: Any) -> Any:
    """
    Recursively convert an object to be JSON-serializable.
    """
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: safe_json_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [safe_json_serialize(item) for item in obj]
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if hasattr(obj, 'model_dump'):
        return safe_json_serialize(obj.model_dump())
    # Fallback: try to convert to string
    try:
        return str(obj)
    except Exception:
        return None