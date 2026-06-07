"""Message serialization — JSON, binary, and schema registry.

Provides pluggable serializers for message values and a schema
registry for enforcing message contracts.
"""

from __future__ import annotations

import json
import struct
from abc import ABC, abstractmethod
from typing import Any, Optional


class Serializer(ABC):
    """Base class for message serializers."""

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        """Serialize data to bytes."""

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to data."""

    @property
    @abstractmethod
    def content_type(self) -> str:
        """MIME content type for this serializer."""


class JsonSerializer(Serializer):
    """JSON serializer — human-readable, widely compatible."""

    def __init__(self, indent: Optional[int] = None, sort_keys: bool = False):
        self._indent = indent
        self._sort_keys = sort_keys

    def serialize(self, data: Any) -> bytes:
        return json.dumps(
            data, indent=self._indent, sort_keys=self._sort_keys
        ).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))

    @property
    def content_type(self) -> str:
        return "application/json"

    def __repr__(self) -> str:
        return "JsonSerializer()"


class BinarySerializer(Serializer):
    """Compact binary serializer using struct packing.

    Supports basic types: int, float, str, bytes, bool, None.
    Format: [1-byte type tag][data]

    Type tags:
        0 = None, 1 = bool, 2 = int (8 bytes), 3 = float (8 bytes),
        4 = str (4-byte length + UTF-8 bytes),
        5 = bytes (4-byte length + raw bytes),
        6 = list (4-byte count + elements),
        7 = dict (4-byte count + key-value pairs)
    """

    TAG_NONE = 0
    TAG_BOOL = 1
    TAG_INT = 2
    TAG_FLOAT = 3
    TAG_STR = 4
    TAG_BYTES = 5
    TAG_LIST = 6
    TAG_DICT = 7

    def serialize(self, data: Any) -> bytes:
        return self._pack(data)

    def deserialize(self, data: bytes) -> Any:
        value, _ = self._unpack(data, 0)
        return value

    def _pack(self, value: Any) -> bytes:
        if value is None:
            return struct.pack("B", self.TAG_NONE)
        elif isinstance(value, bool):
            return struct.pack("BB", self.TAG_BOOL, 1 if value else 0)
        elif isinstance(value, int):
            return struct.pack("!Bq", self.TAG_INT, value)
        elif isinstance(value, float):
            return struct.pack("!Bd", self.TAG_FLOAT, value)
        elif isinstance(value, str):
            encoded = value.encode("utf-8")
            return struct.pack("!BI", self.TAG_STR, len(encoded)) + encoded
        elif isinstance(value, bytes):
            return struct.pack("!BI", self.TAG_BYTES, len(value)) + value
        elif isinstance(value, (list, tuple)):
            parts = [struct.pack("!BI", self.TAG_LIST, len(value))]
            for item in value:
                parts.append(self._pack(item))
            return b"".join(parts)
        elif isinstance(value, dict):
            parts = [struct.pack("!BI", self.TAG_DICT, len(value))]
            for k, v in value.items():
                parts.append(self._pack(k))
                parts.append(self._pack(v))
            return b"".join(parts)
        else:
            raise TypeError(f"Cannot serialize type: {type(value).__name__}")

    def _unpack(self, data: bytes, offset: int) -> tuple[Any, int]:
        tag = struct.unpack_from("B", data, offset)[0]
        offset += 1

        if tag == self.TAG_NONE:
            return None, offset
        elif tag == self.TAG_BOOL:
            val = struct.unpack_from("B", data, offset)[0]
            return bool(val), offset + 1
        elif tag == self.TAG_INT:
            val = struct.unpack_from("!q", data, offset)[0]
            return val, offset + 8
        elif tag == self.TAG_FLOAT:
            val = struct.unpack_from("!d", data, offset)[0]
            return val, offset + 8
        elif tag == self.TAG_STR:
            length = struct.unpack_from("!I", data, offset)[0]
            offset += 4
            val = data[offset:offset + length].decode("utf-8")
            return val, offset + length
        elif tag == self.TAG_BYTES:
            length = struct.unpack_from("!I", data, offset)[0]
            offset += 4
            val = data[offset:offset + length]
            return val, offset + length
        elif tag == self.TAG_LIST:
            count = struct.unpack_from("!I", data, offset)[0]
            offset += 4
            items = []
            for _ in range(count):
                val, offset = self._unpack(data, offset)
                items.append(val)
            return items, offset
        elif tag == self.TAG_DICT:
            count = struct.unpack_from("!I", data, offset)[0]
            offset += 4
            result = {}
            for _ in range(count):
                key, offset = self._unpack(data, offset)
                val, offset = self._unpack(data, offset)
                result[key] = val
            return result, offset
        else:
            raise ValueError(f"Unknown type tag: {tag}")

    @property
    def content_type(self) -> str:
        return "application/x-brokerlite-binary"

    def __repr__(self) -> str:
        return "BinarySerializer()"


class Schema:
    """A message schema — defines required and optional fields with types."""

    def __init__(
        self,
        name: str,
        version: int,
        fields: dict[str, str],
        required: Optional[list[str]] = None,
    ):
        self.name = name
        self.version = version
        self.fields = fields  # field_name -> type_name
        self.required = required or list(fields.keys())

    TYPE_MAP = {
        "string": str,
        "int": int,
        "float": (int, float),
        "bool": bool,
        "list": list,
        "dict": dict,
        "any": object,
    }

    def validate(self, data: dict) -> tuple[bool, list[str]]:
        """Validate data against the schema.

        Returns (is_valid, list_of_errors).
        """
        errors = []

        if not isinstance(data, dict):
            return False, ["Data must be a dictionary"]

        for field_name in self.required:
            if field_name not in data:
                errors.append(f"Missing required field: {field_name!r}")

        for field_name, value in data.items():
            if field_name in self.fields:
                expected_type = self.fields[field_name]
                python_type = self.TYPE_MAP.get(expected_type)
                if python_type and not isinstance(value, python_type):
                    errors.append(
                        f"Field {field_name!r}: expected {expected_type}, "
                        f"got {type(value).__name__}"
                    )

        return len(errors) == 0, errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "fields": self.fields,
            "required": self.required,
        }

    def __repr__(self) -> str:
        return f"Schema(name={self.name!r}, version={self.version})"


class SchemaRegistry:
    """In-memory schema registry for message validation.

    Stores schemas by name and version, validates messages against
    registered schemas, and checks backward/forward compatibility.
    """

    def __init__(self):
        self._schemas: dict[str, dict[int, Schema]] = {}  # name -> {version -> Schema}

    def register(self, schema: Schema) -> None:
        """Register a schema."""
        if schema.name not in self._schemas:
            self._schemas[schema.name] = {}
        self._schemas[schema.name][schema.version] = schema

    def get(self, name: str, version: Optional[int] = None) -> Optional[Schema]:
        """Get a schema. If no version specified, returns the latest."""
        versions = self._schemas.get(name)
        if not versions:
            return None
        if version is not None:
            return versions.get(version)
        latest_version = max(versions.keys())
        return versions[latest_version]

    def validate(self, name: str, data: dict, version: Optional[int] = None) -> tuple[bool, list[str]]:
        """Validate data against a registered schema."""
        schema = self.get(name, version)
        if schema is None:
            return False, [f"Schema {name!r} not found"]
        return schema.validate(data)

    def list_schemas(self) -> dict[str, list[int]]:
        """List all schemas and their versions."""
        return {
            name: sorted(versions.keys())
            for name, versions in self._schemas.items()
        }

    def is_backward_compatible(self, name: str, new_version: int) -> tuple[bool, list[str]]:
        """Check if a new schema version is backward compatible.

        Backward compatible = new schema can read data written by old schema.
        This means the new schema should not add new required fields.
        """
        old_schema = self.get(name, new_version - 1)
        new_schema = self.get(name, new_version)

        if not old_schema or not new_schema:
            return True, []

        errors = []
        old_required = set(old_schema.required)
        new_required = set(new_schema.required)

        new_fields = new_required - old_required
        if new_fields:
            errors.append(
                f"New required fields added (breaks backward compatibility): "
                f"{new_fields}"
            )

        return len(errors) == 0, errors

    def __repr__(self) -> str:
        total = sum(len(v) for v in self._schemas.values())
        return f"SchemaRegistry(schemas={len(self._schemas)}, versions={total})"
