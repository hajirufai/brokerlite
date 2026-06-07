"""Tests for serializers and schema registry."""

import pytest
from brokerlite.serializer import (
    JsonSerializer, BinarySerializer, Schema, SchemaRegistry,
)


class TestJsonSerializer:
    def test_roundtrip_dict(self):
        s = JsonSerializer()
        data = {"name": "Alice", "age": 30}
        raw = s.serialize(data)
        result = s.deserialize(raw)
        assert result == data

    def test_roundtrip_list(self):
        s = JsonSerializer()
        data = [1, 2, 3, "four"]
        assert s.deserialize(s.serialize(data)) == data

    def test_content_type(self):
        assert JsonSerializer().content_type == "application/json"


class TestBinarySerializer:
    def test_roundtrip_none(self):
        s = BinarySerializer()
        assert s.deserialize(s.serialize(None)) is None

    def test_roundtrip_bool(self):
        s = BinarySerializer()
        assert s.deserialize(s.serialize(True)) is True
        assert s.deserialize(s.serialize(False)) is False

    def test_roundtrip_int(self):
        s = BinarySerializer()
        assert s.deserialize(s.serialize(42)) == 42
        assert s.deserialize(s.serialize(-1)) == -1

    def test_roundtrip_float(self):
        s = BinarySerializer()
        val = 3.14159
        assert abs(s.deserialize(s.serialize(val)) - val) < 1e-10

    def test_roundtrip_string(self):
        s = BinarySerializer()
        assert s.deserialize(s.serialize("hello")) == "hello"
        assert s.deserialize(s.serialize("")) == ""

    def test_roundtrip_bytes(self):
        s = BinarySerializer()
        data = b"\x00\x01\x02\xff"
        assert s.deserialize(s.serialize(data)) == data

    def test_roundtrip_list(self):
        s = BinarySerializer()
        data = [1, "two", 3.0, True, None]
        assert s.deserialize(s.serialize(data)) == data

    def test_roundtrip_dict(self):
        s = BinarySerializer()
        data = {"name": "Alice", "age": 30, "active": True}
        assert s.deserialize(s.serialize(data)) == data

    def test_nested_structure(self):
        s = BinarySerializer()
        data = {"users": [{"name": "Bob", "scores": [10, 20]}]}
        assert s.deserialize(s.serialize(data)) == data

    def test_unsupported_type_raises(self):
        s = BinarySerializer()
        with pytest.raises(TypeError):
            s.serialize(set([1, 2, 3]))

    def test_content_type(self):
        assert "binary" in BinarySerializer().content_type


class TestSchema:
    def test_validate_valid(self):
        schema = Schema("order", 1, {
            "product": "string",
            "quantity": "int",
        })
        valid, errors = schema.validate({"product": "Widget", "quantity": 5})
        assert valid is True
        assert errors == []

    def test_validate_missing_required(self):
        schema = Schema("order", 1, {
            "product": "string",
            "quantity": "int",
        })
        valid, errors = schema.validate({"product": "Widget"})
        assert valid is False
        assert any("quantity" in e for e in errors)

    def test_validate_wrong_type(self):
        schema = Schema("order", 1, {"quantity": "int"})
        valid, errors = schema.validate({"quantity": "not_a_number"})
        assert valid is False

    def test_validate_non_dict(self):
        schema = Schema("test", 1, {})
        valid, errors = schema.validate("not a dict")
        assert valid is False

    def test_optional_fields(self):
        schema = Schema(
            "order", 1,
            {"product": "string", "notes": "string"},
            required=["product"],
        )
        valid, _ = schema.validate({"product": "Widget"})
        assert valid is True

    def test_to_dict(self):
        schema = Schema("order", 1, {"product": "string"})
        d = schema.to_dict()
        assert d["name"] == "order"
        assert d["version"] == 1


class TestSchemaRegistry:
    def test_register_and_get(self):
        reg = SchemaRegistry()
        schema = Schema("order", 1, {"product": "string"})
        reg.register(schema)
        result = reg.get("order")
        assert result is not None
        assert result.name == "order"

    def test_get_specific_version(self):
        reg = SchemaRegistry()
        reg.register(Schema("order", 1, {"product": "string"}))
        reg.register(Schema("order", 2, {"product": "string", "qty": "int"}))
        v1 = reg.get("order", 1)
        assert len(v1.fields) == 1

    def test_get_latest(self):
        reg = SchemaRegistry()
        reg.register(Schema("order", 1, {"product": "string"}))
        reg.register(Schema("order", 2, {"product": "string", "qty": "int"}))
        latest = reg.get("order")
        assert latest.version == 2

    def test_get_nonexistent(self):
        reg = SchemaRegistry()
        assert reg.get("nope") is None

    def test_validate(self):
        reg = SchemaRegistry()
        reg.register(Schema("order", 1, {"product": "string"}))
        valid, _ = reg.validate("order", {"product": "Widget"})
        assert valid

    def test_list_schemas(self):
        reg = SchemaRegistry()
        reg.register(Schema("order", 1, {}))
        reg.register(Schema("order", 2, {}))
        reg.register(Schema("event", 1, {}))
        schemas = reg.list_schemas()
        assert sorted(schemas["order"]) == [1, 2]
        assert schemas["event"] == [1]

    def test_backward_compatibility_ok(self):
        reg = SchemaRegistry()
        reg.register(Schema("order", 1, {"product": "string"}, required=["product"]))
        reg.register(Schema("order", 2, {"product": "string", "notes": "string"}, required=["product"]))
        ok, errors = reg.is_backward_compatible("order", 2)
        assert ok is True

    def test_backward_compatibility_broken(self):
        reg = SchemaRegistry()
        reg.register(Schema("order", 1, {"product": "string"}, required=["product"]))
        reg.register(Schema("order", 2, {"product": "string", "qty": "int"}, required=["product", "qty"]))
        ok, errors = reg.is_backward_compatible("order", 2)
        assert ok is False
