import pytest

from iceloadberg.ports.registry import Registry


def test_registry_create():
    registry = Registry()
    registry.register("x", lambda config: ("ok", config["value"]))
    assert registry.create({"type": "x", "value": 123}) == ("ok", 123)


def test_registry_unknown_type():
    registry = Registry()
    with pytest.raises(ValueError):
        registry.create({"type": "unknown"})
