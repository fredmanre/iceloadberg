from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class Registry:
    """
    Minimal plugin registry. Maps config 'type' -> constructor(config) -> instance
    """
    _items = dict[str, Callable[[dict[str, Any]], Any]]

    def register(self,
                 type_name: str,
                 constructor: Callable[[dict[str, Any]], Any]) -> None:
        """
        type_name: unique name for this type of plugin (e.g. "jdbc", "csv", "influxdb")
        constructor: constructor function that takes a config dict and returns an instance of the plugin
        """
        if type_name in self._items:
            raise ValueError(f"Type already registered: {type_name}")
        self._items[type_name] = constructor

    def create(self, config: dict[str, Any]) -> Any:
        type_name = config.get("type")
        if not type_name:
            raise ValueError("Missing required field 'type' in config")
        if type_name not in self._items:
            raise ValueError(
                f"Unknown type: '{type_name}'."
                f"Registered: {sorted(self._items.keys())}"
            )
        return self._items[type_name](config)
