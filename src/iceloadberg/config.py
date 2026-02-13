from __future__ import annotations

import yaml


def load_config(path: str) -> dict:
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception as err:
        raise RuntimeError(
            f"Failed to load config from {path}: {err}"
        ) from err
