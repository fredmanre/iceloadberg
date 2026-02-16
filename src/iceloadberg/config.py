from __future__ import annotations
from typing import Any

import os
import re
import yaml


_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _expand_str(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        var = match.group(1)
        try:
            return os.environ[var]
        except KeyError as exc:
            raise RuntimeError(f"Missing required env var: {var}") from exc

    return _ENV_PATTERN.sub(repl, text)


def _expand_env_vars(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(v) for v in value]
    if isinstance(value, str):
        return _expand_str(value)
    return value


def load_config(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        return _expand_env_vars(raw)
    except Exception as err:
        raise RuntimeError(f"Failed to load config from {path}: {err}") from err
