"""Configuration loading utilities."""

from pathlib import Path
from typing import Any
import yaml


def load_config(path: str) -> dict[str, Any]:
    """Load YAML configuration."""
    with Path(path).open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)
