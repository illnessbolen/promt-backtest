"""Configuration: config.yaml merged with environment variables / .env overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def load_dotenv(path: Path) -> None:
    """Minimal .env loader (KEY=VALUE lines). Existing environment variables win."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass
class Config:
    user: str
    db_path: Path
    log_dir: Path
    log_level: str
    data_api: str
    gamma_api: str
    http: dict[str, Any] = field(default_factory=dict)
    rate_limits: dict[str, float] = field(default_factory=dict)
    sync: dict[str, Any] = field(default_factory=dict)
    fees: dict[str, Any] = field(default_factory=dict)

    @property
    def crypto_fee_rate(self) -> float:
        return float(self.fees.get("crypto_rate", 0.07))


def load_config(path: str | os.PathLike[str] | None = None, root: Path | None = None) -> Config:
    root = root or Path.cwd()
    load_dotenv(root / ".env")
    cfg_path = Path(path or os.environ.get("BOSONA_CONFIG", "config.yaml"))
    if not cfg_path.is_absolute():
        cfg_path = root / cfg_path
    raw: dict[str, Any] = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}

    def resolve(p: str) -> Path:
        q = Path(p)
        return q if q.is_absolute() else root / q

    api = raw.get("api", {})
    return Config(
        user=os.environ.get("BOSONA_USER", raw["user"]).lower(),
        db_path=resolve(os.environ.get("BOSONA_DB_PATH", raw.get("db_path", "data/bosona.db"))),
        log_dir=resolve(os.environ.get("BOSONA_LOG_DIR", raw.get("log_dir", "logs"))),
        log_level=os.environ.get("BOSONA_LOG_LEVEL", raw.get("log_level", "INFO")),
        data_api=api.get("data", "https://data-api.polymarket.com").rstrip("/"),
        gamma_api=api.get("gamma", "https://gamma-api.polymarket.com").rstrip("/"),
        http=raw.get("http", {}),
        rate_limits={k: float(v) for k, v in (raw.get("rate_limits") or {}).items()},
        sync=raw.get("sync", {}),
        fees=raw.get("fees", {}),
    )
