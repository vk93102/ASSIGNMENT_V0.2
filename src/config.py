from __future__ import annotations

import os
from dataclasses import dataclass


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _get_int_optional(name: str, default: int | None) -> int | None:
    raw = os.getenv(name, "").strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _get_float_optional(name: str, default: float | None) -> float | None:
    raw = os.getenv(name, "").strip()
    if raw == "":
        return default
    try:
        return float(raw)
    except Exception:
        return default


@dataclass(frozen=True)
class Config:
    sqlite_max_rows: int = 100
    sqlite_query_timeout_ms: float | None = None 

    schema_max_columns: int = 25
    schema_filter_mode: str = "semantic"  

    pipeline_cache_size: int = 256
    pipeline_cache_ttl_seconds: float | None = 300.0

    fallback_cache_size: int = 256
    fallback_cache_ttl_seconds: float | None = 600.0
    llm_cache_size: int = 128
    llm_cache_ttl_seconds: float | None = None
    llm_max_retries: int = 2
    llm_retry_base_ms: float = 200.0
    llm_timeout_ms: int | None = 120_000
    answer_sample_rows: int = 30
    answer_numeric_stats_rows: int = 200

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            sqlite_max_rows=_get_int("SQLITE_MAX_ROWS", 100),
            sqlite_query_timeout_ms=_get_float_optional("SQLITE_QUERY_TIMEOUT_MS", None),
            schema_max_columns=_get_int("SCHEMA_MAX_COLUMNS", 25),
            schema_filter_mode=(os.getenv("SCHEMA_FILTER_MODE", "semantic") or "semantic").strip().lower(),
            pipeline_cache_size=_get_int("PIPELINE_CACHE_SIZE", 256),
            pipeline_cache_ttl_seconds=_get_float_optional("PIPELINE_CACHE_TTL_SECONDS", 300.0),
            fallback_cache_size=_get_int("FALLBACK_CACHE_SIZE", 256),
            fallback_cache_ttl_seconds=_get_float_optional("FALLBACK_CACHE_TTL_SECONDS", 600.0),
            llm_cache_size=_get_int("LLM_CACHE_SIZE", 128),
            llm_cache_ttl_seconds=_get_float_optional("LLM_CACHE_TTL_SECONDS", None),
            llm_max_retries=_get_int("LLM_MAX_RETRIES", 2),
            llm_retry_base_ms=float(os.getenv("LLM_RETRY_BASE_MS", "200") or "200"),
            llm_timeout_ms=_get_int_optional("LLM_TIMEOUT_MS", 120_000),
            answer_sample_rows=_get_int("ANSWER_SAMPLE_ROWS", 30),
            answer_numeric_stats_rows=_get_int("ANSWER_NUMERIC_STATS_ROWS", 200),
        )
