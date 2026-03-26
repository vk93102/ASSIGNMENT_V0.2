from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path

from src.types import SQLValidationOutput


_DISALLOWED = {
    "delete",
    "update",
    "insert",
    "drop",
    "alter",
    "create",
    "replace",
    "truncate",
    "vacuum",
    "pragma",
    "attach",
    "detach",
    "reindex",
}


def _strip_sql_comments(sql: str) -> str:
    # Remove -- line comments
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    # Remove /* */ block comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


def _normalize_sql(sql: str) -> str:
    sql = sql.strip()
    sql = _strip_sql_comments(sql).strip()
    # Collapse whitespace for easier pattern checks
    sql = re.sub(r"\s+", " ", sql).strip()
    return sql


def _extract_cte_names(sql: str) -> set[str]:
    # Very small CTE name extractor: WITH name AS ( ... ), name2 AS (...)
    lower = sql.lower()
    if not lower.startswith("with "):
        return set()

    names: set[str] = set()
    # Only scan the prefix to avoid catastrophic backtracking
    prefix = sql[: min(len(sql), 5000)]
    for m in re.finditer(r"\bwith\s+|,\s*", prefix, flags=re.IGNORECASE):
        # Start scanning at match end for `name AS (`
        start = m.end()
        mm = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s+as\s*\(", prefix[start:], flags=re.IGNORECASE)
        if mm:
            names.add(mm.group(1))

    # Also handle the first CTE when prefix starts with WITH directly
    m0 = re.match(r"\s*with\s+([A-Za-z_][A-Za-z0-9_]*)\s+as\s*\(", prefix, flags=re.IGNORECASE)
    if m0:
        names.add(m0.group(1))

    return {n.lower() for n in names}


def _find_referenced_tables(sql: str) -> set[str]:
    # Extract identifiers after FROM / JOIN. Ignores subqueries.
    refs: set[str] = set()
    for kw in ("from", "join"):
        for m in re.finditer(rf"\b{kw}\b\s+([^\s,()]+)", sql, flags=re.IGNORECASE):
            token = m.group(1).strip()
            if token.startswith("("):
                continue
            token = token.strip('"`[]')
            # Remove trailing alias marker like table AS t
            token = token.split(".")[0]
            refs.add(token.lower())
    return refs


def _ensure_reasonable_limit(sql: str, limit: int = 100) -> str:
    lower = sql.lower()
    if " limit " in lower or lower.endswith(" limit"):
        return sql
    # If query is aggregating or grouping, don't inject limit (could change results)
    if any(x in lower for x in (" group by ", " count(", " avg(", " sum(", " min(", " max(") ):
        return sql
    return f"{sql} LIMIT {limit}"


class SQLValidator:
    @classmethod
    def validate(
        cls,
        sql: str | None,
        *,
        db_path: str | Path,
        table_name: str,
        allowed_columns: set[str] | None = None,
    ) -> SQLValidationOutput:
        start = time.perf_counter()

        if sql is None:
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error="No SQL provided",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        normalized = _normalize_sql(sql)
        lower = normalized.lower()

        if not (lower.startswith("select ") or lower.startswith("with ")):
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error="Only SELECT queries are allowed.",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        # Reject multiple statements.
        if ";" in normalized:
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error="Multiple statements are not allowed.",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        # Reject disallowed keywords.
        for kw in _DISALLOWED:
            if re.search(rf"\b{re.escape(kw)}\b", lower):
                return SQLValidationOutput(
                    is_valid=False,
                    validated_sql=None,
                    error=f"Disallowed keyword in SQL: {kw.upper()}.",
                    timing_ms=(time.perf_counter() - start) * 1000,
                )

        # Restrict tables.
        cte_names = _extract_cte_names(normalized)
        referenced = _find_referenced_tables(normalized)
        allowed_tables = {table_name.lower(), *cte_names}
        # sqlite_master is a common escape hatch
        if "sqlite_master" in referenced:
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error="Access to sqlite_master is not allowed.",
                timing_ms=(time.perf_counter() - start) * 1000,
            )
        unexpected = {t for t in referenced if t not in allowed_tables}
        if unexpected:
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error=f"Unexpected table(s): {sorted(unexpected)}. Only '{table_name}' is allowed.",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        # Best-effort column allowlist check: only enforce if we can find table-qualified cols.
        if allowed_columns:
            for m in re.finditer(rf"\b{re.escape(table_name)}\.([A-Za-z_][A-Za-z0-9_]*)\b", normalized, flags=re.IGNORECASE):
                col = m.group(1)
                if col not in allowed_columns:
                    return SQLValidationOutput(
                        is_valid=False,
                        validated_sql=None,
                        error=f"Unknown column referenced: {col}",
                        timing_ms=(time.perf_counter() - start) * 1000,
                    )

        validated_sql = _ensure_reasonable_limit(normalized)

        # Validate against SQLite parser and schema.
        try:
            with sqlite3.connect(f"file:{Path(db_path).as_posix()}?mode=ro", uri=True) as conn:
                try:
                    conn.execute("PRAGMA query_only = ON")
                except Exception:
                    pass
                conn.execute(f"EXPLAIN QUERY PLAN {validated_sql}")
        except Exception as exc:
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error=f"SQL failed to parse/plan: {exc}",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        return SQLValidationOutput(
            is_valid=True,
            validated_sql=validated_sql,
            error=None,
            timing_ms=(time.perf_counter() - start) * 1000,
        )
