from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path

from src.support import SQLValidationOutput


# Security strategy: Whitelist-based validation (only SELECT/WITH)
# instead of blacklist. This is safer because:
# 1) Prevents evasion via case variation, comments, concatenation
# 2) Explicit about LLM capabilities (no ambiguity)
# 3) Fail-safe: unknown patterns rejected by default



_ALLOWED = {"select", "with"}

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
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


def _normalize_sql(sql: str) -> str:
    sql = sql.strip()
    sql = _strip_sql_comments(sql).strip()
    sql = re.sub(r"\s+", " ", sql).strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql





""" Detect stacked SQL queries (e.g., 'SELECT ...; DELETE ...').
    Critical for preventing multi-statement injection attacks. LLMs can be
    tricked with hidden prompts like 'SELECT ...; DELETE * FROM'.
    We parse manually (not regex) to handle quotes/comments correctly
    and avoid false positives on strings containing semicolons.
"""

def _has_multiple_statements(sql: str) -> bool:
    in_single = False
    in_double = False
    in_backtick = False
    idxs: list[int] = []
    for i, ch in enumerate(sql):
        if ch == "'" and not in_double and not in_backtick:
            if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                continue
            in_single = not in_single
        elif ch == '"' and not in_single and not in_backtick:
            in_double = not in_double
        elif ch == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
        elif ch == ";" and not in_single and not in_double and not in_backtick:
            idxs.append(i)

    if not idxs:
        return False
    if len(idxs) > 1:
        return True
    tail = sql[idxs[0] + 1 :].strip()
    return tail != ""


def _extract_cte_names(sql: str) -> set[str]:
    lower = sql.lower()
    if not lower.startswith("with "):
        return set()

    names: set[str] = set()
    prefix = sql[: min(len(sql), 5000)]
    for m in re.finditer(r"\bwith\s+|,\s*", prefix, flags=re.IGNORECASE):
        start = m.end()
        mm = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s+as\s*\(", prefix[start:], flags=re.IGNORECASE)
        if mm:
            names.add(mm.group(1))

    m0 = re.match(r"\s*with\s+([A-Za-z_][A-Za-z0-9_]*)\s+as\s*\(", prefix, flags=re.IGNORECASE)
    if m0:
        names.add(m0.group(1))

    return {n.lower() for n in names}


def _find_referenced_tables(sql: str) -> set[str]:
    refs: set[str] = set()
    for kw in ("from", "join"):
        for m in re.finditer(rf"\b{kw}\b\s+([^\s,()]+)", sql, flags=re.IGNORECASE):
            token = m.group(1).strip()
            if token.startswith("("):
                continue
            token = token.strip('"`[]')
            token = token.split(".")[0]
            refs.add(token.lower())
    return refs


def _ensure_reasonable_limit(sql: str, limit: int = 100) -> str:
    lower = sql.lower()
    if " limit " in lower or lower.endswith(" limit"):
        return sql
    if any(x in lower for x in (" group by ", " count(", " avg(", " sum(", " min(", " max(") ):
        return sql
    return f"{sql} LIMIT {limit}"


def _extract_column_identifiers_sqlparse(sql: str) -> set[str]:
    try:
        import sqlparse 
        from sqlparse.sql import Function, Identifier, IdentifierList, Parenthesis, TokenList 
        from sqlparse.tokens import Wildcard  
    except Exception:
        return set()

    try:
        statements = sqlparse.parse(sql)
    except Exception:
        return set()
    if not statements:
        return set()

    names: set[str] = set()
    aliases: set[str] = set()

    function_names = {
        "avg",
        "sum",
        "count",
        "min",
        "max",
        "coalesce",
        "ifnull",
        "nullif",
        "round",
        "cast",
        "substr",
        "lower",
        "upper",
        "abs",
        "length",
        "date",
        "datetime",
        "strftime",
    }

    def _identifier_contains_function(ident: "Identifier") -> bool:
        for t in ident.tokens:
            if isinstance(t, Function):
                return True
        return False

    def collect_aliases(token_list: "TokenList") -> None:
        # PASS 1: Collect all aliases (e.g., 'AVG(...) AS avg_value' -> 'avg_value')
        # This must run before validation because queries like:
        #   SELECT AVG(addiction_level) AS avg_addiction FROM table
        # would fail if we validated columns without knowing about 'avg_addiction' alias
        for tok in token_list.tokens:
            if tok is None:
                continue
            if isinstance(tok, IdentifierList):
                for ident in tok.get_identifiers():
                    if isinstance(ident, Identifier):
                        alias = ident.get_alias()
                        if alias:
                            aliases.add(alias.lower())
            elif isinstance(tok, Identifier):
                alias = tok.get_alias()
                if alias:
                    aliases.add(alias.lower())
            elif isinstance(tok, TokenList):
                collect_aliases(tok)

    def walk(token_list: "TokenList") -> None:
        for tok in token_list.tokens:
            if tok is None:
                continue
            if isinstance(tok, IdentifierList):
                for ident in tok.get_identifiers():
                    if isinstance(ident, Identifier):
                        walk(ident)
            elif isinstance(tok, Identifier):
                if _identifier_contains_function(tok):
                    walk(tok)
                    continue
                real = tok.get_real_name()
                if real:
                    r = real.lower()
                    if r not in aliases and r not in function_names:
                        names.add(r)
            elif isinstance(tok, Function):
                for child in tok.tokens:
                    if isinstance(child, Parenthesis):
                        walk(child)
            elif tok.ttype is Wildcard:
                pass
            elif isinstance(tok, TokenList):
                walk(tok)

    collect_aliases(statements[0])
    walk(statements[0])
    return names


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

        if _has_multiple_statements(normalized):
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error="Multiple statements are not allowed.",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        for kw in _DISALLOWED:
            if re.search(rf"\b{re.escape(kw)}\b", lower):
                return SQLValidationOutput(
                    is_valid=False,
                    validated_sql=None,
                    error=f"Disallowed keyword in SQL: {kw.upper()}.",
                    timing_ms=(time.perf_counter() - start) * 1000,
                )

        cte_names = _extract_cte_names(normalized)
        referenced = _find_referenced_tables(normalized)
        allowed_tables = {table_name.lower(), *cte_names}
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

        if allowed_columns:
            allowed_lower = {c.lower() for c in allowed_columns}
            for m in re.finditer(rf"\b{re.escape(table_name)}\.([A-Za-z_][A-Za-z0-9_]*)\b", normalized, flags=re.IGNORECASE):
                col = m.group(1)
                if col.lower() not in allowed_lower:
                    return SQLValidationOutput(
                        is_valid=False,
                        validated_sql=None,
                        error=f"Unknown column referenced: {col}",
                        timing_ms=(time.perf_counter() - start) * 1000,
                    )

            extracted = _extract_column_identifiers_sqlparse(normalized)
            if extracted:
                ignore = {table_name.lower(), *cte_names}
                for name in extracted:
                    if name in ignore:
                        continue
                    if name in {
                        "select",
                        "from",
                        "where",
                        "group",
                        "order",
                        "limit",
                        "join",
                        "on",
                        "as",
                        "and",
                        "or",
                        "when",
                        "then",
                        "else",
                        "end",
                        "with",
                    }:
                        continue
                    if name not in allowed_lower:
                        return SQLValidationOutput(
                            is_valid=False,
                            validated_sql=None,
                            error=f"Unknown column referenced: {name}",
                            timing_ms=(time.perf_counter() - start) * 1000,
                        )

        validated_sql = _ensure_reasonable_limit(normalized)

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
                error=f"SQL failed to parse/plan: {exc}. Suggestion: verify column names and table name match the schema.",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        return SQLValidationOutput(
            is_valid=True,
            validated_sql=validated_sql,
            error=None,
            timing_ms=(time.perf_counter() - start) * 1000,
        )
