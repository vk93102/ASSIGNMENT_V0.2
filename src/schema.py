from __future__ import annotations

import re
import hashlib
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path


_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "by",
    "can",
    "could",
    "did",
    "do",
    "does",
    "for",
    "from",
    "has",
    "have",
    "highest",
    "how",
    "in",
    "is",
    "level",
    "many",
    "most",
    "of",
    "on",
    "or",
    "the",
    "their",
    "to",
    "top",
    "what",
    "which",
    "with",
}


def _tokenize(text: str) -> list[str]:
    if not text:
        return []
    toks = [t.lower() for t in re.findall(r"[A-Za-z_]+", text)]
    out: list[str] = []
    for t in toks:
        t = t.strip("_")
        if not t or t in _STOPWORDS:
            continue
        out.append(t)
    return out


@dataclass(frozen=True)
class SchemaInfo:
    table_name: str
    columns: list[str]
    column_types: dict[str, str] = field(default_factory=dict)

    def to_prompt_context(self) -> dict:
        return {
            "table": self.table_name,
            "columns": self.columns,
            "column_types": self.column_types,
        }

    def select_relevant_columns(self, question: str, *, max_columns: int = 25) -> list[str]:
        if not question or not self.columns:
            return list(self.columns)

        tokens = [t for t in re.findall(r"[A-Za-z_]+", question.lower()) if t and t not in _STOPWORDS]
        if not tokens:
            return list(self.columns)

        selected: list[str] = []
        for col in self.columns:
            c = col.lower()
            if any(tok in c for tok in tokens):
                selected.append(col)
            if len(selected) >= max_columns:
                break

        return selected if selected else list(self.columns)

    def select_relevant_columns_semantic(self, question: str, *, max_columns: int = 20) -> list[str]:
        if not question or not self.columns:
            return list(self.columns)

        q_tokens = _tokenize(question)
        if not q_tokens:
            return list(self.columns)

        col_tokens: dict[str, set[str]] = {}
        df: dict[str, int] = {}
        for col in self.columns:
            parts = _tokenize(col.replace("_", " "))
            doc = set(parts)
            if not doc:
                continue
            col_tokens[col] = doc
            for t in doc:
                df[t] = df.get(t, 0) + 1

        n_docs = max(1, len(col_tokens))

        def idf(t: str) -> float:
            d = df.get(t, 0)
            return 1.0 + (0.0 if d == 0 else (n_docs / float(d)))

        q_set = set(q_tokens)
        scored: list[tuple[float, str]] = []
        q_lower = question.lower()
        for col, doc in col_tokens.items():
            overlap = q_set & doc
            if not overlap:
                continue
            score = sum(idf(t) for t in overlap)
            if col.lower() in q_lower:
                score *= 1.5
            scored.append((score, col))

        if not scored:
            return list(self.columns)

        scored.sort(key=lambda x: (-x[0], x[1]))
        selected = [c for _, c in scored[: max(1, int(max_columns))]]

        grouping_indicators = (
            " by ",
            "group",
            "between",  
            "across",  
            "compare", 
            "each ", 
            "per ",  
            "for each", 
        )
        
        has_grouping_intent = any(indicator in q_lower for indicator in grouping_indicators)
        
        if has_grouping_intent:
            dimension_candidates = ("age", "gender", "category", "type", "status", "level")
            
            for candidate in dimension_candidates:
                if candidate in (c.lower() for c in self.columns) and candidate not in (c.lower() for c in selected):
                    for original in self.columns:
                        if original.lower() == candidate:
                            selected.insert(0, original)
                            break
                    if len(selected) > int(max_columns):
                        selected = selected[: int(max_columns)]

        return selected

    def fingerprint(self) -> str:
        payload = {
            "table": self.table_name,
            "columns": list(self.columns),
            "types": dict(self.column_types),
        }
        raw = repr(payload).encode("utf-8", errors="ignore")
        return hashlib.sha256(raw).hexdigest()


class SQLiteSchemaIntrospector:
    def __init__(self, db_path: str | Path, *, table_name: str) -> None:
        self.db_path = Path(db_path)
        self.table_name = table_name

    def load(self) -> SchemaInfo:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(f'PRAGMA table_info("{self.table_name}")')
            rows = cur.fetchall()

        columns: list[str] = []
        column_types: dict[str, str] = {}
        for row in rows:
            if len(row) >= 3:
                name = str(row[1])
                typ = str(row[2])
                columns.append(name)
                column_types[name] = typ

        return SchemaInfo(table_name=self.table_name, columns=columns, column_types=column_types)
