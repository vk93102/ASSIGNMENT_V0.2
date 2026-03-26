from __future__ import annotations

import json
import os
import re
import time
from typing import Any

from src.types import SQLGenerationOutput, AnswerGenerationOutput

DEFAULT_MODEL = "openai/gpt-5-nano"


class OpenRouterLLMClient:
    """LLM client using the OpenRouter SDK for chat completions."""

    provider_name = "openrouter"

    def __init__(self, api_key: str, model: str | None = None) -> None:
        try:
            from openrouter import OpenRouter
        except ModuleNotFoundError as exc:
            raise RuntimeError("Missing dependency: install 'openrouter'.") from exc
        self.model = model or os.getenv("OPENROUTER_MODEL", DEFAULT_MODEL)
        self._client = OpenRouter(api_key=api_key)
        self._stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    def _update_usage_stats(self, res: Any) -> None:
        self._stats["llm_calls"] = int(self._stats.get("llm_calls", 0)) + 1

        usage = getattr(res, "usage", None)
        if usage is None and isinstance(res, dict):
            usage = res.get("usage")

        def _get(field: str) -> int:
            if usage is None:
                return 0
            if isinstance(usage, dict):
                v = usage.get(field)
            else:
                v = getattr(usage, field, None)
            try:
                return int(v) if v is not None else 0
            except Exception:
                return 0

        prompt_tokens = _get("prompt_tokens")
        completion_tokens = _get("completion_tokens")
        total_tokens = _get("total_tokens")
        if total_tokens == 0:
            total_tokens = prompt_tokens + completion_tokens

        self._stats["prompt_tokens"] = int(self._stats.get("prompt_tokens", 0)) + prompt_tokens
        self._stats["completion_tokens"] = int(self._stats.get("completion_tokens", 0)) + completion_tokens
        self._stats["total_tokens"] = int(self._stats.get("total_tokens", 0)) + total_tokens

    def _chat(self, messages: list[dict[str, str]], temperature: float, max_tokens: int) -> str:
        res = self._client.chat.send(
            messages=messages,
            model=self.model,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=False,
        )

        # Token counting (required for evaluation).
        self._update_usage_stats(res)

        choices = getattr(res, "choices", None) or []
        if not choices:
            raise RuntimeError("OpenRouter response contained no choices.")
        content = getattr(getattr(choices[0], "message", None), "content", None)
        if not isinstance(content, str):
            raise RuntimeError("OpenRouter response content is not text.")
        return content.strip()

    @staticmethod
    def _extract_sql(text: str) -> str | None:
        raw = text.strip()

        # Strip fenced blocks if present.
        m = re.search(r"```(?:json|sql)?\s*(.*?)```", raw, flags=re.IGNORECASE | re.DOTALL)
        if m:
            raw = m.group(1).strip()

        # Prefer strict JSON: {"sql": "..."}
        if raw.startswith("{") and raw.endswith("}"):
            try:
                parsed = json.loads(raw)
                sql = parsed.get("sql")
                if isinstance(sql, str) and sql.strip():
                    return sql.strip()
                return None
            except json.JSONDecodeError:
                pass

        # Heuristic fallback: find first SELECT or WITH.
        lower = raw.lower()
        idx_select = lower.find("select ")
        idx_with = lower.find("with ")
        idxs = [i for i in (idx_select, idx_with) if i >= 0]
        if not idxs:
            return None
        idx = min(idxs)
        return raw[idx:].strip()

    def generate_sql(self, question: str, context: dict) -> SQLGenerationOutput:
        table = context.get("table", "gaming_mental_health") if isinstance(context, dict) else "gaming_mental_health"
        columns = context.get("columns", []) if isinstance(context, dict) else []

        system_prompt = (
            "You are a careful SQLite analytics SQL assistant. "
            "You MUST output a single JSON object with a single key 'sql'. "
            "Rules: only read-only queries (SELECT / WITH); never use DELETE/UPDATE/INSERT/DROP/ALTER/CREATE/PRAGMA/ATTACH; "
            "use only the provided table and columns; do not reference sqlite_master; "
            "prefer aggregates over raw rows; use LIMIT for top-N."
        )
        user_prompt = (
            f"Table: {table}\n"
            f"Columns: {columns}\n\n"
            f"Question: {question}\n\n"
            "Return JSON only, like: {\"sql\": \"SELECT ...\"}"
        )

        start = time.perf_counter()
        error = None
        sql = None

        try:
            text = self._chat(
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.0,
                max_tokens=240,
            )
            sql = self._extract_sql(text)
        except Exception as exc:
            error = str(exc)

        timing_ms = (time.perf_counter() - start) * 1000
        llm_stats = self.pop_stats()
        llm_stats["model"] = self.model

        return SQLGenerationOutput(
            sql=sql,
            timing_ms=timing_ms,
            llm_stats=llm_stats,
            error=error,
        )

    def generate_answer(self, question: str, sql: str | None, rows: list[dict[str, Any]]) -> AnswerGenerationOutput:
        if not sql:
            return AnswerGenerationOutput(
                answer="I cannot answer this with the available table and schema. Please rephrase using known survey fields.",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": self.model},
                error=None,
            )
        if not rows:
            return AnswerGenerationOutput(
                answer="Query executed, but no rows were returned.",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": self.model},
                error=None,
            )

        # Fast-path: one-row, one-column scalar results (common for COUNT/AVG).
        if len(rows) == 1 and isinstance(rows[0], dict) and len(rows[0]) == 1:
            (k, v), = rows[0].items()
            if isinstance(v, (int, float, str)):
                return AnswerGenerationOutput(
                    answer=f"{k}: {v}",
                    timing_ms=0.0,
                    llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": self.model},
                    error=None,
                )

        system_prompt = (
            "You are a concise analytics assistant. "
            "Use only the provided SQL results. Do not invent data."
        )
        user_prompt = (
            f"Question:\n{question}\n\nSQL:\n{sql}\n\n"
            f"Rows (JSON):\n{json.dumps(rows[:30], ensure_ascii=True)}\n\n"
            "Write a concise answer in plain English."
        )

        start = time.perf_counter()
        error = None
        answer = ""

        try:
            answer = self._chat(
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.2,
                max_tokens=220,
            )
        except Exception as exc:
            error = str(exc)
            answer = f"Error generating answer: {error}"

        timing_ms = (time.perf_counter() - start) * 1000
        llm_stats = self.pop_stats()
        llm_stats["model"] = self.model

        return AnswerGenerationOutput(
            answer=answer,
            timing_ms=timing_ms,
            llm_stats=llm_stats,
            error=error,
        )

    def pop_stats(self) -> dict[str, Any]:
        out = dict(self._stats or {})
        self._stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        return out


def build_default_llm_client() -> OpenRouterLLMClient:
    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required.")
    return OpenRouterLLMClient(api_key=api_key)
