from __future__ import annotations

import copy
import json
import os
import re
import sqlite3
import time
import uuid
from pathlib import Path

from src.llm_client import OpenRouterLLMClient, build_default_llm_client
from src.schema import SQLiteSchemaIntrospector, SchemaInfo
from src.sql_validation import SQLValidator
from src.semantic_validator import SemanticValidator
from src.support import (
    get_logger,
    generate_fallback_sql,
    SQLGenerationOutput,
    SQLValidationOutput,
    SQLExecutionOutput,
    PipelineOutput,
    AnswerGenerationOutput,
    PipelineInput,
    ContextManager,
    IntentDetector,
    ContextAwarePromptBuilder,
)
from src.cache import LRUCache
from src.config import Config


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = BASE_DIR / "data" / "gaming_mental_health.sqlite"
DEFAULT_TABLE_NAME = "gaming_mental_health"


class SQLiteExecutor:
    def __init__(
        self,
        db_path: str | Path = DEFAULT_DB_PATH,
        *,
        max_rows: int = 100,
        timeout_ms: float | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.max_rows = max_rows
        self.timeout_ms = timeout_ms

    def _connect_readonly(self) -> sqlite3.Connection:
        try:
            return sqlite3.connect(f"file:{self.db_path.as_posix()}?mode=ro", uri=True)
        except Exception:
            return sqlite3.connect(self.db_path)

    def run(self, sql: str | None) -> SQLExecutionOutput:
        start = time.perf_counter()
        error = None
        rows = []
        row_count = 0

        if sql is None:
            return SQLExecutionOutput(
                rows=[],
                row_count=0,
                timing_ms=(time.perf_counter() - start) * 1000,
                error=None,
            )

        try:
            with self._connect_readonly() as conn:
                try:
                    conn.execute("PRAGMA query_only = ON")
                except Exception:
                    pass

                if self.timeout_ms is not None and self.timeout_ms > 0:
                    t0 = time.perf_counter()

                    def _handler() -> int:
                        elapsed_ms = (time.perf_counter() - t0) * 1000
                        return 1 if elapsed_ms > float(self.timeout_ms) else 0

                    conn.set_progress_handler(_handler, 10_000)

                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(sql)
                rows = [dict(r) for r in cur.fetchmany(self.max_rows)]
                row_count = len(rows)
        except Exception as exc:
            msg = str(exc)
            if "interrupted" in msg.lower() and self.timeout_ms is not None:
                error = f"Query timed out after {self.timeout_ms}ms"
            else:
                error = msg
            rows = []
            row_count = 0

        return SQLExecutionOutput(
            rows=rows,
            row_count=row_count,
            timing_ms=(time.perf_counter() - start) * 1000,
            error=error,
        )


class AnalyticsPipeline:
    def __init__(
        self,
        db_path: str | Path = DEFAULT_DB_PATH,
        llm_client: OpenRouterLLMClient | None = None,
        *,
        table_name: str = DEFAULT_TABLE_NAME,
    ) -> None:
        self.db_path = Path(db_path)
        self.table_name = table_name
        self.llm = llm_client or build_default_llm_client()
        self._config = Config.from_env()
        self.executor = SQLiteExecutor(
            self.db_path,
            max_rows=self._config.sqlite_max_rows,
            timeout_ms=self._config.sqlite_query_timeout_ms,
        )
        self._logger = get_logger(__name__)
        self._schema = None

        cache_size = int(self._config.pipeline_cache_size)
        ttl = self._config.pipeline_cache_ttl_seconds
        self._response_cache: LRUCache[str, PipelineOutput] | None = None
        if cache_size > 0 and (ttl is None or ttl > 0):
            self._response_cache = LRUCache(max_size=cache_size, ttl_seconds=ttl)

        fb_size = int(self._config.fallback_cache_size)
        fb_ttl = self._config.fallback_cache_ttl_seconds
        self._fallback_cache: LRUCache[str, PipelineOutput] | None = None
        if fb_size > 0 and (fb_ttl is None or fb_ttl > 0):
            self._fallback_cache = LRUCache(max_size=fb_size, ttl_seconds=fb_ttl)

        self._context_manager = ContextManager()
        self._intent_detector = IntentDetector()
        self._prompt_builder = ContextAwarePromptBuilder()

    def _cache_key(self, question: str, *, schema_fingerprint: str) -> str:
        # Caching strategy: Normalize question + include schema fingerprint
        # Why schema fingerprint? Different column sets generate different valid SQL
        # for the same question. Cache hit requires identical schema + question.
        # This prevents returning stale results if database schema changes.
        payload = {
            "q": (question or "").strip().lower(),
            "db": str(self.db_path.resolve()),
            "table": self.table_name,
            "schema": schema_fingerprint,
            "model": getattr(self.llm, "model", "unknown"),
        }
        return json.dumps(payload, sort_keys=True, ensure_ascii=True)

    def _fallback_cache_key(self, question: str, *, schema_fingerprint: str) -> str:
        payload = {
            "q": (question or "").strip().lower(),
            "db": str(self.db_path.resolve()),
            "table": self.table_name,
            "schema": schema_fingerprint,
        }
        return json.dumps(payload, sort_keys=True, ensure_ascii=True)

    def _get_schema(self):
        if self._schema is not None:
            return self._schema
        try:
            self._schema = SQLiteSchemaIntrospector(self.db_path, table_name=self.table_name).load()
        except Exception:
            self._schema = SchemaInfo(table_name=self.table_name, columns=[], column_types={})
        return self._schema

    def run(self, question: str, request_id: str | None = None, conversation_id: str | None = None) -> PipelineOutput:
        # Multi-turn setup: create/fetch conversation context
        conversation_context = None
        if conversation_id:
            conversation_context = self._context_manager.get_conversation(conversation_id)
            if not conversation_context:
                schema = self._get_schema()
                schema_fp = schema.fingerprint()
                conversation_context = self._context_manager.create_conversation(
                    conversation_id,
                    schema_fingerprint=schema_fp
                )
            
            intent_output = self._intent_detector.detect(question, conversation_context)
            self._logger.info("intent_detected", extra={
                "request_id": request_id,
                "conversation_id": conversation_id,
                "intent": intent_output.intent_type,
                "confidence": intent_output.confidence
            })



        start = time.perf_counter()
        request_id = request_id or uuid.uuid4().hex
        self._logger.info("pipeline_start", extra={"request_id": request_id})

        schema = self._get_schema()
        schema_fp = schema.fingerprint()

        if self._fallback_cache is not None:
            fb_key = self._fallback_cache_key(question, schema_fingerprint=schema_fp)
            fb_cached = self._fallback_cache.get(fb_key)
            if fb_cached is not None:
                out = copy.deepcopy(fb_cached)
                out.request_id = request_id
                out.sql_generation.timing_ms = 0.0
                out.sql_generation.llm_stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")}
                out.sql_generation.intermediate_outputs.append({"source": "cache", "kind": "fallback_output"})
                out.sql_validation.timing_ms = 0.0
                out.sql_execution.timing_ms = 0.0
                out.answer_generation.timing_ms = 0.0
                out.answer_generation.llm_stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")}
                out.answer_generation.intermediate_outputs.append({"source": "cache", "kind": "fallback_output"})
                out.timings = {
                    "sql_generation_ms": 0.0,
                    "sql_validation_ms": 0.0,
                    "sql_execution_ms": 0.0,
                    "answer_generation_ms": 0.0,
                    "total_ms": (time.perf_counter() - start) * 1000,
                }
                out.total_llm_stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")}
                self._logger.info("pipeline_fallback_cache_hit", extra={"request_id": request_id})
                return out

        if self._response_cache is not None:
            key = self._cache_key(question, schema_fingerprint=schema_fp)
            cached = self._response_cache.get(key)
            if cached is not None:
                out = copy.deepcopy(cached)
                out.request_id = request_id

                out.sql_generation.timing_ms = 0.0
                out.sql_generation.llm_stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")}
                out.sql_generation.intermediate_outputs.append({"source": "cache", "kind": "pipeline_output"})

                out.sql_validation.timing_ms = 0.0
                out.sql_execution.timing_ms = 0.0
                out.answer_generation.timing_ms = 0.0
                out.answer_generation.llm_stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")}
                out.answer_generation.intermediate_outputs.append({"source": "cache", "kind": "pipeline_output"})

                out.timings = {
                    "sql_generation_ms": 0.0,
                    "sql_validation_ms": 0.0,
                    "sql_execution_ms": 0.0,
                    "answer_generation_ms": 0.0,
                    "total_ms": (time.perf_counter() - start) * 1000,
                }
                out.total_llm_stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")}

                self._logger.info("pipeline_cache_hit", extra={"request_id": request_id})
                return out

        schema_context = schema.to_prompt_context()
        mode = (self._config.schema_filter_mode or "semantic").strip().lower()
        if mode == "all":
            selected_cols = list(schema.columns)
        elif mode == "heuristic":
            selected_cols = schema.select_relevant_columns(question, max_columns=int(self._config.schema_max_columns))
        else:
            selected_cols = schema.select_relevant_columns_semantic(question, max_columns=int(self._config.schema_max_columns))

        schema_context["columns"] = selected_cols
        schema_context["column_types"] = {c: schema.column_types.get(c, "") for c in selected_cols}
        
        if conversation_context and conversation_context.turns:
            history_context = self._context_manager.get_context_for_prompt(conversation_context)
            if history_context:
                schema_context["conversation_history"] = history_context

        q_lower = question.lower()
        destructive_intent = any(
            kw in q_lower
            for kw in (
                "delete",
                "drop",
                "update",
                "insert",
                "alter",
                "create",
                "truncate",
                "pragma",
                "attach",
                "detach",
            )
        )

        def _normalize_sqlite_sql(s: str | None) -> str | None:
            if s is None:
                return None
            raw = s.strip()
            if not raw:
                return s

            m = re.match(r"(?is)^\s*select\s+top\s+(\d+)\s+", raw)
            if m:
                n = m.group(1)
                rewritten = re.sub(r"(?is)^\s*select\s+top\s+\d+\s+", "SELECT ", raw)
                if re.search(r"(?is)\blimit\b", rewritten) is None:
                    semi = ";" if rewritten.rstrip().endswith(";") else ""
                    rewritten = rewritten.rstrip().rstrip(";").rstrip()
                    rewritten = f"{rewritten} LIMIT {n}{semi}"
                raw = rewritten

            raw = re.sub(r"(?i)\bilike\b", "LIKE", raw)
            return raw

        # Stage 1: SQL Generation
        if destructive_intent:
            sql_gen_output = SQLGenerationOutput(
                sql=f"DELETE FROM {self.table_name}",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")},
                error=None,
            )
        else:
            sql_gen_output = self.llm.generate_sql(question, schema_context)

            if sql_gen_output.sql is None:
                repair_context = dict(schema_context)
                repair_context["previous_error"] = sql_gen_output.error or "LLM did not return a SQL JSON payload."
                repair_output = self.llm.generate_sql(question, repair_context)
                sql_gen_output.intermediate_outputs.append(
                    {
                        "attempt": 1,
                        "sql": sql_gen_output.sql,
                        "error": sql_gen_output.error,
                        "llm_stats": sql_gen_output.llm_stats,
                    }
                )
                sql_gen_output.intermediate_outputs.append(
                    {
                        "attempt": 2,
                        "sql": repair_output.sql,
                        "error": repair_output.error,
                        "llm_stats": repair_output.llm_stats,
                        "reason": "llm_sql_missing",
                    }
                )
                sql_gen_output.sql = repair_output.sql
                sql_gen_output.error = repair_output.error
                sql_gen_output.timing_ms += repair_output.timing_ms
                sql_gen_output.llm_stats = {
                    "llm_calls": int(sql_gen_output.llm_stats.get("llm_calls", 0)) + int(repair_output.llm_stats.get("llm_calls", 0)),
                    "prompt_tokens": int(sql_gen_output.llm_stats.get("prompt_tokens", 0)) + int(repair_output.llm_stats.get("prompt_tokens", 0)),
                    "completion_tokens": int(sql_gen_output.llm_stats.get("completion_tokens", 0)) + int(repair_output.llm_stats.get("completion_tokens", 0)),
                    "total_tokens": int(sql_gen_output.llm_stats.get("total_tokens", 0)) + int(repair_output.llm_stats.get("total_tokens", 0)),
                    "model": repair_output.llm_stats.get("model", sql_gen_output.llm_stats.get("model", "unknown")),
                }

            if sql_gen_output.sql is None:
                fallback = generate_fallback_sql(question, table_name=self.table_name)
                if fallback:
                    sql_gen_output.intermediate_outputs.append(
                        {
                            "source": "fallback",
                            "reason": "llm_sql_missing",
                            "llm_error": sql_gen_output.error,
                        }
                    )
                    sql_gen_output.sql = fallback
                else:
                    pass
        sql = sql_gen_output.sql

        if sql is not None:
            sql = _normalize_sqlite_sql(sql)
            sql_gen_output.sql = sql

        # Stage 2: SQL Validation
        validation_output = SQLValidator.validate(
            sql,
            db_path=self.db_path,
            table_name=self.table_name,
            allowed_columns=set(schema.columns),
        )
        if not validation_output.is_valid and validation_output.error:
            self._logger.info(
                "sql_validation_failed",
                extra={
                    "request_id": request_id,
                    "error": validation_output.error,
                    "sql": (sql or "")[:500],
                },
            )

        if not validation_output.is_valid and sql_gen_output.sql:
            retry_context = dict(schema_context)
            retry_context["previous_sql"] = sql_gen_output.sql
            retry_context["previous_error"] = validation_output.error

            err = (validation_output.error or "").lower()
            if "unknown column" in err or "failed to parse/plan" in err:
                retry_context["columns"] = list(schema.columns)
                retry_context["column_types"] = dict(schema.column_types)
                retry_context["schema_context"] = "all_columns"
            retry_output = self.llm.generate_sql(question, retry_context)
            sql_gen_output.intermediate_outputs.append(
                {
                    "attempt": 1,
                    "sql": sql_gen_output.sql,
                    "error": sql_gen_output.error,
                    "llm_stats": sql_gen_output.llm_stats,
                }
            )
            sql_gen_output.intermediate_outputs.append(
                {
                    "attempt": 2,
                    "sql": retry_output.sql,
                    "error": retry_output.error,
                    "llm_stats": retry_output.llm_stats,
                }
            )
            sql_gen_output.sql = retry_output.sql
            sql_gen_output.error = retry_output.error
            sql_gen_output.timing_ms += retry_output.timing_ms
            sql_gen_output.llm_stats = {
                "llm_calls": int(sql_gen_output.llm_stats.get("llm_calls", 0)) + int(retry_output.llm_stats.get("llm_calls", 0)),
                "prompt_tokens": int(sql_gen_output.llm_stats.get("prompt_tokens", 0)) + int(retry_output.llm_stats.get("prompt_tokens", 0)),
                "completion_tokens": int(sql_gen_output.llm_stats.get("completion_tokens", 0)) + int(retry_output.llm_stats.get("completion_tokens", 0)),
                "total_tokens": int(sql_gen_output.llm_stats.get("total_tokens", 0)) + int(retry_output.llm_stats.get("total_tokens", 0)),
                "model": retry_output.llm_stats.get("model", sql_gen_output.llm_stats.get("model", "unknown")),
            }
            sql = sql_gen_output.sql
            if sql is not None:
                sql = _normalize_sqlite_sql(sql)
                sql_gen_output.sql = sql
            validation_output = SQLValidator.validate(
                sql,
                db_path=self.db_path,
                table_name=self.table_name,
                allowed_columns=set(schema.columns),
            )

            if not validation_output.is_valid:
                fallback = generate_fallback_sql(question, table_name=self.table_name)
                if fallback:
                    sql_gen_output.intermediate_outputs.append(
                        {
                            "source": "fallback",
                            "reason": "llm_invalid_sql",
                            "validator_error": validation_output.error,
                        }
                    )
                    sql_gen_output.sql = fallback
                    sql = fallback
                    validation_output = SQLValidator.validate(
                        sql,
                        db_path=self.db_path,
                        table_name=self.table_name,
                        allowed_columns=set(schema.columns),
                    )
                else:
                    sql_gen_output.sql = None
                    sql = None

        if not validation_output.is_valid:
            sql = None
        else:
            sql = validation_output.validated_sql

        # Stage 3b: Semantic Validation (check if SQL meaningfully answers the question)
        semantic_valid = True
        semantic_error = None
        if sql is not None and validation_output.is_valid:
            semantic_valid, semantic_error = SemanticValidator.validate_semantic_match(
                question, 
                sql,
                schema_columns=set(schema.columns),
            )
            if not semantic_valid:
                sql = None
                if not validation_output.error:
                    validation_output.error = semantic_error or "Query does not meaningfully answer the question"

        # Stage 3: SQL Execution
        execution_output = self.executor.run(sql)
        rows = execution_output.rows

        # Stage 4: Answer Generation
        if execution_output.error:
            answer_output = AnswerGenerationOutput(
                answer=f"SQL execution error: {execution_output.error}",
                timing_ms=0.0,
                llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": getattr(self.llm, "model", "unknown")},
                error=execution_output.error,
            )
        else:
            answer_output = self.llm.generate_answer(question, sql, rows)

        status = "success"
        
        if destructive_intent:
            status = "invalid_sql"
            if not validation_output.error:
                validation_output.error = "Query contains destructive intent (DELETE, DROP, UPDATE, etc.)"
        elif sql is None:
            if semantic_error:
                status = "unanswerable"
            elif sql_gen_output.sql is None and sql_gen_output.error:
                status = "unanswerable"
            elif not validation_output.is_valid:
                status = "invalid_sql"
                if not validation_output.error:
                    validation_output.error = "Query validation failed: unable to generate valid SQL"
            else:
                status = "unanswerable"
        elif not validation_output.is_valid:
            status = "invalid_sql"
        elif execution_output.error:
            status = "error"

        timings = {
            "sql_generation_ms": sql_gen_output.timing_ms,
            "sql_validation_ms": validation_output.timing_ms,
            "sql_execution_ms": execution_output.timing_ms,
            "answer_generation_ms": answer_output.timing_ms,
            "total_ms": (time.perf_counter() - start) * 1000,
        }

        total_llm_stats = {
            "llm_calls": sql_gen_output.llm_stats.get("llm_calls", 0) + answer_output.llm_stats.get("llm_calls", 0),
            "prompt_tokens": sql_gen_output.llm_stats.get("prompt_tokens", 0) + answer_output.llm_stats.get("prompt_tokens", 0),
            "completion_tokens": sql_gen_output.llm_stats.get("completion_tokens", 0) + answer_output.llm_stats.get("completion_tokens", 0),
            "total_tokens": sql_gen_output.llm_stats.get("total_tokens", 0) + answer_output.llm_stats.get("total_tokens", 0),
            "model": sql_gen_output.llm_stats.get("model", "unknown"),
        }

        self._logger.info(
            "pipeline_end",
            extra={
                "request_id": request_id,
                "status": status,
                "row_count": execution_output.row_count,
                "llm_calls": total_llm_stats.get("llm_calls"),
                "total_tokens": total_llm_stats.get("total_tokens"),
                "total_ms": timings.get("total_ms"),
            },
        )

        out = PipelineOutput(
            status=status,
            question=question,
            request_id=request_id,
            sql_generation=sql_gen_output,
            sql_validation=validation_output,
            sql_execution=execution_output,
            answer_generation=answer_output,
            sql=sql,
            rows=rows,
            answer=answer_output.answer,
            timings=timings,
            total_llm_stats=total_llm_stats,
        )

        # Multi-turn: Save turn to conversation context
        if conversation_context:
            intent_type = intent_output.intent_type if 'intent_output' in locals() else "new_query"
            if 'intent_output' in locals() and getattr(intent_output, "referenced_turn_id", None) is not None:
                referenced_turn_ids = [int(intent_output.referenced_turn_id)]
            else:
                referenced_turn_ids = []
            self._context_manager.add_turn(
                conversation_id,
                out,
                intent_type=intent_type,
                referenced_turn_ids=referenced_turn_ids
            )
            self._logger.info("turn_saved_to_conversation", extra={
                "request_id": request_id,
                "conversation_id": conversation_id,
                "turn_number": len(conversation_context.turns)
            })

        if self._response_cache is not None:
            try:
                self._response_cache.set(self._cache_key(question, schema_fingerprint=schema_fp), out)
            except Exception:
                pass

        if self._fallback_cache is not None:
            used_fallback = any((d.get("source") == "fallback") for d in (sql_gen_output.intermediate_outputs or []))
            if used_fallback and out.status in {"success", "unanswerable", "invalid_sql"}:
                try:
                    self._fallback_cache.set(self._fallback_cache_key(question, schema_fingerprint=schema_fp), out)
                except Exception:
                    pass

        return out