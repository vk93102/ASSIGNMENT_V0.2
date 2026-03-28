from __future__ import annotations

__test__ = False

import sqlite3
import tempfile
import unittest
import os
import time
from pathlib import Path

from src.schema import SQLiteSchemaIntrospector
from src.sql_validation import SQLValidator
from src.llm_client import OpenRouterLLMClient
from src.support import generate_fallback_sql
from src.cache import LRUCache


class SQLValidatorUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "t.sqlite"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'CREATE TABLE gaming_mental_health ("age" INTEGER, "gender" TEXT, "addiction_level" REAL)'
            )
            conn.commit()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_allows_simple_select(self) -> None:
        out = SQLValidator.validate(
            'SELECT AVG(addiction_level) AS avg_addiction FROM gaming_mental_health',
            db_path=self.db_path,
            table_name="gaming_mental_health",
            allowed_columns={"age", "gender", "addiction_level"},
        )
        self.assertTrue(out.is_valid)
        self.assertIsNotNone(out.validated_sql)

    def test_rejects_delete(self) -> None:
        out = SQLValidator.validate(
            'DELETE FROM gaming_mental_health',
            db_path=self.db_path,
            table_name="gaming_mental_health",
            allowed_columns={"age", "gender", "addiction_level"},
        )
        self.assertFalse(out.is_valid)
        self.assertIn("Only SELECT", (out.error or ""))

    def test_rejects_multiple_statements(self) -> None:
        out = SQLValidator.validate(
            'SELECT 1; SELECT 2',
            db_path=self.db_path,
            table_name="gaming_mental_health",
            allowed_columns={"age", "gender", "addiction_level"},
        )
        self.assertFalse(out.is_valid)
        self.assertIn("Multiple statements", (out.error or ""))

    def test_rejects_sqlite_master(self) -> None:
        out = SQLValidator.validate(
            'SELECT name FROM sqlite_master',
            db_path=self.db_path,
            table_name="gaming_mental_health",
            allowed_columns={"age", "gender", "addiction_level"},
        )
        self.assertFalse(out.is_valid)
        self.assertIn("sqlite_master", (out.error or ""))

    def test_rejects_unknown_unqualified_column(self) -> None:
        out = SQLValidator.validate(
            'SELECT made_up_column FROM gaming_mental_health',
            db_path=self.db_path,
            table_name="gaming_mental_health",
            allowed_columns={"age", "gender", "addiction_level"},
        )
        self.assertFalse(out.is_valid)
        self.assertIn("Unknown column", (out.error or ""))

    def test_ignores_disallowed_keywords_in_comments(self) -> None:
        out = SQLValidator.validate(
            'SELECT AVG(addiction_level) AS avg_addiction FROM gaming_mental_health -- DELETE everything\n',
            db_path=self.db_path,
            table_name="gaming_mental_health",
            allowed_columns={"age", "gender", "addiction_level"},
        )
        self.assertTrue(out.is_valid)

    def test_allows_trailing_semicolon(self) -> None:
        out = SQLValidator.validate(
            'SELECT AVG(addiction_level) AS avg_addiction FROM gaming_mental_health;',
            db_path=self.db_path,
            table_name="gaming_mental_health",
            allowed_columns={"age", "gender", "addiction_level"},
        )
        self.assertTrue(out.is_valid)

    def test_allows_semicolon_inside_string(self) -> None:
        out = SQLValidator.validate(
            "SELECT ';' AS semi FROM gaming_mental_health",
            db_path=self.db_path,
            table_name="gaming_mental_health",
            allowed_columns={"age", "gender", "addiction_level"},
        )
        self.assertTrue(out.is_valid)

    def test_rejects_hidden_multistatement_with_comments(self) -> None:
        out = SQLValidator.validate(
            "SELECT 1 FROM gaming_mental_health; /* ok */ SELECT 2",
            db_path=self.db_path,
            table_name="gaming_mental_health",
            allowed_columns={"age", "gender", "addiction_level"},
        )
        self.assertFalse(out.is_valid)
        self.assertIn("Multiple statements", (out.error or ""))


class SchemaUnitTests(unittest.TestCase):
    def test_introspects_table_columns(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "s.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute('CREATE TABLE gaming_mental_health ("a" INTEGER, "b" TEXT)')
                conn.commit()

            schema = SQLiteSchemaIntrospector(db_path, table_name="gaming_mental_health").load()
            self.assertEqual(schema.table_name, "gaming_mental_health")
            self.assertEqual(schema.columns, ["a", "b"])

    def test_semantic_column_selection_prefers_relevant(self) -> None:
        schema = SQLiteSchemaIntrospector.__new__(SQLiteSchemaIntrospector)  # type: ignore
        from src.schema import SchemaInfo

        s = SchemaInfo(
            table_name="gaming_mental_health",
            columns=["age", "gender", "addiction_level", "anxiety_score", "hours_played"],
            column_types={"age": "INTEGER", "gender": "TEXT", "addiction_level": "REAL", "anxiety_score": "REAL", "hours_played": "REAL"},
        )
        cols = s.select_relevant_columns_semantic("Average addiction level by age", max_columns=3)
        lower = [c.lower() for c in cols]
        self.assertIn("addiction_level", lower)
        self.assertIn("age", lower)


class LLMClientHelpersUnitTests(unittest.TestCase):
    def test_extract_sql_from_json(self) -> None:
        sql = OpenRouterLLMClient._extract_sql('{"sql": "SELECT 1"}')
        self.assertEqual(sql, "SELECT 1")

    def test_extract_sql_from_fenced_json(self) -> None:
        sql = OpenRouterLLMClient._extract_sql('```json\n{"sql": "SELECT 1"}\n```')
        self.assertEqual(sql, "SELECT 1")

    def test_extract_sql_fallback_select(self) -> None:
        sql = OpenRouterLLMClient._extract_sql('Here is your query:\nSELECT 1')
        self.assertEqual(sql, "SELECT 1")

    def test_extract_sql_from_malformed_json_like_output(self) -> None:
        sql = OpenRouterLLMClient._extract_sql('{"sql": SELECT 1}')
        self.assertEqual(sql, "SELECT 1")

    def test_update_usage_stats_from_obj(self) -> None:
        class Usage:
            prompt_tokens = 10
            completion_tokens = 5
            total_tokens = 15

        class Resp:
            usage = Usage()

        client = OpenRouterLLMClient.__new__(OpenRouterLLMClient)
        client._stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        client._update_usage_stats(Resp())
        self.assertEqual(client._stats["llm_calls"], 1)
        self.assertEqual(client._stats["prompt_tokens"], 10)
        self.assertEqual(client._stats["completion_tokens"], 5)
        self.assertEqual(client._stats["total_tokens"], 15)

    def test_result_summarizer_shape(self) -> None:
        client = OpenRouterLLMClient.__new__(OpenRouterLLMClient)
        client._config = None
        summary = OpenRouterLLMClient._summarize_results(  # type: ignore
            client,
            "q",
            "SELECT age, AVG(addiction_level) FROM gaming_mental_health GROUP BY age",
            [{"age": 20, "avg": 1.5}, {"age": 21, "avg": 2.0}],
        )
        self.assertIn("row_count", summary)
        self.assertIn("sample_rows", summary)
        self.assertIn("shape_hints", summary)

    def test_extract_text_from_response_nested_content_parts(self) -> None:
        # Simulates providers that return content as a list of parts rather than a plain string.
        res = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": [
                            {"type": "text", "text": {"value": "{\"sql\": \"SELECT 1\"}"}},
                        ],
                    }
                }
            ]
        }
        text = OpenRouterLLMClient._extract_text_from_response(res, model="stub")
        self.assertEqual(text, '{"sql": "SELECT 1"}')

    def test_extract_text_from_response_output_text_field(self) -> None:
        # Simulates OpenAI Responses-style output_text.
        res = {"output_text": "ok"}
        text = OpenRouterLLMClient._extract_text_from_response(res, model="stub")
        self.assertEqual(text, "ok")


class FallbackSQLUnitTests(unittest.TestCase):
    def test_generates_top5_age_by_addiction(self) -> None:
        sql = generate_fallback_sql(
            "What are the top 5 age groups by average addiction level?",
            table_name="gaming_mental_health",
        )
        self.assertIsNotNone(sql)
        self.assertIn("avg(addiction_level)", sql.lower())
        self.assertIn("group by age", sql.lower())
        self.assertIn("limit 5", sql.lower())

    def test_zodiac_is_unanswerable(self) -> None:
        sql = generate_fallback_sql(
            "Which zodiac sign has the highest stress score?",
            table_name="gaming_mental_health",
        )
        self.assertIsNone(sql)


class SQLiteExecutorUnitTests(unittest.TestCase):
    def test_executor_truncates_rows(self) -> None:
        from src.pipeline import SQLiteExecutor

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "e.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute('CREATE TABLE gaming_mental_health ("age" INTEGER)')
                conn.executemany('INSERT INTO gaming_mental_health(age) VALUES (?)', [(1,), (2,), (3,), (4,), (5,)])
                conn.commit()

            ex = SQLiteExecutor(db_path, max_rows=2)
            out = ex.run('SELECT age FROM gaming_mental_health ORDER BY age')
            self.assertEqual(out.row_count, 2)
            self.assertEqual([r["age"] for r in out.rows], [1, 2])


class PipelineCacheUnitTests(unittest.TestCase):
    def test_pipeline_cache_deduplicates_requests(self) -> None:
        from src.pipeline import AnalyticsPipeline
        from src.support import SQLGenerationOutput, AnswerGenerationOutput

        class StubLLM:
            model = "stub"

            def __init__(self) -> None:
                self.sql_calls = 0
                self.ans_calls = 0

            def generate_sql(self, question: str, context: dict):
                self.sql_calls += 1
                return SQLGenerationOutput(
                    sql="SELECT AVG(addiction_level) AS avg_addiction FROM gaming_mental_health",
                    timing_ms=1.0,
                    llm_stats={"llm_calls": 1, "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2, "model": self.model},
                    error=None,
                )

            def generate_answer(self, question: str, sql: str | None, rows: list[dict]):
                self.ans_calls += 1
                return AnswerGenerationOutput(
                    answer="ok",
                    timing_ms=1.0,
                    llm_stats={"llm_calls": 1, "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2, "model": self.model},
                    error=None,
                )

        prev_size = os.environ.get("PIPELINE_CACHE_SIZE")
        prev_ttl = os.environ.get("PIPELINE_CACHE_TTL_SECONDS")
        os.environ["PIPELINE_CACHE_SIZE"] = "8"
        os.environ["PIPELINE_CACHE_TTL_SECONDS"] = "60"

        try:
            with tempfile.TemporaryDirectory() as td:
                db_path = Path(td) / "p.sqlite"
                with sqlite3.connect(db_path) as conn:
                    conn.execute('CREATE TABLE gaming_mental_health ("addiction_level" REAL)')
                    conn.executemany('INSERT INTO gaming_mental_health(addiction_level) VALUES (?)', [(1.0,), (2.0,), (3.0,)])
                    conn.commit()

                stub = StubLLM()
                p = AnalyticsPipeline(db_path=db_path, llm_client=stub)
                q = "What is the average addiction level?"

                r1 = p.run(q)
                r2 = p.run(q)

                self.assertEqual(stub.sql_calls, 1)
                self.assertEqual(stub.ans_calls, 1)
                self.assertEqual(r1.status, "success")
                self.assertEqual(r2.status, "success")
                self.assertIsNotNone(r2.sql)
                self.assertTrue(r2.total_llm_stats["llm_calls"] >= 0)
        finally:
            if prev_size is None:
                os.environ.pop("PIPELINE_CACHE_SIZE", None)
            else:
                os.environ["PIPELINE_CACHE_SIZE"] = prev_size
            if prev_ttl is None:
                os.environ.pop("PIPELINE_CACHE_TTL_SECONDS", None)
            else:
                os.environ["PIPELINE_CACHE_TTL_SECONDS"] = prev_ttl


class CacheTTLUnitTests(unittest.TestCase):
    def test_lru_cache_ttl_expires(self) -> None:
        c: LRUCache[str, str] = LRUCache(max_size=2, ttl_seconds=0.01)
        c.set("k", "v")
        self.assertEqual(c.get("k"), "v")
        time.sleep(0.03)
        self.assertIsNone(c.get("k"))


if __name__ == "__main__":
    unittest.main()
