from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.schema import SQLiteSchemaIntrospector
from src.sql_validation import SQLValidator
from src.llm_client import OpenRouterLLMClient


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


if __name__ == "__main__":
    unittest.main()
