# Solution Notes

## What I changed

- Implemented a real SQL validation layer in `src/sql_validation.py`.
  - Only allows single-statement `SELECT`/`WITH`.
  - Blocks destructive/unsafe keywords (DDL/DML, `PRAGMA`, `ATTACH`, etc.).
  - Restricts queries to the `gaming_mental_health` table and blocks `sqlite_master`.
  - Uses `EXPLAIN QUERY PLAN` against the actual SQLite DB (read-only) to catch invalid SQL early.

- Added SQLite schema introspection in `src/schema.py` and passed schema context into SQL generation.

- Implemented token usage counting in `src/llm_client.py`.
  - Extracts `prompt_tokens`, `completion_tokens`, `total_tokens` from OpenRouter response usage when available.
  - Aggregates per-request usage into the pipeline output contract.

- Hardened SQL extraction + prompting.
  - Enforces JSON output contract (`{"sql": "..."}`) for reliable parsing.
  - Supports code-fenced JSON.

- Improved execution safety and error reporting in `src/pipeline.py`.
  - Best-effort read-only SQLite connection and `PRAGMA query_only`.
  - Avoids calling the answer LLM when SQL execution fails (returns explicit error).
  - Adds a single retry path for invalid SQL, feeding previous SQL/error back to the model.

- Fixed `scripts/benchmark.py` to use attribute access (`result.status`).

- Added unit tests in `tests/test_unit.py` for:
  - SQL validator safety rules
  - schema introspection
  - SQL extraction
  - token usage parsing

## Why I changed it

These items were the primary correctness and evaluation blockers:
- SQL validation was a stub and allowed unsafe statements.
- Token counting was missing (required by the assignment).
- SQL generation lacked schema context, reducing accuracy.

## Measured impact

I did not run the full benchmark here because it requires:
- A local SQLite DB created from the Kaggle CSV, and
- A valid `OPENROUTER_API_KEY`.

Once configured, run:
- `python3 scripts/benchmark.py --runs 3`

Expected qualitative improvements:
- Fewer invalid SQL generations (schema-aware prompt + retry).
- Lower token usage for simple aggregate questions (scalar fast-path).

## Tradeoffs and next steps

- The validator is regex/heuristic based; a full SQL AST parser would be stricter.
- Add result-shape aware local answer formatting for common chart-like outputs.
- Add caching (schema + prompt-set responses) to reduce latency for repeated runs.
- Optional: add a lightweight semantic verifier for answer correctness (budgeted LLM call).
