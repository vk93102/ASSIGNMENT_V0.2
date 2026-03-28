# Senior Full Stack Engineer (GenAI-Labs) Take-Home Assignment

This repository contains an LLM-driven analytics pipeline that converts natural-language questions into safe SQLite queries over a single table (`gaming_mental_health`) and returns a grounded answer.

Dataset source (CSV): https://www.kaggle.com/datasets/sharmajicoder/gaming-and-mental-health?resource=download

## Goal
Optimize an LLM-driven analytics pipeline while preserving output quality.

Key metrics:
- End-to-end latency (prompt → final answer)
- LLM resources (token usage + call count)
- Output quality and safety (valid SQL, non-hallucinated answers)

## Current Status
The pipeline is functional end-to-end and includes:
- Schema introspection and prompt conditioning (reduces hallucinated columns)
- Defense-in-depth SQL safety (SELECT/WITH allowlist, single-table enforcement, EXPLAIN validation)
- Safe execution (read-only connection, optional timeout, row limiting)
- Reliability features (fallback SQL patterns, single retry on validation failure)
- Observability (request correlation + stage timings + LLM usage stats)
- Optional multi-turn conversation support (conversation context + follow-up intent detection)

## What You Get
- Python pipeline stages:
  - SQL generation (LLM)
  - SQL validation (allowlist + schema/table/column checks + EXPLAIN)
  - SQL execution (read-only, bounded)
  - Answer generation (LLM when needed; fast paths when trivial)
- Single SQLite table with gaming and mental health survey data
- Public tests and benchmark script
- OpenRouter integration via [OpenRouter Python SDK](https://pypi.org/project/openrouter/)
- Configurable model (default: `openai/gpt-5-nano`, override via `OPENROUTER_MODEL`)

## Hard Requirements
1. Do not modify existing public tests in `tests/test_public.py`.
2. Public tests must pass.
3. Keep the project runnable locally with standard Python.
4. Output contract: `AnalyticsPipeline.run()` must return a `PipelineOutput` instance, with each stage producing outputs that conform to the type schemas in `src/types.py`. This enables automated evaluation; submissions that deviate from it cannot be graded correctly.
5. Token counting must be implemented. The baseline includes a skeleton for tracking LLM usage statistics in `src/llm_client.py`, but you must implement the actual token counting. This is required for the efficiency evaluation to work.

Notes:
- Public tests are unchanged and gated by `OPENROUTER_API_KEY`.
- Token/call accounting is surfaced per request in `PipelineOutput.total_llm_stats`.

## Production Readiness Requirements

Your submission **must include** a completed `CHECKLIST.md` file documenting your design decisions and implementation approach across all relevant areas.

## Requirements

- **Python:** 3.13+ (code is also compatible with 3.10+ in practice)
- **Dependencies:** see `requirements.txt` (includes `openrouter`, `pandas`, `sqlparse`, `python-dotenv`, `pytest`)

## Setup

### Data Setup

The dataset (~160MB) is not included in this repository. Download it before running the pipeline:

1. Go to [Kaggle - Gaming and Mental Health](https://www.kaggle.com/datasets/sharmajicoder/gaming-and-mental-health?select=gaming_mental_health_10M_40features.csv)
2. Download `gaming_mental_health_10M_40features.csv` (select this file from the dataset)
3. Place the file in the `data/` directory
4. **Important:** Ensure you download and use all 39 columns—do not drop any columns during download or import

The Kaggle page provides a more detailed description of the dataset, including column definitions and data sources.

```bash
python3 -m pip install -r requirements.txt
python3 scripts/gaming_csv_to_db.py
pytest -q
```

### OpenRouter Setup

This project uses [OpenRouter](https://openrouter.ai/) to access LLMs for SQL generation and answer synthesis. OpenRouter provides a unified API for many models across providers. It offers a **free tier** that lets you use certain models at no cost, which is sufficient for this assignment.

To get started:

1. **Create an account** at [openrouter.ai](https://openrouter.ai/)
2. **Create an API key** in your account settings
3. **Set the API key** in your environment (or copy from `.env.example`):

```bash
set OPENROUTER_API_KEY=<your_key>
```

On Linux/macOS: `export OPENROUTER_API_KEY=<your_key>`

## Running the Pipeline

Single-turn usage:

```python
from src.pipeline import AnalyticsPipeline

p = AnalyticsPipeline()
out = p.run("What are the top 5 age groups by average addiction level?")

print(out.status)
print(out.sql)
print(out.answer)
print(out.total_llm_stats)  # llm_calls + token usage
```

Optional multi-turn usage (follow-ups):

```python
from src.pipeline import AnalyticsPipeline

p = AnalyticsPipeline()
cid = "demo-conversation-1"

out1 = p.run("Average addiction level by gender?", conversation_id=cid)
out2 = p.run("What about males specifically?", conversation_id=cid)
```

## Testing

- Unit + non-LLM tests:
  - `pytest -q`
- Public integration tests (requires `OPENROUTER_API_KEY`):
  - `python3 -m unittest tests.test_public -v`
  - Note: `tests/test_public.py` opts out of pytest collection (`__test__ = False`).

## Benchmark
Run:

```bash
python3 scripts/benchmark.py --runs 3
```

This prints baseline-style latency stats (`avg`, `p50`, `p95`) and success rate.

Notes:
- Benchmark results depend on model/provider latency and your network.
- The benchmark script currently reports latency and success rate; per-request token/call stats exist in `PipelineOutput.total_llm_stats`, but are not aggregated by `scripts/benchmark.py`.

Reference metrics (baseline on reference hardware): avg ~2900ms, p50 ~2500ms, p95 ~4700ms, ~600 tokens/request.

## Deliverables
1. Updated source code
2. Added tests (if any)
3. Completed `CHECKLIST.md` with all sections addressed
4. Short engineering note (`SOLUTION_NOTES.md`) with:
   - What you changed
   - Why you changed it
   - Measured impact (before/after benchmark numbers)
   - Tradeoffs and next steps

## Optional Part: Multi-Turn Conversation Support

This is an **optional** part for candidates who want to demonstrate additional capabilities. It is **not required** for a complete submission, but may contribute to bonus evaluation.

### The Problem

The current pipeline handles single, isolated questions. In real-world scenarios, users often ask follow-up questions that reference previous context:

- "What is the addiction level distribution by gender?"
- Follow-up: "What about males specifically?"
- Follow-up: "Can you explain the highest value?"
- Follow-up: "Now sort by anxiety score instead"

### Implementation Guidelines

- You may implement this however you see fit: extend the existing pipeline, add new modules, or integrate directly into the LLM client.
- No skeleton code or boilerplate is provided - design the solution architecture yourself.
- If implemented, document your approach in `CHECKLIST.md` under a "Follow-Up Questions" section.

In this repo, multi-turn support is implemented via:
- `conversation_id` passed to `AnalyticsPipeline.run(...)`
- an in-memory `ConversationContext` store (`src/context_manager.py`)
- follow-up intent heuristics (`src/intent_detector.py`)

## General Notes
- The baseline intentionally leaves room for substantial optimization.
- Hidden evaluation includes paraphrased prompts and edge/failure cases.
- Public tests are integration tests and require a valid `OPENROUTER_API_KEY`.
- Think beyond the obvious optimizations - the challenge tests your engineering judgment, not just your ability to follow a checklist.

See `CHECKLIST.md` for the production-readiness write-up and `SOLUTION_NOTES.md` for implementation details and measured results.
