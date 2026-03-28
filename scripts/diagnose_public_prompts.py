from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline import AnalyticsPipeline
from scripts.gaming_csv_to_db import DEFAULT_DB_PATH


def main() -> None:
    parser = argparse.ArgumentParser(description="Run each public prompt and print SQL + a small result sample.")
    parser.add_argument("--limit", type=int, default=12, help="Max prompts to print (default: all)")
    parser.add_argument("--rows", type=int, default=3, help="Rows to print per prompt")
    parser.add_argument("--no-answer", action="store_true", help="Skip printing natural language answer")
    args = parser.parse_args()

    prompts = json.loads((PROJECT_ROOT / "tests" / "public_prompts.json").read_text(encoding="utf-8"))
    pipeline = AnalyticsPipeline(db_path=DEFAULT_DB_PATH)

    success = 0
    for i, q in enumerate(prompts[: int(args.limit)], 1):
        out = pipeline.run(q)
        used_fallback = any(
            isinstance(d, dict) and d.get("source") == "fallback"
            for d in (out.sql_generation.intermediate_outputs or [])
        )

        ok = out.status == "success"
        success += int(ok)

        print("\n" + ("=" * 100))
        print(f"[{i}/{min(len(prompts), int(args.limit))}] status={out.status} used_fallback={used_fallback}")
        print(f"question={q!r}")
        print("SQL:")
        print(out.sql or out.sql_generation.sql or "<none>")
        if out.sql_generation.error:
            print(f"sql_generation.error={out.sql_generation.error}")
        if out.sql_validation.error:
            print(f"sql_validation.error={out.sql_validation.error}")
        if out.sql_execution.error:
            print(f"sql_execution.error={out.sql_execution.error}")
        if out.answer_generation.error:
            print(f"answer_generation.error={out.answer_generation.error}")

        if out.rows:
            print(f"Rows (first {int(args.rows)}):")
            for r in out.rows[: int(args.rows)]:
                print(r)
        else:
            print("Rows: <none>")

        if not bool(args.no_answer):
            print("Answer:")
            print(out.answer)

        print(f"total_llm_stats={out.total_llm_stats}")

    denom = min(len(prompts), int(args.limit))
    print(f"\nSUCCESS: {success}/{denom} = {round(100*success/denom, 2) if denom else 0.0}%")


if __name__ == "__main__":
    main()
