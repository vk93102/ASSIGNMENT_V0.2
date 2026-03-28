import argparse
import sqlite3
import sys
from pathlib import Path
import os

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_CSV_PATH = PROJECT_ROOT / "data" / "gaming_mental_health_10M_40features.csv"
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "gaming_mental_health.sqlite"
DEFAULT_TABLE_NAME = "gaming_mental_health"

SQLITE_TYPE_MAP = {
    "int64": "INTEGER",
    "float64": "REAL",
    "bool": "INTEGER",
    "datetime64[ns]": "TEXT",
    "object": "TEXT",
}


def map_pd_dtype_to_sql(dtype) -> str:
    key = str(dtype)
    return SQLITE_TYPE_MAP.get(key, "TEXT")


def create_table_from_df(
    conn: sqlite3.Connection, table_name: str, df: pd.DataFrame, if_exists: str = "fail"
):
    cursor = conn.cursor()

    if if_exists == "replace":
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.commit()

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    table_exists = cursor.fetchone() is not None

    if table_exists:
        if if_exists == "fail":
            raise ValueError(
                f"Table '{table_name}' already exists. Use --if-exists replace or append."
            )
        elif if_exists == "append":
            return

    cols = []
    for col in df.columns:
        coltype = map_pd_dtype_to_sql(df[col].dtype)
        safe_col = col.replace('"', '""')
        cols.append(f'"{safe_col}" {coltype}')

    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'
    cursor.execute(sql)
    conn.commit()


def insert_chunk(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame):
    cursor = conn.cursor()

    cols = ['"{}"'.format(c.replace('"', '""')) for c in df.columns]
    placeholders = ",".join(["?"] * len(df.columns))
    sql = f'INSERT INTO "{table_name}" ({",".join(cols)}) VALUES ({placeholders})'

    df2 = df.where(pd.notna(df), None)
    cursor.executemany(sql, df2.itertuples(index=False, name=None))


def csv_to_sqlite(
    csv_path: Path,
    db_path: Path,
    table_name: str,
    if_exists: str = "fail",
    chunksize: int = 50000,
):
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to database at: {db_path}")
    conn = sqlite3.connect(db_path)

    try:
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = -200000") 
    except Exception:
        pass

    max_rows_env = os.getenv("CSV_TO_SQLITE_MAX_ROWS", "").strip()
    max_rows = int(max_rows_env) if max_rows_env.isdigit() else None

    total_rows = 0
    try:
        first = True
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = pd.read_csv(f, chunksize=chunksize, low_memory=False)
            for chunk in reader:
                if max_rows is not None and total_rows >= max_rows:
                    break

                if max_rows is not None and (total_rows + len(chunk)) > max_rows:
                    chunk = chunk.iloc[: max(0, max_rows - total_rows)].copy()

                if first:
                    create_table_from_df(conn, table_name, chunk, if_exists=if_exists)
                    print(f"Created table '{table_name}' with columns: {list(chunk.columns)}")
                    first = False

                insert_chunk(conn, table_name, chunk)
                conn.commit()
                total_rows += len(chunk)
                print(f"Inserted chunk of {len(chunk)} rows... (total: {total_rows})")

        if max_rows is not None:
            print(f"\nStopped early due to CSV_TO_SQLITE_MAX_ROWS={max_rows}.")

        print(f"\nSuccessfully loaded {total_rows} rows into '{table_name}'")
        print(f"Database saved to: {db_path}")

    finally:
        conn.close()


def verify_database(db_path: Path, table_name: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
    total_rows = cursor.fetchone()[0]

    cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = cursor.fetchall()

    print(f"\n--- Database Verification ---")
    print(f"Table: {table_name}")
    print(f"Total rows: {total_rows:,}")
    print(f"Columns ({len(columns)}):")
    for col in columns:
        cid, name, dtype, notnull, dflt_value, pk = col
        print(f"  - {name} ({dtype})")

    try:
        cursor.execute(f'''
            SELECT gender, COUNT(*) as count
            FROM "{table_name}"
            GROUP BY gender
            ORDER BY count DESC
        ''')
        distribution = cursor.fetchall()
        print(f"\nGender Distribution:")
        for gender, group_count in distribution:
            percentage = (group_count / total_rows) * 100 if total_rows > 0 else 0
            print(f"  {gender}: {group_count:,} ({percentage:.1f}%)")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute(f'''
            SELECT
                CASE
                    WHEN addiction_level < 2 THEN 'Low (0-2)'
                    WHEN addiction_level < 5 THEN 'Medium (2-5)'
                    ELSE 'High (5+)'
                END as bucket,
                COUNT(*) as count
            FROM "{table_name}"
            GROUP BY bucket
            ORDER BY MIN(addiction_level)
        ''')
        distribution = cursor.fetchall()
        print(f"\nAddiction Level Distribution:")
        for bucket, group_count in distribution:
            percentage = (group_count / total_rows) * 100 if total_rows > 0 else 0
            print(f"  {bucket}: {group_count:,} ({percentage:.1f}%)")
    except sqlite3.OperationalError:
        pass

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Convert Gaming Mental Health CSV to SQLite database."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help=f"Path to the CSV file (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to the SQLite DB file (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE_NAME,
        help=f"Name of the table to create (default: {DEFAULT_TABLE_NAME})",
    )
    parser.add_argument(
        "--if-exists",
        choices=["replace", "append", "fail"],
        default="fail",
        help="What to do if table exists (default: fail)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=50000,
        help="Number of rows to read per chunk (default: 50000)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        default=False,
        help="Verify database after creation (default: False)",
    )

    args = parser.parse_args()

    try:
        csv_to_sqlite(
            args.csv,
            args.db,
            args.table,
            if_exists=args.if_exists,
            chunksize=args.chunksize,
        )

        if args.verify:
            verify_database(args.db, args.table)

        print("\n✓ Conversion completed successfully!")
        print(f"\nYou can now use this database with the LLM agent.")
        print(f"Table name: '{args.table}'")
        print(f"Database path: {args.db}")

        return 0

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print(f"\nPlease ensure the CSV file exists at: {args.csv}")
        return 1
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
