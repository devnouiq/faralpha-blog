#!/usr/bin/env python3
"""
One-time (or repeatable) migration: DuckDB files → PostgreSQL.

Why this exists
---------------
Option B only switches the *driver* (Postgres instead of DuckDB). It does **not**
copy rows. A new Postgres database is empty until you migrate or re-run pipelines.

This script:
  1. Connects to Postgres via ``DATABASE_URL`` (or ``--database-url``).
  2. Runs ``init_schema()`` so base tables exist.
  3. Reads ``--market-db`` (default: project ``db/market.duckdb``) read-only.
  4. Copies every user table from that DuckDB into Postgres (append/upsert).
  5. Optionally merges Kite shard DBs (``--intraday-db``) into ``candles`` / ``scrape_log``.

Run on the VM from the project root (with Postgres credentials):

  export DATABASE_URL='postgresql://USER:PASS@127.0.0.1:5432/faralpha'
  cd ~/faralpha-blog-new
  uv run python scripts/migrate_duckdb_to_postgres.py

Optional:

  uv run python scripts/migrate_duckdb_to_postgres.py \\
    --market-db db/market.duckdb \\
    --intraday-db db/intraday.duckdb \\
    --intraday-db db/intraday_15m.duckdb \\
    --intraday-db db/intraday_30m.duckdb \\
    --intraday-db db/intraday_60m.duckdb

Safety: source DuckDB is **read-only**. Does not delete DuckDB files.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Project root = parent of scripts/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))


def _duck_type_to_pg(duck_type: str) -> str:
    u = (duck_type or "VARCHAR").upper()
    if "BIGINT" in u or u == "HUGEINT":
        return "BIGINT"
    if "INT" in u and "BIG" not in u and "POINT" not in u:
        return "INTEGER"
    if "DOUBLE" in u or "FLOAT" in u or "REAL" in u or "DECIMAL" in u:
        return "DOUBLE PRECISION"
    if "BOOL" in u:
        return "BOOLEAN"
    if "DATE" in u and "TIME" not in u and "STAMP" not in u:
        return "DATE"
    if "TIMESTAMP" in u or "TIME" in u:
        return "TIMESTAMP"
    return "TEXT"


def _list_duck_tables(con) -> list[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    ).fetchall()
    return [r[0] for r in rows]


def _describe_columns(con, table: str) -> list[tuple[str, str]]:
    rows = con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table],
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _ensure_table_pg(pg_cur, table: str, columns: list[tuple[str, str]]) -> None:
    cols_sql = ", ".join(
        f'"{c}" {_duck_type_to_pg(dt)}' for c, dt in columns
    )
    pg_cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')


def _pg_quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _cell_to_pg(val):
    """Make values psycopg2-safe: None for missing, native Python types for numpy/pandas scalars."""
    import math

    import numpy as np
    import pandas as pd

    if val is None:
        return None

    # numpy datetime / timedelta (incl. NaT)
    if isinstance(val, np.datetime64):
        if pd.isna(val):
            return None
        return pd.Timestamp(val).to_pydatetime()
    if isinstance(val, np.timedelta64):
        if pd.isna(val):
            return None
        return pd.Timedelta(val).to_pytimedelta()

    # bool_, int64, float64, etc. → plain bool / int / float
    if isinstance(val, np.generic):
        val = val.item()

    if isinstance(val, float) and math.isnan(val):
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def _df_to_tuples(df):
    """Row tuples with NULL-safe cells (avoids NAType / nan adaptation errors)."""
    tuples = []
    for row in df.itertuples(index=False, name=None):
        tuples.append(tuple(_cell_to_pg(x) for x in row))
    return tuples


def _insert_df_chunked(pg_cur, table: str, df, chunk: int = 50_000) -> int:
    """Plain INSERT in chunks (pipeline tables without a known unique key)."""
    from psycopg2.extras import execute_values

    if df is None or len(df) == 0:
        return 0
    cols = list(df.columns)
    col_list = ", ".join(_pg_quote_ident(c) for c in cols)
    n = 0
    for start in range(0, len(df), chunk):
        part = df.iloc[start : start + chunk]
        tuples = _df_to_tuples(part)
        sql = f'INSERT INTO {_pg_quote_ident(table)} ({col_list}) VALUES %s'
        execute_values(pg_cur, sql, tuples, page_size=min(chunk, 5000))
        n += len(part)
    return n


def _insert_df_upsert(
    pg_cur,
    table: str,
    df,
    conflict_cols: str,
    update_cols: str,
    chunk: int = 50_000,
) -> int:
    from psycopg2.extras import execute_values

    if df is None or len(df) == 0:
        return 0
    cols = list(df.columns)
    col_list = ", ".join(_pg_quote_ident(c) for c in cols)
    n = 0
    for start in range(0, len(df), chunk):
        part = df.iloc[start : start + chunk]
        tuples = _df_to_tuples(part)
        sql = (
            f'INSERT INTO {_pg_quote_ident(table)} ({col_list}) VALUES %s '
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_cols}"
        )
        execute_values(pg_cur, sql, tuples, page_size=min(chunk, 5000))
        n += len(part)
    return n


def _build_update_set(cols: list[str], conflict: list[str]) -> str:
    skip = set(conflict)
    parts = [f'{_pg_quote_ident(c)} = EXCLUDED.{_pg_quote_ident(c)}' for c in cols if c not in skip]
    return ", ".join(parts) if parts else _pg_quote_ident(cols[0]) + " = EXCLUDED." + _pg_quote_ident(cols[0])


def migrate_market_db(database_url: str, market_db: Path, dry_run: bool) -> None:
    import duckdb as ddb
    import psycopg2

    if not market_db.exists():
        print(f"ERROR: market DuckDB not found: {market_db}", file=sys.stderr)
        sys.exit(1)

    src = ddb.connect(str(market_db), read_only=True)
    pg = psycopg2.connect(database_url)
    pg.autocommit = True
    pg_cur = pg.cursor()

    os.environ["DATABASE_URL"] = database_url
    from faralpha.utils.db import close_shared, init_schema

    if not dry_run:
        init_schema()
        close_shared()

    tables = _list_duck_tables(src)
    print(f"Tables in {market_db.name}: {len(tables)}")

    # Tables already created by init_schema() — do not re-CREATE with guessed types
    base_tables = frozenset(
        {
            "stocks",
            "prices",
            "benchmark",
            "intraday_candles",
            "candles",
            "scrape_log",
            "live_trades",
            "live_trade_stats",
            "bear_volume_features",
            "orders",
            "intraday_signals",
            "first_hour_cache",
        }
    )

    # Known PKs for upsert (same as app schema)
    upsert_map: dict[str, tuple[str, list[str]]] = {
        "stocks": ("ticker, market", ["ticker", "market"]),
        "prices": ("date, ticker, market", ["date", "ticker", "market"]),
        "benchmark": ("date, ticker", ["date", "ticker"]),
        "intraday_candles": ("ts, ticker, interval", ["ts", "ticker", "interval"]),
        "candles": ("ts, ticker, interval", ["ts", "ticker", "interval"]),
        "scrape_log": ("ticker, interval", ["ticker", "interval"]),
        "live_trade_stats": ("date, market", ["date", "market"]),
        "bear_volume_features": ("date, ticker", ["date", "ticker"]),
        "orders": ("ticker, order_date", ["ticker", "order_date"]),
        "intraday_signals": ("ticker, signal_date", ["ticker", "signal_date"]),
        "first_hour_cache": ("cache_date, token", ["cache_date", "token"]),
    }

    for table in tables:
        cols = _describe_columns(src, table)
        if not cols:
            continue
        count = src.execute(f'SELECT COUNT(*) FROM {_pg_quote_ident(table)}').fetchone()[0]
        if count == 0:
            print(f"  skip empty: {table}")
            continue

        print(f"  migrating {table}: {count:,} rows …")
        if dry_run:
            continue

        df = src.execute(f"SELECT * FROM {_pg_quote_ident(table)}").df()

        if table not in base_tables:
            _ensure_table_pg(pg_cur, table, cols)

        if table == "live_trades":
            # Postgres uses SERIAL — insert with explicit id to preserve
            col_list = ", ".join(_pg_quote_ident(c) for c in df.columns)
            from psycopg2.extras import execute_values

            tuples = _df_to_tuples(df)
            sql = (
                f"INSERT INTO live_trades ({col_list}) VALUES %s "
                "ON CONFLICT (id) DO UPDATE SET "
                "ticker = EXCLUDED.ticker, entry_date = EXCLUDED.entry_date, "
                "exit_date = EXCLUDED.exit_date, entry_price = EXCLUDED.entry_price, "
                "exit_price = EXCLUDED.exit_price, shares = EXCLUDED.shares, "
                "pnl = EXCLUDED.pnl, pnl_pct = EXCLUDED.pnl_pct, exit_reason = EXCLUDED.exit_reason, "
                "hold_days = EXCLUDED.hold_days, strategy = EXCLUDED.strategy, market = EXCLUDED.market, "
                "created_at = EXCLUDED.created_at"
            )
            execute_values(pg_cur, sql, tuples, page_size=2000)
            pg_cur.execute(
                "SELECT setval(pg_get_serial_sequence('live_trades','id'), "
                "COALESCE((SELECT MAX(id) FROM live_trades), 1))"
            )
        elif table in upsert_map:
            conflict_cols, key_cols = upsert_map[table]
            update_cols = _build_update_set(list(df.columns), key_cols)
            _insert_df_upsert(pg_cur, table, df, conflict_cols, update_cols)
        else:
            _insert_df_chunked(pg_cur, table, df)

    src.close()
    pg_cur.close()
    pg.close()
    print("Market DB migration finished.")


def merge_intraday_dbs(database_url: str, paths: list[Path], dry_run: bool) -> None:
    import duckdb as ddb
    import psycopg2
    from psycopg2.extras import execute_values

    if not paths:
        return
    pg = psycopg2.connect(database_url)
    pg.autocommit = True
    pg_cur = pg.cursor()

    for p in paths:
        if not p.exists():
            print(f"  skip missing intraday DB: {p}")
            continue
        src = ddb.connect(str(p), read_only=True)
        tabs = _list_duck_tables(src)
        if "candles" in tabs:
            n = src.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
            if n == 0:
                print(f"  {p.name}: candles empty")
                src.close()
                continue
            print(f"  {p.name}: merging candles ({n:,} rows) …")
            if dry_run:
                src.close()
                continue
            df = src.execute("SELECT * FROM candles").df()
            cols = ["ts", "ticker", "interval", "open", "high", "low", "close", "volume"]
            missing = [c for c in cols if c not in df.columns]
            if missing:
                print(f"    ERROR: candles missing columns {missing}", file=sys.stderr)
                src.close()
                continue
            tuples = _df_to_tuples(df[cols])
            sql = """
                INSERT INTO candles (ts, ticker, interval, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (ts, ticker, interval) DO UPDATE SET
                    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                    close = EXCLUDED.close, volume = EXCLUDED.volume
            """
            execute_values(pg_cur, sql, tuples, page_size=5000)
        if "scrape_log" in tabs:
            n = src.execute("SELECT COUNT(*) FROM scrape_log").fetchone()[0]
            if n > 0:
                print(f"  {p.name}: merging scrape_log ({n:,} rows) …")
                if not dry_run:
                    df = src.execute("SELECT * FROM scrape_log").df()
                    slog_cols = ["ticker", "interval", "last_ts", "candle_count", "updated_at"]
                    slog_cols = [c for c in slog_cols if c in df.columns]
                    if not slog_cols:
                        print("    ERROR: scrape_log has no expected columns", file=sys.stderr)
                    else:
                        sub = df[slog_cols]
                        col_list = ", ".join(slog_cols)
                        tuples = _df_to_tuples(sub)
                        execute_values(
                            pg_cur,
                            f"INSERT INTO scrape_log ({col_list}) VALUES %s "
                            "ON CONFLICT (ticker, interval) DO UPDATE SET "
                            "last_ts = EXCLUDED.last_ts, candle_count = EXCLUDED.candle_count, "
                            "updated_at = EXCLUDED.updated_at",
                            tuples,
                            page_size=2000,
                        )
        src.close()

    pg_cur.close()
    pg.close()
    print("Intraday shard merge finished.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate DuckDB → PostgreSQL")
    parser.add_argument(
        "--market-db",
        type=Path,
        default=PROJECT_ROOT / "db" / "market.duckdb",
        help="Path to market.duckdb",
    )
    parser.add_argument(
        "--intraday-db",
        action="append",
        default=[],
        metavar="PATH",
        help="Kite intraday DuckDB file (repeatable). Merges candles/scrape_log.",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", "").strip(),
        help="Postgres URL (default: env DATABASE_URL)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List tables and row counts only",
    )
    args = parser.parse_args()

    if not args.database_url:
        print("ERROR: set DATABASE_URL or pass --database-url", file=sys.stderr)
        sys.exit(1)

    market = args.market_db
    if not market.is_absolute():
        market = PROJECT_ROOT / market

    intraday_paths: list[Path] = []
    for raw in args.intraday_db:
        p = Path(raw)
        intraday_paths.append(p if p.is_absolute() else PROJECT_ROOT / p)

    if not intraday_paths:
        intraday_paths = [
            p
            for p in (
                PROJECT_ROOT / "db" / "intraday.duckdb",
                PROJECT_ROOT / "db" / "intraday_15m.duckdb",
                PROJECT_ROOT / "db" / "intraday_30m.duckdb",
                PROJECT_ROOT / "db" / "intraday_60m.duckdb",
            )
            if p.exists()
        ]
        if intraday_paths:
            print(
                "Using default intraday DB paths:",
                ", ".join(p.name for p in intraday_paths),
            )

    migrate_market_db(args.database_url, market, args.dry_run)
    merge_intraday_dbs(args.database_url, intraday_paths, args.dry_run)


if __name__ == "__main__":
    main()
