"""
Intraday Database — DuckDB files *or* shared PostgreSQL (same as ``faralpha.utils.db``).

When ``DATABASE_URL`` / ``DB_HOST`` is set, candles live in Postgres ``candles`` table.
Otherwise legacy DuckDB files under ``db/`` (per-interval files for parallel writes).
"""

from __future__ import annotations

import threading

import duckdb
import pandas as pd
from psycopg2.extras import execute_values

from faralpha.config import INTRADAY_DB_PATH, INTRADAY_DB_PATHS, use_postgres_database
from faralpha.utils.logger import get_logger

log = get_logger("intraday_db")

_lock = threading.Lock()
_shared_conn: duckdb.DuckDBPyConnection | None = None

_interval_lock = threading.Lock()
_interval_conns: dict[str, duckdb.DuckDBPyConnection] = {}


def get_conn(read_only: bool = False):
    """Return a cursor — Postgres (shared) or DuckDB (legacy intraday file)."""
    if use_postgres_database():
        from faralpha.utils.db import get_conn as _main

        return _main(read_only=read_only)
    global _shared_conn
    with _lock:
        if _shared_conn is None:
            _shared_conn = duckdb.connect(str(INTRADAY_DB_PATH), read_only=False)
            log.info("Intraday DB opened → %s", INTRADAY_DB_PATH)
        return _shared_conn.cursor()


def get_interval_conn(interval: str, read_only: bool = False):
    """Postgres: same connection as main. DuckDB: per-interval file."""
    if use_postgres_database():
        from faralpha.utils.db import get_conn as _main

        return _main(read_only=read_only)
    db_path = INTRADAY_DB_PATHS.get(interval)
    if db_path is None:
        raise ValueError(f"Unknown interval: {interval}. Valid: {list(INTRADAY_DB_PATHS.keys())}")

    with _interval_lock:
        if interval not in _interval_conns or _interval_conns[interval] is None:
            conn = duckdb.connect(str(db_path), read_only=read_only)
            _interval_conns[interval] = conn
            log.info("Intraday DB [%s] opened → %s", interval, db_path)
        return _interval_conns[interval].cursor()


def close_shared() -> None:
    if use_postgres_database():
        from faralpha.utils.db import close_shared as _close_main

        _close_main()
        return
    global _shared_conn
    with _lock:
        if _shared_conn is not None:
            _shared_conn.close()
            _shared_conn = None
            log.info("Intraday DB closed")


def close_interval(interval: str) -> None:
    if use_postgres_database():
        return
    with _interval_lock:
        conn = _interval_conns.pop(interval, None)
        if conn is not None:
            conn.close()
            log.info("Intraday DB [%s] closed", interval)


def close_all_intervals() -> None:
    if use_postgres_database():
        return
    with _interval_lock:
        for ivl, conn in list(_interval_conns.items()):
            if conn is not None:
                conn.close()
                log.info("Intraday DB [%s] closed", ivl)
        _interval_conns.clear()


_CREATE_CANDLES = """
    CREATE TABLE IF NOT EXISTS candles (
        ts          TIMESTAMP NOT NULL,
        ticker      VARCHAR   NOT NULL,
        interval    VARCHAR   NOT NULL,
        open        DOUBLE,
        high        DOUBLE,
        low         DOUBLE,
        close       DOUBLE,
        volume      BIGINT,
        PRIMARY KEY (ts, ticker, interval)
    );
"""

_CREATE_SCRAPE_LOG = """
    CREATE TABLE IF NOT EXISTS scrape_log (
        ticker      VARCHAR   NOT NULL,
        interval    VARCHAR   NOT NULL,
        last_ts     TIMESTAMP,
        candle_count INTEGER  DEFAULT 0,
        updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ticker, interval)
    );
"""


def init_schema(con=None) -> None:
    close_after = False
    if con is None:
        con = get_conn()
        close_after = True

    if use_postgres_database():
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS candles (
                ts          TIMESTAMP NOT NULL,
                ticker      VARCHAR   NOT NULL,
                interval    VARCHAR   NOT NULL,
                open        DOUBLE PRECISION,
                high        DOUBLE PRECISION,
                low         DOUBLE PRECISION,
                close       DOUBLE PRECISION,
                volume      BIGINT,
                PRIMARY KEY (ts, ticker, interval)
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS scrape_log (
                ticker      VARCHAR   NOT NULL,
                interval    VARCHAR   NOT NULL,
                last_ts     TIMESTAMP,
                candle_count INTEGER  DEFAULT 0,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, interval)
            );
            """
        )
    else:
        con.execute(_CREATE_CANDLES)
        con.execute(_CREATE_SCRAPE_LOG)

    if close_after:
        con.close()


def init_interval_schema(interval: str) -> None:
    _ = interval
    init_schema()


def upsert_candles_from_dataframe(cursor, df: pd.DataFrame) -> None:
    """PostgreSQL upsert for candle rows (replaces DuckDB ``INSERT … FROM df``)."""
    if df is None or df.empty:
        return
    raw = getattr(cursor, "_raw", cursor)
    cols = ["ts", "ticker", "interval", "open", "high", "low", "close", "volume"]
    for c in cols:
        if c not in df.columns:
            raise ValueError(f"upsert_candles_from_dataframe: missing column {c}")
    tuples = [tuple(x) for x in df[cols].to_numpy()]
    sql = """
        INSERT INTO candles (ts, ticker, interval, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (ts, ticker, interval) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
    """
    execute_values(raw, sql, tuples, page_size=5000)
