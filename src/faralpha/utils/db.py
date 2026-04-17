"""
Database helper — DuckDB (default) or PostgreSQL (production VM).

* **DuckDB** — files under ``db/*.duckdb`` when ``DATABASE_URL`` / ``DB_HOST`` are unset.
* **PostgreSQL** — when ``DATABASE_URL`` or ``DB_HOST`` is set (self-hosted on VM).

Uses a singleton connection per backend. Callers use DuckDB-style ``execute(sql, [params])``
with ``?`` placeholders; PostgreSQL path translates them to ``%s``.

Callers still call ``con.close()`` on cursors — for Postgres this closes only the cursor.
"""

from __future__ import annotations

import os
import re
import threading
from typing import Any
from urllib.parse import unquote, urlparse

import duckdb
from faralpha.config import DB_PATH, use_postgres_database
from faralpha.utils.logger import get_logger

log = get_logger("db")

_lock = threading.Lock()
_shared_conn: Any = None


def _pg_dialect_fix(sql: str) -> str:
    s = sql.replace("NULL::DOUBLE", "NULL::double precision")
    s = re.sub(
        r"(?<!\.)\bDOUBLE\b(?!\s+PRECISION)",
        "DOUBLE PRECISION",
        s,
        flags=re.IGNORECASE,
    )
    s = s.replace("DOUBLE PRECISION PRECISION", "DOUBLE PRECISION")
    return s


def _adapt_sql(sql: str) -> str:
    s = _pg_dialect_fix(sql)
    if "?" in s:
        s = s.replace("?", "%s")
    return s


def _connect_postgres():
    import psycopg2

    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        p = urlparse(url)
        sslmode = "require" if p.hostname and "supabase" in (p.hostname or "") else "prefer"
        return psycopg2.connect(url, sslmode=sslmode)
    host = os.environ.get("DB_HOST", "localhost")
    port = int(os.environ.get("DB_PORT", "5432"))
    dbname = os.environ.get("DB_NAME", "postgres")
    user = os.environ.get("DB_USER", "postgres")
    password = unquote(os.environ.get("DB_PASSWORD", ""))
    sslmode = "prefer"
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
    )


class _PgCursor:
    __slots__ = ("_raw",)

    def __init__(self, raw: Any) -> None:
        self._raw = raw

    @property
    def description(self) -> Any:
        return self._raw.description

    def execute(self, sql: str, params: list | tuple | None = None) -> _PgCursor:
        p = tuple(params) if params is not None else None
        self._raw.execute(_adapt_sql(sql), p)
        return self

    def executemany(self, sql: str, seq_of_params: Any) -> _PgCursor:
        self._raw.executemany(_adapt_sql(sql), seq_of_params)
        return self

    def fetchone(self) -> Any:
        return self._raw.fetchone()

    def fetchall(self) -> Any:
        return self._raw.fetchall()

    def df(self) -> Any:
        import pandas as pd

        rows = self._raw.fetchall()
        cols = [d[0] for d in self._raw.description] if self._raw.description else []
        return pd.DataFrame(rows, columns=cols)

    def close(self) -> None:
        try:
            self._raw.close()
        except Exception:
            pass


def _get_conn_postgres(read_only: bool = False) -> _PgCursor:
    global _shared_conn
    with _lock:
        if _shared_conn is None:
            _shared_conn = _connect_postgres()
            _shared_conn.autocommit = True
            log.info("PostgreSQL singleton connection opened")
        return _PgCursor(_shared_conn.cursor())


def _get_conn_duckdb(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    global _shared_conn
    with _lock:
        if _shared_conn is None:
            _shared_conn = duckdb.connect(str(DB_PATH), read_only=False)
            log.info("DuckDB singleton connection opened → %s", DB_PATH)
        return _shared_conn.cursor()


def get_conn(read_only: bool = False) -> Any:
    if use_postgres_database():
        return _get_conn_postgres(read_only)
    return _get_conn_duckdb(read_only)


def close_shared() -> None:
    global _shared_conn
    with _lock:
        if _shared_conn is not None:
            _shared_conn.close()
            _shared_conn = None
            log.info("Database singleton connection closed")


def init_schema(con: Any | None = None) -> None:
    if use_postgres_database():
        _init_schema_postgres(con)
    else:
        _init_schema_duckdb(con)


def _init_schema_postgres(con: _PgCursor | None = None) -> None:
    close_after = False
    if con is None:
        con = get_conn()
        close_after = True

    stmts = [
        """
        CREATE TABLE IF NOT EXISTS stocks (
            ticker          VARCHAR  NOT NULL,
            company         VARCHAR,
            sector          VARCHAR,
            industry        VARCHAR,
            listing_date    DATE,
            delisting_date  DATE,
            sync_fail_count INTEGER  DEFAULT 0,
            market          VARCHAR  NOT NULL,
            PRIMARY KEY (ticker, market)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS prices (
            date    DATE     NOT NULL,
            ticker  VARCHAR  NOT NULL,
            open    DOUBLE PRECISION,
            high    DOUBLE PRECISION,
            low     DOUBLE PRECISION,
            close   DOUBLE PRECISION,
            volume  DOUBLE PRECISION,
            market  VARCHAR  NOT NULL,
            PRIMARY KEY (date, ticker, market)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS benchmark (
            date    DATE     NOT NULL,
            ticker  VARCHAR  NOT NULL,
            close   DOUBLE PRECISION,
            PRIMARY KEY (date, ticker)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS intraday_candles (
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
        """,
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
        """,
        """
        CREATE TABLE IF NOT EXISTS scrape_log (
            ticker      VARCHAR   NOT NULL,
            interval    VARCHAR   NOT NULL,
            last_ts     TIMESTAMP,
            candle_count INTEGER  DEFAULT 0,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, interval)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS live_trades (
            id              SERIAL PRIMARY KEY,
            ticker          VARCHAR   NOT NULL,
            entry_date      DATE      NOT NULL,
            exit_date       DATE,
            entry_price     DOUBLE PRECISION    NOT NULL,
            exit_price      DOUBLE PRECISION,
            shares          DOUBLE PRECISION    NOT NULL,
            pnl             DOUBLE PRECISION,
            pnl_pct         DOUBLE PRECISION,
            exit_reason     VARCHAR,
            hold_days       INTEGER,
            strategy        VARCHAR   DEFAULT 'momentum',
            market          VARCHAR   NOT NULL DEFAULT 'india',
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS live_trade_stats (
            date                DATE    NOT NULL,
            market              VARCHAR NOT NULL DEFAULT 'india',
            recent_trades       INTEGER,
            recent_wins         INTEGER,
            recent_win_rate     DOUBLE PRECISION,
            avg_win_pct         DOUBLE PRECISION,
            avg_loss_pct        DOUBLE PRECISION,
            expectancy          DOUBLE PRECISION,
            consecutive_losses  INTEGER DEFAULT 0,
            circuit_breaker_active BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (date, market)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS bear_volume_features (
            date             DATE    NOT NULL,
            ticker           VARCHAR NOT NULL,
            vol_ratio_5d     DOUBLE PRECISION,
            vol_ratio_20d    DOUBLE PRECISION,
            down_vol_5d      DOUBLE PRECISION,
            down_vol_10d     DOUBLE PRECISION,
            depth_pct        DOUBLE PRECISION,
            consec_down      INTEGER,
            body_ratio_avg5  DOUBLE PRECISION,
            vol_exhaustion   DOUBLE PRECISION,
            avg_down_range5  DOUBLE PRECISION,
            PRIMARY KEY (date, ticker)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            ticker           VARCHAR   NOT NULL,
            order_date       DATE      NOT NULL,
            signal_price     DOUBLE PRECISION NOT NULL,
            max_entry_price  DOUBLE PRECISION NOT NULL,
            initial_stop     DOUBLE PRECISION NOT NULL,
            current_stop     DOUBLE PRECISION NOT NULL,
            trail_pct        DOUBLE PRECISION NOT NULL,
            max_hold_days    INTEGER   NOT NULL,
            exit_date        DATE,
            quantity         INTEGER   NOT NULL,
            filled_qty       INTEGER   DEFAULT 0,
            avg_fill_price   DOUBLE PRECISION DEFAULT 0,
            invest_amount    DOUBLE PRECISION,
            risk_amount      DOUBLE PRECISION,
            risk_pct         DOUBLE PRECISION,
            buy_order_id     VARCHAR,
            sl_order_id      VARCHAR,
            exit_order_id    VARCHAR,
            buy_status       VARCHAR,
            sl_status        VARCHAR,
            status           VARCHAR   NOT NULL,
            exit_price       DOUBLE PRECISION,
            pnl              DOUBLE PRECISION,
            pnl_pct          DOUBLE PRECISION,
            errors           VARCHAR,
            fills            VARCHAR,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, order_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS intraday_signals (
            ticker           VARCHAR   NOT NULL,
            signal_date      DATE      NOT NULL,
            strategy         VARCHAR   NOT NULL DEFAULT 'vwap_reclaim',
            price            DOUBLE PRECISION NOT NULL,
            vwap             DOUBLE PRECISION,
            rvol             DOUBLE PRECISION,
            down_days        INTEGER,
            depth_pct        DOUBLE PRECISION,
            prev_close       DOUBLE PRECISION,
            day_open         DOUBLE PRECISION,
            day_change_pct   DOUBLE PRECISION,
            max_hold_days    INTEGER,
            trailing_stop_pct DOUBLE PRECISION,
            signal_time      TIMESTAMP NOT NULL,
            PRIMARY KEY (ticker, signal_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS first_hour_cache (
            cache_date  DATE    NOT NULL,
            token       INTEGER NOT NULL,
            fh_vol      DOUBLE PRECISION NOT NULL,
            fh_tp_vol   DOUBLE PRECISION NOT NULL,
            last_price  DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (cache_date, token)
        );
        """,
    ]

    for stmt in stmts:
        con.execute(stmt)

    try:
        con.execute(
            "ALTER TABLE stocks ADD COLUMN IF NOT EXISTS sync_fail_count INTEGER DEFAULT 0"
        )
        log.info("Migration: ensured sync_fail_count on stocks")
    except Exception:
        pass

    log.info("Schema initialised (PostgreSQL)")

    if close_after:
        con.close()


def _init_schema_duckdb(con: duckdb.DuckDBPyConnection | None = None) -> None:
    close_after = False
    if con is None:
        con = get_conn()
        close_after = True

    con.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            ticker          VARCHAR  NOT NULL,
            company         VARCHAR,
            sector          VARCHAR,
            industry        VARCHAR,
            listing_date    DATE,
            delisting_date  DATE,
            sync_fail_count INTEGER  DEFAULT 0,  -- consecutive download failures
            market          VARCHAR  NOT NULL,   -- 'india'
            PRIMARY KEY (ticker, market)
        );
    """)

    try:
        con.execute("ALTER TABLE stocks ADD COLUMN sync_fail_count INTEGER DEFAULT 0")
        log.info("Migration: added sync_fail_count column to stocks")
    except Exception:
        pass

    con.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            date    DATE     NOT NULL,
            ticker  VARCHAR  NOT NULL,
            open    DOUBLE,
            high    DOUBLE,
            low     DOUBLE,
            close   DOUBLE,
            volume  DOUBLE,
            market  VARCHAR  NOT NULL,
            PRIMARY KEY (date, ticker, market)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS benchmark (
            date    DATE     NOT NULL,
            ticker  VARCHAR  NOT NULL,
            close   DOUBLE,
            PRIMARY KEY (date, ticker)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS intraday_candles (
            ts          TIMESTAMP NOT NULL,  -- candle open time (IST)
            ticker      VARCHAR   NOT NULL,
            interval    VARCHAR   NOT NULL,  -- '15minute','30minute','60minute'
            open        DOUBLE,
            high        DOUBLE,
            low         DOUBLE,
            close       DOUBLE,
            volume      BIGINT,
            PRIMARY KEY (ts, ticker, interval)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS live_trades (
            id              INTEGER PRIMARY KEY,
            ticker          VARCHAR   NOT NULL,
            entry_date      DATE      NOT NULL,
            exit_date       DATE,
            entry_price     DOUBLE    NOT NULL,
            exit_price      DOUBLE,
            shares          DOUBLE    NOT NULL,
            pnl             DOUBLE,
            pnl_pct         DOUBLE,
            exit_reason     VARCHAR,
            hold_days       INTEGER,
            strategy        VARCHAR   DEFAULT 'momentum',  -- 'momentum' or 'bear_reversal'
            market          VARCHAR   NOT NULL DEFAULT 'india',
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS live_trade_stats (
            date                DATE    NOT NULL,
            market              VARCHAR NOT NULL DEFAULT 'india',
            recent_trades       INTEGER,
            recent_wins         INTEGER,
            recent_win_rate     DOUBLE,
            avg_win_pct         DOUBLE,
            avg_loss_pct        DOUBLE,
            expectancy          DOUBLE,
            consecutive_losses  INTEGER DEFAULT 0,
            circuit_breaker_active BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (date, market)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS bear_volume_features (
            date             DATE    NOT NULL,
            ticker           VARCHAR NOT NULL,
            vol_ratio_5d     DOUBLE,
            vol_ratio_20d    DOUBLE,
            down_vol_5d      DOUBLE,
            down_vol_10d     DOUBLE,
            depth_pct        DOUBLE,
            consec_down      INTEGER,
            body_ratio_avg5  DOUBLE,
            vol_exhaustion   DOUBLE,
            avg_down_range5  DOUBLE,
            PRIMARY KEY (date, ticker)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            ticker           VARCHAR   NOT NULL,
            order_date       DATE      NOT NULL,
            signal_price     DOUBLE    NOT NULL,
            max_entry_price  DOUBLE    NOT NULL,
            initial_stop     DOUBLE    NOT NULL,
            current_stop     DOUBLE    NOT NULL,
            trail_pct        DOUBLE    NOT NULL,
            max_hold_days    INTEGER   NOT NULL,
            exit_date        DATE,
            quantity         INTEGER   NOT NULL,
            filled_qty       INTEGER   DEFAULT 0,
            avg_fill_price   DOUBLE    DEFAULT 0,
            invest_amount    DOUBLE,
            risk_amount      DOUBLE,
            risk_pct         DOUBLE,
            buy_order_id     VARCHAR,
            sl_order_id      VARCHAR,
            exit_order_id    VARCHAR,
            buy_status       VARCHAR,
            sl_status        VARCHAR,
            status           VARCHAR   NOT NULL,
            exit_price       DOUBLE,
            pnl              DOUBLE,
            pnl_pct          DOUBLE,
            errors           VARCHAR,
            fills            VARCHAR,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, order_date)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS intraday_signals (
            ticker           VARCHAR   NOT NULL,
            signal_date      DATE      NOT NULL,
            strategy         VARCHAR   NOT NULL DEFAULT 'vwap_reclaim',
            price            DOUBLE    NOT NULL,
            vwap             DOUBLE,
            rvol             DOUBLE,
            down_days        INTEGER,
            depth_pct        DOUBLE,
            prev_close       DOUBLE,
            day_open         DOUBLE,
            day_change_pct   DOUBLE,
            max_hold_days    INTEGER,
            trailing_stop_pct DOUBLE,
            signal_time      TIMESTAMP NOT NULL,
            PRIMARY KEY (ticker, signal_date)
        );
    """)

    log.info("Schema initialised (DuckDB)")

    if close_after:
        con.close()
