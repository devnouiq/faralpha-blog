"""
Intraday Database — separate DuckDB for Kite intraday candles.

Keeps intraday data (15m/30m/60m OHLCV) completely isolated from the
daily market database so that scraping and experiments never corrupt
the main prices/benchmark data.

Supports per-interval DB files for parallel writes:
  - intraday_15m.duckdb, intraday_30m.duckdb, intraday_60m.duckdb
  - Each has its own write lock → 3 intervals can fetch simultaneously
  - Legacy single-file mode (intraday.duckdb) still supported
"""

import threading

import duckdb
from faralpha.config import INTRADAY_DB_PATH, INTRADAY_DB_PATHS
from faralpha.utils.logger import get_logger

log = get_logger("intraday_db")

_lock = threading.Lock()
_shared_conn: duckdb.DuckDBPyConnection | None = None

# Per-interval singleton connections
_interval_lock = threading.Lock()
_interval_conns: dict[str, duckdb.DuckDBPyConnection] = {}


def get_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a cursor from the singleton intraday DuckDB connection (legacy single-file)."""
    global _shared_conn
    with _lock:
        if _shared_conn is None:
            _shared_conn = duckdb.connect(str(INTRADAY_DB_PATH), read_only=False)
            log.info("Intraday DB opened → %s", INTRADAY_DB_PATH)
        return _shared_conn.cursor()


def get_interval_conn(interval: str, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a cursor for an interval-specific DB file.

    Each interval gets its own DuckDB file → its own write lock →
    3 fetchers can run in parallel (one per interval).
    """
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
    """Close the singleton connection."""
    global _shared_conn
    with _lock:
        if _shared_conn is not None:
            _shared_conn.close()
            _shared_conn = None
            log.info("Intraday DB closed")


def close_interval(interval: str) -> None:
    """Close a per-interval connection."""
    with _interval_lock:
        conn = _interval_conns.pop(interval, None)
        if conn is not None:
            conn.close()
            log.info("Intraday DB [%s] closed", interval)


def close_all_intervals() -> None:
    """Close all per-interval connections."""
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


def init_schema(con: duckdb.DuckDBPyConnection | None = None) -> None:
    """Create intraday tables if they don't exist."""
    close_after = False
    if con is None:
        con = get_conn()
        close_after = True

    con.execute(_CREATE_CANDLES)
    con.execute(_CREATE_SCRAPE_LOG)

    if close_after:
        con.close()


def init_interval_schema(interval: str) -> None:
    """Create tables in an interval-specific DB file."""
    con = get_interval_conn(interval)
    con.execute(_CREATE_CANDLES)
    con.execute(_CREATE_SCRAPE_LOG)
    con.close()
