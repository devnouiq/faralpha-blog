"""
Database helper — DuckDB connection + schema management.

Uses a **singleton connection** so the entire process shares one DuckDB
handle.  This avoids the "can't open a connection with a different
configuration" error that DuckDB raises when mixing read-only and
read-write connections to the same file.

Callers still call ``con.close()`` — it is silently ignored so the
underlying connection stays alive until ``close_shared()`` is called
(at app shutdown).
"""

import threading

import duckdb
from faralpha.config import DB_PATH
from faralpha.utils.logger import get_logger

log = get_logger("db")

_lock = threading.Lock()
_shared_conn: duckdb.DuckDBPyConnection | None = None


def get_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a fresh DuckDB **cursor** from the singleton connection.

    Each caller gets an isolated cursor that is safe for concurrent use
    from multiple threads.  Call ``cursor.close()`` when done — this
    only closes the cursor, not the underlying connection.

    The *read_only* parameter is accepted for backward compatibility
    but ignored — we always open one read-write connection.
    """
    global _shared_conn
    with _lock:
        if _shared_conn is None:
            _shared_conn = duckdb.connect(str(DB_PATH), read_only=False)
            log.info("DuckDB singleton connection opened → %s", DB_PATH)
        return _shared_conn.cursor()


def close_shared() -> None:
    """Close the singleton connection (call once at app shutdown)."""
    global _shared_conn
    with _lock:
        if _shared_conn is not None:
            _shared_conn.close()
            _shared_conn = None
            log.info("DuckDB singleton connection closed")


def init_schema(con: duckdb.DuckDBPyConnection | None = None) -> None:
    """Create all core tables if they don't exist yet."""
    close_after = False
    if con is None:
        con = get_conn()
        close_after = True

    # ── Universe of stocks (point-in-time) ──
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

    # Migration: add sync_fail_count to existing stocks tables
    try:
        con.execute("ALTER TABLE stocks ADD COLUMN sync_fail_count INTEGER DEFAULT 0")
        log.info("Migration: added sync_fail_count column to stocks")
    except Exception:
        pass  # column already exists

    # ── Daily OHLCV ──
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

    # ── Benchmark index (Nifty / S&P 500) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS benchmark (
            date    DATE     NOT NULL,
            ticker  VARCHAR  NOT NULL,
            close   DOUBLE,
            PRIMARY KEY (date, ticker)
        );
    """)

    # ── Intraday OHLCV candles (Kite API) ──
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

    # ── Live trades log (production parity with backtest) ──
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

    # ── Live trade stats (running window for adaptive sizing) ──
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

    # ── Bear volume features (daily-derived weakness signals) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS bear_volume_features (
            date             DATE    NOT NULL,
            ticker           VARCHAR NOT NULL,
            -- Volume ratios
            vol_ratio_5d     DOUBLE,   -- today vol / 5d avg
            vol_ratio_20d    DOUBLE,   -- today vol / 20d avg
            -- Selling pressure
            down_vol_5d      DOUBLE,   -- sum(vol on down days) / sum(vol) over 5d
            down_vol_10d     DOUBLE,   -- sum(vol on down days) / sum(vol) over 10d
            -- Weakness depth
            depth_pct        DOUBLE,   -- (close - high_20d) / high_20d
            consec_down      INTEGER,  -- consecutive down days streak
            body_ratio_avg5  DOUBLE,   -- avg |close-open|/|high-low| over 5d
            -- Exhaustion
            vol_exhaustion   DOUBLE,   -- vol today vs vol at start of drop
            avg_down_range5  DOUBLE,   -- avg (high-low)/close on down days, 5d
            PRIMARY KEY (date, ticker)
        );
    """)

    # ── Auto-trade orders (replaces JSON log) ──
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

    # ── Intraday signals (replaces intraday_signals.json) ──
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

    log.info("Schema initialised")

    if close_after:
        con.close()
