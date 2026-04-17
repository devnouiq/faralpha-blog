#!/usr/bin/env python3
"""
Step 01 — Universe Builder
===========================
Downloads the FULL list of equities for India (NSE).
Stores them in the ``stocks`` table with listing/delisting dates so that
every backtest date uses only stocks that actually existed at that time
(no survivorship bias).

Usage:
    uv run python -m faralpha.pipeline.s01_universe --market india
"""

from __future__ import annotations

import io
import time
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf
from tqdm import tqdm

from faralpha import config
from faralpha.utils.db import get_conn, init_schema
from faralpha.utils.logger import get_logger

log = get_logger("s01_universe")

_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ═══════════════════════════════════════════════════════════
#  INDIA  (NSE)
# ═══════════════════════════════════════════════════════════

_NSE_EQUITY_URLS = [
    "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
]


def _fetch_nse_equity_csv() -> pd.DataFrame:
    """Try multiple NSE archive URLs; fall back to local cache."""
    for url in _NSE_EQUITY_URLS:
        try:
            log.info(f"Fetching NSE equity list from {url}")
            resp = requests.get(url, headers=_HTTP_HEADERS, timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            # Cache for offline use
            cache = config.RAW_DIR / "nse_equity_list.csv"
            cache.write_text(resp.text)
            log.info(f"NSE list: {len(df)} rows (cached → {cache.name})")
            return df
        except Exception as exc:
            log.warning(f"  {url} failed: {exc}")

    # Fallback: local cache
    cache = config.RAW_DIR / "nse_equity_list.csv"
    if cache.exists():
        log.info("Using cached NSE equity list")
        return pd.read_csv(cache)

    raise RuntimeError("Cannot fetch NSE equity list from any source")


def _normalise_nse(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean NSE CSV into a standard DataFrame."""
    raw.columns = [c.strip().upper() for c in raw.columns]

    # Map whatever columns NSE gives us
    col_map: dict[str, str] = {}
    for c in raw.columns:
        cu = c.upper()
        if "SYMBOL" in cu and "ticker" not in col_map.values():
            col_map[c] = "ticker"
        elif "NAME" in cu and "COMPANY" in cu:
            col_map[c] = "company"
        elif "SERIES" in cu:
            col_map[c] = "series"
        elif "LIST" in cu and "DATE" in cu:
            col_map[c] = "listing_date"

    raw = raw.rename(columns=col_map)

    # Keep only regular equity series
    if "series" in raw.columns:
        raw = raw[raw["series"].str.strip() == "EQ"].copy()

    # Parse listing date
    if "listing_date" in raw.columns:
        raw["listing_date"] = pd.to_datetime(
            raw["listing_date"], dayfirst=True, errors="coerce"
        )
    else:
        raw["listing_date"] = pd.NaT

    raw["delisting_date"] = pd.NaT
    raw["market"] = "india"
    raw["sector"] = None
    raw["industry"] = None

    if "company" not in raw.columns:
        raw["company"] = None

    out = raw[
        ["ticker", "company", "sector", "industry", "listing_date", "delisting_date", "market"]
    ].drop_duplicates(subset=["ticker"])
    return out


def _build_india_universe() -> pd.DataFrame:
    raw = _fetch_nse_equity_csv()
    return _normalise_nse(raw)


# ═══════════════════════════════════════════════════════════
#  METADATA ENRICHMENT  (sector, industry, listing_date)
# ═══════════════════════════════════════════════════════════

def _enrich_metadata(df: pd.DataFrame, market: str, limit: int = 0) -> pd.DataFrame:
    """
    Fill missing sector / industry / listing_date from yfinance.

    Args:
        limit: if >0, only enrich this many tickers (for quick testing).
    """
    needs_enrichment = df[
        df["sector"].isna() | df["listing_date"].isna()
    ]["ticker"].tolist()

    if limit > 0:
        needs_enrichment = needs_enrichment[:limit]

    if not needs_enrichment:
        log.info("All metadata already present — nothing to enrich")
        return df

    suffix = config.YF_SUFFIX[market]
    log.info(f"Enriching {len(needs_enrichment)} tickers via yfinance ({market})")

    enriched = 0
    for t in tqdm(needs_enrichment, desc=f"yfinance [{market}]"):
        try:
            info = yf.Ticker(f"{t}{suffix}").info
            idx = df["ticker"] == t

            if df.loc[idx, "sector"].isna().any():
                df.loc[idx, "sector"] = info.get("sector")
            if df.loc[idx, "industry"].isna().any():
                df.loc[idx, "industry"] = info.get("industry")

            # listing_date from firstTradeDateEpochUtc
            if df.loc[idx, "listing_date"].isna().any():
                epoch = info.get("firstTradeDateEpochUtc")
                if epoch:
                    df.loc[idx, "listing_date"] = pd.to_datetime(
                        epoch, unit="s"
                    ).normalize()

            enriched += 1
            time.sleep(0.12)  # rate-limit ~8 req/s
        except Exception:
            pass  # yfinance can fail on obscure tickers — skip silently

    log.info(f"Enriched {enriched}/{len(needs_enrichment)} tickers")
    return df


# ═══════════════════════════════════════════════════════════
#  STORE TO DATABASE
# ═══════════════════════════════════════════════════════════

def _store(df: pd.DataFrame, market: str, con) -> int:
    """Upsert DataFrame into the stocks table. Returns row count.

    Preserves ``sync_fail_count`` and ``delisting_date`` that were set
    by the price-sync process for tickers that still appear in the
    exchange listing.  Tickers removed from the exchange are simply
    dropped (they won't be in *df*).
    """
    # Ensure correct dtypes before insert
    df["listing_date"] = pd.to_datetime(df["listing_date"], errors="coerce")
    df["delisting_date"] = pd.to_datetime(df["delisting_date"], errors="coerce")

    # Save delisting/failure metadata from previous sync runs
    try:
        preserved = con.execute("""
            SELECT ticker, delisting_date, sync_fail_count
            FROM stocks
            WHERE market = ?
              AND (delisting_date IS NOT NULL OR COALESCE(sync_fail_count, 0) > 0)
        """, [market]).df()
    except Exception:
        preserved = pd.DataFrame()

    con.execute("DELETE FROM stocks WHERE market = ?", [market])
    from faralpha.config import use_postgres_database

    if use_postgres_database():
        from psycopg2.extras import execute_values

        raw = getattr(con, "_raw", con)
        cols = [
            "ticker",
            "company",
            "sector",
            "industry",
            "listing_date",
            "delisting_date",
            "market",
        ]
        sub = df[cols]
        tuples = [tuple(x) for x in sub.to_numpy()]
        sql = """
            INSERT INTO stocks (ticker, company, sector, industry,
                listing_date, delisting_date, market)
            VALUES %s
        """
        execute_values(raw, sql, tuples, page_size=2000)
    else:
        try:
            con.connection.register("df", df)
        except Exception:
            pass
        con.execute("""
            INSERT INTO stocks (ticker, company, sector, industry,
                                listing_date, delisting_date, market)
            SELECT ticker, company, sector, industry,
                   listing_date, delisting_date, market
            FROM df
        """)

    # Restore preserved sync metadata for tickers still in the universe
    if not preserved.empty:
        for _, row in preserved.iterrows():
            con.execute("""
                UPDATE stocks
                SET delisting_date  = COALESCE(?, delisting_date),
                    sync_fail_count = COALESCE(?, sync_fail_count)
                WHERE ticker = ? AND market = ?
            """, [row["delisting_date"], row["sync_fail_count"],
                  row["ticker"], market])

    count = con.execute(
        "SELECT COUNT(*) FROM stocks WHERE market = ?", [market]
    ).fetchone()[0]
    return count


# ═══════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════

def run(
    market: str = "both",
    enrich: bool = True,
    enrich_limit: int = 0,
) -> None:
    """
    Build and store the universe for the given market(s).

    Args:
        market:        'india'
        enrich:        call yfinance to fill missing sector/listing_date
        enrich_limit:  if >0, only enrich N tickers (fast testing)
    """
    con = get_conn()
    init_schema(con)

    markets = config.MARKETS if market == "both" else [market]

    for mkt in markets:
        log.info(f"{'═' * 50}")
        log.info(f"Building universe: {mkt.upper()}")
        log.info(f"{'═' * 50}")

        if mkt == "india":
            df = _build_india_universe()
        else:
            raise ValueError(f"Unknown market: {mkt}")

        if enrich:
            df = _enrich_metadata(df, mkt, limit=enrich_limit)

        count = _store(df, mkt, con)
        log.info(f"✓ {mkt.upper()} universe stored: {count} tickers")

    # Summary
    total = con.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    per_market = con.execute(
        "SELECT market, COUNT(*) AS n FROM stocks GROUP BY market"
    ).fetchall()
    log.info(f"Total universe: {total} tickers  {dict(per_market)}")
    con.close()


# ═══════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Build stock universe")
    p.add_argument(
        "--market", default="india", choices=["india"],
        help="Which market to build (default: india)",
    )
    p.add_argument(
        "--no-enrich", action="store_true",
        help="Skip yfinance metadata enrichment (faster)",
    )
    p.add_argument(
        "--enrich-limit", type=int, default=0,
        help="Only enrich N tickers (for quick testing)",
    )
    args = p.parse_args()
    run(
        market=args.market,
        enrich=not args.no_enrich,
        enrich_limit=args.enrich_limit,
    )
