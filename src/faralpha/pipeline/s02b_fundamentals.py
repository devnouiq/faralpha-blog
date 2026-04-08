#!/usr/bin/env python3
"""
Step 02b — Fundamental Data Ingestion
======================================
Fetches quarterly earnings, revenue, and profitability data for all
tickers via yfinance. Stores in ``fundamentals`` table.

Minervini's SEPA requirements (Ch. 2-3):
  - Quarterly EPS growth ≥ 25% YoY
  - Annual EPS accelerating (QoQ within year)
  - Revenue growth positive
  - ROE > 17% (we use 10% as minimum)
  - Profit margins expanding

Data stored per (ticker, report_date):
  eps, eps_yoy_growth, revenue, revenue_yoy_growth, roe,
  gross_margin, net_margin, market

Usage:
    uv run python -m faralpha.pipeline.s02b_fundamentals --market india
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
from tqdm import tqdm

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger

log = get_logger("s02b_fundamentals")


def _fetch_ticker_fundamentals(ticker: str, suffix: str) -> list[dict]:
    """Fetch quarterly earnings/revenue for one ticker via yfinance."""
    try:
        t = yf.Ticker(f"{ticker}{suffix}")

        # Get quarterly financials
        qf = t.quarterly_financials
        qi = t.quarterly_income_stmt

        if qf is None or qf.empty:
            return []

        records = []
        # qf columns are dates, rows are line items
        dates = list(qf.columns)

        for dt in dates:
            rec = {
                "ticker": ticker,
                "report_date": dt.date() if hasattr(dt, 'date') else dt,
            }

            # EPS — try multiple field names
            for fname in ["Basic EPS", "Diluted EPS"]:
                if fname in qf.index:
                    val = qf.loc[fname, dt]
                    if pd.notna(val):
                        rec["eps"] = float(val)
                        break

            # Revenue
            for fname in ["Total Revenue", "Revenue", "Operating Revenue"]:
                if fname in qf.index:
                    val = qf.loc[fname, dt]
                    if pd.notna(val):
                        rec["revenue"] = float(val)
                        break

            # Net Income
            for fname in ["Net Income", "Net Income Common Stockholders"]:
                if fname in qf.index:
                    val = qf.loc[fname, dt]
                    if pd.notna(val):
                        rec["net_income"] = float(val)
                        break

            # Gross Profit
            if "Gross Profit" in qf.index:
                val = qf.loc["Gross Profit", dt]
                if pd.notna(val):
                    rec["gross_profit"] = float(val)

            # EBITDA
            if "EBITDA" in qf.index:
                val = qf.loc["EBITDA", dt]
                if pd.notna(val):
                    rec["ebitda"] = float(val)

            records.append(rec)

        # Also get key stats for ROE
        try:
            info = t.info
            roe = info.get("returnOnEquity")
            if roe is not None:
                for rec in records:
                    rec["roe"] = float(roe)
        except Exception:
            pass

        return records
    except Exception:
        return []


def _compute_growth(df: pd.DataFrame) -> pd.DataFrame:
    """Compute YoY growth rates for EPS and revenue."""
    if df.empty:
        return df

    df = df.sort_values(["ticker", "report_date"])

    # YoY EPS growth: compare to same quarter last year (4 quarters ago)
    if "eps" in df.columns:
        df["eps_4q_ago"] = df.groupby("ticker")["eps"].shift(4)
        df["eps_yoy_growth"] = np.where(
            (df["eps_4q_ago"].notna()) & (df["eps_4q_ago"].abs() > 0.01),
            (df["eps"] - df["eps_4q_ago"]) / df["eps_4q_ago"].abs(),
            np.nan,
        )

    # YoY Revenue growth
    if "revenue" in df.columns:
        df["rev_4q_ago"] = df.groupby("ticker")["revenue"].shift(4)
        df["revenue_yoy_growth"] = np.where(
            (df["rev_4q_ago"].notna()) & (df["rev_4q_ago"].abs() > 0),
            (df["revenue"] - df["rev_4q_ago"]) / df["rev_4q_ago"].abs(),
            np.nan,
        )

    # Gross margin
    if "gross_profit" in df.columns and "revenue" in df.columns:
        df["gross_margin"] = np.where(
            df["revenue"].abs() > 0,
            df["gross_profit"] / df["revenue"],
            np.nan,
        )

    # Net margin
    if "net_income" in df.columns and "revenue" in df.columns:
        df["net_margin"] = np.where(
            df["revenue"].abs() > 0,
            df["net_income"] / df["revenue"],
            np.nan,
        )

    # Clean up temp columns
    for c in ["eps_4q_ago", "rev_4q_ago"]:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)

    return df


def run(market: str = "both", batch_size: int = 50, sleep: float = 0.1) -> None:
    """Fetch and store fundamental data."""
    con = get_conn()
    markets = config.MARKETS if market == "both" else [market]

    all_records: list[dict] = []

    for mkt in markets:
        log.info(f"{'═' * 50}")
        log.info(f"Fundamental data: {mkt.upper()}")
        log.info(f"{'═' * 50}")

        tickers = con.execute(
            "SELECT ticker FROM stocks WHERE market = ?", [mkt]
        ).fetchall()
        tickers = [t[0] for t in tickers]
        suffix = config.YF_SUFFIX[mkt]

        log.info(f"Fetching fundamentals for {len(tickers)} tickers")
        ok = 0
        fail = 0

        for i, tkr in enumerate(tqdm(tickers, desc=f"fundamentals [{mkt}]")):
            recs = _fetch_ticker_fundamentals(tkr, suffix)
            if recs:
                for r in recs:
                    r["market"] = mkt
                all_records.extend(recs)
                ok += 1
            else:
                fail += 1

            if (i + 1) % batch_size == 0:
                log.info(f"  [{mkt}] {i + 1}/{len(tickers)}  ok={ok} fail={fail}")

            time.sleep(sleep)

        log.info(f"✓ {mkt.upper()}: {ok} tickers with data, {fail} without")

    if not all_records:
        log.warning("No fundamental data retrieved.")
        con.execute("DROP TABLE IF EXISTS fundamentals")
        con.execute("""CREATE TABLE fundamentals (
            ticker VARCHAR, report_date DATE, market VARCHAR,
            eps DOUBLE, revenue DOUBLE, net_income DOUBLE,
            eps_yoy_growth DOUBLE, revenue_yoy_growth DOUBLE,
            roe DOUBLE, gross_margin DOUBLE, net_margin DOUBLE
        )""")
        con.close()
        return

    df = pd.DataFrame(all_records)
    df["report_date"] = pd.to_datetime(df["report_date"])

    # Compute growth rates
    df = _compute_growth(df)

    # Store
    log.info(f"Storing {len(df):,} fundamental records…")
    con.execute("DROP TABLE IF EXISTS fundamentals")
    con.execute("CREATE TABLE fundamentals AS SELECT * FROM df")

    n = con.execute("SELECT COUNT(*) FROM fundamentals").fetchone()[0]
    nt = con.execute("SELECT COUNT(DISTINCT ticker) FROM fundamentals").fetchone()[0]
    log.info(f"✓ Fundamentals table: {n:,} rows, {nt} tickers")
    con.close()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Fetch fundamental data")
    p.add_argument("--market", default="india", choices=["india"])
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--sleep", type=float, default=0.1)
    args = p.parse_args()
    run(market=args.market, batch_size=args.batch_size, sleep=args.sleep)
