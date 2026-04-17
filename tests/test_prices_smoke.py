#!/usr/bin/env python3
"""Quick smoke test for s02_prices — downloads 5 tickers and verifies DB."""

from faralpha.utils.db import get_conn, init_schema
from faralpha.pipeline.s02_prices import download_ticker

con = get_conn()
init_schema(con)

test_tickers = [
    ("RELIANCE", "india"),
    ("TCS", "india"),
    ("INFY", "india"),
    ("HDFCBANK", "india"),
    ("ICICIBANK", "india"),
]

for ticker, market in test_tickers:
    df = download_ticker(ticker, market, start="2020-01-01")
    if df.empty:
        print(f"  FAIL  {ticker}.{market} — empty")
    else:
        con.execute("""
            INSERT OR IGNORE INTO prices
            SELECT date, ticker, open, high, low, close, volume, market FROM df
        """)
        print(f"  OK    {ticker}.{market} — {len(df)} rows, "
              f"{df['date'].min()} → {df['date'].max()}")

# Verify
n = con.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
t = con.execute("SELECT COUNT(DISTINCT ticker) FROM prices").fetchone()[0]
print(f"\nDB: {n:,} rows, {t} tickers")

# Clean up test data so full run starts fresh
con.execute("DELETE FROM prices")
print("Test data cleaned — ready for full run")
con.close()
