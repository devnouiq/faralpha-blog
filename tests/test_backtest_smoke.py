#!/usr/bin/env python3
"""Quick smoke test for the backtester with synthetic data."""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from faralpha.pipeline.s08_backtest import Backtester, compute_metrics, print_metrics

np.random.seed(42)

# Create 2 years of daily data for 5 tickers
start = date(2022, 1, 3)
dates = pd.bdate_range(start, periods=504)

tickers = ["AAA", "BBB", "CCC", "DDD", "EEE"]
sectors = ["Tech", "Health", "Tech", "Finance", "Energy"]

rows = []
for i, tk in enumerate(tickers):
    prices = 100 * np.cumprod(1 + np.random.randn(504) * 0.02 + 0.001)
    for j, d in enumerate(dates):
        p = prices[j]
        rows.append({
            "date": d.date(),
            "ticker": tk,
            "close": p,
            "high": p * 1.01,
            "low": p * 0.99,
            "sector": sectors[i],
        })

prices_df = pd.DataFrame(rows)

# Create some candidate signals — on every Friday (to match weekly rebalance)
cand_rows = []
for d in dates:
    if d.weekday() == 4:  # Friday
        for tk in ["AAA", "BBB"]:
            cand_rows.append({
                "date": d.date(),
                "ticker": tk,
                "rs_composite": np.random.rand(),
                "rank_on_day": 1,
                "market": "india",
                "sector": "Tech" if tk == "AAA" else "Health",
            })
candidates_df = pd.DataFrame(cand_rows)

# Regime: all bull
regime_df = pd.DataFrame({
    "date": [d.date() for d in dates],
    "is_bull": True,
})

print(f"Prices: {len(prices_df)} rows")
print(f"Candidates: {len(candidates_df)} rows")
print(f"Regime: {len(regime_df)} rows")

bt = Backtester(prices_df, candidates_df, regime_df, initial_capital=100_000)
bt.run()

print(f"\nTrades: {len(bt.trades)}")
print(f"Snapshots: {len(bt.snapshots)}")

if bt.snapshots:
    metrics = compute_metrics(bt.snapshots, bt.trades)
    print_metrics(metrics, label="SYNTHETIC TEST")

    assert metrics["total_trades"] > 0, "Should have some trades"
    assert metrics["n_years"] > 1.0, "Should cover >1 year"
    assert -100 < metrics["max_drawdown_pct"] <= 0, "Drawdown should be negative"

print("\nBacktest smoke test PASSED")
