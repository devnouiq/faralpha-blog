#!/usr/bin/env python3
"""Quick smoke test for pattern detection functions."""

import pandas as pd
import numpy as np
from faralpha.pipeline.s05_patterns import (
    detect_trend_template, detect_vcp, detect_ipo_base, detect_breakout,
)

np.random.seed(42)
n = 300

# Create synthetic stock in Stage-2 uptrend
close = 100 * np.cumprod(1 + np.random.randn(n) * 0.01 + 0.002)
high = close * (1 + np.abs(np.random.randn(n) * 0.005))
low = close * (1 - np.abs(np.random.randn(n) * 0.005))

df = pd.DataFrame({
    "date": pd.date_range("2023-01-01", periods=n, freq="B"),
    "ticker": "UPTREND",
    "close": close,
    "high": high,
    "low": low,
    "volume": np.random.randint(50000, 200000, n).astype(float),
    # Pre-computed features (simulated)
    "ma50": pd.Series(close).rolling(50).mean(),
    "ma150": pd.Series(close).rolling(150).mean(),
    "ma200": pd.Series(close).rolling(200).mean(),
    "ma200_slope": pd.Series(close).rolling(200).mean().pct_change(20),
    "high_52w": pd.Series(high).rolling(252, min_periods=60).max(),
    "low_52w": pd.Series(low).rolling(252, min_periods=60).min(),
    "volatility_10d": pd.Series(close).pct_change().rolling(10).std(),
    "volatility_20d": pd.Series(close).pct_change().rolling(20).std(),
    "avg_volume_50d": pd.Series(np.random.randint(50000, 200000, n).astype(float)).rolling(50).mean(),
    "avg_volume_10d": pd.Series(np.random.randint(30000, 100000, n).astype(float)).rolling(10).mean(),
    "base_range_30d": 0.08 + np.random.rand(n) * 0.05,  # fairly tight
    "base_high_30": pd.Series(high).rolling(30).max(),
    "volume_ratio": 1.0 + np.random.rand(n),
})

# Test trend template
tt = detect_trend_template(df)
print(f"Trend Template: {tt.sum()} / {len(df)} rows pass ({tt.mean()*100:.1f}%)")
assert tt.dtype == bool

# Test VCP
vcp = detect_vcp(df)
print(f"VCP: {vcp.sum()} / {len(df)} rows pass ({vcp.mean()*100:.1f}%)")
assert vcp.dtype == bool

# Test IPO base (no listing_date column — should return all False)
ipo = detect_ipo_base(df)
print(f"IPO Base (no listing_date): {ipo.sum()} / {len(df)}")
assert ipo.sum() == 0

# Test with listing_date
df["listing_date"] = pd.Timestamp("2023-03-01")
ipo2 = detect_ipo_base(df)
print(f"IPO Base (with listing_date): {ipo2.sum()} / {len(df)}")

# Test breakout
bo = detect_breakout(df)
print(f"Breakout: {bo.sum()} / {len(df)} rows pass ({bo.mean()*100:.1f}%)")
assert bo.dtype == bool

print("\nAll pattern detection tests PASSED")
