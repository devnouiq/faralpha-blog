#!/usr/bin/env python3
"""Quick edge-case test for feature computation."""

import pandas as pd
import numpy as np
from faralpha.pipeline.s03_features import _compute_ticker_features

# Create minimal test data (50 rows - below ma200 requirement)
dates = pd.date_range("2024-01-01", periods=50, freq="B")
df = pd.DataFrame({
    "date": dates,
    "ticker": "TEST",
    "open": 100 + np.random.randn(50).cumsum(),
    "high": 101 + np.random.randn(50).cumsum(),
    "low": 99 + np.random.randn(50).cumsum(),
    "close": 100 + np.random.randn(50).cumsum(),
    "volume": np.random.randint(10000, 100000, 50).astype(float),
})
# Ensure high >= close >= low
df["high"] = df[["open", "high", "close"]].max(axis=1)
df["low"] = df[["open", "low", "close"]].min(axis=1)

result = _compute_ticker_features(df)
print(f"Rows: {len(result)}, Columns: {len(result.columns)}")
print(f"ma200 NaN count: {result['ma200'].isna().sum()} / {len(result)}")
print(f"ma50 NaN count: {result['ma50'].isna().sum()} / {len(result)}")

# Should have all NaN for ma200 (needs 200 periods)
assert result["ma200"].isna().all(), "ma200 should be NaN for <200 rows"
# Should have all NaN for ma50 (needs 50 periods, we have exactly 50)
assert result["ma50"].isna().sum() == 49, f"Expected 49 NaN for ma50, got {result['ma50'].isna().sum()}"

print("Edge case test PASSED")
