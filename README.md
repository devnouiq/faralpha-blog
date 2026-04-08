# Faralpha Quant Trader

Production-grade quant research platform implementing **Mark Minervini's SEPA momentum strategy** for India (NSE) and US equities, with 20 years of historical data and survivorship-bias-safe backtesting.

## Strategy

| Component | Description |
|-----------|-------------|
| **Trend Template** | 6 Minervini conditions (price vs MAs, 52-week range, MA slopes) |
| **VCP (Volatility Contraction Pattern)** | Contracting volatility + volume dry-up in tight base |
| **IPO Base** | First base after IPO (seasoning period) |
| **Breakout** | Price breaking above base high on above-average volume |
| **RS Ranking** | Cross-sectional relative strength (12m/6m/3m composite percentile) |
| **Market Regime** | Benchmark above 200-day MA = bull; below = bear |
| **Portfolio** | Max 10 positions, 8% trailing stop-loss, 30% sector cap |

## Quick Start

```bash
# Install
uv sync

# Run full pipeline (universe → prices → features → signals → backtest)
uv run python -m faralpha.pipeline.run_all --market both

# Or skip download if data already exists
uv run python -m faralpha.pipeline.run_all --market both --skip-download

# Run individual steps
uv run python -m faralpha.cli step features --market india

# View database summary
uv run python -m faralpha.cli info
```

## Dashboard

Real-time trading dashboard with price sync, signal generation, position
tracking, and automated scanning — all through a web UI.

```bash
# ── Option A: Production (single server) ──
cd ui && npm install && npm run build && cd ..
uv run uvicorn faralpha.api.app:app --host 0.0.0.0 --port 8000
# Open http://localhost:8000

# ── Option B: Development (hot reload) ──
# Terminal 1 — API server
uv run uvicorn faralpha.api.app:app --reload --port 8000
# Terminal 2 — React dev server
cd ui && npm run dev
# Open http://localhost:5173
```

### Dashboard Features
- **Sync Prices** — one-click incremental sync for India / US markets
- **Run Pipeline** — features → RS ranking → patterns → regime → signals
- **Buy Signals** — ranked candidates with RS score, pattern type, price
- **Position Tracking** — add your positions, monitor stops in real-time
- **Market Regime** — bull / bear / recovery with breadth indicators
- **Auto Scanner** — configurable interval (15m–4h), runs pipeline automatically
- **Live Alerts** — WebSocket push for buy signals, stop breaches, regime changes

## Pipeline Steps

| Step | Script | Purpose |
|------|--------|---------|
| 01 | `s01_universe.py` | Build stock universe from NSE archives + Wikipedia |
| 02 | `s02_prices.py` | Download 20yr OHLCV via yfinance |
| 03 | `s03_features.py` | Compute MAs, momentum, volatility, base structure |
| 04 | `s04_rs_rank.py` | Cross-sectional relative strength percentiles |
| 05 | `s05_patterns.py` | Detect Trend Template, VCP, IPO Base, Breakout |
| 06 | `s06_regime.py` | Bull/bear market classification |
| 07 | `s07_signals.py` | Combine filters into ranked buy candidates |
| 08 | `s08_backtest.py` | Walk-forward portfolio simulation with P&L tracking |

## Project Structure

```
src/faralpha/
├── config.py              # All strategy parameters
├── cli.py                 # CLI entry point (fqt)
├── utils/
│   ├── db.py              # DuckDB connection + schema
│   └── logger.py          # Console + file logging
├── api/
│   ├── app.py             # FastAPI dashboard server
│   └── sync_prices.py     # Incremental price sync
├── production/
│   ├── alerts.py          # Telegram + console alerts
│   ├── generate_orders.py # Broker-ready order generation
│   ├── risk.py            # VaR, drawdown, sector exposure
│   ├── scanner.py         # Daily cron scanner
│   └── trade_audit.py     # Append-only trade log
└── pipeline/
    ├── s01_universe.py    # Universe builder
    ├── s02_prices.py      # OHLCV download
    ├── s02b_fundamentals.py # Earnings data (US only)
    ├── s03_features.py    # Technical features
    ├── s04_rs_rank.py     # Relative strength
    ├── s05_patterns.py    # Pattern detection
    ├── s06_regime.py      # Market regime
    ├── s07_signals.py     # Signal engine
    ├── s08_backtest.py    # Backtester
    └── run_all.py         # Orchestrator
ui/                         # React dashboard
    ├── src/
    │   ├── App.tsx         # Main dashboard layout
    │   ├── api.ts          # API client
    │   └── components/     # UI components
    └── package.json
db/
    └── market.duckdb      # DuckDB database
```

## Tech Stack

- **Python 3.12** + **uv** for package management
- **DuckDB** for fast analytical queries on disk
- **yfinance** for market data
- **pandas / numpy / scipy** for computation
- **ta** for technical indicators
- **matplotlib** for visualization
