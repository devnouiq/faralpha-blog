"""
FarAlpha Quant Trader — Configuration  v2.0
=============================================
Single source of truth for all pipeline parameters.

Markets: India (NSE)
Strategy: Minervini SEPA momentum with multi-contraction VCP,
          Darvas box, fundamental earnings filters.

References:
  Mark Minervini — "Trade Like a Stock Market Wizard" (2013)
  Mark Minervini — "Think & Trade Like a Champion" (2017)
  Nicolas Darvas — "How I Made $2,000,000 in the Stock Market" (1960)
"""

import os
from pathlib import Path

# ─────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load .env file if present (secrets not in source code)
_env_file = PROJECT_ROOT / ".env"
if _env_file.exists():
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _, _val = _line.partition("=")
                os.environ.setdefault(_key.strip(), _val.strip())

DB_PATH = str(PROJECT_ROOT / "db" / "market.duckdb")
INTRADAY_DB_PATH = str(PROJECT_ROOT / "db" / "intraday.duckdb")
# Per-interval DB files: separate writer locks → true parallel fetching
INTRADAY_DB_PATHS = {
    "15minute": str(PROJECT_ROOT / "db" / "intraday_15m.duckdb"),
    "30minute": str(PROJECT_ROOT / "db" / "intraday_30m.duckdb"),
    "60minute": str(PROJECT_ROOT / "db" / "intraday_60m.duckdb"),
}
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
FEATURES_DIR = DATA_DIR / "features"
RESULTS_DIR = DATA_DIR / "results"
LOGS_DIR = PROJECT_ROOT / "logs"

for _d in [RAW_DIR, FEATURES_DIR, RESULTS_DIR, LOGS_DIR, PROJECT_ROOT / "db"]:
    _d.mkdir(parents=True, exist_ok=True)


def use_postgres_database() -> bool:
    """Use PostgreSQL when ``DATABASE_URL`` or ``DB_HOST`` is set (e.g. VM production).

    If unset, the app keeps using DuckDB files under ``db/`` (local dev / legacy).
    """
    if os.environ.get("DATABASE_URL", "").strip():
        return True
    if os.environ.get("DB_HOST", "").strip():
        return True
    return False


# ─────────────────────────────────────────────
# DATA INGESTION
# ─────────────────────────────────────────────
DATA_START = "2008-01-01"
DATA_END = None
MARKETS = ["india"]
YF_SUFFIX = {"india": ".NS"}
BENCHMARK = {"india": "^CRSLDX"}  # Nifty 500
IPO_SEASONING_DAYS = 60

# ─────────────────────────────────────────────
# TREND TEMPLATE  (Stage-2 uptrend)
# "Trade Like a Stock Market Wizard" Ch.4, p.83-86
#  All 8 conditions from the book, verbatim.
# ─────────────────────────────────────────────
TREND_TEMPLATE = {
    "near_high_pct": 0.75,        # 1. within 25% of 52-week high
    "above_low_pct": 1.30,        # 2. at least 30% above 52-week low
    "price_above_ma10": False,    # DISABLED: blocks early-recovery leaders
    "min_stage2_days": 5,         # 5 days (was 20 — too strict, missed 2009/2020)
}

# ─────────────────────────────────────────────
# VCP  — Multi-Contraction Detection
# "Think & Trade Like a Champion" Ch.7-8
# ─────────────────────────────────────────────
VCP = {
    # Minervini: "The hallmark of a VCP is successive tighter contractions
    #  with volume drying up at each low." (Think & Trade, Ch.7)
    # Removed vol_contraction_ratio — that checked PRICE volatility (10d vs 20d),
    # which is NOT a Minervini concept. He cares about VOLUME drying up (below).
    "volume_dryup_ratio": 0.90,        # avg vol at pivot < 50d avg × 0.90 (relaxed from 0.80)
    "base_range_max": 0.30,            # final contraction range < 30% (Minervini spec: <=30%, preferred <=20%)
    "base_window": 30,
    # Multi-contraction
    "min_contractions": 2,             # Minervini: "at least 2, prefer 3+" (Think & Trade Ch.7)
    "max_base_depth_pct": 0.35,        # max 35% depth (Minervini: first pullback 12-35%)
    "contraction_shrink_ratio": 0.80,
    "pivot_lookback": 65,              # 3 months
}

# ─────────────────────────────────────────────
# DARVAS BOX
# ─────────────────────────────────────────────
DARVAS = {
    "box_min_days": 10,
    "box_max_days": 50,
    "box_max_range_pct": 0.15,
    "confirm_days": 3,
}

# ─────────────────────────────────────────────
# POWER PLAY  (ultra-tight setup)
# ─────────────────────────────────────────────
POWER_PLAY = {
    "max_range_pct": 0.05,
    "min_days": 15,
    "volume_dry_ratio": 0.50,
}

# ─────────────────────────────────────────────
# BREAKOUT
# ─────────────────────────────────────────────
BREAKOUT = {
    "lookback": 30,
    "volume_spike_multiplier": 1.4,  # Minervini: "150% ideal" — 1.4x balances capture vs noise
}

# ─────────────────────────────────────────────
# IPO BASE
# ─────────────────────────────────────────────
IPO_BASE = {
    "min_days": 30,
    "max_days": 180,
    "max_range_pct": 0.25,
}

# ─────────────────────────────────────────────
# RELATIVE STRENGTH
# ─────────────────────────────────────────────
RS = {
    "momentum_window": 252,
    "min_rs_percentile": 0.85,   # Top 15% — Minervini: "I focus on the strongest stocks"
}

# ─────────────────────────────────────────────
# SECTOR MOMENTUM
# ─────────────────────────────────────────────
SECTOR = {
    "min_sector_percentile": 0.70,   # Top 30% sectors (Minervini: buy leaders in leading sectors)
}

# ─────────────────────────────────────────────
# FUNDAMENTALS  (earnings filter)
# "Trade Like a Stock Market Wizard" Ch.2-3
# ─────────────────────────────────────────────
FUNDAMENTALS = {
    # Minervini Ch.2-3: "EPS growth of at least 25% YoY in each of
    #  the last 2 quarters, ideally accelerating"
    # NOTE: yfinance fundamental data is too sparse (~14K records) for hard filtering.
    # Technical filters (Trend Template + VCP + breakout + RS) are the PRIMARY screen.
    # Fundamentals kept as soft/disabled until a premium data source is added.
    "min_eps_growth_qoq_pct": 25,
    "require_accelerating_eps": False,  # Disabled: yfinance data too noisy for acceleration
    "require_positive_revenue_growth": True,
    "min_roe_pct": 0,                   # Disabled: yfinance ROE data unreliable
    "max_stale_days": 120,
    "enabled": True,   # Enabled: earnings quality filter (soft — NaN passes through)
}

# ─────────────────────────────────────────────
# MARKET REGIME  (Minervini Ch.10: market direction filter)
# Uses DUAL regime: fast (50-day) + slow (150-day)
# Bull = bench > 50MA & bench > 150MA (confirmed uptrend)
# Recovery = bench > 50MA but below 150MA (early recovery - trade small)
# Bear = bench < 50MA (fully defensive)
# ─────────────────────────────────────────────
REGIME = {
    "ma_fast": 50,
    "ma_slow": 150,
    "ma_window": 150,  # N500 sweep winner (was 200)
}

# ─────────────────────────────────────────────
# MARKET BREADTH  (Minervini: "internal health")
# Measures % of stocks above their 50-day MA and
# identifies weak market periods for watchlist building.
# During weak markets, we build a watchlist of leaders
# that are resisting the decline — these become the first
# buys when the market recovers.
# ─────────────────────────────────────────────
BREADTH = {
    "weak_market_30d_pct": -0.05,       # index down >5% in 30d = weak market
    "min_relative_strength": 0.05,      # stock must outperform index by >5%
    "watchlist_near_high_pct": 0.80,    # within 20% of 52-week high
    "watchlist_above_ma50": True,       # stock must be above its MA50
    "watchlist_volume_dry": True,       # volume should be contracting (base building)
    "breadth_healthy_pct": 0.50,        # >50% stocks above MA50 = healthy
    "breadth_weak_pct": 0.30,           # <30% stocks above MA50 = weak internals
    "breadth_improving_window": 10,     # days to measure breadth improvement
}

# ─────────────────────────────────────────────
# PORTFOLIO  (Minervini-exact, Ch.11-12)
# Risk-based sizing: risk 1% of account per trade.
# "position_size = account * risk_per_trade / (entry - stop)" 
# ─────────────────────────────────────────────
PORTFOLIO = {
    "max_positions": 5,                 # v14: concentrated (sweep: pos5 > pos8)
    "initial_capital": 1_000_000,
    "risk_per_trade_pct": 0.030,        # v14: 3.0% risk per trade (combo sweep winner)
    "max_portfolio_risk_pct": 0.08,    # 8% total portfolio risk
    "max_position_pct": 0.30,          # v14: 30% cap (concentrated bets)
    "stop_loss_pct": 0.10,             # v11: 10% hard stop (sweep winner: fewer whipsaws)
    "trailing_stop_pct": 0.30,         # v11: 30% trailing from high (sweep winner: let winners run)
    "bear_trailing_stop_pct": 0.10,    # 10% trailing in bear (hold_tight mode)
    "bear_mode": "force_close",          # force_close = sell all in bear, hold_tight = keep with tight trail
    "trailing_stop_activation": 0.10,  # activate trail after +10%
    "use_trailing_stop": True,
    "use_ma50_trailing": False,         # DISABLED: dips to MA50 during normal pullbacks cause premature exits
    "rebalance_freq": "D",
    "max_sector_weight": 0.35,
    "max_chase_pct": 0.05,              # skip if >5% above pivot
    "profit_take_pct": 0.0,              # v11: disabled (sweep winner: let winners ride full trail)
    "profit_take_fraction": 0.50,       # v7.5b (irrelevant when PT=0)
    "time_stop_days": 0,                # DISABLED
    "time_stop_threshold": 0.0,
    "pyramid_enabled": True,
    "pyramid_trigger_pct": 0.05,        # add at +5%
    "pyramid_max_adds": 2,              # v7.5b proven: 2 adds
    "pyramid_size_ratio": 0.50,         # v7.5b proven: 50% of original each time
    # Graduated recovery re-entry (regime_strength)
    "recovery_min_scale": 0.50,         # at MA50: 50% position size
    "recovery_max_scale": 1.00,         # at MA200: 100% position size
    "recovery_max_positions": 6,
    "recovery_position_scale": 0.75,
    # ── v14: Market-aware + concentrated (combo sweep winner) ──
    # CHAMP+br60_25 → CAGR=23.9%, Sharpe=1.04, DD=-36.0%
    # (vs v13: CAGR=20.6%, Sharpe=0.98, DD=-34.5%)
    "breadth_sizing": True,             # v13+: Scale positions by market breadth
    "breadth_full_above": 0.60,         # v14: Full size when breadth ≥ 60% (pickier)
    "breadth_half_below": 0.25,         # v14: Half size when breadth ≤ 25% (stay in longer)
    "reentry_delay_days": 10,           # v13: Wait 10d after bear→bull before entering
    "circuit_breaker_enabled": True,    # v13: Pause entries after consecutive losses
    "circuit_breaker_losses": 3,        # v13: 3 consecutive losses to trigger
    "circuit_breaker_pause_days": 5,    # v13: 5 trading days pause after trigger
    "bear_confirm_days": 0,             # Require N consecutive bear days to force_close
    "breadth_exit_enabled": False,      # Exit when breadth crashes
    "breadth_exit_threshold": 0.25,     # Exit all if breadth drops below this
    # ── v15: Minervini Cash Rule — V2_extreme (structural sweep winner) ──
    # "When market internals deteriorate, I go to cash." — MM
    # Ultra-selective: only fires during true meltdowns (4 exits in 20 years).
    # CAGR=24.2%, Sharpe=1.02 (vs v14 baseline: 23.9%, 1.04)
    "breadth_cash_enabled": True,             # Master switch for cash rule
    "breadth_cash_threshold": 0.20,           # Enter cash when r10 avg breadth < 20%
    "breadth_cash_consecutive": 5,            # …for 5 consecutive trading days
    "breadth_cash_recovery_threshold": 0.35,  # Exit cash when r10 avg breadth > 35%
    "breadth_cash_recovery_consecutive": 2,   # …for 2 consecutive trading days
    "breadth_cash_rolling_window": 10,        # 10-day rolling average smoothing
    "breadth_cash_use_slope": False,
    "breadth_cash_block_buys": True,
}

# ─────────────────────────────────────────────
# INDIA-SPECIFIC PORTFOLIO OVERRIDES
# ─────────────────────────────────────────────

PORTFOLIO_INDIA = {
    # India v16 — sweep winner COMBO_r25_p6_t21_d8_s50 (135 configs, 6 workers)
    # Benchmark: ^CRSLDX (Nifty 500), REGIME: ma_window=150
    #
    # Full (2009-2025): CAGR=29.5%, Sharpe=1.70, DD=-19.6%, 228 trades
    # Train (2009-2020): CAGR=18.8%, Sharpe=1.28, DD=-19.6%
    # Test  (2021-2025): CAGR=57.4%, Sharpe=2.45, DD=-15.3% (NOT overfit!)
    #
    # Evolution:
    #   R10 (realistic):  CAGR=29.5%, Sharpe=1.60, DD=-32.8%  ← old baseline
    #   v16-initial:      CAGR=22.8%, Sharpe=1.38, DD=-27.4%  ← too conservative
    #   v16-sweep winner: CAGR=29.5%, Sharpe=1.70, DD=-19.6%  ← current
    #
    # Key insight: concentrate bets (6 pos, 2.5% risk) + aggressive DD control
    # at -8% threshold with 50% scale-down. Minervini-style: fewer bigger bets
    # with strict risk management when equity drops.
    "max_positions": 6,                   # v16: 6 (was 8) — more concentrated
    "max_position_pct": 0.25,             # 25% cap — lower concentration
    "stop_loss_pct": 0.12,               # 12% stop — critical parameter, never change
    "trailing_stop_pct": 0.21,           # 21% trail — unchanged
    "risk_per_trade_pct": 0.025,         # v16: 2.5% (was 1.8%) — bigger bets
    "pyramid_max_adds": 0,               # no pyramiding (confirmed optimal)
    "profit_take_pct": 0.0,              # OFF — let winners run
    "profit_take_fraction": 0.33,        # irrelevant when PT=0
    "circuit_breaker_losses": 3,          # CB after 3 stops — unchanged
    "circuit_breaker_pause_days": 20,     # 20-day pause — unchanged
    "breadth_cash_enabled": False,        # Cash rule hurts India
    "breadth_full_above": 0.50,
    "breadth_half_below": 0.20,
    "bear_confirm_days": 2,              # 2 days — fast regime exit
    "fundamentals_enabled": False,        # yfinance data too sparse
    # ── v16: Equity DD control — graduated exposure reduction during drawdown ──
    # Smoothly scales down max positions and position sizes as portfolio DD deepens.
    # Prevents repeated full-loading into whipsaw bear markets (e.g. 2018-2019).
    # Sweep winner: tighter trigger (-8%) with aggressive reduction (50% scale, 3 pos).
    "equity_dd_enabled": True,            # Master switch
    "equity_dd_threshold": -0.08,         # v16: -8% (was -10%) — trigger earlier
    "equity_dd_floor": -0.20,            # v16: -20% (was -25%) — tighter floor
    "equity_dd_max_positions": 3,         # v16: 3 (was 4) — fewer positions at floor
    "equity_dd_position_scale": 0.50,     # v16: 50% (was 70%) — cut size more aggressively
}

def get_portfolio(market: str = "india") -> dict:
    """Return merged portfolio config."""
    base = dict(PORTFOLIO)
    base.update(PORTFOLIO_INDIA)
    return base


def apply_market_config(market: str = "india") -> dict:
    """Apply India overrides to the global PORTFOLIO dict.

    Returns the original values so they can be restored later.
    """
    saved = {}
    for k, v in PORTFOLIO_INDIA.items():
        saved[k] = PORTFOLIO.get(k)
        PORTFOLIO[k] = v
    return saved


def restore_config(saved: dict) -> None:
    """Restore PORTFOLIO to pre-override state."""
    for k, v in saved.items():
        if v is None:
            PORTFOLIO.pop(k, None)
        else:
            PORTFOLIO[k] = v


# ─────────────────────────────────────────────
# BACKTEST
# ─────────────────────────────────────────────
BACKTEST = {
    "slippage_bps": 30,            # India midcaps: 30 bps realistic (was 10)
    "commission_bps": 5,
    "start_date": "2009-01-01",    # skip 2008 (incomplete data / market crash entry)
    "entry_delay_days": 1,         # buy at NEXT day's open (0=same-day close, legacy)
    "entry_price_field": "open",   # price field for delayed entries (open|close)
    "train_end": "2020-12-31",     # train period: 2009-2020
    "test_start": "2021-01-01",    # test  period: 2021-2025
}

# ─────────────────────────────────────────────
# BEAR MARKET REVERSAL (Connors RSI-2)
# Larry Connors — proven mean-reversion during bear regimes.
# When main strategy is in cash, capture 3-7 day oversold bounces
# in structurally sound stocks.
# ─────────────────────────────────────────────
BEAR_REVERSAL = {
    "enabled": False,                 # DISABLED — see analysis notes below
    # Tested RSI-2 < 5|10, max 1|3 pos, RS >= 0.50|0.70, stop 5%|7%
    # Results: bear years improve (2015, 2018) but negative expectancy
    # causes equity drag that cascades into worse overall CAGR.
    # Net loss ~₹10M over full period. Keep code for future tuning.
    "rsi_period": 2,                  # RSI lookback (2 = ultra short-term)
    "rsi_entry_threshold": 5,         # enter when RSI(2) < 5 (extremely oversold)
    "rsi_exit_threshold": 65,         # exit when RSI(2) > 65 (bounced)
    "min_rs_percentile": 0.70,        # only buy top-30% RS stocks
    "require_above_ma200": True,      # structural uptrend still intact
    "max_positions": 1,               # max 1 simultaneous reversal position
    "stop_loss_pct": 0.07,            # 7% hard stop (wider to survive bear noise)
    "time_stop_days": 7,              # max 7 trading days hold
    "position_scale": 0.50,           # half-size positions
    "risk_per_trade_pct": 0.01,       # 1% risk per trade
}

# ─────────────────────────────────────────────
# DATA QUALITY
# ─────────────────────────────────────────────
DATA_QUALITY = {
    "max_daily_return": 0.80,
    "min_price": 10.0,             # Minervini: filter out penny stocks (< $10 / ₹10)
    "max_price_india": 3000.0,     # India: skip stocks above ₹3000 (capital allocation constraint)
    "min_avg_volume": 100_000,
    "min_dollar_volume": 2_000_000,
    # Minervini avoids defensive/low-beta sectors
    "exclude_sectors": ["Utilities", "Consumer Staples"],
}

# ─────────────────────────────────────────────
# PRODUCTION / DEPLOYMENT
# ─────────────────────────────────────────────
PRODUCTION = {
    "alert_enabled": False,
    "alert_telegram_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
    "alert_telegram_chat_id": os.environ.get("TELEGRAM_CHAT_ID", ""),
    "schedule_cron": "0 20 * * 1-5",
    "paper_trading": True,
    "risk_max_portfolio_var_pct": 2.0,
}

# ─────────────────────────────────────────────
# KITE (Zerodha) API
# ─────────────────────────────────────────────
KITE = {
    "api_key": os.environ.get("KITE_API_KEY", ""),
    "api_secret": os.environ.get("KITE_API_SECRET", ""),
    "access_token": os.environ.get("KITE_ACCESS_TOKEN", ""),
    # Intraday candle intervals to store
    "intervals": ["15minute", "30minute", "60minute"],
    # How many calendar days of intraday history to keep
    "intraday_lookback_days": 60,
    # Rate limit: Kite allows 3 requests/sec for historical data
    "rate_limit_per_sec": 3,
}

# ─────────────────────────────────────────────
# LIVE TRADE TRACKING
# ─────────────────────────────────────────────
LIVE_TRACKING = {
    "enabled": True,
    # Store last N completed trades for adaptive sizing
    "recent_trades_window": 20,
    # If win rate over last 20 trades drops below this, reduce sizing to 50%
    "min_win_rate_pct": 35.0,
    "reduced_scale": 0.50,
}

# ─────────────────────────────────────────────
# INTRADAY REVERSAL STRATEGY
# ─────────────────────────────────────────────
# Combined parameter sweep (9600 configs, train 2016-2020, test 2021-2025):
#   Sweet spot: D5 R1.5 Dp10% T2% H7d
#   Entry: at signal time (same-day, when RVOL fires & price > VWAP)
#     Close entry:  Train Sharpe 2.50, Test Sharpe 2.70, Test XIRR +1643%
#     T+1 Open:     Train Sharpe 2.28, Test Sharpe 2.47, Test XIRR +1227%
#     → Same-day entry is materially better (+34% XIRR, lower DD)
INTRADAY_REVERSAL = {
    # ── Sweep winner D3 R1.5 T2% H10d (8839 stable / 14400 total) ──
    "default_strategy": "vwap_reclaim",
    "entry_at": "signal",            # buy at signal time (not T+1 open)
    "vwap_reclaim": {
        "min_down_days": 3,
        "min_rvol": 1.5,
        "require_bear": False,
        "max_hold_days": 10,          # max hold period (trailing stop exits earlier)
        "depth_max": None,            # no depth filter — sweep winner uses no cap
        "trailing_stop_pct": 0.02,    # 2% trailing stop from peak (tighter = lower DD)
        "stop_loss_pct": None,        # no fixed SL — trailing handles exits
    },
    # ── Performance: same-day entry (0.3% txn cost included) ──
    "performance": {
        "train_xirr": 1531.7,
        "test_xirr": 2156.7,
        "train_dd": -6.4,
        "test_dd": -7.7,
        "train_sharpe": 5.67,
        "test_sharpe": 7.47,
        "test_win_rate": 55.5,
        "test_trades": 3532,
        "stable_configs": 8839,
        "txn_cost_bps": 30,
    },
    # ── Portfolio constraints ──
    "max_positions": 5,
    "position_size_pct": 0.20,       # 20% per position (5 max)
    "max_portfolio_exposure": 1.0,
    "capital": float(os.environ.get("TRADING_CAPITAL", "0")),  # fallback if Kite margins fail
    "auto_trade": False,             # OFF by default — enable via API toggle
    # ── Rolling rvol tiers (Option C: higher bar early, relaxes over time) ──
    # Prioritizes high-rvol signals (like backtest's sort-by-rvol ranking)
    # without delaying execution. Tiers are (end_time_IST, min_rvol).
    "rvol_tiers_enabled": True,
    "rvol_tiers": [
        ("11:00", 2.0),   # 10:15–11:00: only strong rvol
        ("12:00", 1.5),   # 11:00–12:00: normal threshold (matches config min_rvol)
        ("14:30", 1.5),   # 12:00–14:30: accept anything passing signal filter
    ],
    # ── Breadth zone (sweep: no filter wins — skip/reduce costs CAGR with negligible DD benefit) ──
    "breadth_skip_enabled": False,
    "breadth_skip_low": 0.30,
    "breadth_skip_high": 0.50,
    "breadth_reduce_enabled": False,
    "breadth_reduce_low": 0.30,
    "breadth_reduce_high": 0.55,
    "breadth_reduce_factor": 0.60,
    # ── Live trading hours (IST) ──
    "market_open": "09:15",
    "market_close": "15:30",
    "signal_window_start": "10:15",
    "signal_window_end": "14:30",
    # ── Pre-market watchlist ──
    "watchlist": {
        "min_near_down_days": 3,      # only show stocks that can fire (matches min_down_days)
        "min_avg_volume": 100_000,
        "max_watchlist_size": 100,    # more candidates with lower threshold
    },
}
