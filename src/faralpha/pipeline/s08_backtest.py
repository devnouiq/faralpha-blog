#!/usr/bin/env python3
"""
Step 08 — Backtester  v5.0  (Minervini-Exact Portfolio Engine)
===============================================================
Walk-forward simulation with Minervini-exact risk management:

  v5 Enhancements:
  - Risk-based position sizing: risk 1% per trade, not equal-weight
  - MA50 trailing exit: close when stock violates 50-day MA (Ch.9)
  - Time stop: exit dead money (flat after 40 days)
  - Chase filter: skip entries >5% above pivot
  - Recovery mode: half-size positions during early recovery
  - Dual regime: bull/recovery/bear from s06_regime
  - Parallel backtesting by year for speed

  Existing (from v2-v4):
  - Hard stop-loss: 7% from ENTRY (TLSW Ch.12)
  - Trailing stop: 20% from highest high (after +10% activation)
  - Profit-taking: sell 50% at +20%
  - Pyramiding: add to winners at +5%, up to 2 add-ons at 50% size
  - Sector concentration limits
  - Daily entry (breakout day)

Outputs:
  ``backtest_equity_{mkt}``  — daily equity curve
  ``backtest_trades_{mkt}``  — every entry/exit with P&L
  ``backtest_annual_{mkt}``  — annual returns

Usage:
    uv run python -m faralpha.pipeline.s08_backtest --market both
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger

log = get_logger("s08_backtest")

PC = config.PORTFOLIO
BC = config.BACKTEST


# ═══════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ═══════════════════════════════════════════════════════════

@dataclass
class Position:
    ticker: str
    entry_date: date
    entry_price: float
    shares: float
    highest_price: float
    sector: str | None = None
    pyramid_count: int = 0          # how many add-ons done
    total_cost: float = 0.0         # total $ invested (for avg cost)


@dataclass
class Trade:
    ticker: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    shares: float
    pnl: float
    pnl_pct: float
    exit_reason: str
    hold_days: int = 0


@dataclass
class DailySnapshot:
    date: date
    equity: float
    cash: float
    n_positions: int
    exposure_pct: float


# ═══════════════════════════════════════════════════════════
#  BACKTESTER
# ═══════════════════════════════════════════════════════════

class Backtester:
    """Walk-forward portfolio simulation with dual stops + pyramiding + MA50 exit."""

    def __init__(
        self,
        prices: pd.DataFrame,
        candidates: pd.DataFrame,
        regime: pd.DataFrame,
        initial_capital: float = PC["initial_capital"],
    ):
        self.prices = prices
        self.candidates = candidates
        self.regime = regime

        self.capital = initial_capital
        self.initial_capital = initial_capital
        self.positions: dict[str, Position] = {}
        self.trades: list[Trade] = []
        self.snapshots: list[DailySnapshot] = []
        self._pending_buys: list[dict] = []  # queued buy orders for next-day execution

        self.prices = self.prices.sort_values(["ticker", "date"])
        self._price_idx = self.prices.set_index(["date", "ticker"])

        # Pre-compute MA50 per ticker for MA50 trailing exit
        self._ma50_idx: dict = {}
        if PC.get("use_ma50_trailing", False):
            self._build_ma50_index()

        # Dual regime: bull dates + recovery dates + regime_strength + confirmed bull
        self._regime_set: set = set()
        self._recovery_set: set = set()
        self._confirmed_bull_set: set = set()  # bench > MA200
        self._regime_strength: dict = {}  # date -> 0.0-1.0
        if not regime.empty:
            self._regime_set = set(
                regime.loc[regime["is_bull"], "date"].tolist()
            )
            if "is_recovery" in regime.columns:
                self._recovery_set = set(
                    regime.loc[regime["is_recovery"], "date"].tolist()
                )
            if "is_confirmed_bull" in regime.columns:
                self._confirmed_bull_set = set(
                    regime.loc[regime["is_confirmed_bull"], "date"].tolist()
                )
            if "regime_strength" in regime.columns:
                for _, row in regime.iterrows():
                    self._regime_strength[row["date"]] = row["regime_strength"]

            # v13: Breadth data for market-aware sizing
            if "breadth_pct" in regime.columns:
                self._breadth: dict = {}
                for _, row in regime.iterrows():
                    if pd.notna(row.get("breadth_pct")):
                        self._breadth[row["date"]] = row["breadth_pct"]
            else:
                self._breadth: dict = {}

        # v13: Track state for re-entry delay and circuit breaker
        self._last_bear_exit_date = None  # date of most recent bear force_close
        self._consecutive_stop_losses: int = 0  # count of consecutive stop_entry exits
        self._circuit_breaker_until = None  # date when circuit breaker expires
        self._bear_day_count: int = 0  # consecutive days below MA200

        # v15: Minervini cash rule state — sustained breadth deterioration
        self._in_cash_mode: bool = False
        self._breadth_low_streak: int = 0   # consecutive days breadth < threshold
        self._breadth_high_streak: int = 0  # consecutive days breadth > recovery

        # v16: Equity drawdown control — reduce exposure when portfolio in DD
        self._peak_equity: float = initial_capital

        # v15: Pre-compute rolling average breadth for cash rule
        if self._breadth:
            self._breadth_rolling: dict = {}
            window = PC.get("breadth_cash_rolling_window", 10)
            sorted_dates = sorted(self._breadth.keys())
            vals = [self._breadth[d] for d in sorted_dates]
            for i, d in enumerate(sorted_dates):
                start = max(0, i - window + 1)
                self._breadth_rolling[d] = sum(vals[start:i+1]) / (i - start + 1)
        else:
            self._breadth_rolling: dict = {}

        # v15b: Pre-compute rolling MAX of breadth avg (for slope/ROC signal)
        self._breadth_rolling_max: dict = {}
        if self._breadth_rolling:
            peak_window = PC.get("breadth_cash_slope_window", 60)
            sorted_dates_r = sorted(self._breadth_rolling.keys())
            vals_r = [self._breadth_rolling[d] for d in sorted_dates_r]
            for i, d in enumerate(sorted_dates_r):
                start = max(0, i - peak_window + 1)
                self._breadth_rolling_max[d] = max(vals_r[start:i+1])

        self._rebalance_dates = self._get_rebalance_dates()

    def _build_ma50_index(self) -> None:
        """Pre-compute 50-day MA for all tickers for MA50 trailing exit."""
        log.info("Pre-computing MA50 for trailing exit…")
        grouped = self.prices.groupby("ticker")
        for ticker, grp in grouped:
            grp = grp.sort_values("date")
            ma50 = grp["close"].rolling(50, min_periods=50).mean()
            for dt, val in zip(grp["date"], ma50):
                if pd.notna(val):
                    self._ma50_idx[(dt, ticker)] = val

    def _get_rebalance_dates(self) -> list:
        all_dates = sorted(self.prices["date"].unique())
        freq = PC.get("rebalance_freq", "W-FRI")
        if freq == "D":
            # Daily entry — Minervini enters on breakout day
            return list(all_dates)
        # Weekly: pick last trading day of each week
        df = pd.DataFrame({"date": all_dates})
        df["date_ts"] = pd.to_datetime(df["date"])
        df["week"] = df["date_ts"].dt.isocalendar().week.astype(int)
        df["year"] = df["date_ts"].dt.isocalendar().year.astype(int)
        rebal = df.groupby(["year", "week"])["date"].last().tolist()
        return sorted(rebal)

    def _equity_dd_adjustments(self, dt, max_pos: int, position_scale: float) -> tuple[int, float]:
        """v16: Graduated equity DD control.

        Smoothly reduce max positions and position scale as portfolio DD deepens.
        Returns adjusted (max_pos, position_scale).
        """
        if not PC.get("equity_dd_enabled", False) or self._peak_equity <= 0:
            return max_pos, position_scale

        equity = self._portfolio_value(dt)
        dd = (equity - self._peak_equity) / self._peak_equity  # negative number

        dd_start = PC.get("equity_dd_threshold", -0.10)  # start reducing
        dd_full = PC.get("equity_dd_floor", -0.25)        # max reduction
        dd_min_pos = PC.get("equity_dd_max_positions", 4)
        dd_min_scale = PC.get("equity_dd_position_scale", 0.70)

        if dd >= dd_start:
            return max_pos, position_scale  # no DD adjustment needed

        # Linear interpolation: dd_start → full, dd_full → minimum
        frac = min(1.0, (dd_start - dd) / (dd_start - dd_full))
        adj_max_pos = max(dd_min_pos, int(round(max_pos - frac * (max_pos - dd_min_pos))))
        adj_scale = position_scale * (1.0 - frac * (1.0 - dd_min_scale))

        return adj_max_pos, adj_scale

    def _get_price(self, dt, ticker) -> dict | None:
        try:
            row = self._price_idx.loc[(dt, ticker)]
            return row.to_dict() if hasattr(row, 'to_dict') else None
        except KeyError:
            return None

    def _portfolio_value(self, dt) -> float:
        val = self.capital
        for pos in self.positions.values():
            p = self._get_price(dt, pos.ticker)
            if p:
                val += pos.shares * p["close"]
            else:
                val += pos.shares * pos.highest_price
        return val

    def _check_stops(self, dt) -> None:
        """Exit positions hitting stop-loss, trailing stop, or time stop."""
        to_close: list[tuple[str, str]] = []

        for ticker, pos in self.positions.items():
            p = self._get_price(dt, ticker)
            if p is None:
                to_close.append((ticker, "delisted"))
                continue

            current_price = p["close"]
            pos.highest_price = max(pos.highest_price, p["high"])

            # Hard stop: 7% below ENTRY price (Ch.12: "never more than 7-8%")
            entry_stop = pos.entry_price * (1 - PC["stop_loss_pct"])
            if current_price <= entry_stop:
                to_close.append((ticker, "stop_entry"))
                continue

            # Trailing stop: 20% below highest price — AFTER +10% gain
            # v14: Breadth-based trailing stop tightening — when market breadth
            # is weak, tighten the trailing stop to protect gains earlier.
            # This catches the 2-3 month breadth deterioration before formal
            # bear signals (e.g. Nov 2021→Jan 2022, Dec 2024→Mar 2025).
            if PC.get("use_trailing_stop", True):
                activation = PC.get("trailing_stop_activation", 0.0)
                gain_from_entry = (pos.highest_price - pos.entry_price) / pos.entry_price
                if gain_from_entry >= activation:
                    trail_pct = PC["trailing_stop_pct"]

                    # v14: Tighten trail when breadth is weak
                    if PC.get("breadth_trail_tighten", False) and self._breadth:
                        breadth_now = self._breadth.get(dt, 0.5)
                        tighten_below = PC.get("breadth_trail_tighten_below", 0.40)
                        tighten_pct = PC.get("breadth_trail_tighten_pct", 0.15)
                        if breadth_now < tighten_below:
                            # Linear interpolation: at tighten_below → normal trail,
                            # at 0 → tighten_pct. Smoothly narrows the trailing stop.
                            frac = breadth_now / tighten_below  # 0..1
                            trail_pct = tighten_pct + (trail_pct - tighten_pct) * frac

                    trail_stop = pos.highest_price * (1 - trail_pct)
                    if current_price <= trail_stop:
                        to_close.append((ticker, "stop_trail"))
                        continue

            # MA50 trailing exit (Ch.9: "I use the 50-day as my line in the sand")
            # Only close if stock closes >3% BELOW the 50-day MA after a +20% run.
            # Stocks routinely dip to/just below the 50MA and recover in bull trends.
            if PC.get("use_ma50_trailing", False) and self._ma50_idx:
                ma50_val = self._ma50_idx.get((dt, ticker))
                if ma50_val is not None and ma50_val > 0:
                    ma50_pct_below = (ma50_val - current_price) / ma50_val
                    gain = (pos.highest_price - pos.entry_price) / pos.entry_price
                    # Must have run at least +20% AND be >3% below MA50
                    if gain >= 0.20 and ma50_pct_below > 0.03:
                        to_close.append((ticker, "stop_ma50"))
                        continue

            # Time stop: exit dead money (Minervini: "don't hold laggards")
            # Only kill positions that are LOSING after a long hold. Flat/small-gain
            # positions can still break out. Use 60 days and must be negative.
            time_stop_days = PC.get("time_stop_days", 0)
            if time_stop_days > 0:
                hold_days = (dt - pos.entry_date).days if isinstance(dt, date) and isinstance(pos.entry_date, date) else 0
                if hold_days >= time_stop_days:
                    gain = (current_price - pos.entry_price) / pos.entry_price
                    threshold = PC.get("time_stop_threshold", 0.0)
                    if gain < threshold:
                        to_close.append((ticker, "time_stop"))
                        continue

            # ── Bear trailing stop (hold_tight mode) ──
            # In bear markets with hold_tight, use tighter trailing to protect
            # capital while giving winners room to continue.  This replaces the
            # force-close-all approach and captures V-recoveries (2009, 2020).
            bear_mode = PC.get("bear_mode", "force_close")
            if bear_mode == "hold_tight":
                is_bear_today = (
                    self._regime_set
                    and dt not in self._regime_set
                    and dt not in self._recovery_set
                )
                if is_bear_today:
                    bear_trail = PC.get("bear_trailing_stop_pct", 0.10)
                    bear_trail_price = pos.highest_price * (1 - bear_trail)
                    if current_price <= bear_trail_price:
                        to_close.append((ticker, "stop_bear_trail"))
                        continue

        for ticker, reason in to_close:
            self._close_position(dt, ticker, reason=reason)

    def _check_profit_taking(self, dt) -> None:
        """Sell 1/3 of position at +20% — Minervini: 'always sell into strength'."""
        take_pct = PC.get("profit_take_pct", 0.0)
        take_frac = PC.get("profit_take_fraction", 0.33)
        if take_pct <= 0:
            return

        for ticker, pos in list(self.positions.items()):
            if pos.pyramid_count >= 100:  # already took profit
                continue
            p = self._get_price(dt, ticker)
            if p is None:
                continue
            current_price = p["close"]
            gain = (current_price - pos.entry_price) / pos.entry_price
            if gain >= take_pct:
                sell_shares = pos.shares * take_frac
                if sell_shares <= 0:
                    continue
                exit_price = current_price * (1 - BC["slippage_bps"] / 10_000)
                commission = exit_price * sell_shares * BC["commission_bps"] / 10_000
                proceeds = sell_shares * exit_price - commission
                cost_portion = pos.total_cost * take_frac
                pnl = proceeds - cost_portion
                pnl_pct = pnl / cost_portion if cost_portion > 0 else 0.0
                hold_days = (dt - pos.entry_date).days if isinstance(dt, date) and isinstance(pos.entry_date, date) else 0

                self.capital += proceeds
                pos.shares -= sell_shares
                pos.total_cost -= cost_portion
                # Move stop to breakeven on remaining shares — but do NOT
                # reset entry_price to current (that was a bug: it made the
                # 7% hard stop measure from the high, stopping out winners)
                # Instead, set a flag so profit-take only fires once.
                pos.pyramid_count += 100  # hack: prevent further profit-takes

                self.trades.append(Trade(
                    ticker=ticker,
                    entry_date=pos.entry_date,
                    exit_date=dt,
                    entry_price=pos.entry_price,
                    exit_price=exit_price,
                    shares=sell_shares,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    exit_reason="profit_take",
                    hold_days=hold_days,
                ))

    def _check_pyramids(self, dt) -> None:
        """Add to winning positions aggressively (Minervini pyramiding).
        
        V6 aggressive pyramiding strategy:
        - Trigger at +5%, +10%, +15% (3 adds)
        - Each add is 75% of original position size
        - This means a big winner can grow to 3.25x its original size
        - Combined with 5 max positions = massive concentration in winners
        - Minervini: "When I have a stock working, I add more. When I'm wrong,
          I cut quickly. The result is my winners are much larger than my losers."
        """
        if not PC.get("pyramid_enabled", False):
            return

        max_adds = PC.get("pyramid_max_adds", 3)
        trigger_pct = PC.get("pyramid_trigger_pct", 0.05)
        size_ratio = PC.get("pyramid_size_ratio", 0.75)

        for ticker, pos in list(self.positions.items()):
            real_pyramid_count = pos.pyramid_count % 100  # strip profit_take flag
            if real_pyramid_count >= max_adds:
                continue

            p = self._get_price(dt, ticker)
            if p is None:
                continue

            current_price = p["close"]
            gain_pct = (current_price - pos.entry_price) / pos.entry_price

            # Pyramid trigger: add when gain exceeds threshold
            real_pyramid_count = pos.pyramid_count % 100  # strip profit_take flag
            required_gain = trigger_pct * (real_pyramid_count + 1)
            if gain_pct < required_gain:
                continue

            # Size = original position × ratio (aggressive: 75% of original each time)
            if real_pyramid_count > 0:
                original_value = pos.total_cost / (1 + real_pyramid_count * size_ratio)
            else:
                original_value = pos.total_cost
            add_value = original_value * size_ratio

            # Apply slippage
            buy_price = current_price * (1 + BC["slippage_bps"] / 10_000)
            commission = add_value * BC["commission_bps"] / 10_000

            if add_value + commission > self.capital:
                continue

            add_shares = add_value / buy_price
            self.capital -= (add_value + commission)
            pos.shares += add_shares
            pos.total_cost += add_value
            pos.pyramid_count += 1

    def _close_position(self, dt, ticker: str, reason: str) -> None:
        pos = self.positions.pop(ticker, None)
        if pos is None:
            return

        p = self._get_price(dt, ticker)
        exit_price = p["close"] if p else pos.highest_price

        slippage = exit_price * BC["slippage_bps"] / 10_000
        exit_price -= slippage

        commission = exit_price * pos.shares * BC["commission_bps"] / 10_000

        proceeds = pos.shares * exit_price - commission
        cost = pos.total_cost if pos.total_cost > 0 else pos.shares * pos.entry_price
        pnl = proceeds - cost
        pnl_pct = pnl / cost if cost > 0 else 0.0

        hold_days = (dt - pos.entry_date).days if isinstance(dt, date) and isinstance(pos.entry_date, date) else 0

        self.capital += proceeds

        self.trades.append(Trade(
            ticker=ticker,
            entry_date=pos.entry_date,
            exit_date=dt,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            shares=pos.shares,
            pnl=pnl,
            pnl_pct=pnl_pct,
            exit_reason=reason,
            hold_days=hold_days,
        ))

        # v13: Track consecutive stop_entry losses for circuit breaker
        if reason == "stop_entry":
            self._consecutive_stop_losses += 1
            cb_losses = PC.get("circuit_breaker_losses", 3)
            cb_pause = PC.get("circuit_breaker_pause_days", 10)
            if PC.get("circuit_breaker_enabled", False) and self._consecutive_stop_losses >= cb_losses:
                # Pause new entries for cb_pause trading days
                self._circuit_breaker_until = dt + timedelta(days=int(cb_pause * 1.5))  # ~trading days
        elif reason != "regime_bear":
            # Any non-stop exit resets the counter (trails, profit takes, etc.)
            if pnl > 0:
                self._consecutive_stop_losses = 0

        # v13: Track bear exit date for re-entry delay
        if reason == "regime_bear":
            self._last_bear_exit_date = dt

    def _rebalance(self, dt) -> None:
        """Select new positions from candidates on rebalance day.
        
        v8.2: v7.5b-proven regime handling with improved profit-take & pyramiding.
        v13: Breadth-aware sizing + re-entry delay + circuit breaker + bear confirm.
        v15: Minervini cash rule — sustained breadth deterioration → 100% cash.
        - Bear (bench < MA200): Force-close ALL. Crash protection.
        - Recovery (MA50 < bench < MA200): Buy at graduated size (50-100%).
        - Bull (bench > MA200): Full size buying.
        """
        # ── v15: Minervini Cash Rule ──
        # "When market internals deteriorate, I go to cash." — MM
        # Uses rolling-average breadth (not spot) to smooth daily noise.
        # Hysteresis: different entry/exit thresholds prevent whipsaw.
        if PC.get("breadth_cash_enabled", False) and self._breadth_rolling:
            breadth_avg = self._breadth_rolling.get(dt, 0.5)
            cash_thresh = PC.get("breadth_cash_threshold", 0.35)
            cash_days = PC.get("breadth_cash_consecutive", 5)
            recov_thresh = PC.get("breadth_cash_recovery_threshold", 0.50)
            recov_days = PC.get("breadth_cash_recovery_consecutive", 3)

            if not self._in_cash_mode:
                # Determine if breadth signals weakness
                if PC.get("breadth_cash_use_slope", False) and self._breadth_rolling_max:
                    # v15b: Slope mode — trigger on DROP from recent peak
                    peak = self._breadth_rolling_max.get(dt, 0.5)
                    slope_drop = PC.get("breadth_cash_slope_drop", 0.15)
                    breadth_weak = (peak - breadth_avg) > slope_drop
                else:
                    # Standard: absolute threshold
                    breadth_weak = breadth_avg < cash_thresh

                if breadth_weak:
                    self._breadth_low_streak += 1
                else:
                    self._breadth_low_streak = 0

                # Enter cash after sustained weakness
                if self._breadth_low_streak >= cash_days:
                    # Force close all positions
                    for ticker in list(self.positions.keys()):
                        self._close_position(dt, ticker, reason="cash_rule")

                    if PC.get("breadth_cash_block_buys", True):
                        # Standard: enter cash mode (block buying until recovery)
                        self._in_cash_mode = True
                        self._breadth_high_streak = 0
                    else:
                        # v15b: Close-only — use re-entry delay, resume normal trading
                        self._breadth_low_streak = 0
                        self._last_bear_exit_date = dt
                    return
            else:
                # In cash mode: track recovery using rolling avg
                if breadth_avg >= recov_thresh:
                    self._breadth_high_streak += 1
                else:
                    self._breadth_high_streak = 0

                # Exit cash mode after sustained recovery
                if self._breadth_high_streak >= recov_days:
                    self._in_cash_mode = False
                    self._breadth_low_streak = 0
                    # Fall through to normal rebalance logic
                else:
                    return  # Stay in cash — skip all buying

        # ── Regime check ──
        is_bull = (not self._regime_set) or (dt in self._regime_set)
        is_recovery = dt in self._recovery_set
        regime_strength = self._regime_strength.get(dt, 1.0)

        # Bear: depends on bear_mode setting
        bear_mode = PC.get("bear_mode", "force_close")
        if not is_bull and not is_recovery:
            # Cancel any pending buys — don't execute into a bear market
            self._pending_buys = []
            # v13: Bear confirmation delay — require N consecutive bear days
            bear_confirm = PC.get("bear_confirm_days", 0)
            if bear_confirm > 0:
                self._bear_day_count += 1
                if self._bear_day_count < bear_confirm:
                    return  # Not yet confirmed bear — skip but don't close
            if bear_mode == "force_close":
                # v7.5b proven crash protection: sell all positions
                for ticker in list(self.positions.keys()):
                    self._close_position(dt, ticker, reason="regime_bear")
            return
        else:
            self._bear_day_count = 0  # Reset counter when bull/recovery

        # v13: Breadth-based early exit — close all if breadth crashes
        if PC.get("breadth_exit_enabled", False) and self._breadth:
            breadth_now = self._breadth.get(dt, 0.5)
            exit_thresh = PC.get("breadth_exit_threshold", 0.25)
            if breadth_now < exit_thresh and self.positions:
                for ticker in list(self.positions.keys()):
                    self._close_position(dt, ticker, reason="breadth_crash")
                return

        # v13: Re-entry delay — wait N days after bear→bull transition
        reentry_delay = PC.get("reentry_delay_days", 0)
        if reentry_delay > 0 and self._last_bear_exit_date is not None:
            days_since_bear = (dt - self._last_bear_exit_date).days if isinstance(dt, date) and isinstance(self._last_bear_exit_date, date) else 0
            if days_since_bear < reentry_delay and not self.positions:
                return  # Still in delay period, skip new entries

        # v13: Circuit breaker — pause entries after consecutive stop losses
        if PC.get("circuit_breaker_enabled", False) and self._circuit_breaker_until is not None:
            if dt <= self._circuit_breaker_until:
                # Still in pause period — manage existing but don't enter new
                return
            else:
                # Pause expired — reset
                self._circuit_breaker_until = None
                self._consecutive_stop_losses = 0

        # Get candidates within lookback window
        freq = PC.get("rebalance_freq", "W-FRI")
        lookback = timedelta(days=5) if freq == "D" else timedelta(days=7)
        mask = (self.candidates["date"] <= dt) & (self.candidates["date"] > dt - lookback)
        day_cands = self.candidates[mask]
        if not day_cands.empty:
            day_cands = day_cands.sort_values("date").drop_duplicates(
                subset=["ticker"], keep="last"
            )
        if day_cands.empty:
            return

        rank_col = "composite_score" if "composite_score" in day_cands.columns else "rs_composite"
        day_cands = day_cands.sort_values(rank_col, ascending=False)

        # ── Graduated position sizing based on regime_strength ──
        # At MA50 crossover (regime_strength=0): size = 50% of full
        # At MA200 crossover (regime_strength=1): size = 100% of full
        # Linear scale between: size = 50% + 50% × regime_strength
        min_scale = PC.get("recovery_min_scale", 0.50)
        max_scale = PC.get("recovery_max_scale", 1.00)
        position_scale = min_scale + (max_scale - min_scale) * regime_strength

        # v13: Breadth-based position sizing — scale down in weak breadth
        if PC.get("breadth_sizing", False) and self._breadth:
            breadth_now = self._breadth.get(dt, 0.5)
            breadth_full = PC.get("breadth_full_above", 0.55)
            breadth_half = PC.get("breadth_half_below", 0.35)
            if breadth_now >= breadth_full:
                breadth_scale = 1.0
            elif breadth_now <= breadth_half:
                breadth_scale = 0.50
            else:
                # Linear interpolation between half and full
                breadth_scale = 0.50 + 0.50 * (breadth_now - breadth_half) / (breadth_full - breadth_half)
            position_scale *= breadth_scale

        max_pos = PC["max_positions"]

        # v16: Graduated equity DD control
        max_pos, position_scale = self._equity_dd_adjustments(dt, max_pos, position_scale)

        # Close delisted/halted only
        for ticker in list(self.positions.keys()):
            p = self._get_price(dt, ticker)
            if p is None:
                self._close_position(dt, ticker, reason="rebalance")

        # How many new slots?
        open_slots = max_pos - len(self.positions)
        if open_slots <= 0:
            return

        new_picks = day_cands[~day_cands["ticker"].isin(self.positions.keys())]
        # Also exclude tickers already queued for next-day entry
        pending_tickers = {o["ticker"] for o in self._pending_buys}
        new_picks = new_picks[~new_picks["ticker"].isin(pending_tickers)]
        new_picks = new_picks.head(open_slots)

        total_equity = self._portfolio_value(dt)
        stop_pct = PC["stop_loss_pct"]  # 7%

        entry_delay = BC.get("entry_delay_days", 1)

        for _, row in new_picks.iterrows():
            ticker = row["ticker"]
            p = self._get_price(dt, ticker)
            if p is None or p["close"] <= 0:
                continue

            if entry_delay > 0:
                # ── NEXT-DAY ENTRY: Queue buy for next trading day ──
                # Signal fires at today's close; execution at tomorrow's open.
                # This eliminates lookahead bias (we don't buy at the close
                # that generated the signal).
                self._pending_buys.append({
                    "ticker": ticker,
                    "sector": row.get("sector"),
                    "base_high": row.get("base_high"),
                    "position_scale": position_scale,
                    "signal_date": dt,
                })
                continue

            # ── SAME-DAY ENTRY (legacy, entry_delay=0) ──
            price = p["close"]

            # ── Chase filter: skip if too far above pivot ──
            max_chase = PC.get("max_chase_pct", 0.05)
            if max_chase > 0:
                base_high = row.get("base_high")
                if base_high and pd.notna(base_high) and base_high > 0:
                    if price > base_high * (1 + max_chase):
                        continue

            slippage = price * BC["slippage_bps"] / 10_000
            price += slippage

            # Sector limit
            sector = row.get("sector")
            try:
                sector_valid = bool(sector) and pd.notna(sector)
            except (TypeError, ValueError):
                sector_valid = False
            if sector_valid and PC["max_sector_weight"] < 1.0:
                sector_exposure = sum(
                    pos.shares * (self._get_price(dt, pos.ticker) or {}).get("close", 0)
                    for pos in self.positions.values()
                    if pos.sector == sector
                )
                if total_equity > 0 and sector_exposure / total_equity > PC["max_sector_weight"]:
                    continue

            # ── Risk-based position sizing (Minervini Ch.12) ──
            # "I risk about 1% of my account on each trade"
            # position_value = account × risk_per_trade / stop_loss_pct
            risk_per_trade = PC.get("risk_per_trade_pct", 0.01)
            per_position = (total_equity * risk_per_trade / stop_pct) * position_scale
            # Cap at max position size (20% of equity)
            max_pos_val = total_equity * PC.get("max_position_pct", 0.20)
            per_position = min(per_position, max_pos_val)

            shares = per_position / price
            cost = shares * price
            commission = cost * BC["commission_bps"] / 10_000

            if cost + commission > self.capital:
                continue

            self.capital -= (cost + commission)
            self.positions[ticker] = Position(
                ticker=ticker,
                entry_date=dt,
                entry_price=price,
                shares=shares,
                highest_price=p["high"],
                sector=sector if pd.notna(sector) else None,
                pyramid_count=0,
                total_cost=cost,
            )

    def _execute_pending_buys(self, dt) -> None:
        """Execute queued buy orders at today's open price.

        Called at the very start of the trading day, before stops.
        Orders were queued yesterday by _rebalance() to eliminate
        lookahead bias (signal fires at close T, execution at open T+1).
        """
        if not self._pending_buys:
            return

        entry_field = BC.get("entry_price_field", "open")
        total_equity = self._portfolio_value(dt)
        max_pos = PC["max_positions"]
        stop_pct = PC["stop_loss_pct"]

        # v16: Graduated equity DD control
        max_pos, _ = self._equity_dd_adjustments(dt, max_pos, 1.0)

        # Check regime — skip all orders in bear market
        is_bull = (not self._regime_set) or (dt in self._regime_set)
        is_recovery = dt in self._recovery_set
        if not is_bull and not is_recovery:
            self._pending_buys = []
            return

        # Check circuit breaker
        if PC.get("circuit_breaker_enabled", False) and self._circuit_breaker_until is not None:
            if dt <= self._circuit_breaker_until:
                self._pending_buys = []
                return

        for order in self._pending_buys:
            ticker = order["ticker"]

            # Skip if already in portfolio
            if ticker in self.positions:
                continue
            if len(self.positions) >= max_pos:
                break

            p = self._get_price(dt, ticker)
            if p is None:
                continue

            # Use open price (or close if open not available)
            raw_price = p.get(entry_field)
            if raw_price is None or raw_price <= 0:
                raw_price = p.get("close", 0)
            if raw_price <= 0:
                continue

            price = raw_price

            # Chase filter at execution time
            max_chase = PC.get("max_chase_pct", 0.05)
            if max_chase > 0:
                base_high = order.get("base_high")
                if base_high and pd.notna(base_high) and base_high > 0:
                    if price > base_high * (1 + max_chase):
                        continue

            slippage = price * BC["slippage_bps"] / 10_000
            price += slippage

            # Sector limit check at execution time
            sector = order.get("sector")
            try:
                sector_valid = bool(sector) and pd.notna(sector)
            except (TypeError, ValueError):
                sector_valid = False
            if sector_valid and PC["max_sector_weight"] < 1.0:
                sector_exposure = sum(
                    pos.shares * (self._get_price(dt, pos.ticker) or {}).get("close", 0)
                    for pos in self.positions.values()
                    if pos.sector == sector
                )
                if total_equity > 0 and sector_exposure / total_equity > PC["max_sector_weight"]:
                    continue

            # Position sizing at execution time (uses current equity)
            risk_per_trade = PC.get("risk_per_trade_pct", 0.01)
            position_scale = order.get("position_scale", 1.0)
            # v16: Graduated DD scaling for position size
            _, dd_scale = self._equity_dd_adjustments(dt, 1, position_scale)
            position_scale = dd_scale
            per_position = (total_equity * risk_per_trade / stop_pct) * position_scale
            max_pos_val = total_equity * PC.get("max_position_pct", 0.20)
            per_position = min(per_position, max_pos_val)

            shares = per_position / price
            cost = shares * price
            commission = cost * BC["commission_bps"] / 10_000

            if cost + commission > self.capital:
                continue

            self.capital -= (cost + commission)
            self.positions[ticker] = Position(
                ticker=ticker,
                entry_date=dt,
                entry_price=price,
                shares=shares,
                highest_price=p["high"],
                sector=sector if pd.notna(sector) else None,
                pyramid_count=0,
                total_cost=cost,
            )

        self._pending_buys = []

    def run(self) -> None:
        """Execute the full walk-forward simulation."""
        all_dates = sorted(self.prices["date"].unique())
        rebal_set = set(self._rebalance_dates)
        n_dates = len(all_dates)

        log.info(f"Running backtest over {n_dates} trading days…")

        for i, dt in enumerate(all_dates):
            # 1. Execute pending buys at today's open (from yesterday's signals)
            if BC.get("entry_delay_days", 1) > 0:
                self._execute_pending_buys(dt)

            # 2. Check stops during the day (on close/high/low)
            self._check_stops(dt)
            self._check_profit_taking(dt)
            self._check_pyramids(dt)

            # 3. Generate new signals at close → queue for tomorrow
            if dt in rebal_set:
                self._rebalance(dt)

            equity = self._portfolio_value(dt)
            n_pos = len(self.positions)
            invested = equity - self.capital
            exposure = invested / equity if equity > 0 else 0

            self.snapshots.append(DailySnapshot(
                date=dt, equity=equity, cash=self.capital,
                n_positions=n_pos, exposure_pct=exposure,
            ))

            # v16: Update peak equity (used for graduated DD control)
            if PC.get("equity_dd_enabled", False):
                self._peak_equity = max(self._peak_equity, equity)

            if (i + 1) % 500 == 0:
                log.info(f"  Day {i + 1}/{n_dates}  equity={equity:,.0f}  "
                         f"positions={n_pos}")

        # Close remaining positions at end
        last_dt = all_dates[-1]
        for ticker in list(self.positions.keys()):
            self._close_position(last_dt, ticker, reason="end_of_data")

        log.info(f"Backtest complete: {len(self.trades)} trades")


# ═══════════════════════════════════════════════════════════
#  PERFORMANCE METRICS
# ═══════════════════════════════════════════════════════════

def compute_metrics(snapshots: list[DailySnapshot], trades: list[Trade]) -> dict:
    """Compute standard quant performance metrics."""
    eq = pd.DataFrame([{"date": s.date, "equity": s.equity} for s in snapshots])
    eq["date"] = pd.to_datetime(eq["date"])
    eq = eq.sort_values("date")

    eq["ret"] = eq["equity"].pct_change()
    daily_ret = eq["ret"].dropna()

    total_return = eq["equity"].iloc[-1] / eq["equity"].iloc[0] - 1
    n_years = (eq["date"].iloc[-1] - eq["date"].iloc[0]).days / 365.25
    cagr = (1 + total_return) ** (1 / n_years) - 1 if n_years > 0 else 0

    sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)
              if daily_ret.std() > 0 else 0)

    # Max drawdown
    eq["peak"] = eq["equity"].cummax()
    eq["dd"] = (eq["equity"] - eq["peak"]) / eq["peak"]
    max_dd = eq["dd"].min()

    # Trade stats
    if trades:
        wins = [t for t in trades if t.pnl > 0]
        losses = [t for t in trades if t.pnl <= 0]
        win_rate = len(wins) / len(trades)
        avg_win = np.mean([t.pnl_pct for t in wins]) if wins else 0
        avg_loss = np.mean([t.pnl_pct for t in losses]) if losses else 0
        avg_hold = np.mean([t.hold_days for t in trades])
    else:
        win_rate = avg_win = avg_loss = avg_hold = 0

    avg_exposure = np.mean([s.exposure_pct for s in snapshots])

    return {
        "total_return_pct": total_return * 100,
        "cagr_pct": cagr * 100,
        "sharpe_ratio": sharpe,
        "max_drawdown_pct": max_dd * 100,
        "total_trades": len(trades),
        "win_rate_pct": win_rate * 100,
        "avg_win_pct": avg_win * 100,
        "avg_loss_pct": avg_loss * 100,
        "avg_hold_days": avg_hold,
        "avg_exposure_pct": avg_exposure * 100,
        "n_years": n_years,
    }


def compute_annual_returns(snapshots: list[DailySnapshot]) -> pd.DataFrame:
    """Compute annual returns for benchmark comparison."""
    eq = pd.DataFrame([
        {"date": s.date, "equity": s.equity} for s in snapshots
    ])
    eq["date"] = pd.to_datetime(eq["date"])
    eq = eq.sort_values("date")
    eq["year"] = eq["date"].dt.year

    annual = []
    for year, grp in eq.groupby("year"):
        start_eq = grp["equity"].iloc[0]
        end_eq = grp["equity"].iloc[-1]
        ret = (end_eq / start_eq - 1) * 100
        annual.append({"year": int(year), "return_pct": ret})

    return pd.DataFrame(annual)


def print_metrics(metrics: dict, label: str = "") -> None:
    log.info(f"\n{'═' * 50}")
    log.info(f"  PERFORMANCE — {label}")
    log.info(f"{'═' * 50}")
    log.info(f"  CAGR:           {metrics['cagr_pct']:>8.1f}%")
    log.info(f"  Total Return:   {metrics['total_return_pct']:>8.1f}%")
    log.info(f"  Sharpe Ratio:   {metrics['sharpe_ratio']:>8.2f}")
    log.info(f"  Max Drawdown:   {metrics['max_drawdown_pct']:>8.1f}%")
    log.info(f"  Win Rate:       {metrics['win_rate_pct']:>8.1f}%")
    log.info(f"  Avg Win:        {metrics['avg_win_pct']:>8.1f}%")
    log.info(f"  Avg Loss:       {metrics['avg_loss_pct']:>8.1f}%")
    log.info(f"  Avg Hold Days:  {metrics['avg_hold_days']:>8.1f}")
    log.info(f"  Total Trades:   {metrics['total_trades']:>8d}")
    log.info(f"  Avg Exposure:   {metrics['avg_exposure_pct']:>8.1f}%")
    log.info(f"  Period:         {metrics['n_years']:>8.1f} years")
    log.info(f"{'═' * 50}\n")


def print_annual_comparison(annual: pd.DataFrame, label: str = "") -> None:
    """Print year-by-year comparison with Minervini benchmarks."""
    benchmarks = config.MINERVINI_BENCHMARKS
    log.info(f"\n{'═' * 60}")
    log.info(f"  ANNUAL RETURNS vs MINERVINI — {label}")
    log.info(f"{'─' * 60}")
    log.info(f"  {'Year':>6}  {'System':>10}  {'Minervini':>10}  {'Delta':>10}")
    log.info(f"{'─' * 60}")

    for _, row in annual.iterrows():
        yr = int(row["year"])
        sys_ret = row["return_pct"]
        min_ret = benchmarks.get(yr, None)
        if min_ret is not None:
            delta = sys_ret - min_ret
            log.info(f"  {yr:>6}  {sys_ret:>9.1f}%  {min_ret:>9.1f}%  {delta:>+9.1f}%")
        else:
            log.info(f"  {yr:>6}  {sys_ret:>9.1f}%  {'N/A':>10}  {'':>10}")

    log.info(f"{'═' * 60}\n")


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════

def _run_single_market(mkt: str, con) -> dict | None:
    """Run backtest for a single market. Returns metrics dict or None."""
    log.info(f"{'═' * 50}")
    log.info(f"Backtesting: {mkt.upper()}")
    log.info(f"{'═' * 50}")

    # Apply market-specific portfolio config
    saved = config.apply_market_config(mkt)
    log.info(f"Applied {mkt.upper()} config overrides: "
             + ", ".join(f"{k}={config.PORTFOLIO[k]}" for k in saved)
             if saved else f"Using base config for {mkt.upper()}")

    try:
        return _run_single_market_inner(mkt, con)
    finally:
        config.restore_config(saved)


def _run_single_market_inner(mkt: str, con) -> dict | None:
    """Inner backtest logic for a single market (config already applied)."""

    prices = con.execute("""
        SELECT p.date, p.ticker, p.open, p.close, p.high, p.low, s.sector
        FROM prices p
        JOIN stocks s ON p.ticker = s.ticker AND p.market = s.market
        WHERE p.market = ?
        ORDER BY p.ticker, p.date
    """, [mkt]).df()

    candidates = con.execute(
        "SELECT * FROM candidates WHERE market = ?", [mkt]
    ).df()

    # Load regime with all columns
    try:
        regime = con.execute(
            "SELECT date, is_bull, is_confirmed_bull, is_recovery, is_weak_market, "
            "breadth_pct, breadth_improving, regime_strength "
            "FROM regime WHERE market = ?", [mkt]
        ).df()
    except Exception:
        regime = con.execute(
            "SELECT date, is_bull, is_recovery FROM regime WHERE market = ?", [mkt]
        ).df()

    if prices.empty:
        log.warning(f"No prices for {mkt}")
        return None

    for frame in [prices, candidates, regime]:
        if not frame.empty and "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"]).dt.date

    # ── Filter to backtest start date ──
    # Ensures consistent period regardless of how far back the data goes
    bt_start_str = BC.get("start_date")
    if bt_start_str:
        from datetime import datetime
        bt_start = datetime.strptime(bt_start_str, "%Y-%m-%d").date()
        n_before = len(prices)
        prices = prices[prices["date"] >= bt_start].copy()
        candidates = candidates[candidates["date"] >= bt_start].copy()
        regime = regime[regime["date"] >= bt_start].copy()
        log.info(f"Filtered to dates ≥ {bt_start}: prices {n_before:,} → {len(prices):,}")

    log.info(f"Prices: {len(prices):,} rows, {prices['ticker'].nunique()} tickers")
    log.info(f"Candidates: {len(candidates):,} signals")
    log.info(f"Regime: {len(regime)} days")

    bt = Backtester(prices, candidates, regime)
    bt.run()

    if not bt.snapshots:
        log.warning(f"No snapshots for {mkt}")
        return None

    metrics = compute_metrics(bt.snapshots, bt.trades)
    print_metrics(metrics, label=f"{mkt.upper()} (FULL)")

    annual_df = compute_annual_returns(bt.snapshots)


    # ── Train / Test split metrics ──
    train_end = BC.get("train_end")
    test_start = BC.get("test_start")
    if train_end and test_start:
        from datetime import datetime as _dt
        _train_end_d = _dt.strptime(train_end, "%Y-%m-%d").date()
        _test_start_d = _dt.strptime(test_start, "%Y-%m-%d").date()

        train_snaps = [s for s in bt.snapshots if s.date <= _train_end_d]
        train_trades = [t for t in bt.trades
                        if t.entry_date <= _train_end_d]
        test_snaps = [s for s in bt.snapshots if s.date >= _test_start_d]
        test_trades = [t for t in bt.trades
                       if t.entry_date >= _test_start_d]

        if len(train_snaps) > 1:
            train_metrics = compute_metrics(train_snaps, train_trades)
            print_metrics(train_metrics, label=f"{mkt.upper()} TRAIN (≤{train_end})")
        if len(test_snaps) > 1:
            # Re-base test equity so it starts at initial_capital
            first_eq = test_snaps[0].equity
            if first_eq > 0:
                scale = bt.initial_capital / first_eq
                test_snaps_rebased = [
                    DailySnapshot(
                        date=s.date,
                        equity=s.equity * scale,
                        cash=s.cash * scale,
                        n_positions=s.n_positions,
                        exposure_pct=s.exposure_pct,
                    )
                    for s in test_snaps
                ]
            else:
                test_snaps_rebased = test_snaps
            test_metrics = compute_metrics(test_snaps_rebased, test_trades)
            print_metrics(test_metrics, label=f"{mkt.upper()} TEST  (≥{test_start})")

    # Store results
    eq_df = pd.DataFrame([
        {"date": s.date, "equity": s.equity, "cash": s.cash,
         "n_positions": s.n_positions, "exposure_pct": s.exposure_pct,
         "market": mkt}
        for s in bt.snapshots
    ])
    table_name = f"backtest_equity_{mkt}"
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM eq_df")

    trades_df = pd.DataFrame([
        {"ticker": t.ticker, "entry_date": t.entry_date,
         "exit_date": t.exit_date, "entry_price": t.entry_price,
         "exit_price": t.exit_price, "shares": t.shares,
         "pnl": t.pnl, "pnl_pct": t.pnl_pct,
         "exit_reason": t.exit_reason, "hold_days": t.hold_days,
         "market": mkt}
        for t in bt.trades
    ])
    table_name = f"backtest_trades_{mkt}"
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    if not trades_df.empty:
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM trades_df")

    if not annual_df.empty:
        annual_df["market"] = mkt
        table_name = f"backtest_annual_{mkt}"
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM annual_df")

    log.info(f"✓ {mkt.upper()} backtest results stored")
    return metrics


def run(market: str = "both") -> None:
    """Run the full walk-forward backtest."""
    con = get_conn()
    markets = config.MARKETS if market == "both" else [market]

    for mkt in markets:
        _run_single_market(mkt, con)

    con.close()


# ═══════════════════════════════════════════════════════════
#  PARALLEL PARAMETER SWEEP (for tuning)
# ═══════════════════════════════════════════════════════════

def sweep_params(
    market: str = "india",
    param_grid: dict | None = None,
    max_workers: int = 4,
) -> pd.DataFrame:
    """Run backtest in parallel with different parameter combinations.

    param_grid example:
        {
            "stop_loss_pct": [0.05, 0.07, 0.10],
            "trailing_stop_pct": [0.15, 0.20, 0.25],
            "profit_take_pct": [0.15, 0.20, 0.30],
        }

    Returns DataFrame with one row per run: params + all metrics.
    Each worker gets its own copy of config, modifies it, and runs independently.
    """
    import itertools
    import copy

    if param_grid is None:
        param_grid = {
            "stop_loss_pct": [0.05, 0.07, 0.10],
            "trailing_stop_pct": [0.15, 0.20, 0.25],
            "profit_take_pct": [0.15, 0.20, 0.30],
        }

    # Generate all combinations
    keys = list(param_grid.keys())
    values = list(param_grid.values())
    combos = list(itertools.product(*values))
    log.info(f"Parameter sweep: {len(combos)} combinations across {len(keys)} params")

    # Load data once (shared, read-only)
    con = get_conn()
    prices = con.execute("""
        SELECT p.date, p.ticker, p.open, p.close, p.high, p.low, s.sector
        FROM prices p
        JOIN stocks s ON p.ticker = s.ticker AND p.market = s.market
        WHERE p.market = ?
        ORDER BY p.ticker, p.date
    """, [market]).df()

    candidates = con.execute(
        "SELECT * FROM candidates WHERE market = ?", [market]
    ).df()

    try:
        regime = con.execute(
            "SELECT date, is_bull, is_recovery, is_weak_market, breadth_pct, breadth_improving "
            "FROM regime WHERE market = ?", [market]
        ).df()
    except Exception:
        regime = con.execute(
            "SELECT date, is_bull, is_recovery FROM regime WHERE market = ?", [market]
        ).df()

    con.close()

    for frame in [prices, candidates, regime]:
        if not frame.empty and "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"]).dt.date

    def _run_one(combo_idx_combo):
        """Run a single backtest with overridden params."""
        idx, combo = combo_idx_combo
        overrides = dict(zip(keys, combo))

        # Temporarily override config
        original = {}
        for k, v in overrides.items():
            original[k] = config.PORTFOLIO.get(k)
            config.PORTFOLIO[k] = v

        try:
            bt = Backtester(
                prices.copy(), candidates.copy(), regime.copy(),
                initial_capital=config.PORTFOLIO["initial_capital"],
            )
            bt.run()
            metrics = compute_metrics(bt.snapshots, bt.trades) if bt.snapshots else {}
        finally:
            # Restore
            for k, v in original.items():
                if v is not None:
                    config.PORTFOLIO[k] = v

        result = {**overrides, **metrics, "combo_idx": idx}
        return result

    # Run in parallel
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_run_one, (i, c)): i
            for i, c in enumerate(combos)
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                cagr = result.get("cagr_pct", 0)
                sharpe = result.get("sharpe_ratio", 0)
                params_str = " ".join(f"{k}={result.get(k)}" for k in keys)
                log.info(f"  [{len(results)}/{len(combos)}] CAGR={cagr:.1f}% "
                         f"Sharpe={sharpe:.2f}  {params_str}")
            except Exception as e:
                log.error(f"  Sweep error: {e}")

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values("cagr_pct", ascending=False)
        log.info(f"\n{'═' * 60}")
        log.info(f"  TOP 5 PARAMETER COMBINATIONS")
        log.info(f"{'─' * 60}")
        for _, row in df.head(5).iterrows():
            params_str = " ".join(f"{k}={row.get(k)}" for k in keys)
            log.info(f"  CAGR={row.get('cagr_pct', 0):>6.1f}%  "
                     f"Sharpe={row.get('sharpe_ratio', 0):>5.2f}  "
                     f"DD={row.get('max_drawdown_pct', 0):>6.1f}%  "
                     f"WR={row.get('win_rate_pct', 0):>5.1f}%  {params_str}")
        log.info(f"{'═' * 60}")

    return df


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Run walk-forward backtest")
    p.add_argument("--market", default="india", choices=["india"])
    p.add_argument("--sweep", action="store_true",
                   help="Run parallel parameter sweep instead of single backtest")
    p.add_argument("--workers", type=int, default=4,
                   help="Number of parallel workers for sweep (default: 4)")
    args = p.parse_args()

    if args.sweep:
        results = sweep_params(market=args.market, max_workers=args.workers)
        if not results.empty:
            out_path = config.RESULTS_DIR / "param_sweep.csv"
            results.to_csv(out_path, index=False)
            log.info(f"Sweep results saved to {out_path}")
    else:
        run(market=args.market)
