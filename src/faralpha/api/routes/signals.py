"""Routes: /api/signals/{market}, /api/regime/{market}, /api/backtest/{market}"""

from __future__ import annotations

import pandas as pd
from fastapi import APIRouter

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.api.state import load_positions
from faralpha.api.helpers import (
    get_market_status, position_sizing, compute_trading_status, table_exists,
)

router = APIRouter()


@router.get("/api/signals/{market}")
async def get_signals(market: str, capital: float = 0):
    con = get_conn(read_only=True)
    pf = config.get_portfolio(market)
    try:
        regime_label = "unknown"
        if table_exists(con, "regime"):
            rr = con.execute(
                "SELECT is_bull, is_recovery FROM regime WHERE market = ? ORDER BY date DESC LIMIT 1",
                [market],
            ).fetchone()
            if rr:
                regime_label = "bull" if rr[0] else ("recovery" if rr[1] else "bear")

        if regime_label == "bear" and table_exists(con, "watchlist"):
            df = con.execute(
                "WITH latest_prices AS ( "
                "  SELECT ticker, market, close, volume, date AS price_date "
                "  FROM prices "
                "  WHERE market = ? "
                "  AND (ticker, date) IN ( "
                "    SELECT ticker, MAX(date) FROM prices "
                "    WHERE market = ? GROUP BY ticker "
                "  ) "
                ") "
                "SELECT w.ticker, w.rs_composite, "
                "'watchlist_leader' AS signal_tier, "
                "ROW_NUMBER() OVER (ORDER BY w.rs_composite DESC) AS rank_on_day, "
                "COALESCE(lp.price_date, w.date) AS date, "
                "w.sector, "
                "COALESCE(lp.close, w.close) AS close, "
                "COALESCE(lp.volume, 0) AS volume, "
                "w.rs_composite AS composite_score, "
                "NULL::DOUBLE AS pivot_high, NULL::DOUBLE AS base_high_30, "
                "NULL::DOUBLE AS darvas_top, NULL::DOUBLE AS base_depth "
                "FROM watchlist w "
                "LEFT JOIN latest_prices lp ON lp.ticker = w.ticker "
                "  AND lp.market = w.market "
                "WHERE w.market = ? "
                "AND w.date = (SELECT MAX(date) FROM watchlist WHERE market = ?) "
                "ORDER BY w.rs_composite DESC",
                [market, market, market, market],
            ).df()
        elif table_exists(con, "candidates"):
            df = con.execute(
                "WITH latest_prices AS ( "
                "  SELECT ticker, close AS latest_close, volume AS latest_volume, date AS price_date "
                "  FROM prices "
                "  WHERE market = ? "
                "  AND (ticker, date) IN ( "
                "    SELECT ticker, MAX(date) FROM prices "
                "    WHERE market = ? GROUP BY ticker "
                "  ) "
                ") "
                "SELECT c.ticker, c.rs_composite, c.signal_tier, c.rank_on_day, c.date, "
                "c.sector, COALESCE(lp.latest_close, c.close) AS close, "
                "COALESCE(lp.latest_volume, c.volume) AS volume, c.composite_score, "
                "c.pivot_high, c.base_high_30, c.darvas_top, c.base_depth "
                "FROM candidates c "
                "LEFT JOIN latest_prices lp ON lp.ticker = c.ticker "
                "WHERE c.market = ? "
                "AND c.date = (SELECT MAX(date) FROM candidates WHERE market = ?) "
                "ORDER BY c.rank_on_day",
                [market, market, market, market],
            ).df()
        else:
            return {"market": market, "date": None, "count": 0, "candidates": [], "actions": []}

        positions = load_positions()
        held_tickers = {p["ticker"] for p in positions if p["market"] == market}
        open_slots = max(0, pf["max_positions"] - len(held_tickers))

        rows = []
        actions = []
        for idx, r in df.iterrows():
            close_price = float(r["close"]) if r["close"] else 0
            pivot = float(r["pivot_high"]) if r["pivot_high"] and not pd.isna(r["pivot_high"]) else close_price
            base_depth_val = float(r["base_depth"]) if r["base_depth"] and not pd.isna(r["base_depth"]) else 0

            entry_price = round(close_price * 1.001, 2)
            max_chase = pf.get("max_chase_pct", 0.05)
            max_entry_price = round(pivot * (1 + max_chase), 2)
            stop_price = round(entry_price * (1 - pf["stop_loss_pct"]), 2)
            risk_per_share = round(entry_price - stop_price, 2)

            rows.append({
                "ticker": r["ticker"],
                "rs_composite": round(float(r["rs_composite"]), 4) if r["rs_composite"] else 0,
                "signal_tier": r.get("signal_tier", ""),
                "rank": int(r["rank_on_day"]) if r["rank_on_day"] else 0,
                "date": str(r["date"])[:10],
                "sector": str(r.get("sector", "")),
                "close": close_price,
                "volume": int(r["volume"]) if r["volume"] else 0,
                "score": round(float(r["composite_score"]), 2) if r["composite_score"] else 0,
                "pivot_price": round(pivot, 2),
                "entry_price": entry_price,
                "max_entry_price": max_entry_price,
                "stop_price": stop_price,
                "risk_per_share": risk_per_share,
                "base_depth_pct": round(base_depth_val * 100, 1) if base_depth_val else 0,
                "already_held": r["ticker"] in held_tickers,
            })

            # Generate action instruction for top candidates
            ticker = r["ticker"]
            if ticker in held_tickers:
                continue
            if len(actions) >= open_slots:
                continue
            if regime_label == "bear":
                continue

            actions.append({
                "action": "BUY",
                "ticker": ticker,
                "instruction": f"Buy {ticker} at market open if price is below {max_entry_price:.2f}",
                "entry_price": entry_price,
                "max_entry_price": max_entry_price,
                "stop_price": stop_price,
                "risk_pct": pf["risk_per_trade_pct"] * 100,
                "stop_loss_pct": pf["stop_loss_pct"] * 100,
                "signal_tier": r.get("signal_tier", ""),
                **(position_sizing(capital, pf, entry_price, stop_price) if capital > 0 else {}),
            })

        # ── Pyramid signals for held positions ──
        pyramid_actions = []
        if capital > 0 and pf.get("pyramid_max_adds", 0) > 0:
            pyramid_trigger = pf.get("pyramid_trigger_pct", 0.05)
            for pos in positions:
                if pos["market"] != market:
                    continue
                t = pos["ticker"]
                cp = pos.get("current_price")
                ep = pos.get("entry_price", 0)
                if not cp or ep <= 0:
                    continue
                gain = (cp - ep) / ep
                adds_done = pos.get("pyramid_count", 0)
                max_adds = pf.get("pyramid_max_adds", 2)
                required_gain = pyramid_trigger * (adds_done + 1)
                if gain >= required_gain and adds_done < max_adds:
                    sizing = position_sizing(
                        capital, pf, cp, cp * (1 - pf["stop_loss_pct"]),
                        scale=pf.get("pyramid_size_ratio", 0.50),
                    )
                    pyramid_actions.append({
                        "action": "PYRAMID",
                        "ticker": t,
                        "instruction": f"Add to {t} — up {gain*100:.1f}% from entry (add #{adds_done+1})",
                        "current_price": round(cp, 2),
                        "gain_pct": round(gain * 100, 1),
                        "add_number": adds_done + 1,
                        **sizing,
                    })

        trading_status = compute_trading_status(
            pf, regime_label, positions, market, capital,
        )

        return {
            "market": market,
            "date": rows[0]["date"] if rows else None,
            "count": len(rows),
            "candidates": rows,
            "actions": actions,
            "pyramid_actions": pyramid_actions,
            "open_slots": open_slots,
            "regime": regime_label,
            "trading_status": trading_status,
            "config": {
                "max_positions": pf["max_positions"],
                "stop_loss_pct": pf["stop_loss_pct"] * 100,
                "trailing_stop_pct": pf["trailing_stop_pct"] * 100,
                "risk_per_trade_pct": pf["risk_per_trade_pct"] * 100,
                "max_chase_pct": max_chase * 100,
                "pyramid_enabled": pf.get("pyramid_max_adds", 0) > 0,
                "pyramid_max_adds": pf.get("pyramid_max_adds", 0),
            },
        }
    except Exception as e:
        return {"market": market, "error": str(e), "candidates": [], "actions": []}
    finally:
        con.close()


@router.get("/api/regime/{market}")
async def get_regime(market: str):
    con = get_conn(read_only=True)
    try:
        if not table_exists(con, "regime"):
            return {"market": market, "error": "No regime data"}

        r = con.execute(
            "SELECT date, is_bull, is_recovery, is_weak_market, "
            "regime_strength, breadth_pct, bench_close, bench_ma200, bench_ma50 "
            "FROM regime WHERE market = ? ORDER BY date DESC LIMIT 1",
            [market],
        ).fetchone()

        if not r:
            return {"market": market, "error": "No regime data"}

        breadth_val = r[5]
        # If latest row has no breadth, fetch from most recent row that does
        if breadth_val is None:
            br = con.execute(
                "SELECT breadth_pct FROM regime "
                "WHERE market = ? AND breadth_pct IS NOT NULL "
                "ORDER BY date DESC LIMIT 1",
                [market],
            ).fetchone()
            if br:
                breadth_val = br[0]

        label = "bull" if r[1] else ("recovery" if r[2] else "bear")
        return {
            "market": market,
            "date": str(r[0])[:10],
            "regime": label,
            "is_bull": r[1],
            "is_recovery": r[2],
            "is_weak": r[3],
            "strength": round(r[4], 2) if r[4] is not None else None,
            "breadth_pct": round(breadth_val * 100, 1) if breadth_val is not None else None,
            "benchmark": round(r[6], 2) if r[6] else None,
            "benchmark_ma200": round(r[7], 2) if r[7] else None,
            "benchmark_ma50": round(r[8], 2) if r[8] else None,
        }
    except Exception as e:
        return {"market": market, "error": str(e)}
    finally:
        con.close()


@router.get("/api/backtest/{market}")
async def get_backtest(market: str):
    con = get_conn(read_only=True)
    out: dict = {"market": market}
    try:
        tbl = f"backtest_annual_{market}"
        if table_exists(con, tbl):
            df = con.execute(f"SELECT year, return_pct FROM {tbl} ORDER BY year").df()
            out["annual"] = [
                {"year": int(r["year"]), "return_pct": round(float(r["return_pct"]), 1)}
                for _, r in df.iterrows()
            ]

        tbl = f"backtest_equity_{market}"
        if table_exists(con, tbl):
            df = con.execute(
                f"SELECT date, equity, cash, n_positions, exposure_pct "
                f"FROM {tbl} ORDER BY date DESC LIMIT 60"
            ).df()
            out["equity_recent"] = [
                {
                    "date": str(r["date"]),
                    "equity": round(float(r["equity"]), 0),
                    "cash": round(float(r["cash"]), 0),
                    "n_positions": int(r["n_positions"]),
                    "exposure_pct": round(float(r["exposure_pct"]) * 100, 1),
                }
                for _, r in df.iterrows()
            ]

        tbl = f"backtest_trades_{market}"
        if table_exists(con, tbl):
            df = con.execute(
                f"SELECT * FROM {tbl} ORDER BY exit_date DESC LIMIT 20"
            ).df()
            out["trades_recent"] = [
                {
                    "ticker": r["ticker"],
                    "entry_date": str(r["entry_date"]),
                    "exit_date": str(r["exit_date"]),
                    "entry_price": round(float(r["entry_price"]), 2),
                    "exit_price": round(float(r["exit_price"]), 2),
                    "pnl_pct": round(float(r["pnl_pct"]) * 100, 1),
                    "exit_reason": r["exit_reason"],
                    "hold_days": int(r["hold_days"]),
                }
                for _, r in df.iterrows()
            ]
    except Exception as e:
        out["error"] = str(e)
    finally:
        con.close()
    return out
