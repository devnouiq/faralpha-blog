"""Pipeline runners: sync + features + signals (run in executor threads)."""

from __future__ import annotations

import importlib

from faralpha import config
from faralpha.utils.db import get_conn
from faralpha.utils.logger import get_logger
from faralpha.api.state import broadcast_from_thread, table_exists

log = get_logger("dashboard_api")


def run_pipeline(market: str) -> dict:
    """Run features → signals pipeline (synchronous)."""
    import time as _time
    result: dict = {"steps": [], "errors": []}
    steps = [
        ("features", "faralpha.pipeline.s03_features"),
        ("rs_rank", "faralpha.pipeline.s04_rs_rank"),
        ("patterns", "faralpha.pipeline.s05_patterns"),
        ("regime", "faralpha.pipeline.s06_regime"),
        ("signals", "faralpha.pipeline.s07_signals"),
    ]
    for name, module_path in steps:
        t = _time.perf_counter()
        try:
            mod = importlib.import_module(module_path)
            mod.run(market=market)
            elapsed = _time.perf_counter() - t
            log.info("[%s] Pipeline step '%s' OK  (%.1fs)", market, name, elapsed)
            result["steps"].append({"step": name, "status": "ok", "elapsed_s": round(elapsed, 1)})
        except Exception as e:
            elapsed = _time.perf_counter() - t
            log.error("[%s] Pipeline step '%s' FAILED (%.1fs): %s", market, name, elapsed, e)
            result["steps"].append({"step": name, "status": "error", "error": str(e), "elapsed_s": round(elapsed, 1)})
            result["errors"].append(f"{name}: {e}")
    return result


def run_full_scan(market: str, force: bool = False) -> dict:
    """Sync prices + run pipeline + collect signals (synchronous)."""
    import time as _time
    from faralpha.api.sync_prices import sync_prices_kite

    result: dict = {"sync": None, "pipeline": None, "signals": [], "errors": []}
    t0 = _time.perf_counter()
    log.info("══ Full scan started: market=%s  force=%s ══", market, force)

    # 1 — Sync prices (Kite API, checkpoint-safe)
    broadcast_from_thread("scan_progress", {"step": "sync", "message": "Syncing prices via Kite…"})
    t1 = _time.perf_counter()
    try:
        result["sync"] = sync_prices_kite(market=market, force=force)
        log.info("[%s] Sync complete in %.1fs", market, _time.perf_counter() - t1)
    except Exception as e:
        log.error("[%s] Sync FAILED after %.1fs: %s", market, _time.perf_counter() - t1, e)
        result["errors"].append(f"sync: {e}")

    # 2 — Pipeline
    broadcast_from_thread("scan_progress", {"step": "pipeline", "message": "Running pipeline…"})
    t2 = _time.perf_counter()
    try:
        result["pipeline"] = run_pipeline(market)
        log.info("[%s] Pipeline complete in %.1fs", market, _time.perf_counter() - t2)
    except Exception as e:
        log.error("[%s] Pipeline FAILED after %.1fs: %s", market, _time.perf_counter() - t2, e)
        result["errors"].append(f"pipeline: {e}")

    # 3 — Collect today's signals
    broadcast_from_thread("scan_progress", {"step": "signals", "message": "Collecting signals…"})
    t3 = _time.perf_counter()
    try:
        con = get_conn(read_only=True)
        markets = config.MARKETS if market == "both" else [market]
        for mkt in markets:
            if not table_exists(con, "candidates"):
                log.info("[%s] No candidates table — skipping signal collection", mkt)
                continue
            df = con.execute(
                "SELECT ticker, rs_composite, signal_tier, rank_on_day, sector, close, volume "
                "FROM candidates WHERE market = ? "
                "AND date = (SELECT MAX(date) FROM candidates WHERE market = ?) "
                "ORDER BY rank_on_day",
                [mkt, mkt],
            ).df()
            for _, row in df.iterrows():
                result["signals"].append({
                    "ticker": row["ticker"],
                    "market": mkt,
                    "rs_composite": round(float(row["rs_composite"]), 4) if row["rs_composite"] else 0,
                    "signal_tier": row.get("signal_tier", ""),
                    "rank": int(row["rank_on_day"]) if row["rank_on_day"] else 0,
                    "sector": str(row.get("sector", "")),
                    "close": round(float(row["close"]), 2) if row["close"] else 0,
                    "volume": int(row["volume"]) if row["volume"] else 0,
                })
        log.info("[%s] Signals collected in %.1fs — %d candidates",
                 market, _time.perf_counter() - t3, len(result["signals"]))
        con.close()
    except Exception as e:
        log.error("[%s] Signal collection FAILED: %s", market, e)
        result["errors"].append(f"signals: {e}")

    elapsed = _time.perf_counter() - t0
    log.info("══ Full scan finished: market=%s  %.1fs  signals=%d  errors=%d ══",
             market, elapsed, len(result["signals"]), len(result["errors"]))
    return result
