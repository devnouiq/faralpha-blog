"""
CLI entry point for the faralpha quant trader.

Usage:
    fqt run --market india              # full pipeline
    fqt run --market india --skip-dl   # skip download, run analytics only
    fqt step features --market india   # run a single step
"""
from __future__ import annotations

import argparse
import sys


STEPS = {
    "universe":  "faralpha.pipeline.s01_universe",
    "prices":    "faralpha.pipeline.s02_prices",
    "features":  "faralpha.pipeline.s03_features",
    "rs":        "faralpha.pipeline.s04_rs_rank",
    "patterns":  "faralpha.pipeline.s05_patterns",
    "regime":    "faralpha.pipeline.s06_regime",
    "signals":   "faralpha.pipeline.s07_signals",
    "backtest":  "faralpha.pipeline.s08_backtest",
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="fqt", description="Faralpha Quant Trader")
    sub = parser.add_subparsers(dest="cmd")

    # ---------- run (full pipeline) ----------
    run_p = sub.add_parser("run", help="Run the full pipeline end-to-end")
    run_p.add_argument("--market", default="india", choices=["india"])
    run_p.add_argument("--skip-dl", action="store_true",
                       help="Skip universe + price download (use existing data)")

    # ---------- step (single step) ----------
    step_p = sub.add_parser("step", help="Run a single pipeline step")
    step_p.add_argument("name", choices=list(STEPS.keys()),
                        help="Step to run")
    step_p.add_argument("--market", default="india", choices=["india"])

    # ---------- info ----------
    sub.add_parser("info", help="Show database summary stats")

    # ---------- check ----------
    sub.add_parser("check", help="Run data quality checks")

    args = parser.parse_args(argv)

    if args.cmd == "run":
        from faralpha.pipeline.run_all import run as run_pipeline
        run_pipeline(market=args.market, skip_download=args.skip_dl)

    elif args.cmd == "step":
        import importlib
        mod = importlib.import_module(STEPS[args.name])
        if args.name == "universe":
            mod.run(market=args.market, enrich=False)
        else:
            mod.run(market=args.market)

    elif args.cmd == "info":
        _show_info()

    elif args.cmd == "check":
        from faralpha.pipeline.check_data import run as check_run
        check_run()

    else:
        parser.print_help()
        sys.exit(1)


def _show_info() -> None:
    from faralpha.utils.db import get_conn
    con = get_conn(read_only=True)
    print("\n=== Faralpha Quant Trader — Database Summary ===\n")

    for tbl in ["stocks", "prices", "benchmark", "features", "ranked",
                "signals", "regime", "candidates"]:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  {tbl:20s}  {n:>12,} rows")
        except Exception:
            print(f"  {tbl:20s}  (not created yet)")

    # Backtest tables per market
    for mkt in ("india",):
        for suffix in ("equity", "trades"):
            tbl = f"backtest_{suffix}_{mkt}"
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                print(f"  {tbl:20s}  {n:>12,} rows")
            except Exception:
                pass

    con.close()
    print()


if __name__ == "__main__":
    main()
