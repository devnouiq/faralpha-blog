import { useState, useEffect, useCallback, useRef } from "react";
import type {
  SystemStatus,
  Candidate,
  Position,
  StopAlert,
  RegimeData,
  BacktestData,
  WsEvent,
  BuyAction,
  SignalsResponse,
  TradingStatus,
} from "./types";
import * as api from "./api";
import { useNotifications } from "./hooks/useNotifications";
import Header from "./components/Header";
import SyncPanel from "./components/SyncPanel";
import RegimeCard from "./components/RegimeCard";
import SignalsTable from "./components/SignalsTable";
import StrategyInsight from "./components/StrategyInsight";
import PositionsPanel from "./components/PositionsPanel";
import AlertsFeed from "./components/AlertsFeed";
import BacktestPanel from "./components/BacktestPanel";
import IntradayPanel from "./components/IntradayPanel";

export default function App() {
  const [activeTab, setActiveTab] = useState<"momentum" | "intraday">(
    "momentum",
  );
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [signals, setSignals] = useState<Candidate[]>([]);
  const [signalDate, setSignalDate] = useState<string | null>(null);
  const [signalActions, setSignalActions] = useState<BuyAction[]>([]);
  const [signalMeta, setSignalMeta] = useState<Partial<SignalsResponse>>({});
  const [tradingStatus, setTradingStatus] = useState<TradingStatus | null>(
    null,
  );
  const [regimes, setRegimes] = useState<Record<string, RegimeData>>({});
  const [positions, setPositions] = useState<Position[]>([]);
  const [stopAlerts, setStopAlerts] = useState<StopAlert[]>([]);
  const [backtest, setBacktest] = useState<BacktestData | null>(null);
  const [alerts, setAlerts] = useState<WsEvent[]>([]);
  const market = "india";
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [wsConnected, setWsConnected] = useState(false);
  const [toast, setToast] = useState<{ type: string; message: string } | null>(
    null,
  );
  const [scanProgress, setScanProgress] = useState<{
    step: string;
    message: string;
  } | null>(null);
  const [capital, setCapital] = useState<number>(() => {
    const saved = localStorage.getItem("faralpha_capital");
    return saved ? parseFloat(saved) : 0;
  });
  const wsRef = useRef<WebSocket | null>(null);
  const lastRefreshRef = useRef<number>(0);
  const notifications = useNotifications();
  const notifyRef = useRef(notifications.notify);
  notifyRef.current = notifications.notify;

  // ─── Load everything on mount ─────────────
  const refresh = useCallback(async () => {
    // Debounce: skip if called within 2 seconds of last refresh
    const now = Date.now();
    if (now - lastRefreshRef.current < 2000) return;
    lastRefreshRef.current = now;

    try {
      const [st, indSig, indReg, pos, indBt] = await Promise.all([
        api.fetchStatus(),
        api.fetchSignals("india", capital),
        api.fetchRegime("india"),
        api.fetchPositions(),
        api.fetchBacktest("india"),
      ]);
      setStatus(st);
      setSignals(indSig.candidates);
      setSignalDate(indSig.date);
      setSignalActions(indSig.actions || []);
      setTradingStatus(indSig.trading_status ?? null);
      setSignalMeta({
        open_slots: indSig.open_slots,
        regime: indReg.regime ?? indSig.regime,
        config: indSig.config,
      });
      setRegimes({ india: indReg });
      setPositions(pos.positions);
      setStopAlerts(pos.stop_alerts);
      setBacktest(indBt);
      setError(null);
    } catch (e: any) {
      setError(e.message);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  // Poll status every 30s
  useEffect(() => {
    const id = setInterval(async () => {
      try {
        const st = await api.fetchStatus();
        setStatus(st);
      } catch {}
    }, 30_000);
    return () => clearInterval(id);
  }, []);

  // ─── WebSocket ────────────────────────────
  useEffect(() => {
    const ws = api.connectWs((evt: WsEvent) => {
      if (evt.type === "pong") return;

      // Track pipeline progress
      if (evt.type === "scan_progress") {
        setScanProgress({ step: evt.data?.step, message: evt.data?.message });
        return;
      }
      if (evt.type === "scan_complete") {
        setScanProgress(null);
      }
      if (evt.type === "intraday_sync_complete") {
        setScanProgress(null);
      }

      setAlerts((prev) => [evt, ...prev].slice(0, 50));

      // Toast notifications for important events
      if (evt.type === "buy_signal") {
        const ticker = evt.data?.ticker || "Unknown";
        setToast({
          type: "buy",
          message: `BUY signal: ${ticker} — check Signals table`,
        });
        setTimeout(() => setToast(null), 10000);
      }
      if (evt.type === "sell_signal") {
        const ticker = evt.data?.ticker || "Unknown";
        setToast({
          type: "sell",
          message: `STOP HIT: ${ticker} at ${evt.data?.stop_price?.toFixed(2)} — sell at next open`,
        });
        setTimeout(() => setToast(null), 15000);
      }
      if (evt.type === "scan_complete") {
        const nSig = evt.data?.signals?.length ?? 0;
        const nStop = evt.data?.stop_alerts?.length ?? 0;
        const mkt = evt.data?.market || "";
        if (nSig > 0 || nStop > 0) {
          setToast({
            type: nStop > 0 ? "sell" : "buy",
            message: `Scan done (${mkt}): ${nSig} buy signal${nSig !== 1 ? "s" : ""}${nStop > 0 ? `, ${nStop} stop alert${nStop !== 1 ? "s" : ""}` : ""}`,
          });
          setTimeout(() => setToast(null), 10000);
        }
        refresh();
      }
      if (evt.type === "buy_signal" || evt.type === "sell_signal") {
        refresh();
      }

      // Intraday reversal signal
      if (evt.type === "intraday_signal") {
        const ticker = evt.data?.ticker || "Unknown";
        const hold = evt.data?.max_hold_days || 7;
        const trailPct = evt.data?.trailing_stop_pct || 0.02;
        const trailLabel = `${(trailPct * 100).toFixed(0)}% trail`;
        setToast({
          type: "buy",
          message: `🔥 BUY NOW: ${ticker} @ ₹${evt.data?.price?.toFixed(2)} (RVOL ${evt.data?.rvol?.toFixed(1)}x) → ${trailLabel}, max ${hold}d`,
        });
        setTimeout(() => setToast(null), 15000);
      }

      // Auto-trade order events
      if (evt.type === "order_placed") {
        const o = evt.data;
        setToast({
          type: "buy",
          message: `📋 BUY LIMIT: ${o?.ticker} | Qty ${o?.quantity} @ ₹${o?.max_entry_price?.toFixed(2)} (waiting for fill…)`,
        });
        setTimeout(() => setToast(null), 20000);
      }
      if (evt.type === "buy_filled") {
        const o = evt.data;
        setToast({
          type: "buy",
          message: `✅ FILLED: ${o?.ticker} | ${o?.filled_qty} shares @ ₹${o?.avg_fill_price?.toFixed(2)} | SL ₹${o?.current_stop?.toFixed(2)}`,
        });
        setTimeout(() => setToast(null), 20000);
      }
      if (evt.type === "position_closed") {
        const o = evt.data;
        const emoji = (o?.pnl ?? 0) >= 0 ? "💰" : "🔻";
        setToast({
          type: (o?.pnl ?? 0) >= 0 ? "buy" : "error",
          message: `${emoji} CLOSED: ${o?.ticker} | PnL ₹${o?.pnl?.toFixed(0)} (${o?.pnl_pct?.toFixed(1)}%)`,
        });
        setTimeout(() => setToast(null), 20000);
      }
      if (evt.type === "order_error") {
        const o = evt.data;
        const msg = o?.critical
          ? `🚨 CRITICAL: ${o?.message}`
          : `❌ ORDER ERROR: ${o?.ticker} — ${o?.errors?.[o.errors.length - 1] || o?.error || "unknown"}`;
        setToast({ type: "error", message: msg });
        setTimeout(() => setToast(null), 30000);
      }

      // Skipped signal — max positions reached, manual review needed
      if (evt.type === "signal_skipped") {
        const s = evt.data?.signal;
        const ticker = evt.data?.ticker || s?.ticker || "Unknown";
        setToast({
          type: "sell",
          message: `⚠️ OVERFLOW: ${ticker} @ ₹${s?.price?.toFixed(2)} (RVOL ${s?.rvol?.toFixed(1)}x) — 5 positions full, BUY MANUALLY if desired`,
        });
        setTimeout(() => setToast(null), 60000);
      }

      // Browser push notification (fires even when tab is not focused)
      notifyRef.current(evt);
    });
    ws.onopen = () => setWsConnected(true);
    ws.onclose = () => setWsConnected(false);
    wsRef.current = ws;
    return () => ws.close();
  }, [refresh]); // notifyRef is stable — no need in deps

  // ─── Actions ──────────────────────────────
  const handleSync = async (mkt: string) => {
    setBusy(true);
    setError(null);
    setScanProgress({ step: "sync", message: "Syncing prices…" });
    try {
      await api.triggerSync(mkt, true);
      await refresh();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setBusy(false);
      setScanProgress(null);
    }
  };

  const handlePipeline = async (mkt: string) => {
    setBusy(true);
    setError(null);
    setScanProgress({ step: "pipeline", message: "Running pipeline…" });
    try {
      await api.triggerPipeline(mkt);
      await refresh();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setBusy(false);
      setScanProgress(null);
    }
  };

  const handleScan = async (mkt: string) => {
    setBusy(true);
    setError(null);
    setScanProgress({ step: "sync", message: "Starting scan…" });
    try {
      await api.triggerScan(mkt, true);
      await refresh();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setBusy(false);
      setScanProgress(null);
    }
  };

  const handleUniverse = async () => {
    setBusy(true);
    setError(null);
    setScanProgress({ step: "cleanup", message: "Refreshing universe…" });
    try {
      const res = await api.triggerUniverse("india", true);
      const r = res.result || {};
      const parts: string[] = [];
      if (r.purged) {
        for (const [mkt, info] of Object.entries(r.purged) as [string, any][]) {
          if (info.delisted_removed > 0)
            parts.push(`Purged ${info.delisted_removed} delisted`);
        }
      }
      if (r.india_active) parts.push(`${r.india_active} active`);
      setToast({
        type: "buy",
        message: `Universe refreshed! ${parts.join(" · ")}`,
      });
      setTimeout(() => setToast(null), 10000);
      await refresh();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setBusy(false);
      setScanProgress(null);
    }
  };

  const handleAddPosition = async (pos: {
    ticker: string;
    market: string;
    entry_date: string;
    entry_price: number;
    shares: number;
    notes?: string;
  }) => {
    try {
      await api.addPosition(pos);
      const p = await api.fetchPositions();
      setPositions(p.positions);
      setStopAlerts(p.stop_alerts);
    } catch (e: any) {
      setError(e.message);
    }
  };

  const handleRemovePosition = async (mkt: string, ticker: string) => {
    try {
      await api.removePosition(mkt, ticker);
      const p = await api.fetchPositions();
      setPositions(p.positions);
      setStopAlerts(p.stop_alerts);
    } catch (e: any) {
      setError(e.message);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <Header
        wsConnected={wsConnected}
        busy={busy || (status?.busy ?? false)}
        notificationPermission={notifications.permission}
        onRequestNotifications={notifications.requestPermission}
      />

      {/* Toast notification banner */}
      {toast && (
        <div
          className={`mx-4 mt-2 p-3 rounded-lg text-sm font-semibold flex items-center justify-between animate-pulse ${
            toast.type === "buy"
              ? "bg-emerald-900/70 border border-emerald-700 text-emerald-200"
              : "bg-red-900/70 border border-red-700 text-red-200"
          }`}
        >
          <span>
            {toast.type === "buy" ? "🟢" : "🔴"} {toast.message}
          </span>
          <button
            onClick={() => setToast(null)}
            className="ml-3 opacity-60 hover:opacity-100"
          >
            ✕
          </button>
        </div>
      )}

      {error && (
        <div className="mx-4 mt-2 p-3 bg-red-900/50 border border-red-700 rounded-lg text-red-200 text-sm">
          {error}
          <button
            onClick={() => setError(null)}
            className="ml-3 text-red-400 hover:text-red-200"
          >
            ✕
          </button>
        </div>
      )}

      {/* Capital input */}
      <div className="flex items-center justify-between px-4 pt-4">
        <div className="flex items-center gap-1">
          {/* Tab navigation */}
          <button
            onClick={() => setActiveTab("momentum")}
            className={`px-3 py-1.5 text-sm font-semibold rounded-t-lg transition-colors ${
              activeTab === "momentum"
                ? "bg-slate-800 text-emerald-400 border border-slate-700 border-b-slate-800"
                : "text-slate-500 hover:text-slate-300"
            }`}
          >
            Momentum
          </button>
          <button
            onClick={() => setActiveTab("intraday")}
            className={`px-3 py-1.5 text-sm font-semibold rounded-t-lg transition-colors ${
              activeTab === "intraday"
                ? "bg-slate-800 text-amber-400 border border-slate-700 border-b-slate-800"
                : "text-slate-500 hover:text-slate-300"
            }`}
          >
            Intraday Reversals
          </button>
        </div>
        <div className="flex items-center gap-2">
          <label
            className="text-xs text-slate-500"
            title="Enter your total portfolio value. This is used to calculate position sizes (₹ per trade, number of shares) and risk amounts for each buy signal."
          >
            Capital ₹ <span className="text-slate-600 cursor-help">ⓘ</span>
          </label>
          <input
            type="number"
            value={capital || ""}
            onChange={(e) => {
              const v = parseFloat(e.target.value) || 0;
              setCapital(v);
              localStorage.setItem("faralpha_capital", String(v));
            }}
            placeholder="e.g. 1000000"
            className="w-36 px-2 py-1 text-sm bg-slate-800 border border-slate-700 rounded text-slate-300 placeholder-slate-600 focus:outline-none focus:border-emerald-500"
          />
          {capital > 0 && (
            <span className="text-xs text-slate-600">
              → position sizes shown in signals
            </span>
          )}
        </div>
      </div>

      <div className="px-4 pb-4">
        {activeTab === "momentum" ? (
          <>
            {/* Top row: Sync + Regime */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
              <SyncPanel
                market={market}
                status={status?.markets[market] ?? null}
                busy={busy || (status?.busy ?? false)}
                progressStep={scanProgress?.step ?? null}
                progressMessage={scanProgress?.message ?? null}
                onSync={handleSync}
                onPipeline={handlePipeline}
                onScan={handleScan}
                onUniverse={handleUniverse}
              />
              <RegimeCard india={regimes.india ?? null} />
            </div>

            {/* Strategy insight */}
            <div className="mt-4">
              <StrategyInsight
                market={market}
                regime={regimes[market] ?? null}
                meta={signalMeta}
                candidates={signals}
              />
            </div>

            {/* Signals table */}
            <div className="mt-4">
              <SignalsTable
                market={market}
                candidates={signals}
                date={signalDate}
                actions={signalActions}
                meta={signalMeta}
                stopAlerts={stopAlerts}
                tradingStatus={tradingStatus}
              />
            </div>

            {/* Positions + Alerts row */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
              <PositionsPanel
                positions={positions}
                stopAlerts={stopAlerts}
                market={market}
                onAdd={handleAddPosition}
                onRemove={handleRemovePosition}
              />
              <AlertsFeed alerts={alerts} />
            </div>

            {/* Backtest results */}
            <div className="mt-4">
              <BacktestPanel data={backtest} market={market} />
            </div>
          </>
        ) : null}

        {/* Always mounted so it doesn't re-fetch on tab switch */}
        <div className={`mt-4 ${activeTab === "intraday" ? "" : "hidden"}`}>
          <IntradayPanel />
        </div>
      </div>
    </div>
  );
}
