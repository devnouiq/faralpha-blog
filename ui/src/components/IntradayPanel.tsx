import { useState, useEffect, useRef, useCallback } from 'react'
import { Activity, Radio, Eye, Zap, TrendingDown, TrendingUp, Clock, Shield, AlertTriangle, CheckCircle, RefreshCw, DollarSign } from 'lucide-react'
import * as api from '../api'
import type { IntradayStatus, IntradayWatchlistItem, IntradayRegimeGuide, IntradaySignal } from '../types'

/** Zerodha CNC (delivery) charges for a round-trip trade. */
function calcCharges(buyValue: number, sellValue: number): { total: number; breakdown: { stt: number; exchg: number; sebi: number; gst: number; stamp: number; dp: number } } {
  const turnover = buyValue + sellValue
  const stt = turnover * 0.001        // 0.1% on buy + sell
  const exchg = turnover * 0.0000297   // NSE 0.00297%
  const sebi = turnover * 0.000001     // ₹10 per crore
  const gst = (exchg + sebi) * 0.18    // 18% GST on exchange + SEBI
  const stamp = buyValue * 0.00015     // 0.015% on buy
  const dp = sellValue > 0 ? 15.93 : 0 // DP charge per sell scrip
  const total = stt + exchg + sebi + gst + stamp + dp
  return { total, breakdown: { stt, exchg, sebi, gst, stamp, dp } }
}

function SignalCard({ sig, posSizePct, autoTrade, autoTradeOrders, tickers, compact, onPlaceOrder, onError }: {
  sig: IntradaySignal; posSizePct: number; autoTrade: boolean; autoTradeOrders: any[]; tickers: any[]; compact?: boolean;
  onPlaceOrder?: (ticker: string, price: number, trailPct: number, holdDays: number) => void;
  onError?: (msg: string) => void;
}) {
  const hold = sig.max_hold_days || 7
  const trailPct = sig.trailing_stop_pct || 0.02
  const buyDate = sig.time?.slice(0, 10)
  const sellDate = buyDate ? new Date(new Date(buyDate).getTime() + hold * 86400000).toISOString().slice(0, 10) : ''
  const entryPrice = sig.price
  const maxEntryPrice = entryPrice * 1.005
  const initialStop = entryPrice * (1 - trailPct)
  const riskPerShare = entryPrice - initialStop
  const capital = parseFloat(localStorage.getItem('faralpha_capital') || '0')
  const positionValue = capital > 0 ? capital * posSizePct : 0
  const qty = positionValue > 0 ? Math.floor(positionValue / entryPrice) : 0
  const investAmt = qty * entryPrice
  const riskAmt = qty * riskPerShare
  const dayChg = sig.day_change_pct ?? 0
  const prevClose = sig.prev_close ?? 0
  const isChasing = dayChg > 5
  const tickerClean = sig.ticker?.replace('.NS', '') || ''
  const matchedOrder = autoTradeOrders.find((o: any) => o.ticker === tickerClean)
  const liveTicker = tickers.find((t: any) => t.ticker === sig.ticker)
  const livePrice = liveTicker?.price ?? 0
  const priceAboveMax = livePrice > 0 && livePrice > maxEntryPrice

  return (
    <div className={`rounded-lg border p-3 text-xs space-y-2 ${priceAboveMax ? 'bg-slate-900/40 border-slate-800/40 opacity-50' : isChasing ? 'bg-amber-950/30 border-amber-700/50' : 'bg-slate-800/60 border-slate-700/50'}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="px-1.5 py-0.5 rounded bg-emerald-900/60 text-emerald-300 font-bold text-[11px]">BUY</span>
          <span className="text-slate-100 font-mono font-semibold text-sm">{tickerClean}</span>
          {dayChg !== 0 && (
            <span className={`px-1.5 py-0.5 rounded font-bold text-[11px] ${
              dayChg > 5 ? 'bg-amber-900/60 text-amber-300' :
              dayChg > 0 ? 'bg-emerald-900/40 text-emerald-400' :
              'bg-red-900/40 text-red-400'
            }`}>
              {dayChg > 0 ? '+' : ''}{dayChg.toFixed(1)}% today
            </span>
          )}
          {isChasing && (
            <span className="px-1.5 py-0.5 rounded bg-amber-800/60 text-amber-200 font-semibold text-[10px] animate-pulse">⚠ LATE ENTRY</span>
          )}
          {matchedOrder ? (
            <span className={`px-1.5 py-0.5 rounded font-semibold text-[10px] ${
              matchedOrder.status === 'protected' ? 'bg-emerald-900/60 text-emerald-300' :
              matchedOrder.status === 'bought' ? 'bg-blue-900/60 text-blue-300' :
              matchedOrder.status === 'buy_placed' ? 'bg-amber-900/60 text-amber-300' :
              matchedOrder.status === 'closed' ? 'bg-slate-700/60 text-slate-300' :
              matchedOrder.status === 'UNPROTECTED' ? 'bg-red-600/80 text-white animate-pulse' :
              'bg-slate-800 text-slate-400'
            }`}>
              {matchedOrder.status === 'buy_placed' ? '⏳ LIMIT WAITING' :
               matchedOrder.status === 'bought' ? '✓ FILLED' :
               matchedOrder.status === 'protected' ? '🛡 PROTECTED' :
               matchedOrder.status === 'closed' ? '✓ CLOSED' :
               matchedOrder.status === 'UNPROTECTED' ? '🚨 UNPROTECTED' :
               matchedOrder.status?.toUpperCase()}
              {matchedOrder.avg_fill_price > 0 && ` @ ₹${matchedOrder.avg_fill_price.toFixed(2)}`}
            </span>
          ) : (
            <span className="px-1.5 py-0.5 rounded text-[10px] bg-slate-800/80 text-slate-500">
              {!autoTrade ? '⚡ Auto-Trade OFF' : '—'}
            </span>
          )}
          {!matchedOrder && onPlaceOrder && (
            <button
              onClick={async () => {
                if (!confirm(`Place LIMIT BUY for ${tickerClean}?\n\nSignal: ₹${entryPrice.toFixed(2)}\nLimit: ₹${maxEntryPrice.toFixed(2)} (max)\n${livePrice > 0 ? `CMP: ₹${livePrice.toFixed(2)} — fills at CMP` : ''}`)) return
                try {
                  const res = await api.placeManualOrder(tickerClean, entryPrice, trailPct, hold)
                  if (res.status === 'ok') {
                    onPlaceOrder(tickerClean, entryPrice, trailPct, hold)
                  } else {
                    onError?.(res.message)
                  }
                } catch (e: any) { onError?.(e.message) }
              }}
              className="px-2 py-0.5 rounded text-[10px] font-semibold bg-emerald-900/60 text-emerald-300 border border-emerald-700 hover:bg-emerald-800/60"
            >
              Place Order
            </button>
          )}
        </div>
        <div className="flex items-center gap-3 text-slate-400">
          {livePrice > 0 && (
            <span className={`font-mono font-semibold ${livePrice <= maxEntryPrice ? 'text-emerald-400' : 'text-red-400'}`}>
              CMP ₹{livePrice.toLocaleString()}{livePrice <= maxEntryPrice ? ' ✓' : ' ✗'}
            </span>
          )}
          <span>RVOL {sig.rvol?.toFixed(1)}x</span>
          <span>{sig.down_days}d↓</span>
          {sig.depth_pct != null && <span>{sig.depth_pct.toFixed(0)}%</span>}
          <span className="text-slate-600">{sig.time?.slice(5, 16)}</span>
        </div>
      </div>

      {!compact && (
        <>
          <div className={`grid gap-2 ${prevClose > 0 ? 'grid-cols-6' : 'grid-cols-5'}`}>
            {prevClose > 0 && (
              <div className="bg-slate-700/40 border border-slate-600/30 rounded p-2">
                <div className="text-[10px] text-slate-500 uppercase font-semibold">Prev Close</div>
                <div className="text-slate-300 font-mono font-bold text-sm">₹{prevClose.toLocaleString(undefined, { maximumFractionDigits: 2 })}</div>
                <div className={`text-[10px] font-semibold ${dayChg > 5 ? 'text-amber-400' : dayChg > 0 ? 'text-emerald-500' : 'text-red-500'}`}>{dayChg > 0 ? '+' : ''}{dayChg.toFixed(1)}% intraday</div>
              </div>
            )}
            <div className="bg-emerald-950/40 border border-emerald-800/30 rounded p-2">
              <div className="text-[10px] text-emerald-500 uppercase font-semibold">Signal Price</div>
              <div className="text-emerald-300 font-mono font-bold text-sm">₹{entryPrice.toLocaleString(undefined, { maximumFractionDigits: 2 })}</div>
              <div className="text-[10px] text-emerald-600">buy at or below</div>
            </div>
            <div className="bg-amber-950/40 border border-amber-800/30 rounded p-2">
              <div className="text-[10px] text-amber-500 uppercase font-semibold">Max Entry</div>
              <div className="text-amber-300 font-mono font-bold text-sm">₹{maxEntryPrice.toLocaleString(undefined, { maximumFractionDigits: 2 })}</div>
              <div className="text-[10px] text-amber-600">+0.5% max late</div>
            </div>
            <div className="bg-red-950/40 border border-red-800/30 rounded p-2">
              <div className="text-[10px] text-red-500 uppercase font-semibold">Initial Stop</div>
              <div className="text-red-300 font-mono font-bold text-sm">₹{initialStop.toFixed(2)}</div>
              <div className="text-[10px] text-red-600">{(trailPct * 100).toFixed(0)}% trail from peak</div>
            </div>
            <div className="bg-blue-950/40 border border-blue-800/30 rounded p-2">
              <div className="text-[10px] text-blue-500 uppercase font-semibold">Exit By</div>
              <div className="text-blue-300 font-mono font-bold text-sm">{sellDate.slice(5)}</div>
              <div className="text-[10px] text-blue-600">Max {hold}d hold</div>
            </div>
            {capital > 0 ? (
              <div className="bg-violet-950/40 border border-violet-800/30 rounded p-2">
                <div className="text-[10px] text-violet-500 uppercase font-semibold">Quantity</div>
                <div className="text-violet-300 font-mono font-bold text-sm">{qty.toLocaleString()} shares</div>
                <div className="text-[10px] text-violet-600">₹{(investAmt / 1000).toFixed(0)}K ({(posSizePct * 100).toFixed(0)}%)</div>
              </div>
            ) : (
              <div className="bg-slate-800/60 border border-slate-700/30 rounded p-2">
                <div className="text-[10px] text-slate-500 uppercase font-semibold">Quantity</div>
                <div className="text-slate-500 text-[11px] mt-1">Set capital ↑</div>
              </div>
            )}
          </div>
          {capital > 0 && (
            <div className="flex items-center gap-4 text-[10px] text-slate-500 pl-1">
              <span>Risk: <span className="text-red-400 font-semibold">₹{riskAmt.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span> ({(riskAmt / capital * 100).toFixed(1)}% of capital)</span>
              <span>R:R = <span className="text-emerald-400 font-semibold">1:{(1/trailPct).toFixed(0)}</span> if +{(trailPct * 100 * 2).toFixed(0)}% move</span>
              <span className="text-slate-600">{buyDate}</span>
            </div>
          )}
        </>
      )}
    </div>
  )
}

export default function IntradayPanel() {
  const [status, setStatus] = useState<IntradayStatus | null>(null)
  const [watchlist, setWatchlist] = useState<IntradayWatchlistItem[]>([])
  const [config, setConfig] = useState<any>(null)
  const [regimeGuide, setRegimeGuide] = useState<IntradayRegimeGuide | null>(null)
  const [signalHistory, setSignalHistory] = useState<IntradaySignal[]>([])
  const [loading, setLoading] = useState(true)
  const [syncing, setSyncing] = useState(false)
  const [syncMsg, setSyncMsg] = useState<string | null>(null)
  const [autoTrade, setAutoTrade] = useState(false)
  const [autoTradeOrders, setAutoTradeOrders] = useState<any[]>([])
  const [pendingQueue, setPendingQueue] = useState<any[]>([])
  const [error, setError] = useState<string | null>(null)
  const [logbook, setLogbook] = useState<any[]>([])
  const [logSummary, setLogSummary] = useState<any>(null)
  const [logPage, setLogPage] = useState(0)
  const [logFilter, setLogFilter] = useState<string>('all')
  const LOG_PAGE_SIZE = 15

  const isClosed = (s: string) => s === 'closed' || s === 'exit_sl_breached'
  const filteredLogbook = logFilter === 'all' ? logbook : logbook.filter((t: any) => {
    const isOpen = ['bought', 'protected', 'UNPROTECTED', 'buy_placed'].includes(t.status)
    switch (logFilter) {
      case 'open': return isOpen
      case 'win': return isClosed(t.status) && (t.pnl || 0) > 0
      case 'loss': return isClosed(t.status) && (t.pnl || 0) < 0
      case 'closed': return isClosed(t.status)
      default: return true
    }
  })
  const [wlPage, setWlPage] = useState(0)
  const [monPage, setMonPage] = useState(0)
  const WL_PAGE_SIZE = 20
  const MON_PAGE_SIZE = 20
  const isLiveRef = useRef(false)
  const didInit = useRef(false)

  // Fetch everything once on mount. No polling unless ticker is live.
  useEffect(() => {
    if (didInit.current) return
    didInit.current = true
    ;(async () => {
      try {
        const [st, cfg, rg, wl, sh, os, lb] = await Promise.all([
          api.fetchIntradayStatus(),
          api.fetchIntradayConfig(),
          api.fetchRegimeGuide().catch(() => null),
          api.fetchIntradayWatchlist().catch(() => ({ watchlist: [] })),
          api.fetchSignalHistory().catch(() => ({ signals: [], count: 0 })),
          api.fetchOrdersStatus().catch(() => ({ auto_trade: false, today_orders: 0, orders: [], pending_queue: [] })),
          api.fetchLogbook().catch(() => ({ trades: [], summary: null })),
        ])
        setStatus(st)
        setConfig(cfg)
        setRegimeGuide(rg)
        setWatchlist(wl.watchlist || [])
        setSignalHistory(sh.signals || [])
        setAutoTrade(os.auto_trade)
        setAutoTradeOrders(os.orders || [])
        setPendingQueue(os.pending_queue || [])
        setLogbook(lb.trades || [])
        setLogSummary(lb.summary || null)
        isLiveRef.current = !!st.live_ticker_active
      } catch (e: any) {
        setError(e.message)
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  // WebSocket: receive real-time updates instead of polling
  useEffect(() => {
    const ws = api.connectWs((evt: any) => {
      if (evt.type === 'intraday_status') {
        // Full status snapshot pushed every ~2s from live ticker
        const st = evt.data
        setStatus((prev: any) => prev ? { ...prev, live: st, live_ticker_active: true } : { live: st, live_ticker_active: true })
        setSignalHistory(st?.signal_history || [])
        isLiveRef.current = true
      }
      if (evt.type === 'intraday_signal') {
        // New signal fired — append to history
        setSignalHistory(prev => {
          const sig = evt.data
          if (!sig?.ticker) return prev
          // Dedupe by ticker+day
          const day = (sig.time || '').slice(0, 10)
          const exists = prev.some((s: any) => s.ticker === sig.ticker && (s.time || '').slice(0, 10) === day)
          return exists ? prev : [sig, ...prev]
        })
      }
      if (evt.type === 'orders_update') {
        // Full orders snapshot after any order event
        setAutoTrade(evt.data?.auto_trade ?? false)
        setAutoTradeOrders(evt.data?.orders || [])
        setPendingQueue(evt.data?.pending_queue || [])
      }
      if (evt.type === 'signal_skipped') {
        // New overflow signal — update pending queue
        setPendingQueue(prev => {
          const sig = evt.data?.signal
          if (!sig?.ticker) return prev
          const ticker = sig.ticker.replace('.NS', '')
          const filtered = prev.filter((p: any) =>
            p.ticker?.replace('.NS', '') !== ticker
          )
          return [...filtered, sig]
        })
      }
      if (['order_placed', 'buy_filled', 'position_closed', 'sl_updated', 'order_error'].includes(evt.type)) {
        // Individual order event — refetch orders once as fallback
        api.fetchOrdersStatus().then(os => {
          setAutoTrade(os.auto_trade)
          setAutoTradeOrders(os.orders || [])
          setPendingQueue(os.pending_queue || [])
        }).catch(() => {})
        // Refresh logbook on trade changes
        api.fetchLogbook().then(lb => {
          setLogbook(lb.trades || [])
          setLogSummary(lb.summary || null)
        }).catch(() => {})
      }
    })
    return () => ws.close()
  }, [])

  // Refresh static data (after sync or manual refresh)
  const refreshStatic = useCallback(async () => {
    const [cfg, rg, wl] = await Promise.all([
      api.fetchIntradayConfig(),
      api.fetchRegimeGuide().catch(() => null),
      api.fetchIntradayWatchlist().catch(() => ({ watchlist: [] })),
    ])
    setConfig(cfg)
    setRegimeGuide(rg)
    setWatchlist(wl.watchlist || [])
  }, [])

  // Refresh live state (after start/stop ticker)
  const refreshLive = useCallback(async () => {
    const [st, sh] = await Promise.all([
      api.fetchIntradayStatus(),
      api.fetchSignalHistory().catch(() => ({ signals: [], count: 0 })),
    ])
    setStatus(st)
    setSignalHistory(sh.signals || [])
    isLiveRef.current = !!st.live_ticker_active
  }, [])

  const handleSync = async () => {
    setSyncing(true)
    setSyncMsg('Syncing daily prices…')
    setError(null)
    try {
      const res = await api.syncIntraday()
      if (res.status === 'ok') {
        setSyncMsg(`Done — ${res.watchlist_count} watchlist stocks`)
        await refreshStatic()
      } else {
        setError(res.error || 'Sync failed')
        setSyncMsg(null)
      }
    } catch (e: any) {
      setError(e.message)
      setSyncMsg(null)
    } finally {
      setSyncing(false)
      setTimeout(() => setSyncMsg(null), 8000)
    }
  }

  if (loading) {
    return (
      <div className="bg-slate-900 rounded-xl border border-slate-800 p-6 text-center text-slate-500">
        Loading intraday strategy…
      </div>
    )
  }

  const vwapCfg = config?.vwap_reclaim || {}
  const maxPos = config?.max_positions || 5
  const posSizePct = config?.position_size_pct || 0.20
  const live = status?.live
  const isLive = status?.live_ticker_active
  const marketClosed = live?.market_closed
  const tickers = live?.tickers || []

  return (
    <div className="space-y-4">
      {error && (
        <div className="p-3 bg-red-900/50 border border-red-700 rounded-lg text-red-200 text-sm">
          {error}
          <button onClick={() => setError(null)} className="ml-3 text-red-400 hover:text-red-200">✕</button>
        </div>
      )}

      {/* Sync + Regime */}
      <div className="flex items-center justify-between">
        <div className="flex-1">
          <RegimeGuideCard guide={regimeGuide} />
        </div>
        <div className="ml-4 flex flex-col items-center gap-2">
          <button
            onClick={handleSync}
            disabled={syncing}
            className="px-4 py-3 rounded-lg bg-blue-900/50 text-blue-300 border border-blue-800 hover:bg-blue-800/50 disabled:opacity-50 transition-colors flex items-center gap-2 text-sm font-semibold whitespace-nowrap"
          >
            <RefreshCw className={`w-4 h-4 ${syncing ? 'animate-spin' : ''}`} />
            {syncing ? 'Syncing…' : 'Full Sync'}
          </button>
          <div className="text-[10px] text-slate-500 text-center leading-tight w-24">
            Prices + pipeline + 15m/30m candles
          </div>
          {syncMsg && (
            <div className="text-[10px] text-emerald-400 text-center leading-tight w-32">{syncMsg}</div>
          )}
        </div>
      </div>

      {/* Live Ticker Status Bar */}
      <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <Radio className={`w-4 h-4 ${isLive && !marketClosed ? 'text-emerald-400 animate-pulse' : marketClosed ? 'text-slate-500' : 'text-slate-600'}`} />
            <span className="text-sm font-semibold text-slate-400 uppercase tracking-wider">Live Ticker</span>
          </div>
          <div className={`px-2 py-1 rounded text-xs font-semibold ${
            marketClosed
              ? 'bg-slate-800 text-slate-400 border border-slate-700'
              : isLive
                ? 'bg-emerald-900/50 text-emerald-300 border border-emerald-800'
                : 'bg-slate-800 text-slate-500 border border-slate-700'
          }`}>
            {marketClosed ? 'CLOSED' : isLive ? 'LIVE' : 'OFFLINE'}
          </div>
          {live && (
            <>
              {marketClosed && (
                <span className="text-xs text-slate-500">Market closed — {live.signals_fired} signal{live.signals_fired !== 1 ? 's' : ''} fired today</span>
              )}
              {!marketClosed && (
                <>
                  <span className="text-xs text-slate-500">Watching <span className="text-slate-200 font-semibold">{live.watchlist_size}</span></span>
                  <span className="text-xs">{live.first_hour_done
                    ? <span className="text-emerald-400">1st Hour ✓</span>
                    : <span className="text-amber-400">Accumulating…</span>}
                  </span>
                  <span className="text-xs text-slate-500">Signals <span className="text-amber-400 font-semibold">{live.signals_fired}</span></span>
                  <span className="text-xs text-slate-500">Active <span className="text-slate-200 font-semibold">{live.tokens_with_volume}</span></span>
                </>
              )}
            </>
          )}
          {!isLive && (
            <span className="text-xs text-slate-600">
              Auto-starts on server boot during market hours. If offline, check Kite credentials.
            </span>
          )}
          <div className="flex items-center gap-1.5 ml-auto">
            <button
              onClick={async () => {
                try {
                  const res = autoTrade
                    ? await api.disableAutoTrade()
                    : await api.enableAutoTrade()
                  setAutoTrade(res.auto_trade)
                } catch (e: any) { setError(e.message) }
              }}
              className={`px-2.5 py-1 rounded text-xs font-semibold transition-colors flex items-center gap-1.5 ${
                autoTrade
                  ? 'bg-amber-900/50 text-amber-300 border border-amber-800 hover:bg-amber-800/50'
                  : 'bg-slate-800 text-slate-500 border border-slate-700 hover:bg-slate-700/50'
              }`}
            >
              <DollarSign className="w-3 h-3" />
              {autoTrade ? 'Auto-Trade ON' : 'Auto-Trade'}
            </button>
            {autoTradeOrders.length > 0 && (
              <span className="text-[10px] text-amber-400 font-semibold">{autoTradeOrders.length} orders</span>
            )}
          </div>
        </div>
      </div>

      {/* Today's Signals */}
      {(() => {
        const today = new Date().toISOString().slice(0, 10)
        const todaySignals = signalHistory.filter(s => s.time?.slice(0, 10) === today)
        const pastSignals = signalHistory.filter(s => s.time?.slice(0, 10) !== today)
        return (<>
      <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
          <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2 mb-3">
            <Zap className="w-4 h-4 text-amber-400" />
            Today's Signals
            {todaySignals.length > 0 && <span className="text-amber-400 font-mono">{todaySignals.length}</span>}
          </h3>
          {todaySignals.length === 0 ? (
            <div className="text-center py-6 text-slate-600 text-sm">
              No signals fired yet today. {!isLive ? 'Ticker is offline.' : 'Watching for VWAP reclaims…'}
            </div>
          ) : (
            <div className="space-y-2">
              {todaySignals.map((sig, i) => (
                <SignalCard key={`today-${i}`} sig={sig} posSizePct={posSizePct} autoTrade={autoTrade} autoTradeOrders={autoTradeOrders} tickers={tickers}
                  onPlaceOrder={() => { api.fetchOrdersStatus().then(r => { setAutoTradeOrders(r.orders || []); setPendingQueue(r.pending_queue || []) }).catch(() => {}) }}
                  onError={setError}
                />
              ))}
            </div>
          )}
        </div>

      {/* Past Signals */}
      {pastSignals.length > 0 && (
        <details className="group">
          <summary className="bg-slate-900 rounded-xl border border-slate-800 p-4 cursor-pointer list-none flex items-center gap-2 hover:bg-slate-800/50 transition-colors">
            <Clock className="w-4 h-4 text-slate-500" />
            <span className="text-sm font-semibold text-slate-500 uppercase tracking-wider">Past Signals</span>
            <span className="text-xs text-slate-600 font-mono">{pastSignals.length}</span>
            <span className="ml-auto text-slate-600 text-xs group-open:rotate-180 transition-transform">▼</span>
          </summary>
          <div className="bg-slate-900 rounded-b-xl border border-t-0 border-slate-800 p-4 space-y-2 max-h-[28rem] overflow-y-auto">
            {pastSignals.slice(0, 50).map((sig, i) => (
              <SignalCard key={`past-${i}`} sig={sig} posSizePct={posSizePct} autoTrade={autoTrade} autoTradeOrders={autoTradeOrders} tickers={tickers} compact
                  onPlaceOrder={() => { api.fetchOrdersStatus().then(r => { setAutoTradeOrders(r.orders || []); setPendingQueue(r.pending_queue || []) }).catch(() => {}) }}
                  onError={setError}
                />
            ))}
          </div>
        </details>
      )}
        </>)
      })()}

      {/* Pending Queue — overflow/failed signals awaiting approval */}
      {pendingQueue.length > 0 && (
        <div className="bg-slate-900 rounded-xl border border-amber-600/70 p-4">
          <h3 className="text-sm font-semibold text-amber-400 uppercase tracking-wider flex items-center gap-2 mb-3">
            <AlertTriangle className="w-4 h-4" />
            Pending Approval — {pendingQueue.length} Signal{pendingQueue.length > 1 ? 's' : ''}
          </h3>
          <div className="space-y-2">
            {pendingQueue.map((sig: any, i: number) => {
              const ticker = (sig.ticker || '').replace('.NS', '')
              const reason = sig._skip_reason || 'overflow'
              return (
                <div key={`${ticker}-${i}`} className="flex items-center justify-between bg-amber-950/30 border border-amber-800/40 rounded-lg px-3 py-2">
                  <div className="flex items-center gap-4">
                    <span className="font-mono font-bold text-amber-200">{ticker}</span>
                    <span className="text-emerald-400 font-mono text-sm">₹{sig.price?.toFixed(2)}</span>
                    <span className="text-slate-400 text-xs">RVOL {sig.rvol?.toFixed(1)}x</span>
                    <span className="text-slate-400 text-xs">{sig.down_days}d↓</span>
                    <span className="text-slate-500 text-xs">{sig.time?.slice(11, 16)}</span>
                    <span className="px-1.5 py-0.5 rounded text-[10px] bg-amber-900/60 text-amber-300">{reason === 'max_positions' ? 'POSITIONS FULL' : 'FAILED'}</span>
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={async () => {
                        try {
                          const res = await api.approvePending(ticker)
                          if (res.status === 'ok') {
                            setPendingQueue(prev => prev.filter((p: any) => p.ticker?.replace('.NS', '') !== ticker))
                            setError(null)
                          } else {
                            setError(res.message)
                          }
                        } catch (e: any) { setError(e.message) }
                      }}
                      className="px-3 py-1 rounded text-xs font-semibold bg-emerald-900/60 text-emerald-300 border border-emerald-700 hover:bg-emerald-800/60"
                    >
                      ✓ Approve
                    </button>
                    <button
                      onClick={async () => {
                        await api.dismissPending(ticker).catch(() => {})
                        setPendingQueue(prev => prev.filter((p: any) => p.ticker?.replace('.NS', '') !== ticker))
                      }}
                      className="px-3 py-1 rounded text-xs font-semibold bg-slate-800/60 text-slate-400 border border-slate-700 hover:bg-slate-700/60"
                    >
                      ✗ Dismiss
                    </button>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Auto-Trade Orders */}
      <div className="bg-slate-900 rounded-xl border border-amber-900/50 p-4">
        <h3 className="text-sm font-semibold text-amber-400 uppercase tracking-wider flex items-center gap-2 mb-3">
          <DollarSign className="w-4 h-4" />
          Orders{autoTradeOrders.length > 0 ? ` — ${autoTradeOrders.length}` : ''}
        </h3>
        {autoTradeOrders.length === 0 ? (
          <div className="text-center py-4 text-slate-600 text-sm">
            No orders placed today.{!autoTrade && ' Enable Auto-Trade above to place orders on signals.'}
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-500 border-b border-slate-800">
                  <th className="text-left py-1 pr-3">Ticker</th>
                  <th className="text-right py-1 px-2">Signal ₹</th>
                  <th className="text-right py-1 px-2">Limit ₹</th>
                  <th className="text-right py-1 px-2">SL ₹</th>
                  <th className="text-right py-1 px-2">Qty</th>
                  <th className="text-right py-1 px-2">Invest</th>
                  <th className="text-right py-1 px-2">Risk</th>
                  <th className="text-center py-1 px-2">Status</th>
                  <th className="text-right py-1 px-2">Time</th>
                  <th className="text-center py-1 px-2">Actions</th>
                </tr>
              </thead>
              <tbody>
                {autoTradeOrders.map((o: any, i: number) => (
                  <tr key={i} className="border-b border-slate-800/50 hover:bg-slate-800/30">
                    <td className="py-1.5 pr-3 font-mono text-slate-200 font-semibold">{o.ticker}</td>
                    <td className="py-1.5 px-2 text-right font-mono text-emerald-400">₹{o.signal_price?.toFixed(2)}</td>
                    <td className="py-1.5 px-2 text-right font-mono text-amber-300">₹{o.max_entry_price?.toFixed(2)}</td>
                    <td className="py-1.5 px-2 text-right font-mono text-red-400">₹{(o.current_stop || o.initial_stop)?.toFixed(2)}</td>
                    <td className="py-1.5 px-2 text-right font-mono text-slate-300">{o.quantity}</td>
                    <td className="py-1.5 px-2 text-right text-slate-400">₹{(o.invest_amount / 1000)?.toFixed(0)}K</td>
                    <td className="py-1.5 px-2 text-right text-red-400">₹{o.risk_amount?.toFixed(0)}</td>
                    <td className="py-1.5 px-2 text-center">
                      <span className={`px-1.5 py-0.5 rounded font-semibold text-[10px] ${
                        o.status === 'protected' ? 'bg-emerald-900/60 text-emerald-300' :
                        o.status === 'bought' ? 'bg-blue-900/60 text-blue-300' :
                        o.status === 'buy_placed' ? 'bg-amber-900/60 text-amber-300' :
                        o.status === 'closed' ? 'bg-slate-700/60 text-slate-300' :
                        o.status === 'UNPROTECTED' ? 'bg-red-600/80 text-white animate-pulse' :
                        o.status === 'error' ? 'bg-red-900/60 text-red-300' :
                        o.status === 'cancelled' ? 'bg-slate-800 text-slate-500' :
                        'bg-slate-800 text-slate-400'
                      }`}>
                        {o.status?.toUpperCase()}
                      </span>
                    </td>
                    <td className="py-1.5 px-2 text-right text-slate-500">{o.time?.slice(11, 16)}</td>
                    <td className="py-1.5 px-2 text-center whitespace-nowrap">
                      {(o.status === 'UNPROTECTED' || o.status === 'bought' || o.status === 'protected') && (
                        <button
                          onClick={async () => {
                            if (!confirm(`EXIT ${o.ticker}? This will sell ${o.filled_qty || o.quantity} shares at market.`)) return
                            try {
                              const res = await api.forceExitOrder(o.ticker)
                              if (res.status === 'ok') {
                                setError(null)
                              } else {
                                setError(res.message)
                              }
                            } catch (e: any) { setError(e.message) }
                          }}
                          className="px-2 py-0.5 rounded text-[10px] font-semibold bg-red-900/60 text-red-300 border border-red-800 hover:bg-red-800/60 mr-1"
                        >
                          Exit
                        </button>
                      )}
                      {o.status === 'UNPROTECTED' && (
                        <button
                          onClick={async () => {
                            try {
                              const res = await api.retrySl(o.ticker)
                              if (res.status === 'ok') {
                                setError(null)
                              } else {
                                setError(res.message)
                              }
                            } catch (e: any) { setError(e.message) }
                          }}
                          className="px-2 py-0.5 rounded text-[10px] font-semibold bg-amber-900/60 text-amber-300 border border-amber-800 hover:bg-amber-800/60"
                        >
                          Retry SL
                        </button>
                      )}
                      {o.status === 'error' && (
                        <button
                          onClick={async () => {
                            try {
                              const res = await api.retryFailedOrder(o.ticker)
                              if (res.status === 'ok') {
                                setError(null)
                              } else {
                                setError(res.message)
                              }
                            } catch (e: any) { setError(e.message) }
                          }}
                          className="px-2 py-0.5 rounded text-[10px] font-semibold bg-amber-900/60 text-amber-300 border border-amber-800 hover:bg-amber-800/60"
                        >
                          Retry
                        </button>
                      )}
                      {o.status === 'buy_placed' && (
                        <button
                          onClick={async () => {
                            if (!confirm(`Cancel BUY for ${o.ticker}?`)) return
                            try {
                              const res = await api.forceExitOrder(o.ticker)
                              if (res.status !== 'ok') setError(res.message)
                            } catch (e: any) { setError(e.message) }
                          }}
                          className="px-2 py-0.5 rounded text-[10px] font-semibold bg-slate-700/60 text-slate-300 border border-slate-600 hover:bg-slate-600/60"
                        >
                          Cancel
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        </div>

      {/* Trade Logbook */}
      {logbook.length > 0 && (
        <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
              <Clock className="w-4 h-4 text-cyan-400" />
              Trade Logbook — {logbook.length} Trades
            </h3>
            <div className="flex gap-1">
              {[
                { key: 'all', label: 'All', count: logbook.length },
                { key: 'open', label: 'Open', count: logbook.filter((t: any) => ['bought', 'protected', 'UNPROTECTED', 'buy_placed'].includes(t.status)).length },
                { key: 'closed', label: 'Closed', count: logbook.filter((t: any) => t.status === 'closed' || t.status === 'exit_sl_breached').length },
                { key: 'win', label: 'Win', count: logbook.filter((t: any) => (t.status === 'closed' || t.status === 'exit_sl_breached') && (t.pnl || 0) > 0).length },
                { key: 'loss', label: 'Loss', count: logbook.filter((t: any) => (t.status === 'closed' || t.status === 'exit_sl_breached') && (t.pnl || 0) < 0).length },
              ].filter(f => f.key === 'all' || f.count > 0).map(f => (
                <button
                  key={f.key}
                  onClick={() => { setLogFilter(f.key); setLogPage(0) }}
                  className={`px-2 py-0.5 rounded text-[10px] font-semibold border transition-colors ${
                    logFilter === f.key
                      ? 'bg-cyan-900/60 text-cyan-300 border-cyan-700'
                      : 'bg-slate-800 text-slate-400 border-slate-700 hover:bg-slate-700'
                  }`}
                >
                  {f.label} ({f.count})
                </button>
              ))}
            </div>
          </div>

          {/* Summary bar */}
          {logSummary && (() => {
            // Compute total charges across all closed + open trades
            let totalCharges = 0
            for (const t of logbook) {
              const entry = t.avg_fill_price || 0
              const qty = t.filled_qty || t.quantity || 0
              if (entry <= 0 || qty <= 0) continue
              const buyVal = entry * qty
              const exitPrice = t.exit_price || 0
              const isCl = t.status === 'closed' || t.status === 'exit_sl_breached'
              const sellVal = isCl && exitPrice > 0 ? exitPrice * qty : 0
              const { total } = calcCharges(buyVal, sellVal)
              totalCharges += total
            }
            const netPnl = (logSummary.total_pnl || 0) - totalCharges
            return (
            <div className="grid grid-cols-7 gap-2 mb-4">
              <div className="bg-slate-800/60 rounded p-2 text-center">
                <div className="text-[10px] text-slate-500 uppercase">Gross P&L</div>
                <div className={`font-mono font-bold text-sm ${(logSummary.total_pnl || 0) >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {(logSummary.total_pnl || 0) >= 0 ? '+' : ''}₹{(logSummary.total_pnl || 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                </div>
              </div>
              <div className="bg-slate-800/60 rounded p-2 text-center">
                <div className="text-[10px] text-slate-500 uppercase">Charges</div>
                <div className="font-mono font-bold text-sm text-amber-400" title={`STT + Exchange + SEBI + GST + Stamp + DP`}>
                  ₹{totalCharges.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                </div>
              </div>
              <div className={`rounded p-2 text-center ${netPnl >= 0 ? 'bg-emerald-950/40' : 'bg-red-950/40'}`}>
                <div className="text-[10px] text-slate-500 uppercase">Net P&L</div>
                <div className={`font-mono font-bold text-sm ${netPnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {netPnl >= 0 ? '+' : ''}₹{netPnl.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                </div>
              </div>
              <div className="bg-slate-800/60 rounded p-2 text-center">
                <div className="text-[10px] text-slate-500 uppercase">Win Rate</div>
                <div className={`font-mono font-bold text-sm ${(logSummary.win_rate || 0) >= 50 ? 'text-emerald-400' : 'text-amber-400'}`}>
                  {(logSummary.win_rate || 0).toFixed(0)}%
                </div>
              </div>
              <div className="bg-emerald-950/40 rounded p-2 text-center">
                <div className="text-[10px] text-emerald-600 uppercase">Wins</div>
                <div className="font-mono font-bold text-sm text-emerald-400">{logSummary.wins || 0}</div>
              </div>
              <div className="bg-red-950/40 rounded p-2 text-center">
                <div className="text-[10px] text-red-600 uppercase">Losses</div>
                <div className="font-mono font-bold text-sm text-red-400">{logSummary.losses || 0}</div>
              </div>
              <div className="bg-blue-950/40 rounded p-2 text-center">
                <div className="text-[10px] text-blue-600 uppercase">Open</div>
                <div className="font-mono font-bold text-sm text-blue-400">{logSummary.open || 0}</div>
              </div>
            </div>
            )
          })()}

          {/* Trade table */}
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-500 border-b border-slate-800">
                  <th className="text-left py-1 pr-3">Ticker</th>
                  <th className="text-right py-1 px-2">Date</th>
                  <th className="text-right py-1 px-2">Entry ₹</th>
                  <th className="text-right py-1 px-2">CMP</th>
                  <th className="text-right py-1 px-2">Trail SL</th>
                  <th className="text-right py-1 px-2">Kite SL</th>
                  <th className="text-right py-1 px-2">Exit By</th>
                  <th className="text-right py-1 px-2">Exit ₹</th>
                  <th className="text-right py-1 px-2">Qty</th>
                  <th className="text-right py-1 px-2">Invested</th>
                  <th className="text-right py-1 px-2">P&L</th>
                  <th className="text-right py-1 px-2">P&L %</th>
                  <th className="text-center py-1 px-2">Status</th>
                  <th className="text-center py-1 px-2">Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredLogbook.slice(logPage * LOG_PAGE_SIZE, (logPage + 1) * LOG_PAGE_SIZE).map((t: any, i: number) => {
                  const isOpen = ['bought', 'protected', 'UNPROTECTED', 'buy_placed'].includes(t.status)
                  const liveTkr = tickers.find((tk: any) => tk.ticker === t.ticker || tk.ticker === t.ticker + '.NS')
                  const cmp = liveTkr?.price || 0
                  const entry = t.avg_fill_price || 0
                  const qty = t.filled_qty || t.quantity || 0
                  const trailPct = t.trail_pct || 0.02
                  const storedStop = t.current_stop || t.initial_stop || 0
                  // Trail SL: max of stored stop vs CMP-based trail (stop only moves up)
                  const cmpTrail = (cmp > 0 && entry > 0) ? cmp * (1 - trailPct) : 0
                  const liveStop = isOpen ? Math.max(storedStop, cmpTrail) : storedStop
                  const isCl = t.status === 'closed' || t.status === 'exit_sl_breached'
                  const livePnl = (isOpen && entry > 0 && cmp > 0) ? (cmp - entry) * qty : null
                  const livePnlPct = (isOpen && entry > 0 && cmp > 0) ? (cmp / entry - 1) * 100 : null
                  const displayPnl = isCl ? t.pnl : livePnl
                  const displayPnlPct = isCl ? t.pnl_pct : livePnlPct
                  return (
                  <tr key={`${t.ticker}-${t.order_date}-${i}`} className={`border-b border-slate-800/50 hover:bg-slate-800/30 ${
                    isCl && (t.pnl || 0) > 0 ? 'bg-emerald-950/10' :
                    isCl && (t.pnl || 0) < 0 ? 'bg-red-950/10' : ''
                  }`}>
                    <td className="py-1.5 pr-3 font-mono text-slate-200 font-semibold">{t.ticker}</td>
                    <td className="py-1.5 px-2 text-right text-slate-400">{t.order_date}</td>
                    <td className="py-1.5 px-2 text-right font-mono text-slate-300">
                      {entry > 0 ? `₹${entry.toFixed(2)}` : '—'}
                    </td>
                    <td className={`py-1.5 px-2 text-right font-mono ${
                      isOpen && cmp > 0 && entry > 0
                        ? (cmp >= entry ? 'text-emerald-400' : 'text-red-400')
                        : 'text-slate-500'
                    }`}>
                      {isOpen && cmp > 0 ? `₹${cmp.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : '—'}
                    </td>
                    <td className={`py-1.5 px-2 text-right font-mono ${
                      isOpen && cmp > 0 && liveStop > 0
                        ? (cmp <= liveStop * 1.01 ? 'text-red-400 font-semibold animate-pulse' : 'text-amber-400')
                        : 'text-slate-500'
                    }`}>
                      {isOpen && liveStop > 0 ? `₹${liveStop.toFixed(2)}` : storedStop > 0 ? `₹${storedStop.toFixed(2)}` : '—'}
                    </td>
                    <td className={`py-1.5 px-2 text-right font-mono ${isOpen ? 'text-orange-400' : 'text-slate-500'}`}>
                      {(t.current_stop || t.initial_stop) ? `₹${(t.current_stop || t.initial_stop).toFixed(2)}` : '—'}
                    </td>
                    <td className={`py-1.5 px-2 text-right font-mono text-xs ${isOpen ? 'text-violet-400' : 'text-slate-500'}`}>
                      {t.exit_date ? t.exit_date.slice(5) : '—'}
                    </td>
                    <td className="py-1.5 px-2 text-right font-mono text-slate-300">
                      {t.exit_price ? `₹${t.exit_price.toFixed(2)}` : '—'}
                    </td>
                    <td className="py-1.5 px-2 text-right font-mono text-slate-300">{qty}</td>
                    <td className="py-1.5 px-2 text-right text-slate-400">
                      {t.invest_amount > 0 ? `₹${(t.invest_amount / 1000).toFixed(0)}K` : '—'}
                    </td>
                    <td className={`py-1.5 px-2 text-right font-mono font-semibold ${
                      (displayPnl || 0) > 0 ? 'text-emerald-400' : (displayPnl || 0) < 0 ? 'text-red-400' : 'text-slate-500'
                    }`}>
                      {displayPnl != null ? `${displayPnl > 0 ? '+' : ''}₹${displayPnl.toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '—'}
                    </td>
                    <td className={`py-1.5 px-2 text-right font-mono ${
                      (displayPnlPct || 0) > 0 ? 'text-emerald-400' : (displayPnlPct || 0) < 0 ? 'text-red-400' : 'text-slate-500'
                    }`}>
                      {displayPnlPct != null ? `${displayPnlPct > 0 ? '+' : ''}${displayPnlPct.toFixed(1)}%` : '—'}
                    </td>
                    <td className="py-1.5 px-2 text-center">
                      <span className={`px-1.5 py-0.5 rounded font-semibold text-[10px] ${
                        t.status === 'closed' ? ((t.pnl || 0) >= 0 ? 'bg-emerald-900/60 text-emerald-300' : 'bg-red-900/60 text-red-300') :
                        t.status === 'exit_sl_breached' ? 'bg-red-900/60 text-red-300' :
                        t.status === 'protected' ? 'bg-emerald-900/60 text-emerald-300' :
                        t.status === 'bought' ? 'bg-blue-900/60 text-blue-300' :
                        t.status === 'UNPROTECTED' ? 'bg-red-600/80 text-white' :
                        t.status === 'error' ? 'bg-red-900/60 text-red-300' :
                        'bg-slate-800 text-slate-400'
                      }`}>
                        {t.status === 'closed' ? ((t.pnl || 0) >= 0 ? 'WIN' : 'LOSS') :
                         t.status === 'exit_sl_breached' ? 'SL BREACH' :
                         t.status?.toUpperCase()}
                      </span>
                    </td>
                    <td className="py-1.5 px-2 text-center whitespace-nowrap">
                      {t.status === 'error' && (
                        <button
                          onClick={async () => {
                            try {
                              const res = await api.retryFailedOrder(t.ticker)
                              if (res.status === 'ok') {
                                setError(null)
                                api.fetchOrdersStatus().then(r => { setAutoTradeOrders(r.orders || []) }).catch(() => {})
                                api.fetchLogbook().then(r => { setLogbook(r.trades || []); setLogSummary(r.summary || null) }).catch(() => {})
                              } else {
                                setError(res.message)
                              }
                            } catch (e: any) { setError(e.message) }
                          }}
                          className="px-2 py-0.5 rounded text-[10px] font-semibold bg-amber-900/60 text-amber-300 border border-amber-800 hover:bg-amber-800/60"
                        >
                          Retry
                        </button>
                      )}
                      {isOpen && entry > 0 && (
                        <button
                          onClick={async () => {
                            if (!confirm(`EXIT ${t.ticker}? This will sell ${qty} shares at market.`)) return
                            try {
                              const res = await api.forceExitOrder(t.ticker)
                              if (res.status === 'ok') {
                                setError(null)
                                api.fetchOrdersStatus().then(r => { setAutoTradeOrders(r.orders || []) }).catch(() => {})
                                api.fetchLogbook().then(r => { setLogbook(r.trades || []); setLogSummary(r.summary || null) }).catch(() => {})
                              } else {
                                setError(res.message)
                              }
                            } catch (e: any) { setError(e.message) }
                          }}
                          className="px-2 py-0.5 rounded text-[10px] font-semibold bg-red-900/60 text-red-300 border border-red-800 hover:bg-red-800/60"
                        >
                          Exit
                        </button>
                      )}
                    </td>
                  </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
          {filteredLogbook.length > LOG_PAGE_SIZE && (
            <div className="flex items-center justify-between mt-3 pt-2 border-t border-slate-800">
              <span className="text-xs text-slate-500">
                {logPage * LOG_PAGE_SIZE + 1}–{Math.min((logPage + 1) * LOG_PAGE_SIZE, filteredLogbook.length)} of {filteredLogbook.length}
              </span>
              <div className="flex gap-1">
                <button
                  onClick={() => setLogPage(p => Math.max(0, p - 1))}
                  disabled={logPage === 0}
                  className="px-2 py-0.5 text-xs rounded bg-slate-800 text-slate-400 border border-slate-700 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                  ← Prev
                </button>
                <button
                  onClick={() => setLogPage(p => Math.min(Math.ceil(filteredLogbook.length / LOG_PAGE_SIZE) - 1, p + 1))}
                  disabled={(logPage + 1) * LOG_PAGE_SIZE >= filteredLogbook.length}
                  className="px-2 py-0.5 text-xs rounded bg-slate-800 text-slate-400 border border-slate-700 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                  Next →
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Live Ticker Monitor — Per-Stock Detail */}
      {isLive && tickers.length > 0 && (
        <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
          <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2 mb-3">
            <Activity className="w-4 h-4 text-violet-400" />
            Live Monitor — {tickers.length} Stocks
          </h3>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-500 border-b border-slate-800">
                  <th className="text-left py-1 pr-3">Ticker</th>
                  <th className="text-right py-1 px-2">Price</th>
                  <th className="text-right py-1 px-2">Chg%</th>
                  <th className="text-right py-1 px-2">VWAP</th>
                  <th className="text-right py-1 px-2">vs VWAP</th>
                  <th className="text-right py-1 px-2">RVOL</th>
                  <th className="text-right py-1 px-2">Down</th>
                  <th className="text-center py-1 px-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {tickers.filter(t => t.has_volume).slice(monPage * MON_PAGE_SIZE, (monPage + 1) * MON_PAGE_SIZE).map((t) => (
                  <tr key={t.ticker} className={`border-b border-slate-800/50 ${t.fired ? 'bg-emerald-950/30' : 'hover:bg-slate-800/30'}`}>
                    <td className="py-1.5 pr-3 font-mono text-slate-200">{t.ticker.replace('.NS', '')}</td>
                    <td className="py-1.5 px-2 text-right font-mono text-slate-300">₹{t.price.toLocaleString()}</td>
                    <td className={`py-1.5 px-2 text-right font-mono font-semibold ${
                      (t.day_change_pct || 0) > 2 ? 'text-emerald-300' :
                      (t.day_change_pct || 0) > 0 ? 'text-emerald-500' :
                      (t.day_change_pct || 0) < -2 ? 'text-red-300' :
                      'text-red-500'
                    }`}>
                      {(t.day_change_pct || 0) > 0 ? '+' : ''}{(t.day_change_pct || 0).toFixed(1)}%
                    </td>
                    <td className="py-1.5 px-2 text-right font-mono text-slate-400">₹{t.vwap.toLocaleString()}</td>
                    <td className={`py-1.5 px-2 text-right font-mono ${t.dist_to_vwap_pct > 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                      {t.dist_to_vwap_pct > 0 ? '+' : ''}{t.dist_to_vwap_pct.toFixed(1)}%
                    </td>
                    <td className="py-1.5 px-2 text-right">
                      <RvolBar value={t.rvol} threshold={vwapCfg.min_rvol || 1.5} />
                    </td>
                    <td className="py-1.5 px-2 text-right">
                      <span className={`px-1.5 py-0.5 rounded font-semibold ${
                        t.down_days >= (vwapCfg.min_down_days || 5) ? 'bg-red-900/50 text-red-300' : 'bg-slate-800 text-slate-500'
                      }`}>
                        {t.down_days}d
                      </span>
                    </td>
                    <td className="py-1.5 px-2 text-center">
                      {t.fired ? (
                        <span className="px-1.5 py-0.5 rounded bg-emerald-900/60 text-emerald-300 font-semibold">FIRED ✓</span>
                      ) : t.dist_to_vwap_pct > -1 && t.rvol >= (vwapCfg.min_rvol || 1.5) ? (
                        <span className="px-1.5 py-0.5 rounded bg-amber-900/60 text-amber-300 font-semibold">NEAR</span>
                      ) : (
                        <span className="text-slate-600">watching</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {(() => {
            const filtered = tickers.filter(t => t.has_volume)
            const totalPages = Math.ceil(filtered.length / MON_PAGE_SIZE)
            if (totalPages <= 1) return null
            return (
              <div className="flex items-center justify-between mt-3 pt-2 border-t border-slate-800">
                <span className="text-xs text-slate-500">
                  {monPage * MON_PAGE_SIZE + 1}–{Math.min((monPage + 1) * MON_PAGE_SIZE, filtered.length)} of {filtered.length}
                </span>
                <div className="flex gap-1">
                  <button
                    onClick={() => setMonPage(p => Math.max(0, p - 1))}
                    disabled={monPage === 0}
                    className="px-2 py-0.5 text-xs rounded bg-slate-800 text-slate-400 border border-slate-700 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed"
                  >
                    ← Prev
                  </button>
                  <button
                    onClick={() => setMonPage(p => Math.min(totalPages - 1, p + 1))}
                    disabled={monPage >= totalPages - 1}
                    className="px-2 py-0.5 text-xs rounded bg-slate-800 text-slate-400 border border-slate-700 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed"
                  >
                    Next →
                  </button>
                </div>
              </div>
            )
          })()}
        </div>
      )}

      {/* Pre-Market Watchlist — Ready only */}
      <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
            <Eye className="w-4 h-4 text-cyan-400" />
            Pre-Market Watchlist — {watchlist.filter(w => w.down_days >= (vwapCfg.min_down_days || 3)).length} Ready
          </h3>
          <button
            onClick={refreshStatic}
            className="px-2 py-1 text-xs bg-slate-800 text-slate-400 rounded border border-slate-700 hover:bg-slate-700"
          >
            Refresh
          </button>
        </div>

        {(() => {
          const ready = watchlist.filter(w => {
            const meetsDown = w.down_days >= (vwapCfg.min_down_days || 3)
            const meetsDepth = vwapCfg.depth_max ? w.depth_pct <= vwapCfg.depth_max : true
            return meetsDown && meetsDepth
          })
          if (ready.length === 0) return (
            <div className="text-center py-4 text-slate-600 text-sm">
              No stocks at {vwapCfg.min_down_days || 3}+ down days. Watchlist populates when candidates emerge during corrections.
            </div>
          )
          return (
          <>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-500 border-b border-slate-800">
                  <th className="text-left py-1 pr-3">Ticker</th>
                  <th className="text-right py-1 px-2">Down Days</th>
                  <th className="text-right py-1 px-2">Close</th>
                  <th className="text-right py-1 px-2">Avg Vol</th>
                </tr>
              </thead>
              <tbody>
                {ready.slice(wlPage * WL_PAGE_SIZE, (wlPage + 1) * WL_PAGE_SIZE).map((item) => (
                    <tr key={item.ticker} className="border-b border-slate-800/50 bg-amber-950/20 hover:bg-slate-800/30">
                      <td className="py-1 pr-3 font-mono text-slate-200">{item.ticker.replace('.NS', '')}</td>
                      <td className="py-1 px-2 text-right">
                        <span className="px-1.5 py-0.5 rounded text-xs font-semibold bg-red-900/50 text-red-300">
                          {item.down_days}d
                        </span>
                      </td>
                      <td className="py-1 px-2 text-right font-mono text-slate-300">₹{item.close.toLocaleString()}</td>
                      <td className="py-1 px-2 text-right text-slate-400">{(item.avg_volume / 1000).toFixed(0)}K</td>
                    </tr>
                ))}
              </tbody>
            </table>
          </div>
          {ready.length > WL_PAGE_SIZE && (
            <div className="flex items-center justify-between mt-3 pt-2 border-t border-slate-800">
              <span className="text-xs text-slate-500">
                {wlPage * WL_PAGE_SIZE + 1}–{Math.min((wlPage + 1) * WL_PAGE_SIZE, ready.length)} of {ready.length}
              </span>
              <div className="flex gap-1">
                <button
                  onClick={() => setWlPage(p => Math.max(0, p - 1))}
                  disabled={wlPage === 0}
                  className="px-2 py-0.5 text-xs rounded bg-slate-800 text-slate-400 border border-slate-700 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                  ← Prev
                </button>
                <button
                  onClick={() => setWlPage(p => Math.min(Math.ceil(ready.length / WL_PAGE_SIZE) - 1, p + 1))}
                  disabled={(wlPage + 1) * WL_PAGE_SIZE >= ready.length}
                  className="px-2 py-0.5 text-xs rounded bg-slate-800 text-slate-400 border border-slate-700 hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                  Next →
                </button>
              </div>
            </div>
          )}
          </>
          )
        })()}
      </div>

      {/* Execution Timeline */}
      <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
        <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2 mb-3">
          <Clock className="w-4 h-4 text-blue-400" />
          Daily Execution Flow (IST)
        </h3>
        <div className="space-y-1 text-xs">
          {[
            { time: '08:30', label: 'Pre-Market', desc: `Watchlist: stocks at ${vwapCfg.min_down_days || 5}+ down days, ${vwapCfg.depth_max ? Math.abs(vwapCfg.depth_max * 100) + '%+' : ''} depth`, color: 'text-cyan-400' },
            { time: '09:15', label: 'Market Open', desc: 'Start Kite WebSocket — subscribe to watchlist tickers', color: 'text-emerald-400' },
            { time: '09:15–10:15', label: 'First Hour', desc: 'Accumulate volume, compute running VWAP & RVOL', color: 'text-amber-400' },
            { time: '10:15–14:30', label: 'Signal Window', desc: `Fire BUY when: price > VWAP AND RVOL ≥ ${vwapCfg.min_rvol || 1.5}x — enter immediately at signal price`, color: 'text-red-400' },
            { time: 'T+0 to T+7', label: 'Hold Period', desc: `Max ${vwapCfg.max_hold_days || 7}d from entry. Exit when price drops ${vwapCfg.trailing_stop_pct ? (vwapCfg.trailing_stop_pct * 100) + '%' : '2%'} from peak (trailing stop)`, color: 'text-violet-400' },
            { time: '16:00', label: 'Post-Market', desc: 'Update watchlist for next day, log results', color: 'text-slate-400' },
          ].map((step) => (
            <div key={step.time} className="flex items-start gap-3 py-1.5 border-b border-slate-800/30">
              <span className={`font-mono w-28 shrink-0 ${step.color}`}>{step.time}</span>
              <span className="text-slate-300 font-semibold w-28 shrink-0">{step.label}</span>
              <span className="text-slate-500">{step.desc}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}


/* ═══════════════ Sub-Components ═══════════════ */

function RegimeGuideCard({ guide }: { guide: IntradayRegimeGuide | null }) {
  if (!guide || guide.error) {
    return (
      <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2 mb-2">
          <Shield className="w-4 h-4" />
          Strategy Regime Guide
        </h2>
        <div className="text-sm text-slate-500">Run a scan first to detect market regime.</div>
      </div>
    )
  }

  const isBear = guide.regime === 'bear'
  const isRecovery = guide.regime === 'recovery'
  const isBull = guide.regime === 'bull'

  const borderColor = isBear ? 'border-red-800/60' : isRecovery ? 'border-amber-800/60' : 'border-emerald-800/60'
  const Icon = isBear ? TrendingDown : isRecovery ? Shield : TrendingUp
  const iconColor = isBear ? 'text-red-400' : isRecovery ? 'text-amber-400' : 'text-emerald-400'

  return (
    <div className={`bg-slate-900 rounded-xl border ${borderColor} p-4`}>
      <div className="flex items-start justify-between mb-3">
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
          <Icon className={`w-4 h-4 ${iconColor}`} />
          Strategy Regime Guide — <span className={iconColor}>{guide.regime.toUpperCase()}</span>
        </h2>
        <span className="text-xs text-slate-600">{guide.date}</span>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-3">
        <div className={`rounded-lg p-3 border ${
          guide.recommendation === 'momentum' || guide.recommendation === 'both'
            ? 'bg-emerald-950/30 border-emerald-800/50'
            : 'bg-slate-800/30 border-slate-700/50 opacity-50'
        }`}>
          <div className="flex items-center gap-2 mb-1">
            <TrendingUp className="w-4 h-4 text-emerald-400" />
            <span className="text-xs font-semibold text-emerald-400">MOMENTUM</span>
            {(guide.recommendation === 'momentum' || guide.recommendation === 'both') && (
              <CheckCircle className="w-3 h-3 text-emerald-400" />
            )}
          </div>
          <div className="text-xs text-slate-400">
            {isBull ? 'Active — trend-following in strong market' : isBear ? 'Paused — stops getting hit' : 'Re-entering — selective buys'}
          </div>
        </div>

        <div className={`rounded-lg p-3 border ${
          guide.recommendation === 'reversal' || guide.recommendation === 'both'
            ? 'bg-amber-950/30 border-amber-800/50'
            : 'bg-slate-800/30 border-slate-700/50 opacity-50'
        }`}>
          <div className="flex items-center gap-2 mb-1">
            <TrendingDown className="w-4 h-4 text-amber-400" />
            <span className="text-xs font-semibold text-amber-400">REVERSAL</span>
            {(guide.recommendation === 'reversal' || guide.recommendation === 'both') && (
              <CheckCircle className="w-3 h-3 text-amber-400" />
            )}
          </div>
          <div className="text-xs text-slate-400">
            {isBear ? 'Active — oversold bounces are strongest' : isBull ? 'Standby — few candidates' : 'Active — catching laggards'}
          </div>
        </div>

        <div className="bg-slate-800/30 rounded-lg p-3 border border-slate-700/50">
          <div className="text-xs text-slate-500 mb-1">Market Metrics</div>
          <div className="space-y-1 text-xs">
            <div className="flex justify-between">
              <span className="text-slate-500">Breadth</span>
              <span className={`font-mono ${(guide.metrics.breadth_pct || 0) > 50 ? 'text-emerald-400' : 'text-red-400'}`}>
                {guide.metrics.breadth_pct?.toFixed(1)}%
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-slate-500">Nifty vs 200MA</span>
              <span className={`font-mono ${guide.metrics.nifty_below_200ma ? 'text-red-400' : 'text-emerald-400'}`}>
                {guide.metrics.nifty_below_200ma ? 'Below ↓' : 'Above ↑'}
              </span>
            </div>
            {guide.metrics.strength != null && (
              <div className="flex justify-between">
                <span className="text-slate-500">Strength</span>
                <span className="font-mono text-slate-300">{guide.metrics.strength.toFixed(2)}</span>
              </div>
            )}
          </div>
        </div>
      </div>

      <div className={`text-xs rounded p-2 ${
        isBear ? 'bg-red-950/30 text-red-200/80' : isRecovery ? 'bg-amber-950/30 text-amber-200/80' : 'bg-emerald-950/30 text-emerald-200/80'
      }`}>
        {isBear && <AlertTriangle className="w-3 h-3 inline mr-1" />}
        {guide.message}
      </div>
    </div>
  )
}

function RvolBar({ value, threshold }: { value: number; threshold: number }) {
  const pct = Math.min(value / (threshold * 1.5) * 100, 100)
  const meetsThreshold = value >= threshold
  return (
    <div className="flex items-center gap-1.5">
      <div className="w-12 h-1.5 bg-slate-800 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full ${meetsThreshold ? 'bg-emerald-500' : 'bg-slate-600'}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className={`font-mono w-10 text-right ${meetsThreshold ? 'text-emerald-400 font-semibold' : 'text-slate-500'}`}>
        {value.toFixed(1)}x
      </span>
    </div>
  )
}
