import type { SystemStatus, Candidate, Position, StopAlert, RegimeData, BacktestData, SignalsResponse, IntradayStatus, IntradayWatchlistItem, IntradayRegimeGuide, IntradaySignal } from './types'

const BASE = ''

async function json<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${url}`, init)
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(body.detail || body.error || res.statusText)
  }
  return res.json()
}

// ─── Status ─────────────────────────────────

export function fetchStatus(): Promise<SystemStatus> {
  return json('/api/status')
}

// ─── Sync / Pipeline / Scan ─────────────────

export function triggerSync(market: string, force = false) {
  return json<any>('/api/sync', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ market, force }),
  })
}

export function triggerPipeline(market: string) {
  return json<any>('/api/pipeline', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ market }),
  })
}

export function triggerScan(market: string, force = false) {
  return json<any>('/api/scan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ market, force }),
  })
}

export function triggerUniverse(market: string = 'both', purgeDelisted = true) {
  return json<any>('/api/universe', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ market, purge_delisted: purgeDelisted }),
  })
}

export function triggerCleanup(market: string = 'both') {
  return json<any>('/api/cleanup', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ market }),
  })
}

// ─── Signals ────────────────────────────────

export function fetchSignals(market: string, capital?: number): Promise<SignalsResponse> {
  const params = capital && capital > 0 ? `?capital=${capital}` : ''
  return json(`/api/signals/${market}${params}`)
}

// ─── Regime ─────────────────────────────────

export function fetchRegime(market: string): Promise<RegimeData> {
  return json(`/api/regime/${market}`)
}

// ─── Positions ──────────────────────────────

export function fetchPositions(): Promise<{ positions: Position[]; stop_alerts: StopAlert[] }> {
  return json('/api/positions')
}

export function addPosition(pos: { ticker: string; market: string; entry_date: string; entry_price: number; shares: number; notes?: string }) {
  return json<any>('/api/positions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(pos),
  })
}

export function removePosition(market: string, ticker: string) {
  return json<any>(`/api/positions/${market}/${ticker}`, { method: 'DELETE' })
}

// ─── Backtest ───────────────────────────────

export function fetchBacktest(market: string): Promise<BacktestData> {
  return json(`/api/backtest/${market}`)
}

// ─── Intraday Reversal ──────────────────────

export function fetchIntradayStatus(): Promise<IntradayStatus> {
  return json('/api/intraday/status')
}

export function fetchIntradayConfig(): Promise<any> {
  return json('/api/intraday/config')
}

export function fetchIntradayWatchlist(): Promise<{ watchlist: IntradayWatchlistItem[]; count: number; error?: string }> {
  return json('/api/intraday/watchlist')
}

export function syncIntraday(): Promise<{
  status: string
  message?: string
  watchlist_count?: number
  candles?: Record<string, number>
  error?: string
}> {
  return json('/api/intraday/sync', { method: 'POST' })
}

export function fetchRegimeGuide(): Promise<IntradayRegimeGuide> {
  return json('/api/intraday/regime-guide')
}

export function fetchSignalHistory(): Promise<{ signals: IntradaySignal[]; count: number }> {
  return json('/api/intraday/signals/history')
}

// ─── Auto-Trade Orders ──────────────────────

export function fetchOrdersStatus(): Promise<{ auto_trade: boolean; today_orders: number; orders: any[]; pending_queue: any[] }> {
  return json('/api/orders/status')
}

export function fetchLogbook(): Promise<{ trades: any[]; summary: any }> {
  return json('/api/orders/logbook')
}

export function enableAutoTrade(): Promise<{ auto_trade: boolean }> {
  return json('/api/orders/enable', { method: 'POST' })
}

export function disableAutoTrade(): Promise<{ auto_trade: boolean }> {
  return json('/api/orders/disable', { method: 'POST' })
}

export function forceExitOrder(ticker: string): Promise<any> {
  return json('/api/orders/force-exit', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ticker }),
  })
}

export function retrySl(ticker: string): Promise<any> {
  return json('/api/orders/retry-sl', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ticker }),
  })
}

export function retryFailedOrder(ticker: string): Promise<any> {
  return json('/api/orders/retry-failed', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ticker }),
  })
}

export function placeManualOrder(ticker: string, price: number, trailing_stop_pct?: number, max_hold_days?: number): Promise<any> {
  return json('/api/orders/place', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ticker, price, trailing_stop_pct, max_hold_days }),
  })
}

export function approvePending(ticker: string): Promise<any> {
  return json('/api/orders/approve', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ticker }),
  })
}

export function dismissPending(ticker: string): Promise<any> {
  return json('/api/orders/dismiss', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ticker }),
  })
}

// ─── Scheduler ──────────────────────────────

export function startScheduler(mode: string = 'daily', interval_minutes: number = 60, market: string = 'both') {
  return json<any>('/api/scheduler/start', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mode, interval_minutes, market }),
  })
}

export function stopScheduler() {
  return json<any>('/api/scheduler/stop', { method: 'POST' })
}

export function fetchSchedule() {
  return json<any>('/api/schedule')
}

// ─── WebSocket ──────────────────────────────

export function connectWs(onMessage: (event: any) => void): WebSocket {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const ws = new WebSocket(`${proto}//${window.location.host}/ws/events`)
  let heartbeat: ReturnType<typeof setInterval> | null = null
  ws.onmessage = (e) => {
    try { onMessage(JSON.parse(e.data)) } catch {}
  }
  ws.onopen = () => {
    heartbeat = setInterval(() => { if (ws.readyState === 1) ws.send('ping') }, 30_000)
  }
  ws.onclose = () => {
    if (heartbeat) { clearInterval(heartbeat); heartbeat = null }
  }
  return ws
}

// ─── Kite Auth ──────────────────────────────

export function fetchKiteAuthStatus(): Promise<{ logged_in: boolean; user_name: string | null }> {
  return json('/api/kite/auth-status')
}

export function fetchKiteLoginUrl(): Promise<{ url: string }> {
  return json('/api/kite/login-url')
}

export function submitKiteCallback(request_token: string): Promise<{ success: boolean; user_name: string }> {
  return json('/api/kite/callback', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ request_token }),
  })
}
