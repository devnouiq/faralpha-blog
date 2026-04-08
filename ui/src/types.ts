// ─── Market & Status ────────────────────────

export interface RegimeInfo {
  date: string | null
  regime: string
  is_bull: boolean
  strength: number | null
  breadth_pct: number | null
}

export interface MarketStatus {
  last_price_date: string | null
  n_tickers: number
  n_active_tickers?: number
  n_delisted_tickers?: number
  n_price_rows: number
  regime: RegimeInfo | null
  n_candidates_today: number
  days_stale?: number | null
  freshness?: string
  error?: string
}

export interface ScannerState {
  running: boolean
  mode: string
  interval_minutes: number
  market: string
  last_run: string | null
  last_run_market: string | null
  next_run: string | null
  next_run_market: string | null
  last_result: any
  scans_today: Record<string, string | null>
}

export interface ScheduleTime {
  utc: string
  ist: string
  et: string
  date: string
  countdown: string
  seconds_away: number
}

export interface ScheduleInfo {
  india: ScheduleTime
  next_market: string
}

export interface SystemStatus {
  markets: Record<string, MarketStatus>
  scanner: ScannerState
  schedule: ScheduleInfo
  busy: boolean
  positions: number
  data_type: string
}

// ─── Signals ────────────────────────────────

export interface Candidate {
  ticker: string
  rs_composite: number
  signal_tier: string
  rank: number
  date: string
  sector: string
  close: number
  volume: number
  score: number
  // Actionable entry/exit info
  pivot_price?: number
  entry_price?: number
  max_entry_price?: number
  stop_price?: number
  risk_per_share?: number
  base_depth_pct?: number
  already_held?: boolean
}

export interface BuyAction {
  action: string
  ticker: string
  instruction: string
  entry_price: number
  max_entry_price: number
  stop_price: number
  risk_pct: number
  stop_loss_pct: number
  signal_tier: string
  // Position sizing (when capital provided)
  position_value?: number
  shares?: number
  risk_amount?: number
  capital_pct?: number
}

export interface PyramidAction {
  action: string
  ticker: string
  instruction: string
  current_price: number
  gain_pct: number
  add_number: number
  position_value?: number
  shares?: number
  risk_amount?: number
  capital_pct?: number
}

export interface PositionGuidance {
  ticker: string
  action: string
  urgency: string
  instruction: string
  current_price: number
  entry_price: number
  gain_pct: number
  hard_stop: number
  trail_stop: number
  active_stop: number
}

export interface TradingStatus {
  overall: string
  overall_message: string
  regime: string
  equity_dd: {
    active: boolean
    level?: string
    current_dd_pct?: number
    threshold_pct?: number
    floor_pct?: number
    effective_max_positions?: number
    effective_position_scale?: number
    message?: string
  }
  circuit_breaker: {
    active: boolean
    consecutive_losses?: number
    threshold?: number
    pause_days?: number
    message?: string
  }
  positions_held: number
  max_positions: number
  position_guidance: PositionGuidance[]
}

export interface SignalsResponse {
  market: string
  date: string | null
  count: number
  candidates: Candidate[]
  actions: BuyAction[]
  pyramid_actions?: PyramidAction[]
  open_slots: number
  regime: string
  trading_status?: TradingStatus
  config: {
    max_positions: number
    stop_loss_pct: number
    trailing_stop_pct: number
    risk_per_trade_pct: number
    max_chase_pct: number
    pyramid_enabled?: boolean
    pyramid_max_adds?: number
  }
}

// ─── Positions ──────────────────────────────

export interface Position {
  ticker: string
  market: string
  entry_date: string
  entry_price: number
  shares: number
  highest_price: number
  notes: string
  current_price?: number
  price_date?: string
  entry_stop?: number
  trail_stop?: number
  active_stop?: number
  pnl_pct?: number
  stop_type?: string
  stop_distance_pct?: number
  gain_from_entry_pct?: number
  trailing_stop_pct?: number
  stop_loss_pct?: number
}

export interface StopAlert {
  ticker: string
  market: string
  current_price: number
  stop_price: number
  stop_type: string
  loss_pct: number
  action: string
}

// ─── Backtest ───────────────────────────────

export interface AnnualReturn {
  year: number
  return_pct: number
}

export interface BacktestData {
  market: string
  annual?: AnnualReturn[]
  equity_recent?: Array<{
    date: string
    equity: number
    cash: number
    n_positions: number
    exposure_pct: number
  }>
  trades_recent?: Array<{
    ticker: string
    entry_date: string
    exit_date: string
    entry_price: number
    exit_price: number
    pnl_pct: number
    exit_reason: string
    hold_days: number
  }>
  error?: string
}

// ─── Regime API ─────────────────────────────

export interface RegimeData {
  market: string
  date: string
  regime: string
  is_bull: boolean
  is_recovery: boolean
  is_weak: boolean
  strength: number | null
  breadth_pct: number | null
  benchmark: number | null
  benchmark_ma200: number | null
  benchmark_ma50: number | null
  error?: string
}

// ─── Intraday Reversal ──────────────────────

export interface IntradayWatchlistItem {
  ticker: string
  down_days: number
  depth_pct: number
  avg_volume: number
  close: number
  sector: string
}

export interface IntradayScrapeStatus {
  candles: number
  tickers: number
  from: string | null
  to: string | null
  source?: string
}

export interface IntradayStrategyConfig {
  min_down_days: number
  min_rvol: number
  require_bear: boolean
  max_hold_days: number
  depth_max?: number
  trailing_stop_pct?: number
  stop_loss_pct?: number | null
}

export interface IntradayStatus {
  live_ticker_active: boolean
  strategy_config: IntradayStrategyConfig
  live?: {
    date: string
    watchlist_size: number
    first_hour_done: boolean
    market_closed: boolean
    signals_fired: number
    tokens_with_volume: number
    vwap: Record<string, number>
    rvol: Record<string, number>
    tickers?: IntradayTickerDetail[]
    signal_history?: IntradaySignal[]
  }
  scrape: Record<string, IntradayScrapeStatus>
}

export interface IntradayTickerDetail {
  ticker: string
  price: number
  vwap: number
  rvol: number
  down_days: number
  depth_pct: number
  dist_to_vwap_pct: number
  fired: boolean
  has_volume: boolean
  day_change_pct?: number
  prev_close?: number
}

export interface IntradaySignal {
  type: string
  strategy: string
  ticker: string
  price: number
  vwap: number
  rvol: number
  down_days: number
  depth_pct?: number
  prev_close?: number
  day_open?: number
  day_change_pct?: number
  max_hold_days?: number
  trailing_stop_pct?: number
  stop_loss_pct?: number | null
  time: string
}

export interface IntradayRegimeGuide {
  regime: string
  date: string
  recommendation: string
  primary_strategy: string
  secondary_strategy: string
  message: string
  urgency: string
  metrics: {
    strength: number | null
    breadth_pct: number
    nifty_below_200ma: boolean
    benchmark: number | null
    benchmark_ma200: number | null
  }
  error?: string
}

export interface IntradayPerformance {
  median_xirr: number
  holdout_xirr: number
  worst_dd: number
  holdout_dd: number
  median_sharpe: number
  stable_configs: number
  txn_cost_bps: number
}

// ─── Alerts ─────────────────────────────────

export interface WsEvent {
  type: string
  data: any
  ts: string
}
