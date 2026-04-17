import { useState } from 'react'
import { Target, AlertTriangle, ShieldCheck, TrendingUp, AlertCircle, Pause, Activity, ChevronLeft, ChevronRight } from 'lucide-react'
import type { Candidate, BuyAction, StopAlert, SignalsResponse, TradingStatus } from '../types'

interface Props {
  market: string
  candidates: Candidate[]
  date: string | null
  actions: BuyAction[]
  meta: Partial<SignalsResponse>
  stopAlerts: StopAlert[]
  tradingStatus: TradingStatus | null
}

export default function SignalsTable({ market, candidates, date, actions, meta, stopAlerts, tradingStatus }: Props) {
  const regime = meta.regime ?? 'unknown'
  const openSlots = meta.open_slots ?? 0
  const cfg = meta.config
  const PAGE_SIZE = 20
  const [page, setPage] = useState(0)
  const totalPages = Math.max(1, Math.ceil(candidates.length / PAGE_SIZE))
  const paged = candidates.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)

  const marketStopAlerts = stopAlerts.filter(a => a.market === market)

  return (
    <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
      {/* Stop alerts banner — most urgent, shown first */}
      {marketStopAlerts.length > 0 && (
        <div className="mb-4 bg-red-950/50 border border-red-800 rounded-lg p-3">
          <div className="flex items-center gap-2 mb-2">
            <AlertTriangle className="w-4 h-4 text-red-400" />
            <span className="text-sm font-bold text-red-400 uppercase">
              Stop-Loss Triggered — SELL at Next Open
            </span>
          </div>
          {marketStopAlerts.map(a => (
            <div key={a.ticker} className="flex items-center gap-3 py-1 text-sm">
              <span className="font-bold text-red-300">{a.ticker}</span>
              <span className="text-slate-400">
                Price {a.current_price.toFixed(2)} hit {a.stop_type} stop at {a.stop_price.toFixed(2)}
              </span>
              <span className="text-red-400 font-mono">{a.loss_pct.toFixed(1)}%</span>
            </div>
          ))}
        </div>
      )}

      {/* Action required banner — buy signals */}
      {actions.length > 0 && (
        <div className="mb-4 bg-emerald-950/50 border border-emerald-800 rounded-lg p-3">
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp className="w-4 h-4 text-emerald-400" />
            <span className="text-sm font-bold text-emerald-400 uppercase">
              Buy at Next Market Open ({openSlots} slot{openSlots !== 1 ? 's' : ''} available)
            </span>
          </div>
          {actions.map(a => (
            <div key={a.ticker} className="py-1.5 border-b border-emerald-900/50 last:border-0">
              <div className="flex items-center gap-3 text-sm">
                <span className="font-bold text-emerald-300 w-24">{a.ticker}</span>
                <span className="text-slate-300">{a.instruction}</span>
              </div>
              <div className="flex gap-4 mt-0.5 text-xs text-slate-500 ml-24">
                <span>Entry ~{a.entry_price.toFixed(2)}</span>
                <span>Max {a.max_entry_price.toFixed(2)}</span>
                <span className="text-red-400">Stop {a.stop_price.toFixed(2)} ({a.stop_loss_pct.toFixed(0)}%)</span>
                <span>Risk {a.risk_pct.toFixed(1)}%/trade</span>
                {a.position_value && a.shares && (
                  <>
                    <span className="text-cyan-400 font-semibold">₹{a.position_value.toLocaleString()}</span>
                    <span className="text-cyan-400">{a.shares} shares</span>
                    <span className="text-slate-600">{a.capital_pct?.toFixed(1)}% of capital</span>
                  </>
                )}
                <span className="text-slate-600">{a.signal_tier}</span>
              </div>
            </div>
          ))}
          {cfg && (
            <div className="mt-2 text-[10px] text-slate-600 flex gap-3">
              <span>Max {cfg.max_positions} positions</span>
              <span>Stop {cfg.stop_loss_pct.toFixed(0)}%</span>
              <span>Trail {cfg.trailing_stop_pct.toFixed(0)}%</span>
              <span>Risk {cfg.risk_per_trade_pct.toFixed(1)}%/trade</span>
            </div>
          )}
        </div>
      )}

      {/* ── Trading Status Dashboard ── */}
      {tradingStatus && (
        <div className="mb-4">
          {/* Overall status banner */}
          <div className={`rounded-lg p-3 mb-2 border ${
            tradingStatus.overall === 'NO_TRADE' ? 'bg-red-950/50 border-red-800' :
            tradingStatus.overall === 'PAUSED' ? 'bg-orange-950/50 border-orange-800' :
            tradingStatus.overall === 'REDUCED' ? 'bg-amber-950/50 border-amber-800' :
            tradingStatus.overall === 'FULL' ? 'bg-blue-950/50 border-blue-800' :
            'bg-emerald-950/50 border-emerald-800'
          }`}>
            <div className="flex items-center gap-2 mb-1">
              {tradingStatus.overall === 'NO_TRADE' && <AlertCircle className="w-4 h-4 text-red-400" />}
              {tradingStatus.overall === 'PAUSED' && <Pause className="w-4 h-4 text-orange-400" />}
              {tradingStatus.overall === 'REDUCED' && <AlertTriangle className="w-4 h-4 text-amber-400" />}
              {tradingStatus.overall === 'ACTIVE' && <TrendingUp className="w-4 h-4 text-emerald-400" />}
              {tradingStatus.overall === 'FULL' && <Activity className="w-4 h-4 text-blue-400" />}
              <span className={`text-sm font-bold uppercase ${
                tradingStatus.overall === 'NO_TRADE' ? 'text-red-400' :
                tradingStatus.overall === 'PAUSED' ? 'text-orange-400' :
                tradingStatus.overall === 'REDUCED' ? 'text-amber-400' :
                tradingStatus.overall === 'FULL' ? 'text-blue-400' :
                'text-emerald-400'
              }`}>
                {tradingStatus.overall === 'NO_TRADE' ? '🛑 Do Not Trade' :
                 tradingStatus.overall === 'PAUSED' ? '⏸ Trading Paused' :
                 tradingStatus.overall === 'REDUCED' ? '⚠️ Reduced Exposure' :
                 tradingStatus.overall === 'FULL' ? '📊 Portfolio Full' :
                 '✅ Active — Buy Signals Available'}
              </span>
            </div>
            <p className="text-xs text-slate-400">{tradingStatus.overall_message}</p>
          </div>

          {/* Equity DD warning */}
          {tradingStatus.equity_dd.active && (
            <div className="bg-amber-950/30 border border-amber-900/50 rounded-lg p-2.5 mb-2">
              <div className="flex items-center gap-2 mb-1">
                <AlertTriangle className="w-3.5 h-3.5 text-amber-400" />
                <span className="text-xs font-bold text-amber-400 uppercase">Equity Drawdown Protection Active</span>
              </div>
              <p className="text-xs text-slate-400">
                Portfolio is {Math.abs(tradingStatus.equity_dd.current_dd_pct ?? 0).toFixed(1)}% below peak
                (trigger: {Math.abs(tradingStatus.equity_dd.threshold_pct ?? 8)}%, floor: {Math.abs(tradingStatus.equity_dd.floor_pct ?? 20)}%).
                Max positions reduced to {tradingStatus.equity_dd.effective_max_positions},
                position sizes at {((tradingStatus.equity_dd.effective_position_scale ?? 1) * 100).toFixed(0)}%.
              </p>
            </div>
          )}

          {/* Circuit breaker warning */}
          {tradingStatus.circuit_breaker.active && (
            <div className="bg-orange-950/30 border border-orange-900/50 rounded-lg p-2.5 mb-2">
              <div className="flex items-center gap-2 mb-1">
                <Pause className="w-3.5 h-3.5 text-orange-400" />
                <span className="text-xs font-bold text-orange-400 uppercase">Circuit Breaker Triggered</span>
              </div>
              <p className="text-xs text-slate-400">
                {tradingStatus.circuit_breaker.consecutive_losses} consecutive stop-outs hit
                (threshold: {tradingStatus.circuit_breaker.threshold}).
                No new entries for {tradingStatus.circuit_breaker.pause_days} trading days.
              </p>
            </div>
          )}

          {/* Position guidance */}
          {tradingStatus.position_guidance.length > 0 && (
            <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-2.5">
              <div className="text-xs font-bold text-slate-400 uppercase mb-2">
                Position Management ({tradingStatus.positions_held}/{tradingStatus.max_positions} slots)
              </div>
              {tradingStatus.position_guidance.map(g => (
                <div key={g.ticker} className={`flex items-center gap-3 py-1.5 border-b border-slate-700/50 last:border-0 text-xs ${
                  g.urgency === 'critical' ? 'bg-red-950/20' : ''
                }`}>
                  <span className={`font-bold w-20 ${
                    g.action === 'SELL' ? 'text-red-400' : 'text-slate-300'
                  }`}>
                    {g.ticker}
                  </span>
                  <span className={`font-mono w-16 text-right ${
                    g.gain_pct >= 0 ? 'text-emerald-400' : 'text-red-400'
                  }`}>
                    {g.gain_pct >= 0 ? '+' : ''}{g.gain_pct.toFixed(1)}%
                  </span>
                  <span className={`flex-1 ${
                    g.urgency === 'critical' ? 'text-red-300 font-semibold' :
                    g.urgency === 'warning' ? 'text-amber-300' :
                    'text-slate-400'
                  }`}>
                    {g.instruction}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* No action banner when in bear */}
      {regime === 'bear' && candidates.length > 0 && !tradingStatus && (
        <div className="mb-4 bg-amber-950/30 border border-amber-900/50 rounded-lg p-3">
          <div className="flex items-center gap-2">
            <ShieldCheck className="w-4 h-4 text-amber-400" />
            <span className="text-sm text-amber-300">
              Market is in <strong>BEAR</strong> regime — no new buys per strategy rules.
              {candidates.length} candidate{candidates.length !== 1 ? 's' : ''} on watchlist for when market recovers.
            </span>
          </div>
        </div>
      )}

      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
          <Target className="w-4 h-4" />
          {regime === 'bear' ? 'Watchlist' : 'Buy Signals'} — 🇮🇳 India
        </h2>
        <div className="flex items-center gap-3 text-xs text-slate-500">
          {date && <span>Signal date: {date}</span>}
          <span className="font-semibold text-emerald-400">{candidates.length} candidates</span>
        </div>
      </div>

      {candidates.length === 0 ? (
        <div className="text-center py-8 text-slate-500 text-sm">
          No buy signals. Run a scan after market close to check.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-slate-500 uppercase border-b border-slate-800">
                <th className="text-left py-2 px-2">#</th>
                <th className="text-left py-2 px-2">Ticker</th>
                <th className="text-right py-2 px-2">RS</th>
                <th className="text-center py-2 px-2">Tier</th>
                <th className="text-right py-2 px-2">Close</th>
                <th className="text-right py-2 px-2">Pivot</th>
                <th className="text-right py-2 px-2">Entry</th>
                <th className="text-right py-2 px-2">Stop</th>
                <th className="text-right py-2 px-2">Score</th>
              </tr>
            </thead>
            <tbody>
              {paged.map((c, i) => (
                <tr
                  key={c.ticker}
                  className={`border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors ${
                    c.already_held ? 'opacity-50' : ''
                  }`}
                >
                  <td className="py-2 px-2 text-slate-500 font-mono">{c.rank || page * PAGE_SIZE + i + 1}</td>
                  <td className="py-2 px-2">
                    <span className="font-semibold text-emerald-400">{c.ticker}</span>
                    {c.already_held && <span className="text-[10px] ml-1 text-slate-500">(held)</span>}
                  </td>
                  <td className="py-2 px-2 text-right font-mono">
                    <span className={c.rs_composite >= 0.9 ? 'text-emerald-300' : 'text-slate-300'}>
                      {c.rs_composite.toFixed(3)}
                    </span>
                  </td>
                  <td className="py-2 px-2 text-center">
                    <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
                      c.signal_tier === 'vcp' ? 'bg-emerald-900/50 text-emerald-300' :
                      c.signal_tier === 'darvas' ? 'bg-blue-900/50 text-blue-300' :
                      c.signal_tier === 'power_play' ? 'bg-purple-900/50 text-purple-300' :
                      'bg-slate-800 text-slate-400'
                    }`}>
                      {c.signal_tier || '—'}
                    </span>
                  </td>
                  <td className="py-2 px-2 text-right font-mono text-slate-300">
                    {c.close ? c.close.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '—'}
                  </td>
                  <td className="py-2 px-2 text-right font-mono text-slate-400">
                    {c.pivot_price ? c.pivot_price.toFixed(2) : '—'}
                  </td>
                  <td className="py-2 px-2 text-right font-mono text-emerald-300">
                    {c.max_entry_price ? '≤' + c.max_entry_price.toFixed(2) : '—'}
                  </td>
                  <td className="py-2 px-2 text-right font-mono text-red-400">
                    {c.stop_price ? c.stop_price.toFixed(2) : '—'}
                  </td>
                  <td className="py-2 px-2 text-right font-mono text-amber-400">
                    {c.score ? c.score.toFixed(1) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Pagination */}
      {candidates.length > PAGE_SIZE && (
        <div className="flex items-center justify-between mt-3 pt-2 border-t border-slate-800">
          <span className="text-xs text-slate-500">
            {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, candidates.length)} of {candidates.length}
          </span>
          <div className="flex items-center gap-1">
            <button
              disabled={page === 0}
              onClick={() => setPage(p => p - 1)}
              className="p-1 rounded hover:bg-slate-800 disabled:opacity-30 disabled:cursor-not-allowed text-slate-400"
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
            {Array.from({ length: totalPages }, (_, i) => (
              <button
                key={i}
                onClick={() => setPage(i)}
                className={`w-7 h-7 rounded text-xs font-mono ${
                  i === page ? 'bg-emerald-600/40 text-emerald-300' : 'text-slate-500 hover:bg-slate-800'
                }`}
              >
                {i + 1}
              </button>
            )).slice(Math.max(0, page - 2), page + 3)}
            <button
              disabled={page >= totalPages - 1}
              onClick={() => setPage(p => p + 1)}
              className="p-1 rounded hover:bg-slate-800 disabled:opacity-30 disabled:cursor-not-allowed text-slate-400"
            >
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}

      {/* Legend */}
      <div className="mt-3 text-[10px] text-slate-600 flex flex-wrap gap-3">
        <span><strong>Close</strong> = breakout day closing price</span>
        <span><strong>Pivot</strong> = breakout level (base high)</span>
        <span><strong>Entry</strong> = max price to pay (pivot + {cfg?.max_chase_pct?.toFixed(0) ?? 5}% chase limit)</span>
        <span><strong>Stop</strong> = hard stop-loss ({cfg?.stop_loss_pct?.toFixed(0) ?? 15}% below entry)</span>
      </div>
    </div>
  )
}
