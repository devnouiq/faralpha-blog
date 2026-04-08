import { useState } from 'react'
import { Briefcase, Plus, Trash2, AlertTriangle, TrendingDown, TrendingUp, Shield, ChevronDown, ChevronUp } from 'lucide-react'
import type { Position, StopAlert } from '../types'

interface Props {
  positions: Position[]
  stopAlerts: StopAlert[]
  market: string
  onAdd: (pos: { ticker: string; market: string; entry_date: string; entry_price: number; shares: number; notes?: string }) => void
  onRemove: (market: string, ticker: string) => void
}

export default function PositionsPanel({ positions, stopAlerts, market, onAdd, onRemove }: Props) {
  const [showForm, setShowForm] = useState(false)
  const [form, setForm] = useState({ ticker: '', entry_price: '', shares: '', notes: '' })
  const [expandedTicker, setExpandedTicker] = useState<string | null>(null)

  const filtered = positions.filter(p => p.market === market)
  const alertTickers = new Set(stopAlerts.map(a => a.ticker))

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!form.ticker || !form.entry_price || !form.shares) return
    onAdd({
      ticker: form.ticker.toUpperCase(),
      market,
      entry_date: new Date().toISOString().slice(0, 10),
      entry_price: parseFloat(form.entry_price),
      shares: parseFloat(form.shares),
      notes: form.notes,
    })
    setForm({ ticker: '', entry_price: '', shares: '', notes: '' })
    setShowForm(false)
  }

  return (
    <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
          <Briefcase className="w-4 h-4" />
          Tracked Positions
          {filtered.length > 0 && (
            <span className="text-emerald-400 font-mono">({filtered.length})</span>
          )}
        </h2>
        <button
          onClick={() => setShowForm(!showForm)}
          className="flex items-center gap-1 px-2 py-1 bg-slate-800 hover:bg-slate-700 rounded text-xs text-slate-300 transition-colors"
        >
          <Plus className="w-3.5 h-3.5" />
          Add
        </button>
      </div>

      {/* Stop alerts banner */}
      {stopAlerts.filter(a => a.market === market).length > 0 && (
        <div className="bg-red-950/50 border border-red-800/50 rounded-lg p-3 mb-3">
          <div className="flex items-center gap-2 text-red-400 text-sm font-semibold mb-1">
            <AlertTriangle className="w-4 h-4" />
            Stop Alerts
          </div>
          {stopAlerts.filter(a => a.market === market).map(a => (
            <div key={a.ticker} className="text-xs text-red-300 ml-6">
              <strong>{a.ticker}</strong> — {a.stop_type} stop breached at {a.stop_price.toFixed(2)}{' '}
              (current: {a.current_price.toFixed(2)}, {a.loss_pct.toFixed(1)}%)
            </div>
          ))}
        </div>
      )}

      {/* Add form */}
      {showForm && (
        <form onSubmit={handleSubmit} className="bg-slate-800/50 rounded-lg p-3 mb-3 space-y-2">
          <div className="grid grid-cols-3 gap-2">
            <input
              placeholder="Ticker"
              value={form.ticker}
              onChange={e => setForm({ ...form, ticker: e.target.value })}
              className="bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-emerald-500"
            />
            <input
              placeholder="Entry Price"
              type="number"
              step="0.01"
              value={form.entry_price}
              onChange={e => setForm({ ...form, entry_price: e.target.value })}
              className="bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-emerald-500"
            />
            <input
              placeholder="Shares"
              type="number"
              value={form.shares}
              onChange={e => setForm({ ...form, shares: e.target.value })}
              className="bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-emerald-500"
            />
          </div>
          <div className="flex gap-2">
            <input
              placeholder="Notes (optional)"
              value={form.notes}
              onChange={e => setForm({ ...form, notes: e.target.value })}
              className="flex-1 bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-emerald-500"
            />
            <button type="submit" className="px-3 py-1.5 bg-emerald-600 hover:bg-emerald-500 rounded text-sm font-medium">
              Add
            </button>
          </div>
        </form>
      )}

      {/* Positions table */}
      {filtered.length === 0 ? (
        <div className="text-center py-6 text-slate-500 text-sm">
          No tracked positions for {market === 'india' ? 'India' : 'US'}.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-slate-500 uppercase border-b border-slate-800">
                <th className="text-left py-2 px-2">Ticker</th>
                <th className="text-right py-2 px-2">Entry</th>
                <th className="text-right py-2 px-2">Current</th>
                <th className="text-right py-2 px-2">High</th>
                <th className="text-right py-2 px-2">P&L</th>
                <th className="text-right py-2 px-2">Sell @</th>
                <th className="text-right py-2 px-2">Room</th>
                <th className="text-center py-2 px-2"></th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(p => {
                const isAlert = alertTickers.has(p.ticker)
                const stopDist = p.stop_distance_pct ?? 0
                const stopType = p.stop_type ?? 'entry'
                const isTrailing = stopType === 'trailing'
                return (
                  <>
                    <tr
                      key={p.ticker}
                      className={`border-b border-slate-800/50 transition-colors cursor-pointer ${
                        isAlert ? 'bg-red-950/20' : 'hover:bg-slate-800/30'
                      }`}
                      onClick={() => setExpandedTicker(expandedTicker === p.ticker ? null : p.ticker)}
                    >
                      <td className="py-2 px-2 font-semibold text-slate-200">
                        {isAlert && <AlertTriangle className="w-3.5 h-3.5 text-red-400 inline mr-1" />}
                        {p.ticker}
                        <span className="ml-1 text-slate-600">
                          {expandedTicker === p.ticker ? <ChevronUp className="w-3 h-3 inline" /> : <ChevronDown className="w-3 h-3 inline" />}
                        </span>
                      </td>
                      <td className="py-2 px-2 text-right font-mono text-slate-400">
                        {p.entry_price.toFixed(2)}
                      </td>
                      <td className="py-2 px-2 text-right font-mono text-slate-300">
                        {p.current_price?.toFixed(2) ?? '—'}
                      </td>
                      <td className="py-2 px-2 text-right font-mono text-slate-500">
                        {p.highest_price?.toFixed(2) ?? '—'}
                      </td>
                      <td className={`py-2 px-2 text-right font-mono font-semibold ${
                        (p.pnl_pct ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'
                      }`}>
                        {p.pnl_pct != null ? `${p.pnl_pct >= 0 ? '+' : ''}${p.pnl_pct.toFixed(1)}%` : '—'}
                      </td>
                      <td className="py-2 px-2 text-right">
                        <span className="font-mono text-amber-400 font-semibold">
                          {p.active_stop?.toFixed(2) ?? '—'}
                        </span>
                        <span className={`ml-1 text-[10px] px-1 py-0.5 rounded ${
                          isTrailing
                            ? 'bg-blue-900/50 text-blue-300'
                            : 'bg-slate-800 text-slate-400'
                        }`}>
                          {isTrailing ? 'TRAIL' : 'HARD'}
                        </span>
                      </td>
                      <td className={`py-2 px-2 text-right font-mono text-xs ${
                        stopDist < 3 ? 'text-red-400 font-semibold' : stopDist < 6 ? 'text-amber-400' : 'text-slate-400'
                      }`}>
                        {stopDist > 0 ? `${stopDist.toFixed(1)}%` : '—'}
                      </td>
                      <td className="py-2 px-2 text-center">
                        <button
                          onClick={(e) => { e.stopPropagation(); onRemove(p.market, p.ticker); }}
                          className="text-slate-500 hover:text-red-400 transition-colors"
                          title="Remove position"
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </td>
                    </tr>
                    {/* Expanded exit detail row */}
                    {expandedTicker === p.ticker && (
                      <tr key={`${p.ticker}-detail`} className="bg-slate-800/40">
                        <td colSpan={8} className="py-2 px-3">
                          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
                            <div>
                              <span className="text-slate-500">Hard Stop</span>
                              <div className="font-mono text-slate-300">
                                {p.entry_stop?.toFixed(2) ?? '—'}
                                <span className="text-slate-500 ml-1">({((p.stop_loss_pct ?? 0.12) * 100).toFixed(0)}% below entry)</span>
                              </div>
                            </div>
                            <div>
                              <span className="text-slate-500">Trailing Stop</span>
                              <div className="font-mono text-blue-300">
                                {p.trail_stop?.toFixed(2) ?? '—'}
                                <span className="text-slate-500 ml-1">({((p.trailing_stop_pct ?? 0.18) * 100).toFixed(0)}% below high)</span>
                              </div>
                            </div>
                            <div>
                              <span className="text-slate-500">Gain from Entry</span>
                              <div className={`font-mono ${(p.gain_from_entry_pct ?? 0) > 0 ? 'text-emerald-400' : 'text-slate-400'}`}>
                                {p.gain_from_entry_pct != null ? `${p.gain_from_entry_pct >= 0 ? '+' : ''}${p.gain_from_entry_pct.toFixed(1)}%` : '—'}
                                <span className="text-slate-500 ml-1">(peak run)</span>
                              </div>
                            </div>
                            <div>
                              <span className="text-slate-500">Exit Strategy</span>
                              <div className="text-slate-300">
                                {isTrailing ? (
                                  <span className="flex items-center gap-1">
                                    <TrendingDown className="w-3 h-3 text-blue-400" />
                                    Trailing stop active — sell at {p.active_stop?.toFixed(2)}
                                  </span>
                                ) : (
                                  <span className="flex items-center gap-1">
                                    <Shield className="w-3 h-3 text-amber-400" />
                                    Hard stop — sell at {p.active_stop?.toFixed(2)}
                                  </span>
                                )}
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </>
                )
              })}
            </tbody>
          </table>
          {/* Legend */}
          <div className="flex gap-4 mt-2 text-[10px] text-slate-500 px-2">
            <span><span className="text-amber-400">Sell @</span> = active stop price (higher of hard or trailing)</span>
            <span><span className="text-blue-300">TRAIL</span> = trailing stop ({((filtered[0]?.trailing_stop_pct ?? 0.18) * 100).toFixed(0)}% from peak)</span>
            <span>Room = distance from current price to stop</span>
          </div>
        </div>
      )}
    </div>
  )
}
