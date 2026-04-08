import { BarChart3 } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts'
import type { BacktestData } from '../types'

interface Props {
  data: BacktestData | null
  market: string
}

export default function BacktestPanel({ data, market }: Props) {
  if (!data || data.error) {
    return (
      <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2 mb-3">
          <BarChart3 className="w-4 h-4" />
          Backtest Performance
        </h2>
        <div className="text-center py-6 text-slate-500 text-sm">
          No backtest data available. Run the full pipeline to generate backtests.
        </div>
      </div>
    )
  }

  const annual = data.annual ?? []
  const trades = data.trades_recent ?? []
  const equity = data.equity_recent ?? []

  // Summary stats (from latest equity point)
  const latestEquity = equity.length > 0 ? equity[0] : null

  return (
    <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2 mb-3">
        <BarChart3 className="w-4 h-4" />
        Backtest — 🇮🇳 India
      </h2>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Annual returns chart */}
        {annual.length > 0 && (
          <div>
            <h3 className="text-xs text-slate-500 mb-2">Annual Returns (%)</h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={annual} margin={{ top: 5, right: 5, bottom: 5, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis
                  dataKey="year"
                  tick={{ fill: '#64748b', fontSize: 10 }}
                  axisLine={{ stroke: '#334155' }}
                />
                <YAxis
                  tick={{ fill: '#64748b', fontSize: 10 }}
                  axisLine={{ stroke: '#334155' }}
                  tickFormatter={(v) => `${v}%`}
                />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px' }}
                  labelStyle={{ color: '#94a3b8' }}
                  formatter={(value: number) => [`${value.toFixed(1)}%`, 'Return']}
                />
                <Bar dataKey="return_pct" radius={[2, 2, 0, 0]}>
                  {annual.map((entry, index) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={entry.return_pct >= 0 ? '#34d399' : '#f87171'}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Recent trades */}
        <div>
          <h3 className="text-xs text-slate-500 mb-2">Recent Trades</h3>
          {latestEquity && (
            <div className="grid grid-cols-3 gap-2 mb-3">
              <div className="bg-slate-800/50 rounded p-2 text-center">
                <div className="text-xs text-slate-500">Equity</div>
                <div className="text-sm font-mono text-emerald-400">
                  {(latestEquity.equity / 1e6).toFixed(1)}M
                </div>
              </div>
              <div className="bg-slate-800/50 rounded p-2 text-center">
                <div className="text-xs text-slate-500">Positions</div>
                <div className="text-sm font-mono text-slate-200">{latestEquity.n_positions}</div>
              </div>
              <div className="bg-slate-800/50 rounded p-2 text-center">
                <div className="text-xs text-slate-500">Exposure</div>
                <div className="text-sm font-mono text-slate-200">{latestEquity.exposure_pct.toFixed(0)}%</div>
              </div>
            </div>
          )}

          <div className="max-h-[152px] overflow-y-auto">
            {trades.length === 0 ? (
              <div className="text-sm text-slate-500 text-center py-4">No trades</div>
            ) : (
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-slate-500 uppercase border-b border-slate-800">
                    <th className="text-left py-1 px-1">Ticker</th>
                    <th className="text-right py-1 px-1">P&L</th>
                    <th className="text-left py-1 px-1">Exit</th>
                    <th className="text-right py-1 px-1">Days</th>
                  </tr>
                </thead>
                <tbody>
                  {trades.slice(0, 10).map((t, i) => (
                    <tr key={i} className="border-b border-slate-800/30">
                      <td className="py-1 px-1 font-medium text-slate-300">{t.ticker}</td>
                      <td className={`py-1 px-1 text-right font-mono ${t.pnl_pct >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                        {t.pnl_pct >= 0 ? '+' : ''}{t.pnl_pct.toFixed(1)}%
                      </td>
                      <td className="py-1 px-1 text-slate-500">{t.exit_reason}</td>
                      <td className="py-1 px-1 text-right text-slate-500">{t.hold_days}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
