import { TrendingUp, TrendingDown, Shield } from 'lucide-react'
import type { RegimeData } from '../types'

interface Props {
  india: RegimeData | null
}

function RegimeBadge({ data }: { data: RegimeData | null }) {
  if (!data || data.error) {
    return (
      <div className="bg-slate-800/50 rounded-lg p-3 text-center">
        <div className="text-sm text-slate-500">No data</div>
      </div>
    )
  }

  const isBull = data.regime === 'bull'
  const isRecovery = data.regime === 'recovery'
  const color = isBull ? 'emerald' : isRecovery ? 'amber' : 'red'
  const Icon = isBull ? TrendingUp : isRecovery ? Shield : TrendingDown

  return (
    <div className={`bg-${color}-950/30 border border-${color}-800/50 rounded-lg p-3`}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <Icon className={`w-5 h-5 text-${color}-400`} />
          <span className={`text-sm font-bold uppercase text-${color}-400`}>
            {data.regime}
          </span>
        </div>
        <span className="text-xs text-slate-500">{data.date}</span>
      </div>
      <div className="grid grid-cols-2 gap-2 text-xs">
        {data.breadth_pct !== null && (
          <div>
            <span className="text-slate-500">Breadth:</span>
            <span className={`ml-1 font-mono ${data.breadth_pct > 50 ? 'text-emerald-400' : 'text-red-400'}`}>
              {data.breadth_pct.toFixed(1)}%
            </span>
          </div>
        )}
        {data.strength !== null && (
          <div>
            <span className="text-slate-500">Strength:</span>
            <span className="ml-1 font-mono text-slate-300">{data.strength.toFixed(2)}</span>
          </div>
        )}
        {data.benchmark !== null && (
          <div>
            <span className="text-slate-500">Bench:</span>
            <span className="ml-1 font-mono text-slate-300">{data.benchmark.toLocaleString()}</span>
          </div>
        )}
        {data.benchmark_ma200 !== null && (
          <div>
            <span className="text-slate-500">MA200:</span>
            <span className="ml-1 font-mono text-slate-300">{data.benchmark_ma200.toLocaleString()}</span>
          </div>
        )}
      </div>
    </div>
  )
}

export default function RegimeCard({ india }: Props) {
  return (
    <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3">
        Market Regime
      </h2>
      <div className="space-y-3">
        <div>
          <div className="text-xs text-slate-500 mb-1">🇮🇳 India (Nifty 500)</div>
          <RegimeBadge data={india} />
        </div>
      </div>
    </div>
  )
}
