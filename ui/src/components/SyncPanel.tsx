import { RefreshCw, Play, Zap, Database, Calendar, AlertTriangle, Loader2, RotateCw } from 'lucide-react'
import type { MarketStatus } from '../types'

interface Props {
  market: string
  status: MarketStatus | null
  busy: boolean
  progressStep?: string | null
  progressMessage?: string | null
  onSync: (market: string) => void
  onPipeline: (market: string) => void
  onScan: (market: string) => void
  onUniverse: () => void
}

export default function SyncPanel({ market, status, busy, progressStep, progressMessage, onSync, onPipeline, onScan, onUniverse }: Props) {
  const lastDate = status?.last_price_date
  const nTickers = status?.n_tickers ?? 0
  const nActive = status?.n_active_tickers ?? nTickers
  const nDelisted = status?.n_delisted_tickers ?? 0
  const nRows = status?.n_price_rows ?? 0
  const daysStale = status?.days_stale ?? null
  const freshness = status?.freshness ?? 'unknown'

  const freshnessColor = freshness === 'fresh'
    ? 'text-emerald-400'
    : freshness === 'stale'
      ? 'text-amber-400'
      : freshness === 'very_stale'
        ? 'text-red-400'
        : 'text-slate-400'

  const freshnessBg = freshness === 'fresh'
    ? 'bg-emerald-950/30 border-emerald-900/50'
    : freshness === 'stale'
      ? 'bg-amber-950/30 border-amber-900/50'
      : freshness === 'very_stale'
        ? 'bg-red-950/30 border-red-900/50'
        : 'bg-slate-800/50 border-slate-700'

  const staleLabel = daysStale === null
    ? 'No data'
    : daysStale === 0
      ? 'Up to date'
      : daysStale === 1
        ? 'Fresh — yesterday\'s close'
        : `${daysStale} days old`

  return (
    <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3">
        Data &amp; Pipeline
      </h2>

      {/* Data freshness banner */}
      <div className={`rounded-lg border p-2.5 mb-3 ${freshnessBg}`}>
        <div className="flex items-center gap-2">
          {(freshness === 'stale' || freshness === 'very_stale') && (
            <AlertTriangle className={`w-4 h-4 flex-shrink-0 ${freshnessColor}`} />
          )}
          <div className="flex-1">
            <div className={`text-sm font-semibold ${freshnessColor}`}>
              {staleLabel}
            </div>
            <div className="text-xs text-slate-500">
              Last sync: {lastDate ?? 'never'} · All data is <strong>daily</strong> (end-of-day)
            </div>
          </div>
          {freshness !== 'fresh' && (
            <button
              disabled={busy}
              onClick={() => onScan(market)}
              className="px-3 py-1.5 bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 rounded-lg text-xs font-semibold text-white transition-colors flex-shrink-0"
            >
              <Zap className="w-3 h-3 inline mr-1" />
              Update Now
            </button>
          )}
        </div>
      </div>

      {/* Pipeline progress */}
      {busy && progressMessage && (
        <div className="rounded-lg border border-blue-900/50 bg-blue-950/30 p-2.5 mb-3">
          <div className="flex items-center gap-2">
            <Loader2 className="w-4 h-4 text-blue-400 animate-spin flex-shrink-0" />
            <div className="flex-1 min-w-0">
              <div className="text-sm font-semibold text-blue-300">{progressMessage}</div>
              <div className="mt-1.5 h-1.5 bg-slate-800 rounded-full overflow-hidden">
                <div
                  className="h-full bg-blue-500 rounded-full transition-all duration-500"
                  style={{ width: progressStep === 'sync' ? '33%' : progressStep === 'pipeline' ? '66%' : progressStep === 'signals' ? '90%' : progressStep === 'cleanup' ? '30%' : progressStep === 'universe' ? '70%' : '100%' }}
                />
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-2 gap-3 mb-4">
        <div className="bg-slate-800/50 rounded-lg p-2.5 text-center">
          <Database className="w-4 h-4 text-slate-500 mx-auto mb-1" />
          <div className="text-xs text-slate-400">Active Tickers</div>
          <div className="text-sm font-mono text-slate-200">{nActive.toLocaleString()}</div>
        </div>
        <div className="bg-slate-800/50 rounded-lg p-2.5 text-center">
          <Database className="w-4 h-4 text-slate-500 mx-auto mb-1" />
          <div className="text-xs text-slate-400">Price Rows</div>
          <div className="text-sm font-mono text-slate-200">{nRows.toLocaleString()}</div>
        </div>
      </div>

      {/* Action buttons */}
      <div className="flex gap-2">
        <button
          disabled={busy}
          onClick={() => onSync(market)}
          className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 bg-slate-800 hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg text-sm font-medium transition-colors"
          title="Download latest daily prices from Yahoo Finance"
        >
          <RefreshCw className="w-3.5 h-3.5" />
          Sync Prices
        </button>
        <button
          disabled={busy}
          onClick={() => onPipeline(market)}
          className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 bg-slate-800 hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg text-sm font-medium transition-colors"
          title="Run features → RS rank → patterns → regime → signals"
        >
          <Play className="w-3.5 h-3.5" />
          Run Pipeline
        </button>
        <button
          disabled={busy}
          onClick={() => onScan(market)}
          className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg text-sm font-semibold transition-colors"
          title="Sync prices + run full pipeline + check stops"
        >
          <Zap className="w-3.5 h-3.5" />
          Full Scan
        </button>
      </div>

      {/* Monthly maintenance */}
      <div className="mt-2">
        <button
          disabled={busy}
          onClick={onUniverse}
          className="w-full flex items-center justify-center gap-1.5 px-3 py-2 bg-violet-900/40 hover:bg-violet-800/50 border border-violet-700/40 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg text-sm font-medium text-violet-300 transition-colors"
          title="Re-download stock listings from exchanges, purge delisted stocks & their data (run once a month)"
        >
          <RotateCw className="w-3.5 h-3.5" />
          Refresh Universe &amp; Purge Delisted
          <span className="text-[10px] text-violet-400/60 ml-1">(monthly)</span>
        </button>
      </div>
    </div>
  )
}
