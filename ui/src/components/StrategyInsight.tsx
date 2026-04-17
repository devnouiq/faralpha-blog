import { Info, ShieldCheck, Target, Activity } from 'lucide-react'
import type { RegimeData, SignalsResponse, Candidate } from '../types'

interface Props {
  market: string
  regime: RegimeData | null
  meta: Partial<SignalsResponse>
  candidates: Candidate[]
}

function RegimeExplainer({ regime }: { regime: RegimeData | null }) {
  if (!regime) return null

  const isBull = regime.regime === 'bull'
  const isRecovery = regime.regime === 'recovery'
  const isBear = regime.regime === 'bear'

  return (
    <div className="space-y-2">
      <h3 className="text-xs font-bold text-slate-400 uppercase flex items-center gap-1.5">
        <Activity className="w-3.5 h-3.5" />
        Market Regime — {isBull ? '🟢 Bull' : isRecovery ? '🟡 Recovery' : '🔴 Bear'}
      </h3>

      {isBull && (
        <div className="text-xs text-slate-400 space-y-1">
          <p className="text-emerald-400 font-semibold">✅ Full buying mode</p>
          <p>NIFTY 500 is above both 50-day and 200-day moving averages.
            Market breadth ({regime.breadth_pct?.toFixed(0)}% of stocks above MA200) confirms broad participation.
            The strategy actively generates buy signals for qualifying stocks.</p>
        </div>
      )}

      {isRecovery && (
        <div className="text-xs text-slate-400 space-y-1">
          <p className="text-amber-400 font-semibold">⚠️ Cautious buying — reduced exposure</p>
          <p>Market shows early recovery signs but hasn't confirmed a full bull trend.
            The strategy may generate limited buy signals with tighter risk controls.
            Watch for breadth improvement above 50%.</p>
        </div>
      )}

      {isBear && (
        <div className="text-xs text-slate-400 space-y-1">
          <p className="text-red-400 font-semibold">🛑 Sitting out — cash is king</p>
          <p>Benchmark is below its 200-day MA and breadth is weak ({regime.breadth_pct?.toFixed(0)}% of stocks above MA200).
            Historically, buying in this environment has negative expected returns.
            The strategy holds 100% cash and builds a watchlist for when conditions improve.</p>
          <p className="text-slate-500 italic">
            "The goal of a superior stock trader is not to buy every dip — it's to avoid the big losses
            and be fully invested when the trend turns." — Mark Minervini
          </p>
        </div>
      )}
    </div>
  )
}

export default function StrategyInsight({ market, regime, meta, candidates }: Props) {
  const cfg = meta.config
  // Always use the CURRENT regime from the dedicated regime endpoint.
  // meta.regime may be stale (from the signal generation date, not today).
  const isBear = regime?.regime === 'bear'

  return (
    <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3 flex items-center gap-2">
        <Info className="w-4 h-4" />
        Strategy Insight — 🇮🇳 India
      </h2>

      <div className="space-y-4">
          <RegimeExplainer regime={regime} />

          {/* Current Decision */}
          <div className="space-y-2">
            <h3 className="text-xs font-bold text-slate-400 uppercase flex items-center gap-1.5">
              {isBear ? <ShieldCheck className="w-3.5 h-3.5" /> : <Target className="w-3.5 h-3.5" />}
              Current Decision
            </h3>
            {isBear ? (
              <div className="bg-red-950/20 border border-red-900/30 rounded-lg p-2.5 text-xs text-slate-400 space-y-1">
                <p><strong className="text-red-400">Action: HOLD CASH</strong></p>
                <p>No new positions opened. Existing stops still monitored.</p>
                <p>{candidates.length} stocks pass SEPA screening and are on the watchlist. When the
                  regime flips to Bull or Recovery, the top-ranked candidates will generate buy signals.</p>
              </div>
            ) : candidates.length > 0 ? (
              <div className="bg-emerald-950/20 border border-emerald-900/30 rounded-lg p-2.5 text-xs text-slate-400 space-y-1">
                <p><strong className="text-emerald-400">Action: BUY top {meta.open_slots ?? '?'} candidates at next open</strong></p>
                <p>{candidates.length} stocks pass all Minervini SEPA criteria. Buy the highest-ranked
                  ones up to max {cfg?.max_positions ?? '?'} positions, with stops placed {cfg?.stop_loss_pct?.toFixed(0) ?? '?'}% below entry.</p>
              </div>
            ) : (
              <div className="bg-slate-800/30 border border-slate-700/50 rounded-lg p-2.5 text-xs text-slate-400">
                <p>No qualifying candidates right now. Run a full scan after market close to check.</p>
              </div>
            )}
          </div>
      </div>
    </div>
  )
}
