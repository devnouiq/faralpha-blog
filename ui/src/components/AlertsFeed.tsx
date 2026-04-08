import { Bell } from 'lucide-react'
import type { WsEvent } from '../types'

interface Props {
  alerts: WsEvent[]
}

function formatTime(iso: string) {
  return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function AlertItem({ event }: { event: WsEvent }) {
  const t = event.type
  const d = event.data

  if (t === 'buy_signal') {
    return (
      <div className="flex items-start gap-2 py-1.5 border-b border-slate-800/50">
        <span className="text-emerald-400 text-sm">🟢</span>
        <div className="flex-1 min-w-0">
          <div className="text-sm">
            <span className="font-semibold text-emerald-400">BUY</span>
            <span className="text-slate-300 ml-1">{d.ticker}</span>
            {d.rs_composite && (
              <span className="text-slate-500 ml-2 text-xs">RS={d.rs_composite.toFixed(3)}</span>
            )}
          </div>
        </div>
        <span className="text-xs text-slate-600 whitespace-nowrap">{formatTime(event.ts)}</span>
      </div>
    )
  }

  if (t === 'sell_signal') {
    return (
      <div className="flex items-start gap-2 py-1.5 border-b border-slate-800/50">
        <span className="text-red-400 text-sm">🔴</span>
        <div className="flex-1 min-w-0">
          <div className="text-sm">
            <span className="font-semibold text-red-400">SELL</span>
            <span className="text-slate-300 ml-1">{d.ticker}</span>
            <span className="text-red-300 ml-2 text-xs">
              {d.stop_type} stop @ {d.stop_price?.toFixed(2)} ({d.loss_pct?.toFixed(1)}%)
            </span>
          </div>
        </div>
        <span className="text-xs text-slate-600 whitespace-nowrap">{formatTime(event.ts)}</span>
      </div>
    )
  }

  if (t === 'scan_complete') {
    const nSignals = d.signals?.length ?? 0
    const nAlerts = d.stop_alerts?.length ?? 0
    return (
      <div className="flex items-start gap-2 py-1.5 border-b border-slate-800/50">
        <span className="text-blue-400 text-sm">📊</span>
        <div className="flex-1 min-w-0">
          <div className="text-sm text-slate-300">
            Scan complete — <span className="text-emerald-400 font-medium">{nSignals} signals</span>
            {nAlerts > 0 && <span className="text-red-400 font-medium ml-1">, {nAlerts} stop alerts</span>}
          </div>
        </div>
        <span className="text-xs text-slate-600 whitespace-nowrap">{formatTime(event.ts)}</span>
      </div>
    )
  }

  if (t === 'scan_progress') {
    return (
      <div className="flex items-start gap-2 py-1.5 border-b border-slate-800/50">
        <span className="text-amber-400 text-sm">⏳</span>
        <div className="text-sm text-slate-400">{d.message || d.step}</div>
        <span className="text-xs text-slate-600 whitespace-nowrap ml-auto">{formatTime(event.ts)}</span>
      </div>
    )
  }

  if (t === 'error') {
    return (
      <div className="flex items-start gap-2 py-1.5 border-b border-slate-800/50">
        <span className="text-red-400 text-sm">⚠️</span>
        <div className="text-sm text-red-300">{d.message}</div>
        <span className="text-xs text-slate-600 whitespace-nowrap ml-auto">{formatTime(event.ts)}</span>
      </div>
    )
  }

  return (
    <div className="flex items-start gap-2 py-1.5 border-b border-slate-800/50">
      <span className="text-slate-500 text-sm">ℹ️</span>
      <div className="text-sm text-slate-400">{t}: {JSON.stringify(d).slice(0, 100)}</div>
      <span className="text-xs text-slate-600 whitespace-nowrap ml-auto">{formatTime(event.ts)}</span>
    </div>
  )
}

export default function AlertsFeed({ alerts }: Props) {
  return (
    <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2 mb-3">
        <Bell className="w-4 h-4" />
        Live Alerts
        {alerts.length > 0 && (
          <span className="text-xs bg-slate-800 text-slate-400 px-1.5 py-0.5 rounded-full">{alerts.length}</span>
        )}
      </h2>

      <div className="max-h-64 overflow-y-auto">
        {alerts.length === 0 ? (
          <div className="text-center py-6 text-slate-500 text-sm">
            No alerts yet. Start the scanner or run a scan to see live events.
          </div>
        ) : (
          alerts.map((a, i) => <AlertItem key={`${a.ts}-${i}`} event={a} />)
        )}
      </div>
    </div>
  )
}
