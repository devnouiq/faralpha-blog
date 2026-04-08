import { useState, useEffect } from 'react'
import { Timer, Play, Square, Clock, Calendar, Info } from 'lucide-react'
import type { ScannerState, ScheduleInfo } from '../types'

interface Props {
  scanner: ScannerState | null
  schedule: ScheduleInfo | null
  busy: boolean
  onStartDaily: () => void
  onStartInterval: (interval: number, market: string) => void
  onStop: () => void
}

export default function ScannerControl({ scanner, schedule, busy, onStartDaily, onStartInterval, onStop }: Props) {
  const [mode, setMode] = useState<'daily' | 'interval'>('daily')
  const [interval, setInterval_] = useState(60)
  const [scanMarket, setScanMarket] = useState('both')
  const running = scanner?.running ?? false

  // Live countdown
  const [countdown, setCountdown] = useState<Record<string, string>>({})

  useEffect(() => {
    if (!schedule) return
    const tick = () => {
      const now = Date.now()
      const result: Record<string, string> = {}
      const s = schedule.india
      if (s) {
        const diff = Math.max(0, new Date(s.utc).getTime() - now)
        const h = Math.floor(diff / 3600000)
        const m = Math.floor((diff % 3600000) / 60000)
        const sec = Math.floor((diff % 60000) / 1000)
        result.india = h > 0 ? `${h}h ${m}m` : `${m}m ${sec}s`
      }
      setCountdown(result)
    }
    tick()
    const id = setInterval(tick, 1000)
    return () => clearInterval(id)
  }, [schedule])

  return (
    <div className="bg-slate-900 rounded-xl border border-slate-800 p-4">
      <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3 flex items-center gap-2">
        <Timer className="w-4 h-4" />
        Auto Scanner
      </h2>

      {/* How it works — always visible when stopped */}
      {!running && (
        <div className="bg-blue-950/30 border border-blue-900/50 rounded-lg p-3 mb-3">
          <div className="flex items-start gap-2">
            <Info className="w-4 h-4 text-blue-400 mt-0.5 flex-shrink-0" />
            <div className="text-xs text-blue-200/80 leading-relaxed">
              <strong>Daily mode (recommended):</strong> Auto-scans once after market closes — 
              India at 4:30 PM IST. You get a real-time alert when buy candidates 
              appear or stops are hit. Act at next day&apos;s market open.
            </div>
          </div>
        </div>
      )}

      {/* Schedule preview */}
      {schedule && (
        <div className="mb-3">
          <div className="bg-slate-800/50 rounded-lg p-2.5">
            <div className="flex items-center gap-1.5 mb-1">
              <span className="text-sm">🇮🇳</span>
              <span className="text-xs text-slate-400">India — 4:30 PM IST</span>
              {running && (
                <span className="text-[10px] bg-emerald-900/50 text-emerald-400 px-1.5 py-0.5 rounded ml-auto">NEXT</span>
              )}
            </div>
            <div className="text-sm font-mono text-slate-200">{schedule.india.ist}</div>
            <div className="text-xs text-slate-500">{schedule.india.date}</div>
            {running && (
              <div className="text-xs font-mono text-amber-400 mt-1">
                <Clock className="w-3 h-3 inline mr-1" />
                {countdown.india || schedule.india.countdown}
              </div>
            )}
            {scanner?.scans_today?.india && (
              <div className="text-[10px] text-emerald-500 mt-1">✓ Scanned today</div>
            )}
          </div>
        </div>
      )}

      {/* Status line */}
      <div className="flex items-center gap-2 mb-3">
        <div className={`w-2.5 h-2.5 rounded-full ${running ? 'bg-emerald-400 animate-pulse' : 'bg-slate-600'}`} />
        <span className={`text-sm font-medium ${running ? 'text-emerald-400' : 'text-slate-400'}`}>
          {running
            ? scanner?.mode === 'daily'
              ? 'Daily schedule active'
              : `Running every ${scanner?.interval_minutes}m`
            : 'Stopped'}
        </span>
        {running && scanner?.last_run && (
          <span className="text-xs text-slate-500 ml-auto">
            Last: {new Date(scanner.last_run).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            {scanner.last_run_market ? ` (${scanner.last_run_market})` : ''}
          </span>
        )}
      </div>

      {/* Mode selector — only when stopped */}
      {!running && (
        <div className="mb-3">
          <div className="flex rounded-lg overflow-hidden border border-slate-700 mb-2">
            <button
              onClick={() => setMode('daily')}
              className={`flex-1 px-3 py-1.5 text-xs font-medium transition-colors ${
                mode === 'daily'
                  ? 'bg-emerald-600/30 text-emerald-400 border-r border-slate-700'
                  : 'bg-slate-800 text-slate-400 border-r border-slate-700 hover:bg-slate-750'
              }`}
            >
              <Calendar className="w-3 h-3 inline mr-1" />
              Daily Auto
            </button>
            <button
              onClick={() => setMode('interval')}
              className={`flex-1 px-3 py-1.5 text-xs font-medium transition-colors ${
                mode === 'interval'
                  ? 'bg-emerald-600/30 text-emerald-400'
                  : 'bg-slate-800 text-slate-400 hover:bg-slate-750'
              }`}
            >
              <Timer className="w-3 h-3 inline mr-1" />
              Custom Interval
            </button>
          </div>

          {mode === 'interval' && (
            <div className="flex gap-2">
              <div className="flex-1">
                <label className="text-xs text-slate-500 block mb-1">Interval</label>
                <select
                  value={interval}
                  onChange={e => setInterval_(Number(e.target.value))}
                  className="w-full bg-slate-800 border border-slate-700 rounded-lg px-2.5 py-1.5 text-sm text-slate-200 focus:outline-none focus:ring-1 focus:ring-emerald-500"
                >
                  <option value={60}>1 hour</option>
                  <option value={120}>2 hours</option>
                  <option value={240}>4 hours</option>
                  <option value={480}>8 hours</option>
                </select>
              </div>
              <div className="flex-1">
                <label className="text-xs text-slate-500 block mb-1">Market</label>
                <select
                  value={scanMarket}
                  onChange={e => setScanMarket(e.target.value)}
                  className="w-full bg-slate-800 border border-slate-700 rounded-lg px-2.5 py-1.5 text-sm text-slate-200 focus:outline-none focus:ring-1 focus:ring-emerald-500"
                >
                  <option value="india">India</option>
                </select>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Toggle button */}
      <button
        disabled={busy}
        onClick={() =>
          running
            ? onStop()
            : mode === 'daily'
              ? onStartDaily()
              : onStartInterval(interval, scanMarket)
        }
        className={`w-full flex items-center justify-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold transition-colors disabled:opacity-50 ${
          running
            ? 'bg-red-600/80 hover:bg-red-500 text-white'
            : 'bg-emerald-600/80 hover:bg-emerald-500 text-white'
        }`}
      >
        {running ? <Square className="w-4 h-4" /> : <Play className="w-4 h-4" />}
        {running
          ? 'Stop Scanner'
          : mode === 'daily'
            ? 'Enable Daily Scanner'
            : 'Start Scanner'
        }
      </button>
    </div>
  )
}
