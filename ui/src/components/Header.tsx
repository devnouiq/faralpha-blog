import { Activity, Wifi, WifiOff, Loader2, Bell, BellOff, BellRing } from 'lucide-react'
import KiteLogin from './KiteLogin'

interface Props {
  wsConnected: boolean
  busy: boolean
  notificationPermission: 'default' | 'granted' | 'denied'
  onRequestNotifications: () => void
}

export default function Header({ wsConnected, busy, notificationPermission, onRequestNotifications }: Props) {
  const connClass = wsConnected ? 'text-emerald-400' : 'text-red-400'

  const NotifIcon = notificationPermission === 'granted' ? BellRing
    : notificationPermission === 'denied' ? BellOff
    : Bell

  const notifColor = notificationPermission === 'granted' ? 'text-emerald-400'
    : notificationPermission === 'denied' ? 'text-red-400'
    : 'text-slate-400'

  const notifLabel = notificationPermission === 'granted' ? 'Notifications on'
    : notificationPermission === 'denied' ? 'Notifications blocked'
    : 'Enable notifications'

  return (
    <header className="bg-slate-900 border-b border-slate-800 px-4 py-3 flex items-center justify-between">
      <div className="flex items-center gap-3">
        <Activity className="w-6 h-6 text-emerald-400" />
        <h1 className="text-lg font-bold tracking-tight">
          <span className="text-emerald-400">FarAlpha</span>
          <span className="text-slate-300 ml-1 font-normal">Quant Trader</span>
        </h1>
      </div>
      <div className="flex items-center gap-3 text-sm">
        {busy && (
          <span className="flex items-center gap-1.5 text-amber-400">
            <Loader2 className="w-4 h-4 animate-spin" />
            Working...
          </span>
        )}
        <KiteLogin />
        <button
          onClick={notificationPermission !== 'granted' ? onRequestNotifications : undefined}
          title={notifLabel}
          className={`flex items-center gap-1.5 px-2 py-1 rounded ${notifColor} ${
            notificationPermission !== 'granted'
              ? 'hover:bg-slate-800 cursor-pointer border border-slate-700'
              : 'cursor-default'
          }`}
        >
          <NotifIcon className="w-4 h-4" />
          <span className="text-xs hidden sm:inline">{notifLabel}</span>
        </button>
        <span className={'flex items-center gap-1.5 ' + connClass}>
          {wsConnected ? <Wifi className="w-4 h-4" /> : <WifiOff className="w-4 h-4" />}
          {wsConnected ? 'Live' : 'Disconnected'}
        </span>
      </div>
    </header>
  )
}
