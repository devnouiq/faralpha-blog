import { useCallback, useEffect, useRef, useState } from 'react'
import type { WsEvent } from '../types'

/**
 * Browser push notification hook for trading alerts.
 * Uses the Web Notifications API (works in Chrome, Edge, Firefox, Safari).
 *
 * - Requests permission on mount (once)
 * - Provides notify() to fire a notification for any WsEvent
 * - Handles buy_signal, sell_signal, and scan_complete events
 * - Clicks on the notification focus the app window
 */

type Permission = 'default' | 'granted' | 'denied'

export function useNotifications() {
  const [permission, setPermission] = useState<Permission>(
    typeof Notification !== 'undefined' ? (Notification.permission as Permission) : 'denied'
  )
  const supported = typeof Notification !== 'undefined'
  const activeNotifs = useRef<Set<Notification>>(new Set())

  // Re-check permission periodically (browser may have changed it)
  useEffect(() => {
    if (!supported) return
    const check = () => setPermission(Notification.permission as Permission)
    // Check on visibility change (user may have changed in browser settings)
    document.addEventListener('visibilitychange', check)
    return () => document.removeEventListener('visibilitychange', check)
  }, [supported])

  // Cleanup open notifications on unmount
  useEffect(() => {
    return () => {
      activeNotifs.current.forEach((n) => n.close())
      activeNotifs.current.clear()
    }
  }, [])

  const requestPermission = useCallback(async () => {
    if (!supported) return 'denied' as Permission
    const p = await Notification.requestPermission()
    setPermission(p as Permission)
    return p as Permission
  }, [supported])

  const notify = useCallback(
    (event: WsEvent) => {
      if (!supported || permission !== 'granted') return

      let title = ''
      let body = ''
      let icon = ''
      let tag = ''

      const d = event.data

      switch (event.type) {
        case 'buy_signal': {
          const ticker = d?.ticker || 'Unknown'
          title = `🟢 BUY Signal: ${ticker}`
          body = `RS=${d?.rs_composite?.toFixed(3) ?? '—'} | Entry ≤${d?.max_entry_price?.toFixed(2) ?? '—'} | Stop ${d?.stop_price?.toFixed(2) ?? '—'}`
          tag = `buy-${ticker}-${event.ts}`
          break
        }
        case 'sell_signal': {
          const ticker = d?.ticker || 'Unknown'
          title = `🔴 STOP HIT: ${ticker}`
          body = `${d?.stop_type ?? ''} stop @ ${d?.stop_price?.toFixed(2) ?? '—'} (${d?.loss_pct?.toFixed(1) ?? '—'}%) — sell at next open`
          tag = `sell-${ticker}-${event.ts}`
          break
        }
        case 'scan_complete': {
          const nSig = d?.signals?.length ?? 0
          const nStop = d?.stop_alerts?.length ?? 0
          if (nSig === 0 && nStop === 0) return // skip empty scans
          const mkt = d?.market || ''
          title = `📊 Scan Complete (${mkt.toUpperCase()})`
          body = `${nSig} buy signal${nSig !== 1 ? 's' : ''}${nStop > 0 ? `, ${nStop} stop alert${nStop !== 1 ? 's' : ''}` : ''}`
          tag = `scan-${mkt}-${event.ts}`
          break
        }
        default:
          return // Only notify for trading-relevant events
      }

      const n = new Notification(title, {
        body,
        icon: icon || '/favicon.ico',
        tag,
        requireInteraction: event.type === 'sell_signal', // keep sell alerts until dismissed
      })

      activeNotifs.current.add(n)

      n.onclick = () => {
        window.focus()
        n.close()
      }

      n.onclose = () => {
        activeNotifs.current.delete(n)
      }

      // Auto-close buy/scan notifications after 15s
      if (event.type !== 'sell_signal') {
        setTimeout(() => n.close(), 15_000)
      }
    },
    [supported, permission]
  )

  return {
    supported,
    permission,
    enabled: permission === 'granted',
    requestPermission,
    notify,
  }
}
