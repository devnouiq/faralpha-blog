import { useState, useEffect, useCallback } from 'react'
import { LogIn, LogOut, User, Loader2, ExternalLink } from 'lucide-react'
import * as api from '../api'

interface Props {
  onLoginChange?: (loggedIn: boolean) => void
}

export default function KiteLogin({ onLoginChange }: Props) {
  const [loggedIn, setLoggedIn] = useState<boolean | null>(null)
  const [userName, setUserName] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [showTokenInput, setShowTokenInput] = useState(false)
  const [requestToken, setRequestToken] = useState('')

  const checkStatus = useCallback(async () => {
    try {
      const res = await api.fetchKiteAuthStatus()
      setLoggedIn(res.logged_in)
      setUserName(res.user_name)
      onLoginChange?.(res.logged_in)
    } catch {
      setLoggedIn(false)
      setUserName(null)
    }
  }, [onLoginChange])

  useEffect(() => { checkStatus() }, [checkStatus])

  // Check URL for request_token (Zerodha redirects back with it)
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const token = params.get('request_token')
    const status = params.get('status')
    if (token && status === 'success') {
      // Clean URL
      window.history.replaceState({}, '', window.location.pathname)
      handleTokenExchange(token)
    }
  }, [])

  const handleLogin = async () => {
    setError(null)
    try {
      const { url } = await api.fetchKiteLoginUrl()
      // Open in same window — Zerodha will redirect back
      window.location.href = url
    } catch (e: any) {
      setError(e.message || 'Failed to get login URL')
    }
  }

  const handleTokenExchange = async (token: string) => {
    setLoading(true)
    setError(null)
    try {
      const res = await api.submitKiteCallback(token)
      setLoggedIn(true)
      setUserName(res.user_name)
      setShowTokenInput(false)
      setRequestToken('')
      onLoginChange?.(true)
    } catch (e: any) {
      setError(e.message || 'Token exchange failed')
    } finally {
      setLoading(false)
    }
  }

  const handleManualSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const token = requestToken.trim()
    if (token) handleTokenExchange(token)
  }

  // Loading state
  if (loggedIn === null) {
    return (
      <div className="flex items-center gap-1.5 text-slate-400 text-sm">
        <Loader2 className="w-4 h-4 animate-spin" />
      </div>
    )
  }

  // Logged in
  if (loggedIn) {
    return (
      <div className="flex items-center gap-1.5 text-emerald-400 text-sm">
        <User className="w-4 h-4" />
        <span className="hidden sm:inline">{userName || 'Zerodha'}</span>
      </div>
    )
  }

  // Not logged in
  return (
    <div className="flex items-center gap-2">
      {error && (
        <span className="text-red-400 text-xs max-w-[200px] truncate" title={error}>
          {error}
        </span>
      )}

      {showTokenInput ? (
        <form onSubmit={handleManualSubmit} className="flex items-center gap-1.5">
          <input
            type="text"
            value={requestToken}
            onChange={e => setRequestToken(e.target.value)}
            placeholder="Paste request_token"
            className="bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 w-44
                       focus:outline-none focus:border-emerald-500"
            autoFocus
          />
          <button
            type="submit"
            disabled={loading || !requestToken.trim()}
            className="px-2 py-1 rounded text-xs font-medium bg-emerald-600 text-white
                       hover:bg-emerald-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : 'Submit'}
          </button>
          <button
            type="button"
            onClick={() => { setShowTokenInput(false); setError(null) }}
            className="text-slate-400 text-xs hover:text-slate-200"
          >
            ✕
          </button>
        </form>
      ) : (
        <div className="flex items-center gap-1.5">
          <button
            onClick={handleLogin}
            className="flex items-center gap-1.5 px-3 py-1 rounded text-xs font-medium
                       bg-blue-600 text-white hover:bg-blue-500 border border-blue-500"
          >
            <LogIn className="w-3.5 h-3.5" />
            Login with Zerodha
          </button>
          <button
            onClick={() => setShowTokenInput(true)}
            title="Paste request_token manually"
            className="text-slate-400 hover:text-slate-200 text-xs underline"
          >
            manual
          </button>
        </div>
      )}
    </div>
  )
}
