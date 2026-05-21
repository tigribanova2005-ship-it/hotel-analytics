import { useState, useEffect } from 'react'
import { fetchTLHotels } from '../api'

const HOTEL_ORDER = [
  'italiana', 'nevsky', 'rubinstein', 'centralniy', 'gold', 'point', 'lesnaya',
]

const HOTEL_DISPLAY = {
  italiana:   'Итальянская',
  nevsky:     'Невский',
  rubinstein: 'Рубинштейна',
  centralniy: 'Центральный',
  gold:       'Голд',
  point:      'Поинт',
  lesnaya:    'Лесная Ривьера',
}

function fmtMoney(n) {
  if (!n) return '—'
  return Number(n).toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function fmtInt(n) {
  if (n == null || n === 0) return '—'
  return Number(n).toLocaleString('ru-RU')
}

function BarRow({ label, value, maxValue, color }) {
  const pct = maxValue > 0 ? Math.round((value / maxValue) * 100) : 0
  return (
    <div className="flex items-center gap-2 mb-2">
      <div className="w-28 text-xs text-muted text-right shrink-0 truncate" title={label}>
        {label}
      </div>
      <div className="flex-1 bg-surface2 rounded-full h-4 min-w-0">
        <div
          className="h-4 rounded-full transition-all duration-500"
          style={{ width: `${Math.max(pct, 2)}%`, backgroundColor: color }}
        />
      </div>
      <div className="w-16 text-xs tabular-nums text-fg text-right shrink-0">
        {value > 0 ? Number(value).toLocaleString('ru-RU') : '—'}
      </div>
    </div>
  )
}

function SkeletonBar() {
  return (
    <div className="flex items-center gap-2 mb-2">
      <div className="w-28 h-4 bg-border rounded animate-pulse" />
      <div className="flex-1 h-4 bg-border rounded-full animate-pulse" />
      <div className="w-16 h-4 bg-border rounded animate-pulse" />
    </div>
  )
}

export default function HotelsPage({ period }) {
  const [tlData,   setTlData]   = useState(null)
  const [loading,  setLoading]  = useState(true)
  const [error,    setError]    = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchTLHotels({ period })
      .then(setTlData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [period])

  // TravelLine not connected
  if (!loading && tlData && !tlData.available) {
    return (
      <div className="rounded-lg border border-border p-6 bg-surface">
        <div className="flex items-start gap-3">
          <span className="text-2xl">🔌</span>
          <div>
            <h3 className="font-semibold text-fg mb-1">TravelLine API не подключён</h3>
            <p className="text-sm text-muted mb-3">
              Для отображения данных по отелям необходимо подключить интеграцию с TravelLine.
            </p>
            <ol className="text-sm text-muted list-decimal list-inside space-y-1">
              <li>Получите API-ключ в личном кабинете TravelLine</li>
              <li>Добавьте переменную окружения <code className="bg-surface2 px-1 rounded">TRAVELLINE_API_KEY</code> на сервере</li>
              <li>Перезапустите backend-сервис</li>
            </ol>
          </div>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="rounded-md bg-red-50 dark:bg-red-900/30 border border-red-300 dark:border-red-700
                      text-red-700 dark:text-red-300 px-4 py-3 text-sm">
        <strong>Ошибка загрузки:</strong> {error}
      </div>
    )
  }

  const hotels = tlData?.hotels ?? []

  // Sort by order
  const sorted = [...hotels].sort((a, b) => {
    const ai = HOTEL_ORDER.indexOf(a.hotel_key)
    const bi = HOTEL_ORDER.indexOf(b.hotel_key)
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi)
  })

  const maxBookings   = Math.max(...sorted.map(h => h.bookings ?? 0), 1)
  const maxRevenue    = Math.max(...sorted.map(h => h.revenue  ?? 0), 1)
  const totalBookings = sorted.reduce((s, h) => s + (h.bookings ?? 0), 0)
  const totalRevenue  = sorted.reduce((s, h) => s + (h.revenue  ?? 0), 0)

  return (
    <div className="flex flex-col gap-6">

      {/* Bar charts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">

        {/* Bookings chart */}
        <div className="bg-surface rounded-lg border border-border p-5">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted mb-4">
            Броней с сайта
          </h3>
          {loading
            ? Array.from({ length: 7 }).map((_, i) => <SkeletonBar key={i} />)
            : sorted.map(h => (
                <BarRow
                  key={h.hotel_key}
                  label={h.name || HOTEL_DISPLAY[h.hotel_key] || h.hotel_key}
                  value={h.bookings ?? 0}
                  maxValue={maxBookings}
                  color="#0D2B4E"
                />
              ))
          }
        </div>

        {/* Revenue chart */}
        <div className="bg-surface rounded-lg border border-border p-5">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-muted mb-4">
            Выручка с сайта ₽
          </h3>
          {loading
            ? Array.from({ length: 7 }).map((_, i) => <SkeletonBar key={i} />)
            : sorted.map(h => (
                <BarRow
                  key={h.hotel_key}
                  label={h.name || HOTEL_DISPLAY[h.hotel_key] || h.hotel_key}
                  value={h.revenue ?? 0}
                  maxValue={maxRevenue}
                  color="#0D2B4E"
                />
              ))
          }
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded-lg border border-border shadow-sm">
        <table className="w-full text-sm border-collapse" style={{ minWidth: '600px' }}>
          <thead>
            <tr className="bg-surface border-b-2 border-border">
              <th className="text-left py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider min-w-[160px]">
                Отель
              </th>
              <th className="text-right py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider">
                Броней с сайта
              </th>
              <th className="text-right py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider">
                Выручка сайт ₽
              </th>
              <th className="text-right py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider">
                Всего броней TL
              </th>
              <th className="text-right py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider">
                Выручка всего TL
              </th>
            </tr>
          </thead>
          <tbody>
            {loading
              ? Array.from({ length: 7 }).map((_, i) => (
                  <tr key={i} className={i % 2 === 0 ? 'bg-surface' : 'bg-surface2'}>
                    {Array.from({ length: 5 }).map((_, j) => (
                      <td key={j} className="py-3 px-4">
                        <div className="h-4 bg-border rounded animate-pulse" />
                      </td>
                    ))}
                  </tr>
                ))
              : sorted.map((h, i) => (
                  <tr
                    key={h.hotel_key}
                    className={`border-b border-border ${i % 2 === 0 ? 'bg-surface' : 'bg-surface2'}`}
                  >
                    <td className="py-3 px-4 font-medium text-fg whitespace-nowrap">
                      {h.name || HOTEL_DISPLAY[h.hotel_key] || h.hotel_key}
                    </td>
                    <td className="py-3 px-4 text-right tabular-nums text-fg">
                      {fmtInt(h.bookings)}
                    </td>
                    <td className="py-3 px-4 text-right tabular-nums text-fg">
                      {fmtMoney(h.revenue)}
                    </td>
                    <td className="py-3 px-4 text-right tabular-nums text-muted">—</td>
                    <td className="py-3 px-4 text-right tabular-nums text-muted">—</td>
                  </tr>
                ))
            }

            {/* Totals */}
            {!loading && (
              <tr className="border-t-2 bg-surface font-semibold" style={{ borderColor: '#0D2B4E66' }}>
                <td className="py-3 px-4" style={{ color: '#0D2B4E' }}>Итого</td>
                <td className="py-3 px-4 text-right tabular-nums text-fg">
                  {fmtInt(totalBookings)}
                </td>
                <td className="py-3 px-4 text-right tabular-nums text-fg">
                  {fmtMoney(totalRevenue)}
                </td>
                <td className="py-3 px-4 text-right text-muted">—</td>
                <td className="py-3 px-4 text-right text-muted">—</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
