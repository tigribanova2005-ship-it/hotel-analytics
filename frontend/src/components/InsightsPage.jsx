import { useEffect, useState } from 'react'
import { fetchChannelData } from '../api'

function fmt(n, type) {
  if (n == null) return '—'
  const v = Number(n)
  if (type === 'money')   return v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
  if (type === 'percent') return v.toFixed(1) + '%'
  if (type === 'int')     return v.toLocaleString('ru-RU')
  return String(n)
}

function TopChannelBar({ channels, totalVisitors }) {
  const sorted = Object.entries(channels)
    .map(([name, row]) => ({ name, visitors: row.visitors ?? 0 }))
    .filter(c => c.visitors > 0)
    .sort((a, b) => b.visitors - a.visitors)
    .slice(0, 5)

  const max = sorted[0]?.visitors || 1

  return (
    <div className="flex flex-col gap-2">
      {sorted.map(({ name, visitors }) => {
        const pct = totalVisitors > 0 ? (visitors / totalVisitors * 100) : 0
        return (
          <div key={name} className="flex items-center gap-3">
            <div className="w-32 text-xs text-fg truncate shrink-0">{name}</div>
            <div className="flex-1 h-3 bg-border rounded-full overflow-hidden">
              <div
                className="h-full rounded-full"
                style={{ width: `${(visitors / max * 100).toFixed(1)}%`, background: '#0D2B4E' }}
              />
            </div>
            <div className="w-16 text-right text-xs tabular-nums text-muted">
              {fmt(visitors, 'int')}
            </div>
            <div className="w-10 text-right text-xs tabular-nums text-muted">
              {pct.toFixed(1)}%
            </div>
          </div>
        )
      })}
    </div>
  )
}

function InsightCard({ title, children }) {
  return (
    <div
      className="bg-white dark:bg-surface rounded-lg shadow-sm p-4 flex flex-col gap-3"
      style={{ borderTop: '2px solid #0D2B4E' }}
    >
      <div className="text-xs font-semibold uppercase tracking-wider text-muted">{title}</div>
      {children}
    </div>
  )
}

export default function InsightsPage({ period, section, tlData, budgets = [] }) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchChannelData({ section, period, hotel: 'all' })
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [period, section])

  const channels     = data?.channels ?? {}
  const totals       = data?.totals   ?? {}
  const totalVisitors = totals.visitors || 0
  const totalBookings = totals.bookings || 0

  // Best channel by bookings
  const bestByBookings = Object.entries(channels)
    .filter(([, r]) => (r.bookings ?? 0) > 0)
    .sort(([, a], [, b]) => (b.bookings ?? 0) - (a.bookings ?? 0))[0]

  // Lowest bounce rate (with meaningful traffic)
  const lowestBounce = Object.entries(channels)
    .filter(([, r]) => (r.visitors ?? 0) >= 50 && r.bounces != null)
    .sort(([, a], [, b]) => (a.bounces ?? 100) - (b.bounces ?? 100))[0]

  // Most calls
  const mostCalls = Object.entries(channels)
    .filter(([, r]) => (r.calls ?? 0) > 0)
    .sort(([, a], [, b]) => (b.calls ?? 0) - (a.calls ?? 0))[0]

  // Conversion rate (bookings / visitors)
  const convRate = totalVisitors > 0 && totalBookings > 0
    ? (totalBookings / totalVisitors * 100)
    : null

  const tlOk = tlData?.available === true
  const tlRevenue  = tlOk ? tlData.total_revenue  : null
  const tlBookings = tlOk ? tlData.total_bookings : null

  const insights = [
    tlOk && {
      label: 'Доход с сайта (TravelLine)',
      value: fmt(tlRevenue, 'money'),
      sub:   fmt(tlBookings, 'int') + ' броней · ср. чек ' + fmt(tlData.avg_check, 'money'),
      color: '#3A5C2E',
    },
    !tlOk && {
      label: 'TravelLine не подключён',
      value: 'Нет данных',
      sub:   'Добавьте TRAVELLINE_API_KEY в .env',
      color: '#6b7a99',
    },
  ].filter(Boolean).concat([
    bestByBookings && {
      label: 'Лучший канал по бронированиям',
      value: bestByBookings[0],
      sub:   fmt(bestByBookings[1].bookings, 'int') + ' броней',
      color: '#3A5C2E',
    },
    lowestBounce && {
      label: 'Наименьший процент отказов',
      value: lowestBounce[0],
      sub:   fmt(lowestBounce[1].bounces, 'percent') + ' отказов',
      color: '#0D2B4E',
    },
    mostCalls && {
      label: 'Больше всего звонков',
      value: mostCalls[0],
      sub:   fmt(mostCalls[1].calls, 'int') + ' звонков',
      color: '#e07b00',
    },
    convRate != null && {
      label: 'Общая конверсия',
      value: fmt(convRate, 'percent'),
      sub:   fmt(totalBookings, 'int') + ' броней / ' + fmt(totalVisitors, 'int') + ' посетителей',
      color: '#0D2B4E',
    },
  ].filter(Boolean))

  return (
    <div className="flex flex-col gap-5">
      {error && (
        <div className="rounded-md bg-red-50 dark:bg-red-900/30 border border-red-300 dark:border-red-700
                        text-red-700 dark:text-red-300 px-4 py-3 text-sm">
          <strong>Ошибка:</strong> {error}
        </div>
      )}

      {/* Insight cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {loading
          ? Array.from({ length: 4 }, (_, i) => (
              <div key={i} className="bg-white dark:bg-surface rounded-lg shadow-sm p-4 flex flex-col gap-2"
                   style={{ borderTop: '2px solid #0D2B4E' }}>
                <div className="h-3 w-28 bg-border rounded animate-pulse" />
                <div className="h-5 w-20 bg-border rounded animate-pulse mt-1" />
                <div className="h-3 w-24 bg-border rounded animate-pulse" />
              </div>
            ))
          : insights.map(({ label, value, sub, color }) => (
              <div
                key={label}
                className="bg-white dark:bg-surface rounded-lg shadow-sm px-4 py-3 flex flex-col gap-1"
                style={{ borderTop: `2px solid ${color}` }}
              >
                <div className="text-xs uppercase tracking-wider text-muted">{label}</div>
                <div className="text-base font-bold text-fg leading-snug">{value}</div>
                <div className="text-xs text-muted">{sub}</div>
              </div>
            ))
        }
      </div>

      {/* Top channels by traffic */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <InsightCard title="Топ-5 каналов по трафику">
          {loading
            ? Array.from({ length: 5 }, (_, i) => (
                <div key={i} className="flex items-center gap-3">
                  <div className="h-3 w-32 bg-border rounded animate-pulse" />
                  <div className="flex-1 h-3 bg-border rounded animate-pulse" />
                </div>
              ))
            : <TopChannelBar channels={channels} totalVisitors={totalVisitors} />
          }
        </InsightCard>

        <InsightCard title="Сводка по каналам">
          {loading
            ? Array.from({ length: 5 }, (_, i) => (
                <div key={i} className="h-4 w-full bg-border rounded animate-pulse" />
              ))
            : (
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left pb-2 text-muted font-medium">Канал</th>
                    <th className="text-right pb-2 text-muted font-medium">Посетители</th>
                    <th className="text-right pb-2 text-muted font-medium">Броней</th>
                    <th className="text-right pb-2 text-muted font-medium">Звонков</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(channels)
                    .filter(([, r]) => (r.visitors ?? 0) > 0 || (r.bookings ?? 0) > 0)
                    .sort(([, a], [, b]) => (b.visitors ?? 0) - (a.visitors ?? 0))
                    .map(([name, row]) => (
                      <tr key={name} className="border-b border-border last:border-0">
                        <td className="py-1.5 text-fg truncate max-w-[120px]">{name}</td>
                        <td className="py-1.5 text-right tabular-nums text-fg">{fmt(row.visitors, 'int')}</td>
                        <td className="py-1.5 text-right tabular-nums text-fg">{fmt(row.bookings || null, 'int')}</td>
                        <td className="py-1.5 text-right tabular-nums text-fg">{fmt(row.calls || null, 'int')}</td>
                      </tr>
                    ))
                  }
                  <tr className="border-t border-border font-semibold">
                    <td className="py-1.5 text-fg">Итого</td>
                    <td className="py-1.5 text-right tabular-nums text-fg">{fmt(totals.visitors, 'int')}</td>
                    <td className="py-1.5 text-right tabular-nums text-fg">{fmt(totals.bookings || null, 'int')}</td>
                    <td className="py-1.5 text-right tabular-nums text-fg">{fmt(totals.calls || null, 'int')}</td>
                  </tr>
                </tbody>
              </table>
            )
          }
        </InsightCard>
      </div>
    </div>
  )
}
