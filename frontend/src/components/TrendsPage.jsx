import { useEffect, useState } from 'react'
import { fetchHistory } from '../api'

const MONTHS_SHORT = ['Янв','Фев','Мар','Апр','Май','Июн','Июл','Авг','Сен','Окт','Ноя','Дек']

function fmtInt(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString('ru-RU')
}

function monthLabel(period) {
  if (!period) return ''
  const [y, mo] = period.split('-').map(Number)
  return `${MONTHS_SHORT[mo - 1]} ${y}`
}

// Compute min/max for scaling SVG points
function scalePoints(values, x0, xStep, yTop, yBot) {
  const valid = values.filter(v => v != null && v > 0)
  const min = Math.min(...valid, 0)
  const max = Math.max(...valid, 1)
  return values.map((v, i) => {
    const x = x0 + i * xStep
    const pct = max === min ? 0.5 : (v - min) / (max - min)
    const y = yBot - pct * (yBot - yTop)
    return `${x},${y}`
  })
}

function LineChart({ months, visitors, bookings }) {
  if (!months || months.length === 0) return null

  const W = 600
  const H = 200
  const padL = 50
  const padR = 20
  const padT = 15
  const padB = 30

  const n = months.length
  const xStep = n > 1 ? (W - padL - padR) / (n - 1) : 0

  const visPoints = scalePoints(visitors, padL, xStep, padT, H - padB)
  const bkPoints  = scalePoints(bookings,  padL, xStep, padT, H - padB)

  const visMax  = Math.max(...visitors.filter(Boolean), 1)
  const bkMax   = Math.max(...bookings.filter(Boolean),  1)

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: '220px' }}>
      {/* Y-axis left (visitors) */}
      {[0, 0.5, 1].map(t => {
        const y = (H - padB) - t * (H - padB - padT)
        return (
          <g key={t}>
            <line x1={padL} y1={y} x2={W - padR} y2={y} stroke="#dde2ed" strokeWidth="1" />
            <text x={padL - 4} y={y + 4} textAnchor="end" fontSize="9" fill="#6b7a99">
              {Math.round(visMax * t).toLocaleString('ru-RU')}
            </text>
          </g>
        )
      })}

      {/* Y-axis right (bookings) */}
      {[0, 0.5, 1].map(t => {
        const y = (H - padB) - t * (H - padB - padT)
        return (
          <text key={t} x={W - padR + 4} y={y + 4} textAnchor="start" fontSize="9" fill="#C4956A">
            {Math.round(bkMax * t)}
          </text>
        )
      })}

      {/* X-axis labels */}
      {months.map((p, i) => {
        const x = padL + i * xStep
        return (
          <text key={p} x={x} y={H - 5} textAnchor="middle" fontSize="9" fill="#6b7a99">
            {monthLabel(p)}
          </text>
        )
      })}

      {/* Visitors line */}
      <polyline
        points={visPoints.join(' ')}
        fill="none"
        stroke="#0D2B4E"
        strokeWidth="2"
        strokeLinejoin="round"
      />
      {visPoints.map((pt, i) => {
        const [x, y] = pt.split(',').map(Number)
        return <circle key={i} cx={x} cy={y} r="3" fill="#0D2B4E" />
      })}

      {/* Bookings dashed line */}
      <polyline
        points={bkPoints.join(' ')}
        fill="none"
        stroke="#C4956A"
        strokeWidth="2"
        strokeDasharray="5,3"
        strokeLinejoin="round"
      />
      {bkPoints.map((pt, i) => {
        const [x, y] = pt.split(',').map(Number)
        return <circle key={i} cx={x} cy={y} r="3" fill="#C4956A" />
      })}

      {/* Legend */}
      <rect x={padL} y={padT - 12} width="8" height="8" fill="#0D2B4E" rx="1" />
      <text x={padL + 10} y={padT - 5} fontSize="9" fill="#0D2B4E">Визиты</text>
      <line x1={padL + 55} y1={padT - 8} x2={padL + 63} y2={padT - 8} stroke="#C4956A" strokeWidth="2" strokeDasharray="3,2" />
      <circle cx={padL + 59} cy={padT - 8} r="3" fill="#C4956A" />
      <text x={padL + 67} y={padT - 5} fontSize="9" fill="#C4956A">Брони</text>
    </svg>
  )
}

function TrendArrow({ cur, prev }) {
  if (cur == null || prev == null || prev === 0) return <span className="text-muted">—</span>
  const pct = ((cur - prev) / prev) * 100
  const color = pct >= 0 ? '#3A5C2E' : '#7A1500'
  const arrow = pct >= 0 ? '▲' : '▼'
  return (
    <span className="text-xs font-medium" style={{ color }}>
      {arrow} {Math.abs(pct).toFixed(1)}%
    </span>
  )
}

export default function TrendsPage({ period, section }) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchHistory({ period, section })
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [period, section])

  const months   = data?.months ?? []
  const histData = data?.data   ?? []

  const visitors  = histData.map(d => d.visitors  ?? 0)
  const bookings  = histData.map(d => d.bookings  ?? 0)
  const calls     = histData.map(d => d.calls     ?? 0)

  const last = histData.length - 1
  const prev = last - 1

  const ROWS = [
    { label: 'Визиты',  values: visitors },
    { label: 'Брони',   values: bookings },
    { label: 'Звонки',  values: calls    },
  ]

  return (
    <div className="flex flex-col gap-5">
      {error && (
        <div className="rounded-md bg-red-50 dark:bg-red-900/30 border border-red-300 dark:border-red-700
                        text-red-700 dark:text-red-300 px-4 py-3 text-sm">
          <strong>Ошибка:</strong> {error}
        </div>
      )}

      {/* SVG line chart */}
      <div className="bg-surface rounded-lg border border-border p-5">
        <h3 className="text-xs font-semibold uppercase tracking-wider text-muted mb-3">
          Динамика за 6 месяцев
        </h3>
        {loading ? (
          <div className="h-40 bg-border rounded animate-pulse" />
        ) : (
          <LineChart months={months} visitors={visitors} bookings={bookings} />
        )}
      </div>

      {/* Data table */}
      <div className="overflow-x-auto rounded-lg border border-border shadow-sm">
        <table className="w-full text-sm border-collapse" style={{ minWidth: '560px' }}>
          <thead>
            <tr className="bg-surface border-b-2 border-border">
              <th className="text-left py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider min-w-[120px]">
                Показатель
              </th>
              {loading
                ? Array.from({ length: 6 }).map((_, i) => (
                    <th key={i} className="py-3 px-3">
                      <div className="h-4 w-12 bg-border rounded animate-pulse mx-auto" />
                    </th>
                  ))
                : months.map(p => (
                    <th key={p} className="text-right py-3 px-3 font-semibold text-muted text-xs uppercase tracking-wider whitespace-nowrap">
                      {monthLabel(p)}
                    </th>
                  ))
              }
              <th className="text-right py-3 px-3 font-semibold text-xs uppercase tracking-wider whitespace-nowrap"
                  style={{ color: '#0D2B4E' }}>
                Тренд
              </th>
            </tr>
          </thead>
          <tbody>
            {loading
              ? ROWS.map((r, i) => (
                  <tr key={i} className={i % 2 === 0 ? 'bg-surface' : 'bg-surface2'}>
                    <td className="py-3 px-4">
                      <div className="h-4 w-20 bg-border rounded animate-pulse" />
                    </td>
                    {Array.from({ length: 7 }).map((_, j) => (
                      <td key={j} className="py-3 px-3">
                        <div className="h-4 w-16 bg-border rounded animate-pulse ml-auto" />
                      </td>
                    ))}
                  </tr>
                ))
              : ROWS.map((r, i) => (
                  <tr key={r.label} className={`border-b border-border ${i % 2 === 0 ? 'bg-surface' : 'bg-surface2'}`}>
                    <td className="py-3 px-4 font-medium text-fg whitespace-nowrap">{r.label}</td>
                    {r.values.map((v, j) => (
                      <td key={j} className={`py-3 px-3 text-right tabular-nums ${j === last ? 'font-semibold text-fg' : 'text-muted'}`}>
                        {fmtInt(v || null)}
                      </td>
                    ))}
                    <td className="py-3 px-3 text-right">
                      <TrendArrow cur={r.values[last]} prev={r.values[prev]} />
                    </td>
                  </tr>
                ))
            }
          </tbody>
        </table>
      </div>
    </div>
  )
}
