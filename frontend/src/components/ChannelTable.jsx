import { useState } from 'react'

export const CHANNELS = [
  'Яндекс.Директ',
  'Яндекс.Карты',
  'Поисковый (SEO)',
  '2GIS',
  'Прямые заходы',
  'Рассылки TravelLine',
  'Google Карты',
  'ВКонтакте',
  'Прочее',
]

const DEFAULT_INFLUENCE = {
  'Яндекс.Директ':    'increase',
  'Яндекс.Карты':     'max',
  '2GIS':             'launch',
  'ВКонтакте':        'launch',
  'Поисковый (SEO)':  'increase',
}

const INFLUENCE_BADGE = {
  increase: { icon: '▲', label: 'Рост',    color: '#3A5C2E' },
  max:      { icon: '●', label: 'Макс',    color: '#0D2B4E' },
  launch:   { icon: '◆', label: 'Запуск',  color: '#e07b00' },
  none:     { icon: '—', label: '',         color: '#6b7a99' },
}

// ── Format helpers ─────────────────────────────────────────────────────────────

function fmtMoney(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function fmtInt(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString('ru-RU')
}

function fmtPercent(n) {
  if (n == null) return '—'
  return Number(n).toFixed(1) + '%'
}

// ── Editable budget cell ───────────────────────────────────────────────────────

function BudgetCell({ value, onSave }) {
  const [editing, setEditing] = useState(false)
  const [input,   setInput]   = useState('')

  function commit() {
    setEditing(false)
    if (input !== '') onSave(Number(input))
  }

  if (editing) {
    return (
      <input
        autoFocus
        type="number"
        className="w-24 bg-input border rounded px-2 py-0.5 text-right text-sm text-fg
                   focus:outline-none tabular-nums"
        style={{ borderColor: '#0D2B4E' }}
        value={input}
        onChange={e => setInput(e.target.value)}
        onBlur={commit}
        onKeyDown={e => {
          if (e.key === 'Enter')  commit()
          if (e.key === 'Escape') setEditing(false)
        }}
      />
    )
  }

  return (
    <button
      onClick={() => { setInput(value ?? ''); setEditing(true) }}
      className="text-right w-full tabular-nums text-muted hover:text-fg transition-colors underline decoration-dotted"
      title="Нажмите, чтобы ввести бюджет"
    >
      {value != null ? fmtMoney(value) : '+ ввести'}
    </button>
  )
}

// ── Influence badge ────────────────────────────────────────────────────────────

function InfluenceBadge({ channel, budgetMap }) {
  const influence = (budgetMap[channel]?.influence) ?? DEFAULT_INFLUENCE[channel] ?? 'none'
  const cfg = INFLUENCE_BADGE[influence] ?? INFLUENCE_BADGE.none
  return (
    <span className="text-xs font-medium" style={{ color: cfg.color }}>
      {cfg.icon}{cfg.label ? ' ' + cfg.label : ''}
    </span>
  )
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function SkeletonRow({ index }) {
  const cols = 9
  return (
    <tr className={index % 2 === 0 ? 'bg-surface' : 'bg-surface2'}>
      <td className="py-3 px-4 bg-inherit">
        <div className="h-4 w-32 bg-border rounded animate-pulse" />
      </td>
      {Array.from({ length: cols }).map((_, i) => (
        <td key={i} className="py-3 px-3">
          <div className="h-4 w-14 bg-border rounded animate-pulse ml-auto" />
        </td>
      ))}
    </tr>
  )
}

// ── Main table ────────────────────────────────────────────────────────────────

export default function ChannelTable({ data, budgetMap = {}, loading, onBudgetSave }) {
  const channels    = data?.channels ?? {}
  const totals      = data?.totals   ?? {}
  const totalVisitors = totals.visitors || 1

  // Compute totals for cost-per-booking
  let totalBudget   = 0
  let totalBookings = 0
  for (const ch of CHANNELS) {
    const budget = budgetMap[ch]?.amount
    const bookings = channels[ch]?.bookings ?? 0
    if (budget != null) totalBudget += budget
    totalBookings += bookings
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-border shadow-sm">
      <table className="w-full text-sm border-collapse table-sticky" style={{ minWidth: '1100px' }}>

        {/* Header */}
        <thead>
          <tr className="bg-surface border-b-2 border-border">
            <th className="text-left py-3 px-4 font-semibold text-muted bg-surface min-w-[180px]
                           text-xs uppercase tracking-wider sticky left-0 z-10">
              Канал
            </th>
            {[
              'Бюджет ₽', 'Трафик %', 'Броней', 'Доход ₽', 'Ср.чек ₽',
              'Ст-сть брони', 'ROAS', 'Влияние бюджета',
            ].map(h => (
              <th key={h}
                className="text-right py-3 px-3 font-semibold text-muted text-xs uppercase
                           tracking-wider whitespace-nowrap min-w-[90px]">
                {h}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {loading
            ? CHANNELS.map((_, i) => <SkeletonRow key={i} index={i} />)
            : CHANNELS.map((channel, i) => {
                const row      = channels[channel] ?? {}
                const budget   = budgetMap[channel]?.amount ?? null
                const visitors = row.visitors ?? 0
                const bookings = row.bookings ?? 0
                const trafficPct = totalVisitors > 0
                  ? (visitors / totalVisitors * 100).toFixed(1) + '%'
                  : '0.0%'
                const costPerBooking = (budget && bookings > 0)
                  ? fmtMoney(budget / bookings)
                  : '—'

                return (
                  <tr
                    key={channel}
                    className={`border-b border-border transition-colors ${
                      i % 2 === 0 ? 'bg-surface' : 'bg-surface2'
                    }`}
                    style={{ '--tw-bg-opacity': 1 }}
                    onMouseEnter={e => e.currentTarget.style.background = '#0D2B4E11'}
                    onMouseLeave={e => e.currentTarget.style.background = ''}
                  >
                    <td className="py-3 px-4 font-medium text-fg bg-inherit whitespace-nowrap sticky left-0 z-10">
                      {channel}
                    </td>

                    {/* Бюджет */}
                    <td className="py-3 px-3 text-right tabular-nums">
                      <BudgetCell
                        value={budget}
                        onSave={v => onBudgetSave(channel, v)}
                      />
                    </td>

                    {/* Трафик % */}
                    <td className="py-3 px-3 text-right tabular-nums text-fg">
                      {trafficPct}
                    </td>

                    {/* Броней */}
                    <td className="py-3 px-3 text-right tabular-nums text-fg">
                      {fmtInt(bookings || null)}
                    </td>

                    {/* Доход — future */}
                    <td className="py-3 px-3 text-right text-muted">—</td>

                    {/* Ср.чек — future */}
                    <td className="py-3 px-3 text-right text-muted">—</td>

                    {/* Ст-сть брони */}
                    <td className="py-3 px-3 text-right tabular-nums text-fg">
                      {costPerBooking}
                    </td>

                    {/* ROAS — future */}
                    <td className="py-3 px-3 text-right text-muted">—</td>

                    {/* Влияние бюджета */}
                    <td className="py-3 px-3 text-right">
                      <InfluenceBadge channel={channel} budgetMap={budgetMap} />
                    </td>
                  </tr>
                )
              })
          }

          {/* Totals row */}
          {!loading && (
            <tr className="border-t-2 bg-surface font-semibold" style={{ borderColor: '#0D2B4E66' }}>
              <td className="py-3 px-4 bg-surface sticky left-0 z-10" style={{ color: '#0D2B4E' }}>
                Итого
              </td>
              <td className="py-3 px-3 text-right tabular-nums text-fg">
                {totalBudget > 0 ? fmtMoney(totalBudget) : '—'}
              </td>
              <td className="py-3 px-3 text-right tabular-nums text-fg">100%</td>
              <td className="py-3 px-3 text-right tabular-nums text-fg">
                {fmtInt(totalBookings || null)}
              </td>
              <td className="py-3 px-3 text-right text-muted">—</td>
              <td className="py-3 px-3 text-right text-muted">—</td>
              <td className="py-3 px-3 text-right tabular-nums text-fg">
                {totalBudget > 0 && totalBookings > 0
                  ? fmtMoney(totalBudget / totalBookings)
                  : '—'}
              </td>
              <td className="py-3 px-3 text-right text-muted">—</td>
              <td className="py-3 px-3 text-right" />
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}
