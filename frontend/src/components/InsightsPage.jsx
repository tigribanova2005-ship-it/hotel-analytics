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

function EmojiInsightCard({ icon, title, text, color }) {
  return (
    <div
      className="bg-white dark:bg-surface rounded-lg shadow-sm px-4 py-4 flex gap-3"
      style={{ borderLeft: `3px solid ${color || '#0D2B4E'}` }}
    >
      <span className="text-2xl shrink-0 mt-0.5">{icon}</span>
      <div>
        <div className="font-bold text-sm text-fg mb-1">{title}</div>
        <div className="text-sm text-muted leading-relaxed">{text}</div>
      </div>
    </div>
  )
}

function InsightCard({ title, children }) {
  return (
    <div
      className="bg-white dark:bg-surface rounded-lg shadow-sm p-4 flex flex-col gap-3"
      style={{ borderLeft: '3px solid #0D2B4E' }}
    >
      <div className="text-xs font-semibold uppercase tracking-wider text-muted">{title}</div>
      {children}
    </div>
  )
}

function buildInsights(channels, totals, tlData) {
  const totalVisitors = totals.visitors || 0
  const totalBookings = totals.bookings || 0
  const result = []

  const byTraffic = Object.entries(channels)
    .filter(([, r]) => (r.visitors ?? 0) > 0)
    .sort(([, a], [, b]) => (b.visitors ?? 0) - (a.visitors ?? 0))
  if (byTraffic.length > 0) {
    const [name, row] = byTraffic[0]
    const pct = totalVisitors > 0 ? (row.visitors / totalVisitors * 100).toFixed(1) : '0'
    result.push({
      icon: '📊',
      title: `Лидер по трафику: ${name}`,
      text: `Канал «${name}» привлекает ${pct}% от всего трафика — ${fmt(row.visitors, 'int')} посетителей.`,
    })
  }

  const byBookings = Object.entries(channels)
    .filter(([, r]) => (r.bookings ?? 0) > 0)
    .sort(([, a], [, b]) => (b.bookings ?? 0) - (a.bookings ?? 0))
  if (byBookings.length > 0) {
    const [name, row] = byBookings[0]
    result.push({
      icon: '🏆',
      title: `Лучший по бронированиям: ${name}`,
      text: `Канал «${name}» обеспечил ${fmt(row.bookings, 'int')} броней — лучший результат среди всех каналов.`,
    })
  }

  const lowestBounce = Object.entries(channels)
    .filter(([, r]) => (r.visitors ?? 0) >= 50 && r.bounces != null)
    .sort(([, a], [, b]) => (a.bounces ?? 100) - (b.bounces ?? 100))
  if (lowestBounce.length > 0) {
    const [name, row] = lowestBounce[0]
    result.push({
      icon: '🎯',
      title: `Наибольшая вовлечённость: ${name}`,
      text: `Канал «${name}» показывает наименьший процент отказов — ${fmt(row.bounces, 'percent')}. Аудитория максимально вовлечена.`,
    })
  }

  const withDelta = Object.entries(channels)
    .filter(([, r]) => r.visitors_delta != null)
    .sort(([, a], [, b]) => Math.abs(b.visitors_delta ?? 0) - Math.abs(a.visitors_delta ?? 0))
  if (withDelta.length > 0) {
    const [name, row] = withDelta[0]
    const delta = row.visitors_delta
    if (Math.abs(delta) > 5) {
      const dir = delta > 0 ? 'вырос' : 'снизился'
      result.push({
        icon: delta > 0 ? '📈' : '📉',
        title: `Динамика трафика: ${name}`,
        text: `Трафик канала «${name}» ${dir} на ${Math.abs(delta).toFixed(1)}% по сравнению с аналогичным периодом прошлого года.`,
        color: delta > 0 ? '#3A5C2E' : '#7A1500',
      })
    }
  }

  if (tlData?.available && (tlData?.total_revenue ?? 0) > 0) {
    result.push({
      icon: '💰',
      title: 'Выручка через сайт (TravelLine)',
      text: `За период зафиксировано ${fmt(tlData.total_bookings, 'int')} броней на сумму ${fmt(tlData.total_revenue, 'money')}. Средний чек — ${fmt(tlData.avg_check, 'money')}.`,
      color: '#3A5C2E',
    })
  }

  const bookingsNoBudget = Object.entries(channels)
    .filter(([, r]) => (r.bookings ?? 0) > 0 && (r.costs == null || r.costs === 0))
  if (bookingsNoBudget.length > 0) {
    const names = bookingsNoBudget.slice(0, 2).map(([n]) => n).join(', ')
    result.push({
      icon: '💡',
      title: 'Возможность: каналы без указанного бюджета',
      text: `Каналы ${names} приносят брони, но бюджет не задан. Заполните данные для расчёта стоимости брони и ROAS.`,
    })
  }

  return result
}

export default function InsightsPage({ data, tlData }) {
  const loading       = !data
  const channels      = data?.channels ?? {}
  const totals        = data?.totals   ?? {}
  const totalVisitors = totals.visitors || 0
  const hasData       = Object.keys(channels).length > 0

  const emojiInsights = hasData ? buildInsights(channels, totals, tlData) : []

  return (
    <div className="flex flex-col gap-5">

      {/* Emoji insight cards */}
      {loading ? (
        <div className="flex flex-col gap-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <div
              key={i}
              className="bg-white dark:bg-surface rounded-lg shadow-sm px-4 py-4 flex gap-3"
              style={{ borderLeft: '3px solid #0D2B4E' }}
            >
              <div className="w-8 h-8 bg-border rounded animate-pulse shrink-0" />
              <div className="flex-1 flex flex-col gap-2">
                <div className="h-4 w-48 bg-border rounded animate-pulse" />
                <div className="h-3 w-full bg-border rounded animate-pulse" />
                <div className="h-3 w-3/4 bg-border rounded animate-pulse" />
              </div>
            </div>
          ))}
        </div>
      ) : !hasData ? (
        <div className="rounded-lg border border-border bg-surface px-6 py-10 text-center text-muted text-sm">
          Недостаточно данных для формирования выводов. Выберите другой период или раздел.
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          {emojiInsights.map((ins, i) => (
            <EmojiInsightCard key={i} {...ins} />
          ))}
        </div>
      )}

      {/* Support charts */}
      {!loading && hasData && (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <InsightCard title="Топ-5 каналов по трафику">
            <TopChannelBar channels={channels} totalVisitors={totalVisitors} />
          </InsightCard>

          <InsightCard title="Сводка по каналам">
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
          </InsightCard>
        </div>
      )}
    </div>
  )
}
