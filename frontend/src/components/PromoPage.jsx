import { useEffect, useState } from 'react'
import { fetchTLPromos } from '../api'

function fmtMoney(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function fmtInt(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString('ru-RU')
}

function fmtDiscount(d) {
  if (d == null) return '—'
  if (typeof d === 'number') return d.toFixed(1) + '%'
  return String(d)
}

export default function PromoPage({ period }) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchTLPromos({ period })
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [period])

  const promos    = data?.promos    ?? []
  const available = data?.available ?? true

  // TravelLine not connected
  if (!loading && !available) {
    return (
      <div className="rounded-lg border border-border p-6 bg-surface">
        <div className="flex items-start gap-3">
          <span className="text-2xl">🔌</span>
          <div>
            <h3 className="font-semibold text-fg mb-1">TravelLine API не подключён</h3>
            <p className="text-sm text-muted mb-3">
              Для отображения данных по промокодам необходимо подключить интеграцию с TravelLine.
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

  if (!loading && promos.length === 0) {
    return (
      <div className="rounded-lg border border-border bg-surface px-6 py-10 text-center text-muted text-sm">
        Промокоды за период не найдены
      </div>
    )
  }

  // Sort by total (сумма) descending
  const sorted = [...promos].sort((a, b) => (b.total ?? 0) - (a.total ?? 0))

  const totalUses   = sorted.reduce((s, p) => s + (p.uses  ?? 0), 0)
  const totalAmount = sorted.reduce((s, p) => s + (p.total ?? 0), 0)

  return (
    <div className="flex flex-col gap-4">
      <div className="overflow-x-auto rounded-lg border border-border shadow-sm">
        <table className="w-full text-sm border-collapse" style={{ minWidth: '500px' }}>
          <thead>
            <tr className="bg-surface border-b-2 border-border">
              <th className="text-left py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider min-w-[160px]">
                Промокод
              </th>
              <th className="text-right py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider">
                Скидка
              </th>
              <th className="text-right py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider">
                Применений
              </th>
              <th className="text-right py-3 px-4 font-semibold text-muted text-xs uppercase tracking-wider">
                Сумма ₽
              </th>
            </tr>
          </thead>
          <tbody>
            {loading
              ? Array.from({ length: 5 }).map((_, i) => (
                  <tr key={i} className={i % 2 === 0 ? 'bg-surface' : 'bg-surface2'}>
                    {Array.from({ length: 4 }).map((_, j) => (
                      <td key={j} className="py-3 px-4">
                        <div className="h-4 bg-border rounded animate-pulse" />
                      </td>
                    ))}
                  </tr>
                ))
              : sorted.map((promo, i) => (
                  <tr
                    key={promo.code || i}
                    className={`border-b border-border ${i % 2 === 0 ? 'bg-surface' : 'bg-surface2'}`}
                    onMouseEnter={e => e.currentTarget.style.background = '#0D2B4E0d'}
                    onMouseLeave={e => e.currentTarget.style.background = ''}
                  >
                    <td className="py-3 px-4 font-medium text-fg font-mono">
                      {promo.code || '—'}
                    </td>
                    <td className="py-3 px-4 text-right tabular-nums text-fg">
                      {fmtDiscount(promo.discount)}
                    </td>
                    <td className="py-3 px-4 text-right tabular-nums text-fg">
                      {fmtInt(promo.uses)}
                    </td>
                    <td className="py-3 px-4 text-right tabular-nums text-fg">
                      {fmtMoney(promo.total)}
                    </td>
                  </tr>
                ))
            }

            {/* Totals */}
            {!loading && sorted.length > 0 && (
              <tr className="border-t-2 bg-surface font-semibold" style={{ borderColor: '#0D2B4E66' }}>
                <td className="py-3 px-4" style={{ color: '#0D2B4E' }} colSpan={2}>Итого</td>
                <td className="py-3 px-4 text-right tabular-nums text-fg">
                  {fmtInt(totalUses)}
                </td>
                <td className="py-3 px-4 text-right tabular-nums text-fg">
                  {fmtMoney(totalAmount)}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
