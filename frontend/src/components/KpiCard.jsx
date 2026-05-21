function formatValue(value, fmt) {
  if (value == null) return '—'
  const n = Number(value)
  switch (fmt) {
    case 'money':
      return n.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
    case 'int':
      return n.toLocaleString('ru-RU')
    case 'roi':
      return n.toFixed(2) + 'x'
    default:
      return String(value)
  }
}

function formatDelta(delta) {
  if (delta == null) return null
  const n = Number(delta)
  const sign = n >= 0 ? '▲' : '▼'
  return `${sign} ${Math.abs(n).toFixed(1)}% к пр.году`
}

export default function KpiCard({ label, value, fmt, delta, stub, loading }) {
  const deltaStr = formatDelta(delta)
  const deltaPositive = delta != null && Number(delta) >= 0

  return (
    <div
      className="bg-white dark:bg-surface rounded-lg shadow-sm px-4 py-3"
      style={{ borderTop: '2px solid #0D2B4E' }}
    >
      <div className="text-xs uppercase tracking-wider text-muted mb-1">
        {label}
      </div>

      {loading ? (
        <div className="h-6 w-24 bg-border rounded animate-pulse mt-1" />
      ) : (
        <div className="text-xl font-bold text-fg">
          {formatValue(value, fmt)}
        </div>
      )}

      {deltaStr && !loading && (
        <div
          className="text-xs mt-1 font-medium"
          style={{ color: deltaPositive ? '#3A5C2E' : '#7A1500' }}
        >
          {deltaStr}
        </div>
      )}

      {stub && !loading && (
        <div className="text-xs text-muted mt-1">
          TravelLine не подкл.
        </div>
      )}
    </div>
  )
}
