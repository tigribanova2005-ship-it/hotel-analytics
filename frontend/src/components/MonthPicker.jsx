const MONTHS = [
  'Январь','Февраль','Март','Апрель','Май','Июнь',
  'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь',
]

function shift(period, delta) {
  const [y, m] = period.split('-').map(Number)
  const d = new Date(y, m - 1 + delta)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`
}

export default function MonthPicker({ period, setPeriod }) {
  const [year, month] = period.split('-').map(Number)

  return (
    <div className="flex items-center gap-1 bg-surface border border-border rounded-md px-2 py-1.5 select-none">
      <button
        onClick={() => setPeriod(shift(period, -1))}
        className="w-6 h-6 flex items-center justify-center text-muted hover:text-white rounded transition-colors"
        aria-label="Предыдущий месяц"
      >
        ‹
      </button>
      <span className="text-sm font-medium text-white min-w-[130px] text-center">
        {MONTHS[month - 1]} {year}
      </span>
      <button
        onClick={() => setPeriod(shift(period, +1))}
        className="w-6 h-6 flex items-center justify-center text-muted hover:text-white rounded transition-colors"
        aria-label="Следующий месяц"
      >
        ›
      </button>
    </div>
  )
}
