import { useState } from 'react'

export const CHANNELS = [
  'Поиск брендовый',
  'Поиск общий',
  'Яндекс.Директ',
  'Яндекс.Карты',
  'Google.Карты',
  '2ГИС',
  'Прямой трафик',
  'ВКонтакте',
  'Telegram',
  'Рассылки TravelLine',
]

const COLUMNS = [
  { key: 'visitors',       label: 'Посетители',        fmt: 'int',     tooltip: 'Уникальные посетители из Яндекс.Метрики' },
  { key: 'visitors_delta', label: '△%',                fmt: 'delta',   tooltip: 'Изменение к прошлому месяцу' },
  { key: 'bounces',        label: 'Отказы',            fmt: 'percent', tooltip: 'Доля визитов < 15 сек с одной страницей' },
  { key: 'room_interest',  label: 'Интерес к номерам', fmt: 'int',     tooltip: 'Цель "Выбор номера" (/search)' },
  { key: 'calls',          label: 'Звонки',            fmt: 'int',     tooltip: 'Цель "Звонок" по номерам отелей' },
  { key: 'bookings',       label: 'Брони',             fmt: 'int',     tooltip: 'Цель "Бронирование" (Travelline)' },
  { key: 'revenue',        label: 'Доход',             fmt: 'money',   tooltip: 'Выручка по подтверждённым броням' },
  { key: 'avg_check',      label: 'Средний чек',       fmt: 'money',   tooltip: 'Доход / Брони' },
  { key: 'costs',          label: 'Затраты',           fmt: 'money',   editable: true, tooltip: 'Бюджет (вручную, кроме Директа)' },
  { key: 'cpl',            label: 'CPL',               fmt: 'money',   tooltip: 'Затраты / (Звонки + Брони)' },
  { key: 'roi',            label: 'ROI',               fmt: 'roi',     tooltip: '(Доход − Затраты) / Затраты × 100%' },
]

// ── Форматирование ────────────────────────────────────────────────────────────

function format(value, fmt) {
  if (value == null) return '—'
  const n = Number(value)
  switch (fmt) {
    case 'int':     return n.toLocaleString('ru-RU')
    case 'money':   return n.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
    case 'percent': return n.toFixed(1) + '%'
    case 'delta':   return (n > 0 ? '+' : '') + n.toFixed(1) + '%'
    case 'roi':     return (n > 0 ? '+' : '') + n.toFixed(0) + '%'
    default:        return String(value)
  }
}

function deltaClass(value, fmt) {
  if (fmt !== 'delta' && fmt !== 'roi') return ''
  if (value == null) return ''
  const n = Number(value)
  if (n > 0) return 'text-emerald-600 dark:text-emerald-400 font-medium'
  if (n < 0) return 'text-red-600 dark:text-red-400 font-medium'
  return ''
}

// ── Инлайн-редактирование бюджета ─────────────────────────────────────────────

function EditableCell({ value, onSave }) {
  const [editing, setEditing] = useState(false)
  const [input, setInput]     = useState('')

  if (editing) {
    return (
      <input
        autoFocus
        type="number"
        className="w-24 bg-input border border-gold rounded px-2 py-0.5 text-right text-sm text-fg
                   focus:outline-none tabular-nums"
        value={input}
        onChange={e => setInput(e.target.value)}
        onBlur={() => { setEditing(false); if (input !== '') onSave(Number(input)) }}
        onKeyDown={e => {
          if (e.key === 'Enter')  { setEditing(false); if (input !== '') onSave(Number(input)) }
          if (e.key === 'Escape') setEditing(false)
        }}
      />
    )
  }

  return (
    <button
      onClick={() => { setInput(value ?? ''); setEditing(true) }}
      className="text-right w-full tabular-nums text-muted hover:text-gold transition-colors underline decoration-dotted"
      title="Нажмите, чтобы ввести бюджет"
    >
      {value != null ? format(value, 'money') : '+ ввести'}
    </button>
  )
}

// ── Скелетон ─────────────────────────────────────────────────────────────────

function SkeletonRow({ index }) {
  return (
    <tr className={index % 2 === 0 ? 'bg-surface' : 'bg-surface2'}>
      <td className="py-3 px-4 bg-inherit">
        <div className="h-4 w-32 bg-border rounded animate-pulse" />
      </td>
      {COLUMNS.map(col => (
        <td key={col.key} className="py-3 px-3">
          <div className="h-4 w-16 bg-border rounded animate-pulse ml-auto" />
        </td>
      ))}
    </tr>
  )
}

// ── Тултип ────────────────────────────────────────────────────────────────────

function Tooltip({ text, children }) {
  return (
    <span className="relative group inline-flex items-center">
      {children}
      <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 hidden group-hover:block
                       bg-fg text-surface rounded px-2 py-1 text-xs whitespace-nowrap z-10 shadow-lg">
        {text}
      </span>
    </span>
  )
}

// ── Главная таблица ───────────────────────────────────────────────────────────

export default function ChannelTable({ data, loading, onBudgetSave }) {
  const channels = data?.channels ?? {}
  const totals   = data?.totals   ?? {}

  return (
    <div className="overflow-x-auto rounded-lg border border-border shadow-sm">
      <table className="w-full text-sm border-collapse table-sticky min-w-[1100px]">

        {/* Шапка */}
        <thead>
          <tr className="bg-surface border-b-2 border-border">
            <th className="text-left py-3 px-4 font-semibold text-muted bg-surface min-w-[185px] text-xs uppercase tracking-wider">
              Канал
            </th>
            {COLUMNS.map(col => (
              <th key={col.key} className="text-right py-3 px-3 font-semibold text-muted text-xs uppercase tracking-wider whitespace-nowrap min-w-[90px]">
                <Tooltip text={col.tooltip}>
                  <span className="cursor-help border-b border-dotted border-muted">{col.label}</span>
                </Tooltip>
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {loading
            ? CHANNELS.map((_, i) => <SkeletonRow key={i} index={i} />)
            : CHANNELS.map((channel, i) => {
                const row = channels[channel] ?? {}
                return (
                  <tr
                    key={channel}
                    className={`border-b border-border hover:bg-gold/5 transition-colors ${
                      i % 2 === 0 ? 'bg-surface' : 'bg-surface2'
                    }`}
                  >
                    <td className="py-3 px-4 font-medium text-fg bg-inherit whitespace-nowrap">
                      {channel}
                    </td>
                    {COLUMNS.map(col => (
                      <td
                        key={col.key}
                        className={`py-3 px-3 text-right tabular-nums whitespace-nowrap ${deltaClass(row[col.key], col.fmt)}`}
                      >
                        {col.editable
                          ? <EditableCell value={row[col.key]} onSave={v => onBudgetSave(channel, v)} />
                          : <span className={row[col.key] == null ? 'text-border' : 'text-fg'}>
                              {format(row[col.key], col.fmt)}
                            </span>
                        }
                      </td>
                    ))}
                  </tr>
                )
              })
          }

          {/* Итого */}
          {!loading && (
            <tr className="border-t-2 border-gold/60 bg-surface font-semibold">
              <td className="py-3 px-4 text-gold bg-surface">Итого</td>
              {COLUMNS.map(col => (
                <td key={col.key} className={`py-3 px-3 text-right tabular-nums whitespace-nowrap ${deltaClass(totals[col.key], col.fmt)}`}>
                  <span className={totals[col.key] == null ? 'text-muted' : 'text-fg'}>
                    {format(totals[col.key], col.fmt)}
                  </span>
                </td>
              ))}
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}
