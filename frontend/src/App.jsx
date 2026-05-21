import { useState, useEffect, useCallback } from 'react'
import Header from './components/Header'
import SectionTabs from './components/SectionTabs'
import MonthPicker from './components/MonthPicker'
import HotelSelector from './components/HotelSelector'
import ChannelTable from './components/ChannelTable'
import KpiCard from './components/KpiCard'
import HotelsPage from './components/HotelsPage'
import TrendsPage from './components/TrendsPage'
import PromoPage from './components/PromoPage'
import InsightsPage from './components/InsightsPage'
import LoginPage from './components/LoginPage'
import {
  fetchChannelData, fetchTLBookings, fetchBudgets,
  saveBudget, isAuthenticated, logout,
} from './api'

function currentPeriod() {
  const d = new Date()
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`
}

const MONTHS = ['Январь','Февраль','Март','Апрель','Май','Июнь',
                'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь']

const HOTEL_NAMES = {
  rubinstein: 'Рубинштейна', italiana: 'Итальянская', nevsky: 'Невский',
  gold: 'Голд', centralniy: 'Центральный', point: 'Поинт', lesnaya: 'Лесная Ривьера',
}

const SECTION_LABELS = {
  hotels: 'Отели Калейдоскоп', franchise: 'Франшиза', uk: 'Управляющая компания',
}

const NAV_TABS = [
  { id: 'channels', label: 'Каналы' },
  { id: 'hotels',   label: 'Отели' },
  { id: 'trends',   label: 'Динамика' },
  { id: 'promos',   label: 'Промокоды' },
  { id: 'insights', label: 'Выводы AI' },
]

const PAID_CHANNELS = ['Яндекс.Директ', 'Яндекс.Карты', '2GIS', 'ВКонтакте']

export default function App() {
  const [authed,  setAuthed]  = useState(isAuthenticated)
  const [dark,    setDark]    = useState(false)
  const [page,    setPage]    = useState('channels')
  const [section, setSection] = useState('hotels')
  const [period,  setPeriod]  = useState(currentPeriod)
  const [hotel,   setHotel]   = useState('all')
  const [data,    setData]    = useState(null)
  const [tlData,  setTlData]  = useState(null)
  const [budgets, setBudgets] = useState([])
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
  }, [dark])

  const load = useCallback(() => {
    if (!authed) return
    setLoading(true)
    setError(null)
    Promise.all([
      fetchChannelData({ section, period, hotel }),
      fetchTLBookings({ period }),
      fetchBudgets({ section, period, hotel }),
    ])
      .then(([channels, tl, budgetList]) => {
        setData(channels)
        setTlData(tl)
        setBudgets(budgetList)
      })
      .catch(e => {
        if (e.message.startsWith('401') || e.message.startsWith('403')) {
          logout()
          setAuthed(false)
        } else {
          setError(e.message)
        }
      })
      .finally(() => setLoading(false))
  }, [section, period, hotel, authed])

  useEffect(() => { load() }, [load])

  async function handleBudgetSave(channel, value) {
    await saveBudget({ period, section, hotel, channel, amount: value })
    load()
  }

  function handleLogout() {
    logout()
    setAuthed(false)
    setData(null)
  }

  if (!authed) return <LoginPage onLogin={() => setAuthed(true)} />

  const [y, m] = period.split('-').map(Number)
  const periodLabel = `${MONTHS[m - 1]} ${y}`
  const hotelLabel  = hotel !== 'all' ? ` · ${HOTEL_NAMES[hotel] ?? hotel}` : ''

  const totalVisitors = data?.totals?.visitors ?? null
  const totalCalls    = data?.totals?.calls ?? null
  const tlAvailable   = tlData?.available === true
  const totalBookings = tlAvailable ? tlData.total_bookings : null
  const totalRevenue  = tlAvailable ? tlData.total_revenue  : null
  const avgCheck      = tlAvailable ? tlData.avg_check      : null
  const totalBudget   = budgets.reduce((s, b) => s + (b.amount || 0), 0) || null
  const roi           = (totalRevenue && totalBudget) ? totalRevenue / totalBudget : null

  const paidBudget   = budgets.filter(b => PAID_CHANNELS.includes(b.channel))
                               .reduce((s, b) => s + (b.amount || 0), 0)
  const paidBookings = PAID_CHANNELS.reduce(
    (s, ch) => s + (data?.channels?.[ch]?.bookings || 0), 0
  )
  const cpl = (paidBudget && paidBookings) ? paidBudget / paidBookings : null

  const kpis = [
    { label: 'Бюджет',             value: totalBudget,   fmt: 'money', delta: null },
    { label: 'Доход с сайта',      value: totalRevenue,  fmt: 'money', delta: null, stub: !tlAvailable },
    { label: 'Броней с сайта',     value: totalBookings, fmt: 'int',   delta: null, stub: !tlAvailable },
    { label: 'Средний чек',        value: avgCheck,      fmt: 'money', delta: null, stub: !tlAvailable },
    { label: 'ROI (Доход/Бюджет)', value: roi,           fmt: 'roi',   delta: null },
    { label: 'CPL платных каналов',value: cpl,           fmt: 'money', delta: null },
    { label: 'Визиты',             value: totalVisitors, fmt: 'int',   delta: data?.totals?.visitors_delta ?? null },
    { label: 'Звонки',             value: totalCalls,    fmt: 'int',   delta: null },
  ]

  const budgetMap = Object.fromEntries(budgets.map(b => [b.channel, b]))

  return (
    <div className="min-h-screen bg-bg flex flex-col">
      <Header
        dark={dark}
        onToggleTheme={() => setDark(d => !d)}
        onLogout={handleLogout}
        periodLabel={periodLabel}
      />

      <div className="flex-1 flex flex-col min-w-0">
        {/* Section tabs */}
        <div className="px-6 bg-surface border-b border-border">
          <SectionTabs section={section} setSection={s => { setSection(s); setHotel('all') }} />
        </div>

        {/* Page navigation */}
        <div className="px-6 bg-surface border-b border-border flex gap-0">
          {NAV_TABS.map(tab => (
            <button
              key={tab.id}
              onClick={() => setPage(tab.id)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 -mb-px transition-colors ${
                page === tab.id
                  ? 'border-[#0D2B4E] text-[#0D2B4E]'
                  : 'border-transparent text-muted hover:text-fg'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="flex-1 flex flex-col px-6 py-4 gap-4">
          {/* Toolbar */}
          <div className="flex items-center gap-3 flex-wrap">
            <MonthPicker period={period} setPeriod={setPeriod} />
            {section === 'hotels' && (
              <HotelSelector hotel={hotel} setHotel={setHotel} />
            )}
            <span className="text-xs text-muted uppercase tracking-wider">
              {SECTION_LABELS[section]}{hotelLabel} · {periodLabel}
            </span>
            {loading && (
              <span className="text-muted text-sm animate-pulse ml-auto">Загрузка…</span>
            )}
          </div>

          {error && (
            <div className="rounded-md bg-red-50 dark:bg-red-900/30 border border-red-300 dark:border-red-700
                            text-red-700 dark:text-red-300 px-4 py-3 text-sm">
              <strong>Ошибка:</strong> {error}
            </div>
          )}

          {page === 'channels' && !error && (
            <>
              <div className="grid grid-cols-2 sm:grid-cols-4 xl:grid-cols-8 gap-3">
                {kpis.map(k => (
                  <KpiCard key={k.label} {...k} loading={loading} />
                ))}
              </div>
              <ChannelTable
                data={data}
                budgetMap={budgetMap}
                loading={loading}
                onBudgetSave={handleBudgetSave}
              />
            </>
          )}

          {page === 'hotels'   && <HotelsPage  period={period} />}
          {page === 'trends'   && <TrendsPage  period={period} section={section} />}
          {page === 'promos'   && <PromoPage   period={period} />}
          {page === 'insights' && (
            <InsightsPage period={period} section={section} tlData={tlData} budgets={budgets} />
          )}
        </div>
      </div>
    </div>
  )
}
