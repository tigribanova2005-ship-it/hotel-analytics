import { useState, useEffect, useCallback } from 'react'
import Header from './components/Header'
import SectionTabs from './components/SectionTabs'
import MonthPicker from './components/MonthPicker'
import HotelSelector from './components/HotelSelector'
import ChannelTable from './components/ChannelTable'
import LoginPage from './components/LoginPage'
import { fetchChannelData, saveBudget, isAuthenticated, logout } from './api'

function currentPeriod() {
  const d = new Date()
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`
}

const MONTHS = ['Январь','Февраль','Март','Апрель','Май','Июнь',
                'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь']

const HOTEL_NAMES = {
  rubinstein: 'Рубинштейна', italiana: 'Итальянская', nevsky: 'Невский',
  gold: 'GOLD', centralniy: 'Центральный', point: 'Поинт', lesnaya: 'Лесная Ривьера',
}

const SECTION_LABELS = {
  hotels: 'Отели Калейдоскоп', franchise: 'Франшиза', uk: 'Управляющая компания',
}

export default function App() {
  const [authed,  setAuthed]  = useState(isAuthenticated)
  const [dark,    setDark]    = useState(false)
  const [section, setSection] = useState('hotels')
  const [period,  setPeriod]  = useState(currentPeriod)
  const [hotel,   setHotel]   = useState('all')
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
  }, [dark])

  const load = useCallback(() => {
    if (!authed) return
    setLoading(true)
    setError(null)
    fetchChannelData({ section, period, hotel })
      .then(setData)
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

  if (!authed) {
    return <LoginPage onLogin={() => setAuthed(true)} />
  }

  const [y, m] = period.split('-').map(Number)
  const periodLabel = `${MONTHS[m - 1]} ${y}`
  const hotelLabel  = hotel !== 'all' ? ` · ${HOTEL_NAMES[hotel] ?? hotel}` : ''

  return (
    <div className="min-h-screen bg-bg flex flex-col">
      <Header dark={dark} onToggleTheme={() => setDark(d => !d)} onLogout={handleLogout} />

      <div className="flex-1 flex flex-col px-6 py-4 gap-4 min-w-0">
        <SectionTabs section={section} setSection={s => { setSection(s); setHotel('all') }} />

        <div className="flex items-center gap-3 flex-wrap">
          <MonthPicker period={period} setPeriod={setPeriod} />
          {section === 'hotels' && (
            <HotelSelector hotel={hotel} setHotel={setHotel} />
          )}
          {loading && (
            <span className="text-muted text-sm animate-pulse">Загрузка…</span>
          )}
        </div>

        <div className="text-xs text-muted uppercase tracking-wider">
          {SECTION_LABELS[section]}{hotelLabel} · {periodLabel}
        </div>

        {error && (
          <div className="rounded-md bg-red-50 dark:bg-red-900/30 border border-red-300 dark:border-red-700
                          text-red-700 dark:text-red-300 px-4 py-3 text-sm">
            <strong>Ошибка загрузки:</strong> {error}
          </div>
        )}

        {!error && (
          <ChannelTable
            data={data}
            loading={loading}
            onBudgetSave={handleBudgetSave}
          />
        )}
      </div>
    </div>
  )
}
