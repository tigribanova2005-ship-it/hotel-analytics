import { useState, useEffect, useCallback } from 'react'
import Header from './components/Header'
import SectionTabs from './components/SectionTabs'
import MonthPicker from './components/MonthPicker'
import HotelSelector from './components/HotelSelector'
import ChannelTable from './components/ChannelTable'
import { fetchChannelData, saveBudget } from './api'

function currentPeriod() {
  const d = new Date()
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`
}

export default function App() {
  const [section, setSection]   = useState('hotels')
  const [period,  setPeriod]    = useState(currentPeriod)
  const [hotel,   setHotel]     = useState('all')
  const [data,    setData]      = useState(null)
  const [loading, setLoading]   = useState(false)
  const [error,   setError]     = useState(null)

  const load = useCallback(() => {
    setLoading(true)
    setError(null)
    fetchChannelData({ section, period, hotel })
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [section, period, hotel])

  useEffect(() => { load() }, [load])

  async function handleBudgetSave(channel, value) {
    await saveBudget({ period, section, hotel, channel, amount: value })
    load()
  }

  return (
    <div className="min-h-screen bg-bg flex flex-col">
      <Header />

      <div className="flex-1 flex flex-col px-6 py-4 gap-4 min-w-0">
        <SectionTabs section={section} setSection={s => { setSection(s); setHotel('all') }} />

        {/* Toolbar */}
        <div className="flex items-center gap-3 flex-wrap">
          <MonthPicker period={period} setPeriod={setPeriod} />
          {section === 'hotels' && (
            <HotelSelector hotel={hotel} setHotel={setHotel} />
          )}
          {loading && (
            <span className="text-muted text-sm animate-pulse">Загрузка данных…</span>
          )}
        </div>

        {/* Period label */}
        <div className="text-xs text-muted uppercase tracking-wider">
          {sectionLabel(section)}{hotel !== 'all' && section === 'hotels' ? ` · ${hotelName(hotel)}` : ''} · {periodLabel(period)}
        </div>

        {/* Content */}
        {error && (
          <div className="rounded-md bg-red-900/30 border border-red-700 text-red-300 px-4 py-3 text-sm">
            Ошибка загрузки: {error}
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

function sectionLabel(s) {
  return { hotels: 'Отели Калейдоскоп', franchise: 'Франшиза', uk: 'Управляющая компания' }[s] ?? s
}

function hotelName(id) {
  const map = {
    rubinstein: 'Рубинштейна', italiana: 'Итальянская', nevsky: 'Невский',
    gold: 'GOLD', centralniy: 'Центральный', point: 'Поинт', lesnaya: 'Лесная Ривьера',
  }
  return map[id] ?? id
}

function periodLabel(p) {
  const [y, m] = p.split('-').map(Number)
  const months = ['Январь','Февраль','Март','Апрель','Май','Июнь',
                  'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь']
  return `${months[m - 1]} ${y}`
}
