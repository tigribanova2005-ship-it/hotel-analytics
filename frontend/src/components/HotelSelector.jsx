export const HOTELS = [
  { id: 'all',        name: 'Все отели' },
  { id: 'rubinstein', name: 'Рубинштейна' },
  { id: 'italiana',   name: 'Итальянская' },
  { id: 'nevsky',     name: 'Невский' },
  { id: 'gold',       name: 'GOLD' },
  { id: 'centralniy', name: 'Центральный' },
  { id: 'point',      name: 'Поинт' },
  { id: 'lesnaya',    name: 'Лесная Ривьера' },
]

export default function HotelSelector({ hotel, setHotel }) {
  return (
    <select
      value={hotel}
      onChange={e => setHotel(e.target.value)}
      className="bg-surface border border-border text-white text-sm rounded-md px-3 py-1.5 cursor-pointer
                 focus:outline-none focus:border-gold transition-colors"
    >
      {HOTELS.map(h => (
        <option key={h.id} value={h.id}>{h.name}</option>
      ))}
    </select>
  )
}
