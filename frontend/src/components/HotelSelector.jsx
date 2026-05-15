export default function HotelSelector({ hotel, setHotel }) {
  return (
    <select
      value={hotel}
      onChange={e => setHotel(e.target.value)}
      className="bg-surface border border-border text-fg text-sm rounded-md px-3 py-1.5
                 cursor-pointer focus:outline-none focus:border-gold transition-colors"
    >
      <option value="all">Все отели</option>
      <optgroup label="— Городские —">
        <option value="rubinstein">Рубинштейна</option>
        <option value="italiana">Итальянская</option>
        <option value="nevsky">Невский</option>
        <option value="gold">GOLD</option>
        <option value="centralniy">Центральный</option>
        <option value="point">Поинт</option>
      </optgroup>
      <optgroup label="— Загородные —">
        <option value="lesnaya">Лесная Ривьера</option>
      </optgroup>
    </select>
  )
}
