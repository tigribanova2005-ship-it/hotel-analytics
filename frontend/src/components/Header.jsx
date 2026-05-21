export default function Header({ dark, onToggleTheme, onLogout, periodLabel }) {
  return (
    <header
      style={{ backgroundColor: '#1A1212' }}
      className="px-6 h-14 flex items-center gap-3 shrink-0"
    >
      {/* Logo */}
      <div
        style={{ background: 'linear-gradient(135deg, #C4956A 0%, #a07040 100%)' }}
        className="w-8 h-8 rounded-full flex items-center justify-center shrink-0"
      >
        <span className="text-sm font-bold text-white">K</span>
      </div>

      {/* Title */}
      <span className="font-semibold text-white text-[15px] tracking-tight whitespace-nowrap">
        УК Калейдоскоп — Интернет-маркетинг
      </span>

      {/* Period label */}
      {periodLabel && (
        <span className="text-gray-400 text-sm ml-1 whitespace-nowrap">
          · {periodLabel}
        </span>
      )}

      {/* Right controls */}
      <div className="ml-auto flex items-center gap-2">
        <button
          onClick={onToggleTheme}
          title={dark ? 'Переключить на светлую тему' : 'Переключить на тёмную тему'}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-gray-600
                     text-gray-300 hover:text-white hover:border-gray-400 transition-colors text-sm"
        >
          {dark ? '☀ Светлая' : '☾ Тёмная'}
        </button>
        <button
          onClick={onLogout}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-gray-600
                     text-gray-300 hover:text-white hover:border-red-500 transition-colors text-sm"
        >
          Выйти
        </button>
      </div>
    </header>
  )
}
