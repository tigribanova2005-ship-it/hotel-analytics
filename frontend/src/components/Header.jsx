export default function Header({ dark, onToggleTheme }) {
  return (
    <header className="bg-surface border-b border-border px-6 h-14 flex items-center gap-3 shrink-0">
      <div className="w-7 h-7 rounded-md bg-gradient-to-br from-gold to-[#e07b7b] flex items-center justify-center">
        <span className="text-xs font-bold text-white">A</span>
      </div>
      <span className="font-semibold text-fg text-[15px] tracking-tight">Analytics Metrika</span>
      <span className="text-muted text-sm">/ Калейдоскоп</span>

      <div className="ml-auto">
        <button
          onClick={onToggleTheme}
          title={dark ? 'Переключить на светлую тему' : 'Переключить на тёмную тему'}
          className="flex items-center gap-2 px-3 py-1.5 rounded-md border border-border
                     bg-surface2 text-muted hover:text-fg hover:border-gold transition-colors text-sm"
        >
          {dark ? '☀ Светлая' : '☾ Тёмная'}
        </button>
      </div>
    </header>
  )
}
