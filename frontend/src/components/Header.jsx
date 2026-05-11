export default function Header() {
  return (
    <header className="bg-surface border-b border-border px-6 h-14 flex items-center gap-3 shrink-0">
      <div className="w-7 h-7 rounded-md bg-gradient-to-br from-gold to-[#e07b7b] flex items-center justify-center">
        <span className="text-xs font-bold text-[#0d1117]">A</span>
      </div>
      <span className="font-semibold text-white text-[15px] tracking-tight">Analytics Metrika</span>
      <span className="text-muted text-sm">/ Калейдоскоп</span>
    </header>
  )
}
