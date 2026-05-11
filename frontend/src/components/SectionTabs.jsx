const TABS = [
  { id: 'hotels',    label: 'Отели Калейдоскоп' },
  { id: 'franchise', label: 'Франшиза' },
  { id: 'uk',        label: 'Управляющая компания' },
]

export default function SectionTabs({ section, setSection }) {
  return (
    <div className="flex gap-0 border-b border-border">
      {TABS.map(tab => (
        <button
          key={tab.id}
          onClick={() => setSection(tab.id)}
          className={`
            px-5 py-3 text-sm font-medium transition-colors border-b-2 -mb-px
            ${section === tab.id
              ? 'border-gold text-gold'
              : 'border-transparent text-muted hover:text-white'
            }
          `}
        >
          {tab.label}
        </button>
      ))}
    </div>
  )
}
