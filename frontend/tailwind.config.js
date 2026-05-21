/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        bg:       'var(--color-bg)',
        surface:  'var(--color-surface)',
        surface2: 'var(--color-surface2)',
        border:   'var(--color-border)',
        muted:    'var(--color-muted)',
        fg:       'var(--color-fg)',
        input:    'var(--color-input)',
        brand: {
          dark:    '#1A1212',
          navy:    '#0D2B4E',
          green:   '#3A5C2E',
          burgundy:'#7A1500',
          beige:   '#C4956A',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
