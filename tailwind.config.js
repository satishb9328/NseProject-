/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: 'class',
  content: [
    './Nse_Project/**/*.html',
    './Nse_Project/**/*.js',
    './Nse_Project/**/*.py',
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        display: ['"Space Grotesk"', 'Inter', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      colors: {
        ink: {
          950: '#04060c',
          900: '#070b14',
          850: '#0a0f1a',
          800: '#0d1424',
          700: '#141d33',
          600: '#1c2740',
          500: '#26314f',
        },
        neon: {
          green: '#22ff88',
          mint: '#34d399',
          red: '#ff3b5c',
          rose: '#fb7185',
          amber: '#ffb020',
          blue: '#38bdf8',
          violet: '#a78bfa',
        },
      },
      boxShadow: {
        'glow-green': '0 0 24px -6px rgba(34, 255, 136, 0.55)',
        'glow-red': '0 0 24px -6px rgba(255, 59, 92, 0.55)',
        'glow-blue': '0 0 24px -6px rgba(56, 189, 248, 0.45)',
        card: '0 14px 40px -18px rgba(0, 0, 0, 0.7)',
      },
    },
  },
  plugins: [],
}
