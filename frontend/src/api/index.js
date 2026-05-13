const BASE = '/api/v1'   // backend работает на /api/v1/...

async function request(path, options = {}) {
  const res = await fetch(BASE + path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`${res.status}: ${text || res.statusText}`)
  }
  return res.json()
}

export function fetchChannelData({ section, period, hotel }) {
  const q = new URLSearchParams({ section, period })
  if (hotel && hotel !== 'all') q.set('hotel', hotel)
  return request(`/channels?${q}`)
}

export function fetchBudgets({ section, period, hotel }) {
  const q = new URLSearchParams({ section, period })
  if (hotel && hotel !== 'all') q.set('hotel', hotel)
  return request(`/budgets?${q}`)
}

export function saveBudget(budget) {
  return request('/budgets', {
    method: 'POST',
    body: JSON.stringify(budget),
  })
}
