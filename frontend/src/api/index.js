const BASE = '/api'

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

/**
 * Получить данные по каналам за период.
 * @param {{ section: string, period: string, hotel: string }} params
 * @returns {Promise<{ channels: object, totals: object, period: string }>}
 */
export function fetchChannelData({ section, period, hotel }) {
  const q = new URLSearchParams({ section, period })
  if (hotel && hotel !== 'all') q.set('hotel', hotel)
  return request(`/channels?${q}`)
}

/**
 * Получить сохранённые бюджеты за период.
 */
export function fetchBudgets({ section, period, hotel }) {
  const q = new URLSearchParams({ section, period })
  if (hotel && hotel !== 'all') q.set('hotel', hotel)
  return request(`/budgets?${q}`)
}

/**
 * Сохранить бюджет канала вручную.
 * @param {{ period, section, hotel, channel, amount }} budget
 */
export function saveBudget(budget) {
  return request('/budgets', {
    method: 'POST',
    body: JSON.stringify(budget),
  })
}
