const BASE = '/api/v1'

function getToken() {
  return localStorage.getItem('access_token')
}

async function request(path, options = {}) {
  const token = getToken()
  const res = await fetch(BASE + path, {
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    ...options,
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`${res.status}: ${text || res.statusText}`)
  }
  return res.json()
}

export async function login(email, password) {
  const res = await fetch(BASE + '/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  })
  if (!res.ok) {
    const data = await res.json().catch(() => ({}))
    throw new Error(data.detail || 'Ошибка входа')
  }
  const data = await res.json()
  localStorage.setItem('access_token', data.access_token)
  return data
}

export function logout() {
  localStorage.removeItem('access_token')
}

export function isAuthenticated() {
  return !!getToken()
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

export function fetchTLBookings({ period }) {
  const q = new URLSearchParams({ period })
  return request(`/travelline/bookings?${q}`)
}

export function fetchTLHotels({ period }) {
  const q = new URLSearchParams({ period })
  return request(`/travelline/hotels?${q}`)
}

export function fetchTLPromos({ period }) {
  const q = new URLSearchParams({ period })
  return request(`/travelline/promos?${q}`)
}

export function fetchHistory({ period, section }) {
  const q = new URLSearchParams({ period, section })
  return request(`/channels/history?${q}`)
}
