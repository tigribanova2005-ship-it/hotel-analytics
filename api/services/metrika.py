"""
Интеграция с Яндекс.Метрика API (stat/v1/data).

Счётчик: 40050615
Документация: https://yandex.ru/dev/metrika/doc/api2/stat/objectmodel/data.html
"""

import calendar
import os
from datetime import date
from typing import Optional

import httpx

COUNTER_ID = "40050615"
METRIKA_STAT_URL = "https://api-metrika.yandex.net/stat/v1/data"

# ── ID целей ──────────────────────────────────────────────────────────────────

# Брони (Travelline widget)
BOOKING_GOALS: dict[str, int] = {
    "rubinstein": 130914232,
    "italiana":   130932334,
    "nevsky":     130931911,
    "gold":       130932682,
    "centralniy": 130932886,
    "point":      543295822,
    "lesnaya":    353072087,
    "total":      130914901,
}

# Звонки
CALL_GOALS: dict[str, int] = {
    "italiana":   145399474,
    "gold":       145399738,
    "centralniy": 145399876,
    "nevsky":     145399951,
    "rubinstein": 145400482,
    "800":        146794417,
}

# Все суммарные goal-метрики для запроса
ALL_BOOKING_GOAL_IDS = list(BOOKING_GOALS.values())
ALL_CALL_GOAL_IDS    = list(CALL_GOALS.values())

# ID целей "интерес к номерам" (страница /search по каждому отелю)
# Добавить реальные ID когда будут известны
ROOM_INTEREST_GOALS: dict[str, int] = {}


# ── Маппинг каналов ───────────────────────────────────────────────────────────
#
# Логика классификации строки (utm_source, utm_medium, utm_campaign, traffic_source):
#   1. Яндекс.Карты   — utm_source содержит "yandex" И "map"
#   2. Google.Карты   — utm_source содержит "google" И "map"
#   3. 2ГИС           — utm_source содержит "2gis"
#   4. Яндекс.Директ  — utm_medium in (cpc, paid, cpm) + source=yandex
#   5. ВКонтакте      — utm_source in (vk, vkontakte)
#   6. Telegram       — utm_source contains "telegram"
#   7. TravelLine     — utm_source contains "travelline" OR medium=email
#   8. Поиск брендовый— traffic_source=search + campaign содержит бренд-слова
#   9. Поиск общий    — traffic_source=search, без UTM
#  10. Прямой трафик  — traffic_source=direct, без UTM
#
# Всё остальное (рефераллы, неизвестное) — None, строка игнорируется

BRAND_KEYWORDS = {"kaleid", "калейд", "brand", "бренд"}


def classify_channel(
    utm_source: str,
    utm_medium: str,
    utm_campaign: str,
    traffic_source: str,
) -> Optional[str]:
    src = utm_source.strip().lower()
    med = utm_medium.strip().lower()
    cmp = utm_campaign.strip().lower()
    trf = traffic_source.strip().lower()

    # Карты — всегда раньше общего поиска
    if src and "yandex" in src and "map" in src:
        return "Яндекс.Карты"
    if src and "google" in src and "map" in src:
        return "Google.Карты"
    if src and "2gis" in src:
        return "2ГИС"

    # Яндекс.Директ
    if src == "yandex" and med in ("cpc", "paid", "cpm"):
        return "Яндекс.Директ"

    # ВКонтакте
    if src in ("vk", "vkontakte") or src.startswith("vk."):
        return "ВКонтакте"

    # Telegram
    if "telegram" in src or src == "tg":
        return "Telegram"

    # Рассылки TravelLine
    if "travelline" in src or med == "email":
        return "Рассылки TravelLine"

    # Органический поиск — по типу трафика (нет UTM)
    if trf == "organic" or (trf == "search" and not src):
        # Брендовый — если кампания содержит бренд-слово (нетипично для органики,
        # но на случай if utm-тегирование есть)
        if any(kw in cmp for kw in BRAND_KEYWORDS):
            return "Поиск брендовый"
        return "Поиск общий"

    # Прямой трафик
    if trf == "direct" and not src:
        return "Прямой трафик"

    # Остальное — без классификации
    return None


# ── Вспомогательные функции ───────────────────────────────────────────────────

def period_dates(period: str) -> tuple[date, date]:
    """'2025-05' → (date(2025,5,1), date(2025,5,31))"""
    year, month = map(int, period.split("-"))
    last = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last)


def prev_period(period: str) -> str:
    """'2025-05' → '2025-04'"""
    year, month = map(int, period.split("-"))
    if month == 1:
        return f"{year - 1}-12"
    return f"{year}-{month - 1:02d}"


def _goal_metrics_str(goal_ids: list[int]) -> str:
    return ",".join(f"ym:s:goal{g}reaches" for g in goal_ids)


async def fetch_raw(
    token: str,
    date1: str,
    date2: str,
    section_filter: Optional[str] = None,
) -> list[dict]:
    """
    Один запрос к Метрике.
    Возвращает сырой список строк data[].
    """
    all_goal_ids = ALL_BOOKING_GOAL_IDS + ALL_CALL_GOAL_IDS + list(ROOM_INTEREST_GOALS.values())

    metrics = "ym:s:visits,ym:s:bounceRate"
    if all_goal_ids:
        metrics += "," + _goal_metrics_str(all_goal_ids)

    params = {
        "id":         COUNTER_ID,
        "metrics":    metrics,
        "dimensions": "ym:s:lastTrafficSource,ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMCampaign",
        "date1":      date1,
        "date2":      date2,
        "limit":      1000,
        "lang":       "ru",
    }

    if section_filter:
        params["filters"] = section_filter

    headers = {"Authorization": f"OAuth {token}"}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(METRIKA_STAT_URL, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json().get("data", [])


# ── Агрегация ─────────────────────────────────────────────────────────────────

def _empty() -> dict:
    return {
        "visitors":      0,
        "bounces":       0.0,   # будет усредняться взвешенно
        "room_interest": 0,
        "calls":         0,
        "bookings":      0,
        "_visits":       0,     # для взвешенного среднего отказов
    }


def aggregate(rows: list[dict]) -> dict[str, dict]:
    """
    Принимает сырые строки Метрики, возвращает словарь channel → агрегаты.
    """
    result: dict[str, dict] = {}

    booking_count      = len(ALL_BOOKING_GOAL_IDS)
    call_count         = len(ALL_CALL_GOAL_IDS)
    room_interest_count = len(ROOM_INTEREST_GOALS)

    for row in rows:
        dims    = row.get("dimensions", [])
        metrics = row.get("metrics",    [])
        if len(dims) < 4 or not metrics:
            continue

        traffic_source = dims[0].get("name") or ""
        utm_source     = dims[1].get("name") or ""
        utm_medium     = dims[2].get("name") or ""
        utm_campaign   = dims[3].get("name") or ""

        channel = classify_channel(utm_source, utm_medium, utm_campaign, traffic_source)
        if not channel:
            continue

        if channel not in result:
            result[channel] = _empty()

        visits      = float(metrics[0] or 0)
        bounce_rate = float(metrics[1] or 0)

        r = result[channel]
        # Взвешенное среднее отказов
        total_v = r["_visits"] + visits
        if total_v > 0:
            r["bounces"] = (r["bounces"] * r["_visits"] + bounce_rate * visits) / total_v
        r["_visits"]  += visits
        r["visitors"] += int(visits)

        # Цели: брони
        idx = 2
        for _ in range(booking_count):
            if idx < len(metrics):
                r["bookings"] += int(metrics[idx] or 0)
            idx += 1

        # Цели: звонки
        for _ in range(call_count):
            if idx < len(metrics):
                r["calls"] += int(metrics[idx] or 0)
            idx += 1

        # Цели: интерес к номерам
        for _ in range(room_interest_count):
            if idx < len(metrics):
                r["room_interest"] += int(metrics[idx] or 0)
            idx += 1

    # Убрать служебное поле
    for ch in result:
        result[ch].pop("_visits", None)

    return result


def compute_derived(row: dict, costs: Optional[float]) -> dict:
    """Добавляет CPL, ROI, avg_check в строку канала."""
    r = dict(row)
    r["costs"] = costs

    leads = (r.get("calls") or 0) + (r.get("bookings") or 0)
    revenue = r.get("revenue")  # может быть None (TravelLine ещё не подключён)

    r["cpl"] = (costs / leads) if (costs and leads > 0) else None
    r["roi"] = ((revenue - costs) / costs * 100) if (revenue and costs and costs > 0) else None
    r["avg_check"] = (revenue / r["bookings"]) if (revenue and r.get("bookings")) else None

    return r


def compute_delta(current: int, previous: int) -> Optional[float]:
    if not previous:
        return None
    return round((current - previous) / previous * 100, 1)


def compute_totals(channels: dict) -> dict:
    t = {"visitors": 0, "room_interest": 0, "calls": 0, "bookings": 0,
         "revenue": None, "costs": None}

    for row in channels.values():
        t["visitors"]      += row.get("visitors") or 0
        t["room_interest"] += row.get("room_interest") or 0
        t["calls"]         += row.get("calls") or 0
        t["bookings"]      += row.get("bookings") or 0
        if row.get("revenue") is not None:
            t["revenue"] = (t["revenue"] or 0) + row["revenue"]
        if row.get("costs") is not None:
            t["costs"] = (t["costs"] or 0) + row["costs"]

    leads   = t["calls"] + t["bookings"]
    revenue = t.get("revenue")
    costs   = t.get("costs")

    t["cpl"]       = (costs / leads) if (costs and leads > 0) else None
    t["roi"]       = ((revenue - costs) / costs * 100) if (revenue and costs and costs > 0) else None
    t["avg_check"] = (revenue / t["bookings"]) if (revenue and t["bookings"]) else None

    return t
