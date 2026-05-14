"""
app/api/v1/channels.py

GET /api/v1/channels — таблица каналов за месяц.
Использует ту же авторизацию и TokenService что и остальные эндпоинты.
"""

import asyncio
import calendar
from datetime import date
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.models.website import Website
from app.services.token_service import TokenService

router = APIRouter(prefix="/channels", tags=["channels"])

METRIKA_STAT_URL = "https://api-metrika.yandex.net/stat/v1/data"

# ── ID целей бронирования ─────────────────────────────────────────────────────
BOOKING_GOAL_IDS = [130914232, 130932334, 130931911, 130932682,
                    130932886, 543295822, 353072087, 130914901]

# ── ID целей звонков ──────────────────────────────────────────────────────────
CALL_GOAL_IDS = [145399474, 145399738, 145399876, 145399951, 145400482, 146794417]

# ── Фильтры по разделу сайта ──────────────────────────────────────────────────
SECTION_FILTERS: dict[str, Optional[str]] = {
    "hotels":    None,
    "franchise": "ym:s:URLPath=~'/franshiza/'",
    "uk":        "ym:s:URLPath=~'/uk/'",
}

ALL_CHANNELS = [
    "Поиск брендовый", "Поиск общий", "Яндекс.Директ", "Яндекс.Карты",
    "Google.Карты", "2ГИС", "Прямой трафик", "ВКонтакте",
    "Telegram", "Рассылки TravelLine",
]

BRAND_KEYWORDS = {"kaleid", "калейд", "brand", "бренд"}


# ── Вспомогательные функции ───────────────────────────────────────────────────

def _period_dates(period: str) -> tuple[date, date]:
    year, month = map(int, period.split("-"))
    last = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last)


def _prev_period(period: str) -> str:
    year, month = map(int, period.split("-"))
    return f"{year - 1}-12" if month == 1 else f"{year}-{month - 1:02d}"


def _classify(utm_source: str, utm_medium: str, utm_campaign: str,
               traffic_source: str) -> Optional[str]:
    src = utm_source.lower().strip()
    med = utm_medium.lower().strip()
    cmp = utm_campaign.lower().strip()
    trf = traffic_source.lower().strip()

    if src and "yandex" in src and "map" in src:             return "Яндекс.Карты"
    if src and "google" in src and "map" in src:             return "Google.Карты"
    if src and "2gis"   in src:                              return "2ГИС"
    if src == "yandex" and med in ("cpc", "paid", "cpm"):   return "Яндекс.Директ"
    if src in ("vk", "vkontakte") or src.startswith("vk."): return "ВКонтакте"
    if "telegram" in src or src == "tg":                     return "Telegram"
    if "travelline" in src or med == "email":                return "Рассылки TravelLine"
    if (trf in ("organic", "search")) and not src:
        return "Поиск брендовый" if any(k in cmp for k in BRAND_KEYWORDS) else "Поиск общий"
    if trf == "direct" and not src:                          return "Прямой трафик"
    return None


async def _fetch(token: str, counter_id: str, date1: str, date2: str,
                 section_filter: Optional[str]) -> list[dict]:
    goal_metrics = ",".join(
        f"ym:s:goal{g}reaches"
        for g in BOOKING_GOAL_IDS + CALL_GOAL_IDS
    )
    params: dict = {
        "id":         counter_id,
        "metrics":    f"ym:s:visits,ym:s:bounceRate,{goal_metrics}",
        "dimensions": "ym:s:lastTrafficSource,ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMCampaign",
        "date1":      date1,
        "date2":      date2,
        "limit":      1000,
        "lang":       "ru",
    }
    if section_filter:
        params["filters"] = section_filter

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            METRIKA_STAT_URL,
            headers={"Authorization": f"OAuth {token}"},
            params=params,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])


def _aggregate(rows: list[dict]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    n_book, n_call = len(BOOKING_GOAL_IDS), len(CALL_GOAL_IDS)

    for row in rows:
        dims    = row.get("dimensions", [])
        metrics = row.get("metrics",    [])
        if len(dims) < 4 or not metrics:
            continue

        channel = _classify(
            dims[1].get("name") or "", dims[2].get("name") or "",
            dims[3].get("name") or "", dims[0].get("name") or "",
        )
        if not channel:
            continue

        if channel not in result:
            result[channel] = {"visitors": 0, "bounces": 0.0,
                               "bookings": 0, "calls": 0, "room_interest": 0, "_w": 0}

        r, visits, bounce = result[channel], float(metrics[0] or 0), float(metrics[1] or 0)
        total_w = r["_w"] + visits
        if total_w > 0:
            r["bounces"] = (r["bounces"] * r["_w"] + bounce * visits) / total_w
        r["_w"] += visits
        r["visitors"] += int(visits)

        idx = 2
        for _ in range(n_book):
            if idx < len(metrics): r["bookings"] += int(metrics[idx] or 0)
            idx += 1
        for _ in range(n_call):
            if idx < len(metrics): r["calls"] += int(metrics[idx] or 0)
            idx += 1

    for ch in result:
        result[ch].pop("_w", None)
    return result


def _with_derived(row: dict, costs: Optional[float] = None) -> dict:
    r = dict(row)
    r["costs"] = costs
    leads, revenue = (r.get("calls") or 0) + (r.get("bookings") or 0), r.get("revenue")
    r["cpl"]       = (costs / leads)                          if (costs and leads)              else None
    r["roi"]       = ((revenue - costs) / costs * 100)        if (revenue and costs)            else None
    r["avg_check"] = (revenue / r["bookings"])                if (revenue and r.get("bookings")) else None
    return r


def _totals(channels: dict) -> dict:
    t = dict(visitors=0, room_interest=0, calls=0, bookings=0, revenue=None, costs=None)
    for row in channels.values():
        t["visitors"]      += row.get("visitors")      or 0
        t["room_interest"] += row.get("room_interest") or 0
        t["calls"]         += row.get("calls")         or 0
        t["bookings"]      += row.get("bookings")      or 0
        if row.get("revenue") is not None:
            t["revenue"] = (t["revenue"] or 0) + row["revenue"]
        if row.get("costs") is not None:
            t["costs"] = (t["costs"] or 0) + row["costs"]
    leads, revenue, costs = t["calls"] + t["bookings"], t.get("revenue"), t.get("costs")
    t["cpl"]       = (costs / leads)                   if (costs and leads)   else None
    t["roi"]       = ((revenue - costs) / costs * 100) if (revenue and costs) else None
    t["avg_check"] = (revenue / t["bookings"])          if (revenue and t["bookings"]) else None
    return t


# ── Эндпоинт ─────────────────────────────────────────────────────────────────

@router.get("")
async def get_channels(
    period:       str           = Query(...,      description="YYYY-MM, например 2026-05"),
    section:      str           = Query("hotels", description="hotels | franchise | uk"),
    hotel:        Optional[str] = Query(None),
    current_user: User          = Depends(get_current_user),
    db:           AsyncSession  = Depends(get_db),
):
    # ── Получаем токен из БД (так же, как в metrika.py) ─────────────────────
    token = await TokenService.get_decrypted_token(db, str(current_user.id))
    if not token:
        raise HTTPException(403, "Токен Яндекс.Метрики не найден. "
                                 "Добавьте токен в настройках сайта.")

    # ── Получаем counter_id из website пользователя ──────────────────────────
    result = await db.execute(
        select(Website).where(Website.user_id == current_user.id).limit(1)
    )
    website    = result.scalar_one_or_none()
    counter_id = str(website.counter_id) if website else "40050615"

    if section not in SECTION_FILTERS:
        raise HTTPException(400, f"Неизвестный раздел: {section}")

    try:
        d1, d2   = _period_dates(period)
        pd1, pd2 = _period_dates(_prev_period(period))
    except ValueError:
        raise HTTPException(400, f"Неверный формат периода: {period}")

    sf = SECTION_FILTERS[section]

    try:
        cur_rows, prv_rows = await asyncio.gather(
            _fetch(token, counter_id, d1.isoformat(), d2.isoformat(), sf),
            _fetch(token, counter_id, pd1.isoformat(), pd2.isoformat(), sf),
        )
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"Яндекс.Метрика API: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(502, f"Ошибка запроса к Метрике: {e}")

    cur = _aggregate(cur_rows)
    prv = _aggregate(prv_rows)

    channels: dict[str, dict] = {}
    for ch in ALL_CHANNELS:
        row     = cur.get(ch, {"visitors": 0, "bounces": 0.0,
                               "bookings": 0, "calls": 0, "room_interest": 0})
        prv_vis = (prv.get(ch) or {}).get("visitors") or 0
        cur_vis = row.get("visitors") or 0
        row["visitors_delta"] = (
            round((cur_vis - prv_vis) / prv_vis * 100, 1) if prv_vis else None
        )
        channels[ch] = _with_derived(row)

    return {"channels": channels, "totals": _totals(channels),
            "period": period, "section": section}
