"""
backend_additions/travelline.py

TravelLine API integration.
Base URL: https://partner.tlintegration.com/api/webpms/v1/
Auth: X-API-KEY header from env var TRAVELLINE_API_KEY.
"""

import os
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User

router = APIRouter(prefix="/travelline", tags=["travelline"])

TL_BASE_URL = "https://partner.tlintegration.com/api/webpms/v1"

# Property IDs for each hotel
HOTEL_PROPERTIES = {
    "italiana":    6857,
    "nevsky":      8059,
    "rubinstein":  12178,
    "centralniy":  15958,
    "gold":        15960,
    "point":       19402,
    "lesnaya":     43901,
}

HOTEL_NAMES = {
    "italiana":   "Итальянская",
    "nevsky":     "Невский",
    "rubinstein": "Рубинштейна",
    "centralniy": "Центральный",
    "gold":       "Голд",
    "point":      "Поинт",
    "lesnaya":    "Лесная Ривьера",
}

# Display order for hotels
HOTEL_ORDER = ["italiana", "nevsky", "rubinstein", "centralniy", "gold", "point", "lesnaya"]


def _get_api_key() -> Optional[str]:
    return os.environ.get("TRAVELLINE_API_KEY") or None


def _is_cancelled(booking: dict) -> bool:
    status = (booking.get("status") or "").lower()
    return "cancel" in status or "отмен" in status


def _period_dates(period: str) -> tuple[str, str]:
    """Return (date_from, date_to) as ISO strings for the given YYYY-MM period."""
    import calendar
    year, month = map(int, period.split("-"))
    last = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last:02d}"


async def _fetch_bookings_for_property(
    client: httpx.AsyncClient,
    api_key: str,
    property_id: int,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """Fetch bookings for a single property from TravelLine API."""
    try:
        resp = await client.get(
            f"{TL_BASE_URL}/bookings",
            headers={"X-API-KEY": api_key},
            params={
                "propertyId": property_id,
                "dateFrom": date_from,
                "dateTo": date_to,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        # Response may be {"items": [...]} or a list
        if isinstance(data, list):
            return data
        return data.get("items", [])
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"TravelLine API error {e.response.status_code}: {e.response.text[:300]}")
    except Exception:
        return []


@router.get("/bookings")
async def get_tl_bookings(
    period: str = Query(..., description="YYYY-MM"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Aggregate bookings across all hotels for the given period."""
    api_key = _get_api_key()
    if not api_key:
        return {
            "available": False,
            "total_bookings": 0,
            "total_revenue": 0,
            "avg_check": None,
            "by_hotel": [],
        }

    date_from, date_to = _period_dates(period)

    import asyncio
    async with httpx.AsyncClient(timeout=30) as client:
        tasks = [
            _fetch_bookings_for_property(client, api_key, prop_id, date_from, date_to)
            for prop_id in HOTEL_PROPERTIES.values()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    total_bookings = 0
    total_revenue = 0.0
    by_hotel = []

    for key, bookings in zip(HOTEL_PROPERTIES.keys(), results):
        if isinstance(bookings, Exception):
            bookings = []
        active = [b for b in bookings if not _is_cancelled(b)]
        count = len(active)
        revenue = sum(float(b.get("totalAmount") or b.get("total_amount") or 0) for b in active)
        total_bookings += count
        total_revenue += revenue
        by_hotel.append({
            "hotel_key":  key,
            "property_id": HOTEL_PROPERTIES[key],
            "name":       HOTEL_NAMES[key],
            "bookings":   count,
            "revenue":    revenue,
        })

    # Sort by display order
    by_hotel.sort(key=lambda h: HOTEL_ORDER.index(h["hotel_key"]) if h["hotel_key"] in HOTEL_ORDER else 99)

    avg_check = (total_revenue / total_bookings) if total_bookings > 0 else None

    return {
        "available":      True,
        "total_bookings": total_bookings,
        "total_revenue":  total_revenue,
        "avg_check":      avg_check,
        "by_hotel":       by_hotel,
    }


@router.get("/hotels")
async def get_tl_hotels(
    period: str = Query(..., description="YYYY-MM"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Per-hotel breakdown of bookings and revenue."""
    api_key = _get_api_key()
    if not api_key:
        return {
            "available": False,
            "hotels": [],
        }

    date_from, date_to = _period_dates(period)

    import asyncio
    async with httpx.AsyncClient(timeout=30) as client:
        tasks = [
            _fetch_bookings_for_property(client, api_key, prop_id, date_from, date_to)
            for prop_id in HOTEL_PROPERTIES.values()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    hotels = []
    for key, bookings in zip(HOTEL_PROPERTIES.keys(), results):
        if isinstance(bookings, Exception):
            bookings = []
        active = [b for b in bookings if not _is_cancelled(b)]
        count = len(active)
        revenue = sum(float(b.get("totalAmount") or b.get("total_amount") or 0) for b in active)
        hotels.append({
            "hotel_key":   key,
            "property_id": HOTEL_PROPERTIES[key],
            "name":        HOTEL_NAMES[key],
            "bookings":    count,
            "revenue":     revenue,
        })

    hotels.sort(key=lambda h: HOTEL_ORDER.index(h["hotel_key"]) if h["hotel_key"] in HOTEL_ORDER else 99)

    return {
        "available": True,
        "hotels":    hotels,
    }


@router.get("/promos")
async def get_tl_promos(
    period: str = Query(..., description="YYYY-MM"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Promo code usage statistics for the given period."""
    api_key = _get_api_key()
    if not api_key:
        return {
            "available": False,
            "promos": [],
        }

    date_from, date_to = _period_dates(period)

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{TL_BASE_URL}/promo-codes",
                headers={"X-API-KEY": api_key},
                params={"dateFrom": date_from, "dateTo": date_to},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"TravelLine API error {e.response.status_code}: {e.response.text[:300]}")

    if isinstance(data, list):
        items = data
    else:
        items = data.get("items", [])

    promos = []
    for item in items:
        code = item.get("code") or item.get("promoCode") or ""
        discount = item.get("discount") or item.get("discountPercent") or 0
        uses = item.get("uses") or item.get("usageCount") or 0
        total = float(item.get("total") or item.get("totalAmount") or 0)
        promos.append({
            "code":     code,
            "discount": discount,
            "uses":     uses,
            "total":    total,
        })

    return {
        "available": True,
        "promos":    promos,
    }
