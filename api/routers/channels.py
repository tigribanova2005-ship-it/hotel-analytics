"""
GET /api/channels — главная точка данных для таблицы каналов.

Добавить в main FastAPI app:
    from api.routers.channels import router as channels_router
    app.include_router(channels_router)
"""

import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.services.metrika import (
    aggregate,
    compute_delta,
    compute_derived,
    compute_totals,
    fetch_raw,
    period_dates,
    prev_period,
)

router = APIRouter(prefix="/api", tags=["channels"])

# Фильтры по разделам сайта
SECTION_FILTERS: dict[str, Optional[str]] = {
    "hotels":    None,                              # весь сайт
    "franchise": "ym:s:URLPath=~'/franshiza/'",
    "uk":        "ym:s:URLPath=~'/uk/'",
}


@router.get("/channels")
async def get_channels(
    section: str = Query("hotels", description="hotels | franchise | uk"),
    period:  str = Query(...,       description="YYYY-MM"),
    hotel:   Optional[str] = Query(None, description="rubinstein | italiana | ... | all"),
):
    token = os.getenv("METRIKA_TOKEN")
    if not token:
        raise HTTPException(500, "METRIKA_TOKEN не задан в .env")

    if section not in SECTION_FILTERS:
        raise HTTPException(400, f"Неизвестный раздел: {section}. Допустимые: {list(SECTION_FILTERS)}")

    try:
        d1, d2 = period_dates(period)
    except ValueError:
        raise HTTPException(400, f"Неверный формат периода: {period}. Ожидается YYYY-MM")

    prev = prev_period(period)
    pd1, pd2 = period_dates(prev)

    section_filter = SECTION_FILTERS[section]

    # Параллельно запрашиваем текущий и прошлый месяц
    try:
        import asyncio
        current_rows, prev_rows = await asyncio.gather(
            fetch_raw(token, d1.isoformat(), d2.isoformat(), section_filter),
            fetch_raw(token, pd1.isoformat(), pd2.isoformat(), section_filter),
        )
    except Exception as e:
        raise HTTPException(502, f"Ошибка Яндекс.Метрика API: {e}")

    current_agg = aggregate(current_rows)
    prev_agg    = aggregate(prev_rows)

    # Получить бюджеты из БД (заглушка — замените на реальный запрос к вашей БД)
    budgets = await _load_budgets(period, section, hotel)

    # Собрать итоговые строки с дельтой и производными метриками
    channels: dict[str, dict] = {}
    for channel in _ALL_CHANNELS:
        cur  = current_agg.get(channel, {})
        prv  = prev_agg.get(channel, {})

        cur["visitors_delta"] = compute_delta(
            cur.get("visitors") or 0,
            prv.get("visitors") or 0,
        )

        row = compute_derived(cur, costs=budgets.get(channel))
        channels[channel] = row

    totals = compute_totals(channels)

    return {
        "channels": channels,
        "totals":   totals,
        "period":   period,
        "section":  section,
    }


# Порядок каналов совпадает с фронтендом
_ALL_CHANNELS = [
    "Поиск брендовый",
    "Поиск общий",
    "Яндекс.Директ",
    "Яндекс.Карты",
    "Google.Карты",
    "2ГИС",
    "Прямой трафик",
    "ВКонтакте",
    "Telegram",
    "Рассылки TravelLine",
]


async def _load_budgets(period: str, section: str, hotel: Optional[str]) -> dict[str, Optional[float]]:
    """
    Заглушка: возвращает пустой словарь.
    Замените на запрос к вашей БД PostgreSQL:

        SELECT channel, amount FROM budgets
        WHERE period = :period AND section = :section AND hotel = :hotel
    """
    return {}
