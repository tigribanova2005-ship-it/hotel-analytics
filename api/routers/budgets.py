"""
GET  /api/budgets — получить бюджеты за период
POST /api/budgets — сохранить бюджет канала вручную

Добавить в main FastAPI app:
    from api.routers.budgets import router as budgets_router
    app.include_router(budgets_router)

Таблица в PostgreSQL (создать один раз):
    CREATE TABLE IF NOT EXISTS channel_budgets (
        id        SERIAL PRIMARY KEY,
        period    CHAR(7)      NOT NULL,   -- '2025-05'
        section   VARCHAR(32)  NOT NULL,   -- 'hotels' | 'franchise' | 'uk'
        hotel     VARCHAR(32)  NOT NULL DEFAULT 'all',
        channel   VARCHAR(64)  NOT NULL,
        amount    NUMERIC(14,2) NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE (period, section, hotel, channel)
    );
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api", tags=["budgets"])


class BudgetIn(BaseModel):
    period:  str            = Field(..., example="2025-05")
    section: str            = Field(..., example="hotels")
    hotel:   str            = Field("all", example="all")
    channel: str            = Field(..., example="Яндекс.Директ")
    amount:  float          = Field(..., ge=0, example=150000)


class BudgetOut(BaseModel):
    period:  str
    section: str
    hotel:   str
    channel: str
    amount:  float


@router.get("/budgets", response_model=list[BudgetOut])
async def get_budgets(
    period:  str = Query(..., description="YYYY-MM"),
    section: str = Query("hotels"),
    hotel:   Optional[str] = Query("all"),
):
    """
    Возвращает все сохранённые бюджеты за указанный период/раздел/отель.

    Заглушка — замените тело на реальный запрос к БД:

        rows = await db.fetch_all(
            "SELECT period, section, hotel, channel, amount FROM channel_budgets "
            "WHERE period=:period AND section=:section AND hotel=:hotel",
            {"period": period, "section": section, "hotel": hotel or "all"}
        )
        return [BudgetOut(**r) for r in rows]
    """
    # TODO: заменить на запрос к БД
    return []


@router.post("/budgets", response_model=BudgetOut, status_code=200)
async def upsert_budget(body: BudgetIn):
    """
    Создаёт или обновляет бюджет канала (UPSERT по period+section+hotel+channel).

    Заглушка — замените тело на реальный UPSERT:

        await db.execute(
            \"\"\"
            INSERT INTO channel_budgets (period, section, hotel, channel, amount)
            VALUES (:period, :section, :hotel, :channel, :amount)
            ON CONFLICT (period, section, hotel, channel)
            DO UPDATE SET amount = EXCLUDED.amount, updated_at = now()
            \"\"\",
            body.model_dump()
        )
        return BudgetOut(**body.model_dump())
    """
    # TODO: заменить на запрос к БД
    return BudgetOut(**body.model_dump())
