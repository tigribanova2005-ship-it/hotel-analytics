"""
app/api/v1/budgets.py

GET  /api/v1/budgets — получить бюджеты за период
POST /api/v1/budgets — сохранить бюджет канала вручную

Таблица в PostgreSQL (создаётся автоматически через Base.metadata.create_all,
если добавить модель в app/models/__init__.py):

    CREATE TABLE IF NOT EXISTS channel_budgets (
        id         SERIAL PRIMARY KEY,
        period     CHAR(7)       NOT NULL,
        section    VARCHAR(32)   NOT NULL,
        hotel      VARCHAR(32)   NOT NULL DEFAULT 'all',
        channel    VARCHAR(64)   NOT NULL,
        amount     NUMERIC(14,2) NOT NULL,
        updated_at TIMESTAMPTZ   DEFAULT now(),
        UNIQUE (period, section, hotel, channel)
    );
"""

from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/budgets", tags=["budgets"])


class BudgetIn(BaseModel):
    period:  str   = Field(...,   example="2026-05")
    section: str   = Field(...,   example="hotels")
    hotel:   str   = Field("all", example="all")
    channel: str   = Field(...,   example="Яндекс.Директ")
    amount:  float = Field(...,   ge=0, example=150000)


class BudgetOut(BaseModel):
    period:  str
    section: str
    hotel:   str
    channel: str
    amount:  float


@router.get("", response_model=list[BudgetOut])
async def get_budgets(
    period:  str           = Query(..., description="YYYY-MM"),
    section: str           = Query("hotels"),
    hotel:   Optional[str] = Query("all"),
):
    """
    Возвращает бюджеты за период.

    TODO: заменить заглушку на запрос к БД через вашу сессию SQLAlchemy:

        from app.db.session import get_db
        from sqlalchemy import text

        async with get_db() as db:
            rows = await db.execute(
                text("SELECT period, section, hotel, channel, amount "
                     "FROM channel_budgets "
                     "WHERE period=:period AND section=:section AND hotel=:hotel"),
                {"period": period, "section": section, "hotel": hotel or "all"}
            )
            return [BudgetOut(**r._mapping) for r in rows]
    """
    return []  # заглушка


@router.post("", response_model=BudgetOut)
async def upsert_budget(body: BudgetIn):
    """
    Сохраняет или обновляет бюджет (UPSERT).

    TODO: заменить заглушку на UPSERT через вашу сессию SQLAlchemy:

        from app.db.session import get_db
        from sqlalchemy import text

        async with get_db() as db:
            await db.execute(
                text(\"\"\"
                    INSERT INTO channel_budgets (period, section, hotel, channel, amount)
                    VALUES (:period, :section, :hotel, :channel, :amount)
                    ON CONFLICT (period, section, hotel, channel)
                    DO UPDATE SET amount = EXCLUDED.amount, updated_at = now()
                \"\"\"),
                body.model_dump()
            )
            await db.commit()
    """
    return BudgetOut(**body.model_dump())  # заглушка
