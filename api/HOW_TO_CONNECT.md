# Подключение новых роутеров к вашему FastAPI backend

## 1. Добавьте роутеры в main.py (или app.py)

```python
from api.routers.channels import router as channels_router
from api.routers.budgets  import router as budgets_router

app.include_router(channels_router)
app.include_router(budgets_router)
```

## 2. Добавьте зависимость httpx

```bash
pip install httpx
```

или в requirements.txt:
```
httpx>=0.27
```

## 3. Убедитесь, что в .env есть токен Метрики

```
METRIKA_TOKEN=ваш_токен_здесь
```

## 4. Создайте таблицу бюджетов в PostgreSQL

```sql
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
```

## 5. Замените заглушки на реальные запросы к БД

В `api/routers/budgets.py` — замените `# TODO` на запросы через ваш `db` объект.
В `api/routers/channels.py` — замените `_load_budgets()` на реальный запрос.

## Новые эндпоинты

| Метод | URL | Описание |
|-------|-----|----------|
| GET | `/api/channels?period=2025-05&section=hotels&hotel=all` | Таблица каналов |
| GET | `/api/budgets?period=2025-05&section=hotels` | Бюджеты за период |
| POST | `/api/budgets` | Сохранить бюджет канала |
