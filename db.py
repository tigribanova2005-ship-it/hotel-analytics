"""
db.py
Финальная согласованная версия.
Совместима с main.py.
"""

import logging
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# ENGINE
# ─────────────────────────────────────────────

def get_engine(db_path: str):
    return sa.create_engine(f"sqlite:///{db_path}", future=True)


# ─────────────────────────────────────────────
# INIT DB
# ─────────────────────────────────────────────

def init_db(engine):
    with engine.begin() as conn:

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bookings (
            booking_id TEXT PRIMARY KEY,
            client_id TEXT,
            hotel_name TEXT,
            hotel_type TEXT,
            amount REAL,
            checkin_date DATE,
            channel TEXT,
            source TEXT,
            confidence_level TEXT,
            report_year INTEGER,
            report_month INTEGER,
            source_file TEXT
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS guests (
            guest_id TEXT PRIMARY KEY,
            booking_id TEXT,
            client_id TEXT,
            hotel_name TEXT,
            normalized_name TEXT,
            citizenship TEXT,
            phone TEXT,
            email TEXT,
            birth_date DATE,
            checkin_date DATE,
            confidence_level TEXT,
            report_year INTEGER,
            report_month INTEGER,
            source_file TEXT
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS clients (
            client_id TEXT PRIMARY KEY,
            normalized_name TEXT,
            best_phone TEXT,
            best_email TEXT,
            birth_date DATE,
            citizenship TEXT,
            confidence_level TEXT
        )
        """))


# ─────────────────────────────────────────────
# HOTELS
# ─────────────────────────────────────────────

def upsert_hotels(engine, hotels_dict):
    df = pd.DataFrame(hotels_dict)
    if df.empty:
        return
    df.to_sql("dim_hotels", engine, if_exists="replace", index=False)


# ─────────────────────────────────────────────
# LOADERS
# ─────────────────────────────────────────────

def load_bookings(engine, df: pd.DataFrame):
    if df.empty:
        return
    df.to_sql("bookings", engine, if_exists="append", index=False)


def load_guests(engine, df: pd.DataFrame):
    if df.empty:
        return
    df.to_sql("guests", engine, if_exists="append", index=False)


def load_clients(engine, df: pd.DataFrame):
    if df.empty:
        return
    df.to_sql("clients", engine, if_exists="append", index=False)


def load_marketing_costs(engine, df: pd.DataFrame):
    if df.empty:
        return
    df.to_sql("marketing_costs", engine, if_exists="replace", index=False)


# ─────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────

def save_metrics(engine, metrics_df: pd.DataFrame):
    if metrics_df.empty:
        return

    # Всегда перезаписываем — аналитическая таблица
    metrics_df.to_sql(
        "client_metrics",
        engine,
        if_exists="replace",
        index=False
    )


# ─────────────────────────────────────────────
# QUERY
# ─────────────────────────────────────────────

def query(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)