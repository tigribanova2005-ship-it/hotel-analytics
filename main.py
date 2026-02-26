"""
main.py
Точка входа. Запускает полный pipeline.
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from settings import HOTELS
from ingest import load_all_data
from transform import transform_bookings, transform_guests, build_clients_table
from identify import enrich_bookings_with_client_id
from db import (
    get_engine,
    init_db,
    upsert_hotels,
    load_clients,
    load_bookings,
    load_guests,
    save_metrics,
    query,
)
from metrics import (
    build_client_metrics,
    cohort_retention,
    channel_kpi,
    guest_migration_analysis,
    ltv_by_confidence,
    segment_guests,
)
from strategic import print_full_report


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("main")


# ─────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────

def run_pipeline(data_path: str, db_path: str) -> None:

    logger.info("=== Инициализация базы данных ===")
    engine = get_engine(db_path)
    init_db(engine)
    upsert_hotels(engine, HOTELS)

    logger.info("=== Загрузка и нормализация данных ===")
    raw = load_all_data(data_path)

    if "bookings" not in raw and "guests" not in raw:
        logger.error("Данные не найдены. Проверьте путь и имена файлов.")
        return

    # ─────────────────────────
    # GUESTS
    # ─────────────────────────

    guests_df = pd.DataFrame()

    if "guests" in raw:
        logger.info("=== Трансформация гостевых данных ===")
        guests_df = transform_guests(raw["guests"])

        clients_df = build_clients_table(guests_df)

        load_clients(engine, clients_df)
        load_guests(engine, guests_df)

        logger.info(
            f"Гостей: {len(guests_df)}, "
            f"уникальных клиентов: {clients_df['client_id'].nunique()}"
        )

    # ─────────────────────────
    # BOOKINGS
    # ─────────────────────────

    bookings_df = pd.DataFrame()

    if "bookings" in raw:
        logger.info("=== Трансформация бронирований ===")
        bookings_df = transform_bookings(raw["bookings"])

        if not guests_df.empty:
            bookings_df = enrich_bookings_with_client_id(
                bookings_df,
                guests_df,
            )

        load_bookings(engine, bookings_df)
        logger.info(f"Бронирований: {len(bookings_df)}")

    if bookings_df.empty:
        logger.warning("Бронирований нет — метрики не рассчитываются")
        return

    # ─────────────────────────
    # METRICS
    # ─────────────────────────

    logger.info("=== Расчёт клиентских метрик ===")

    client_metrics_df = build_client_metrics(bookings_df)

    if not guests_df.empty:
        guest_types = segment_guests(guests_df, client_metrics_df)
        client_metrics_df = client_metrics_df.merge(
            guest_types,
            on="client_id",
            how="left",
        )

    save_metrics(engine, client_metrics_df)

    # ─────────────────────────
    # ANALYTICS
    # ─────────────────────────

    logger.info("=== Аналитика ===")

    retention_df = cohort_retention(bookings_df)

    kpi_city = channel_kpi(
        bookings_df,
        client_metrics_df,
        marketing_costs=None,
        hotel_type_filter="city",
    )

    kpi_country = channel_kpi(
        bookings_df,
        client_metrics_df,
        marketing_costs=None,
        hotel_type_filter="country",
    )

    migration = guest_migration_analysis(
        bookings_df,
        client_metrics_df,
    )

    ltv_conf = ltv_by_confidence(
        client_metrics_df,
        bookings_df,
    )

    logger.info("=== Стратегический отчёт ===")

    print_full_report(
        channel_kpi_city=kpi_city,
        channel_kpi_country=kpi_country,
        client_metrics=client_metrics_df,
        cohort_retention=retention_df,
        migration_data=migration,
        ltv_confidence=ltv_conf,
    )

    _save_analytics(
        engine,
        kpi_city,
        kpi_country,
        retention_df,
        migration,
        ltv_conf,
    )

    logger.info("✅ Pipeline завершён успешно")


# ─────────────────────────────────────────────
# SAVE ANALYTICS
# ─────────────────────────────────────────────

def _save_analytics(engine, kpi_city, kpi_country, retention_df, migration, ltv_conf):

    if not kpi_city.empty:
        kpi_city.to_sql(
            "analytics_channel_kpi_city",
            engine,
            if_exists="replace",
            index=False,
        )

    if not kpi_country.empty:
        kpi_country.to_sql(
            "analytics_channel_kpi_country",
            engine,
            if_exists="replace",
            index=False,
        )

    if not retention_df.empty:
        retention_df.to_sql(
            "analytics_cohort_retention",
            engine,
            if_exists="replace",
            index=False,
        )

    if not ltv_conf.empty:
        ltv_conf.to_sql(
            "analytics_ltv_confidence",
            engine,
            if_exists="replace",
            index=False,
        )

    if migration.get("detail") is not None and not migration["detail"].empty:
        migration["detail"].to_sql(
            "analytics_guest_migration",
            engine,
            if_exists="replace",
            index=False,
        )

    logger.info("Аналитические таблицы сохранены в БД")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

def main():

    parser = argparse.ArgumentParser(description="Hotel Analytics Pipeline")

    parser.add_argument(
        "--data-path",
        default="data/raw",
        help="Путь к папке с xlsx-файлами Travelline",
    )

    parser.add_argument(
        "--db",
        default="analytics.db",
        help="Путь к SQLite-базе",
    )

    args = parser.parse_args()

    run_pipeline(args.data_path, args.db)


if __name__ == "__main__":
    main()
