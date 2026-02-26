"""
analytics/metrics.py
────────────────────
Расчёт клиентских метрик:
  - LTV (all-time, 12m, 24m)
  - retention (когортный анализ по годам)
  - частота визитов
  - длительность отношений
  - сегментация

Все расчёты векторизованы через pandas.
"""

import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from settings import SEGMENT_LTV_THRESHOLDS, LTV_WINDOWS

logger = logging.getLogger(__name__)

TODAY = pd.Timestamp(date.today())


# ─── 1. Клиентская витрина ────────────────────────────────────────────────────

def build_client_metrics(
    bookings: pd.DataFrame,
    reference_date: pd.Timestamp = TODAY,
) -> pd.DataFrame:
    """
    Принимает bookings с полями:
      client_id, hotel_type, amount, checkin_date, channel, confidence_level

    Возвращает DataFrame client_metrics.
    """
    bk = bookings.copy()
    bk["checkin_date"] = pd.to_datetime(bk["checkin_date"], errors="coerce")
    bk["amount"] = pd.to_numeric(bk["amount"], errors="coerce").fillna(0)

    # ─── Общие агрегаты ───────────────────────────────────────────────────────
    grp = bk.groupby("client_id")

    total_visits   = grp["booking_id"].nunique().rename("total_visits")
    total_revenue  = grp["amount"].sum().rename("total_revenue")
    first_visit    = grp["checkin_date"].min().rename("first_visit_date")
    last_visit     = grp["checkin_date"].max().rename("last_visit_date")
    first_channel  = grp.apply(
        lambda g: g.sort_values("checkin_date")["channel"].iloc[0]
        if not g.empty else None
    ).rename("first_channel")
    first_hotel_type = grp.apply(
        lambda g: g.sort_values("checkin_date")["hotel_type"].iloc[0]
        if not g.empty else None
    ).rename("first_hotel_type")

    # ─── По типу отеля ───────────────────────────────────────────────────────
    city_bk    = bk[bk["hotel_type"] == "city"]
    country_bk = bk[bk["hotel_type"] == "country"]

    city_visits   = city_bk.groupby("client_id")["booking_id"].nunique().rename("city_visits")
    country_visits = country_bk.groupby("client_id")["booking_id"].nunique().rename("country_visits")
    city_revenue   = city_bk.groupby("client_id")["amount"].sum().rename("city_revenue")
    country_revenue = country_bk.groupby("client_id")["amount"].sum().rename("country_revenue")

    # ─── LTV windows ─────────────────────────────────────────────────────────
    ltv_all  = total_revenue.rename("ltv_all_time")

    cutoff_12m = reference_date - pd.Timedelta(days=365)
    cutoff_24m = reference_date - pd.Timedelta(days=730)
    ltv_12m = (
        bk[bk["checkin_date"] >= cutoff_12m]
        .groupby("client_id")["amount"].sum()
        .rename("ltv_12m")
    )
    ltv_24m = (
        bk[bk["checkin_date"] >= cutoff_24m]
        .groupby("client_id")["amount"].sum()
        .rename("ltv_24m")
    )

    # ─── Сборка витрины ──────────────────────────────────────────────────────
    metrics = pd.concat([
        total_visits, city_visits, country_visits,
        total_revenue, city_revenue, country_revenue,
        ltv_all, ltv_12m, ltv_24m,
        first_visit, last_visit,
        first_channel, first_hotel_type,
    ], axis=1).fillna(0)

    metrics["city_visits"]    = metrics["city_visits"].astype(int)
    metrics["country_visits"] = metrics["country_visits"].astype(int)

    # ─── Производные метрики ─────────────────────────────────────────────────
    metrics["avg_check"] = np.where(
        metrics["total_visits"] > 0,
        metrics["total_revenue"] / metrics["total_visits"],
        0,
    )

    metrics["first_visit_date"] = pd.to_datetime(metrics["first_visit_date"])
    metrics["last_visit_date"]  = pd.to_datetime(metrics["last_visit_date"])

    metrics["relationship_days"] = (
        (metrics["last_visit_date"] - metrics["first_visit_date"])
        .dt.days.fillna(0).astype(int)
    )

    # Частота: среднее количество дней между визитами
    freq = grp.apply(_visit_frequency)
    metrics["visit_frequency_days"] = freq

    # Флаг миграции city→country
    metrics["migrated_to_country"] = (
        (metrics["city_visits"] > 0) & (metrics["country_visits"] > 0)
    ).astype(int)

    # Confidence: берём лучший уровень из бронирований клиента
    conf_order = {"high": 0, "medium": 1, "low_dob": 2, "low": 3, "unknown": 4}
    conf = (
        bk.assign(_rank=bk["confidence_level"].map(conf_order).fillna(9))
        .sort_values("_rank")
        .groupby("client_id")["confidence_level"]
        .first()
        .rename("confidence_level")
    )
    metrics = metrics.join(conf)

    # ─── Сегментация ─────────────────────────────────────────────────────────
    metrics = _segment_by_ltv(metrics)
    metrics = _segment_by_frequency(metrics)

    metrics = metrics.reset_index()
    logger.info(f"Метрики рассчитаны для {len(metrics)} клиентов")
    return metrics


def _visit_frequency(group: pd.DataFrame) -> float:
    """Средний интервал между визитами в днях."""
    dates = group["checkin_date"].dropna().sort_values()
    if len(dates) < 2:
        return np.nan
    diffs = dates.diff().dt.days.dropna()
    return float(diffs.mean()) if not diffs.empty else np.nan


def _segment_by_ltv(df: pd.DataFrame) -> pd.DataFrame:
    q_high = df["ltv_all_time"].quantile(1 - SEGMENT_LTV_THRESHOLDS["high"])
    q_mid  = df["ltv_all_time"].quantile(1 - SEGMENT_LTV_THRESHOLDS["mid"])

    conditions = [
        df["ltv_all_time"] >= q_high,
        df["ltv_all_time"] >= q_mid,
    ]
    choices = ["high", "mid"]
    df["segment_ltv"] = np.select(conditions, choices, default="low")
    return df


def _segment_by_frequency(df: pd.DataFrame) -> pd.DataFrame:
    """frequent = 3+ визитов за период."""
    df["segment_frequency"] = np.where(df["total_visits"] >= 3, "frequent", "rare")
    return df


# ─── 2. Когортный retention ───────────────────────────────────────────────────

def cohort_retention(bookings: pd.DataFrame) -> pd.DataFrame:
    """
    Когортный анализ по году первого визита.
    Возвращает таблицу: cohort_year × visit_year → retention_rate
    """
    bk = bookings.copy()
    bk["checkin_date"] = pd.to_datetime(bk["checkin_date"], errors="coerce")
    bk["visit_year"] = bk["checkin_date"].dt.year

    first_year = (
        bk.groupby("client_id")["visit_year"].min().rename("cohort_year")
    )
    bk = bk.join(first_year, on="client_id")

    cohort_sizes = bk.groupby("cohort_year")["client_id"].nunique().rename("cohort_size")

    retention = (
        bk.groupby(["cohort_year", "visit_year"])["client_id"]
        .nunique()
        .reset_index(name="returning_clients")
    )
    retention = retention.merge(cohort_sizes, on="cohort_year")
    retention["retention_rate"] = retention["returning_clients"] / retention["cohort_size"]
    retention["years_since_first"] = retention["visit_year"] - retention["cohort_year"]

    return retention


def cohort_retention_pivot(bookings: pd.DataFrame) -> pd.DataFrame:
    """Сводная матрица retention (cohort_year × years_since_first)."""
    ret = cohort_retention(bookings)
    pivot = ret.pivot_table(
        index="cohort_year",
        columns="years_since_first",
        values="retention_rate",
        aggfunc="first",
    )
    pivot.columns = [f"Y+{c}" for c in pivot.columns]
    return pivot


# ─── 3. Маркетинговые KPI ─────────────────────────────────────────────────────

def channel_kpi(
    bookings: pd.DataFrame,
    client_metrics: pd.DataFrame,
    marketing_costs: pd.DataFrame = None,
    hotel_type_filter: str = None,
) -> pd.DataFrame:
    """
    KPI по каналам:
      - выручка, кол-во клиентов, LTV (по первому каналу привлечения)
      - доля возвратных
      - ROI (если есть marketing_costs)
      - CAC/LTV ratio

    hotel_type_filter: 'city' | 'country' | None (все)
    """
    bk = bookings.copy()
    if hotel_type_filter:
        bk = bk[bk["hotel_type"] == hotel_type_filter]

    bk["amount"] = pd.to_numeric(bk["amount"], errors="coerce").fillna(0)

    # Выручка и клиенты по каналу
    channel_revenue = bk.groupby("channel").agg(
        total_revenue=("amount", "sum"),
        total_bookings=("booking_id", "nunique"),
        unique_clients=("client_id", "nunique"),
    ).reset_index()

    # Возвратные гости по каналу
    clients_with_channel = bk.merge(
        client_metrics[["client_id", "total_visits", "segment_ltv", "first_channel", "ltv_all_time"]],
        on="client_id",
        how="left",
    )
    returning = (
        clients_with_channel[clients_with_channel["total_visits"] > 1]
        .groupby("channel")["client_id"]
        .nunique()
        .rename("returning_clients")
        .reset_index()
    )

    # LTV по каналу первого привлечения
    channel_ltv = (
        client_metrics.groupby("first_channel")
        .agg(
            avg_ltv=("ltv_all_time", "mean"),
            median_ltv=("ltv_all_time", "median"),
            total_ltv=("ltv_all_time", "sum"),
        )
        .reset_index()
        .rename(columns={"first_channel": "channel"})
    )

    result = (
        channel_revenue
        .merge(returning, on="channel", how="left")
        .merge(channel_ltv, on="channel", how="left")
    )

    result["returning_clients"] = result["returning_clients"].fillna(0).astype(int)
    result["retention_share"] = np.where(
        result["unique_clients"] > 0,
        result["returning_clients"] / result["unique_clients"],
        0,
    )
    result["avg_check"] = np.where(
        result["total_bookings"] > 0,
        result["total_revenue"] / result["total_bookings"],
        0,
    )

    # ROI и CAC/LTV если есть данные
    if marketing_costs is not None and not marketing_costs.empty:
        cac_by_channel = marketing_costs.groupby("channel")["cac_amount"].sum().reset_index()
        if hotel_type_filter:
            cac_typed = marketing_costs[marketing_costs["hotel_type"] == hotel_type_filter]
            cac_by_channel = cac_typed.groupby("channel")["cac_amount"].sum().reset_index()

        result = result.merge(cac_by_channel, on="channel", how="left")
        result["cac_amount"] = result["cac_amount"].fillna(0)
        result["roi"] = np.where(
            result["cac_amount"] > 0,
            (result["total_ltv"] - result["cac_amount"]) / result["cac_amount"],
            np.nan,
        )
        result["cac_ltv_ratio"] = np.where(
            result["total_ltv"] > 0,
            result["cac_amount"] / result["total_ltv"],
            np.nan,
        )
    else:
        result["cac_amount"]   = np.nan
        result["roi"]          = np.nan
        result["cac_ltv_ratio"] = np.nan

    return result.sort_values("total_ltv", ascending=False)


# ─── 4. Анализ перетока city → country ────────────────────────────────────────

def guest_migration_analysis(
    bookings: pd.DataFrame,
    client_metrics: pd.DataFrame,
) -> dict:
    """
    Анализирует переток гостей из городских в загородный.
    """
    bk = bookings.copy()
    bk["checkin_date"] = pd.to_datetime(bk["checkin_date"], errors="coerce")

    # Клиенты, которые были в обоих типах
    migrated = client_metrics[client_metrics["migrated_to_country"] == 1]["client_id"].tolist()

    migration_detail = (
        bk[bk["client_id"].isin(migrated)]
        .groupby(["client_id", "hotel_type"])
        .agg(
            visits=("booking_id", "nunique"),
            revenue=("amount", "sum"),
            first_date=("checkin_date", "min"),
        )
        .reset_index()
    )

    # Направление: city first → country или наоборот
    first_types = (
        bk[bk["client_id"].isin(migrated)]
        .sort_values("checkin_date")
        .groupby("client_id")["hotel_type"]
        .first()
        .rename("first_type")
    )
    migration_direction = first_types.value_counts()

    return {
        "total_migrated_clients": len(migrated),
        "pct_of_all_clients": len(migrated) / client_metrics["client_id"].nunique() if len(client_metrics) > 0 else 0,
        "migration_direction": migration_direction.to_dict(),
        "detail": migration_detail,
    }


# ─── 5. LTV с фильтром по confidence ─────────────────────────────────────────

def ltv_by_confidence(
    client_metrics: pd.DataFrame,
    bookings: pd.DataFrame,
) -> pd.DataFrame:
    """
    Сравнение LTV для high-confidence vs all clients.
    """
    high_ids = client_metrics[
        client_metrics["confidence_level"] == "high"
    ]["client_id"].tolist()

    def _stats(ids, label):
        df = client_metrics[client_metrics["client_id"].isin(ids)] if ids else client_metrics
        return {
            "segment":       label,
            "n_clients":     len(df),
            "avg_ltv":       df["ltv_all_time"].mean(),
            "median_ltv":    df["ltv_all_time"].median(),
            "total_revenue": df["total_revenue"].sum(),
            "avg_visits":    df["total_visits"].mean(),
        }

    return pd.DataFrame([
        _stats(high_ids, "LTV_high_confidence"),
        _stats(None,     "LTV_all"),
    ])


# ─── 6. Сегментация гостей ───────────────────────────────────────────────────

def segment_guests(
    guests: pd.DataFrame,
    client_metrics: pd.DataFrame,
) -> pd.DataFrame:
    """
    Определяет тип гостя по полю guests_count / состав:
    solo, couple, family, corporate
    """
    g = guests.copy()

    def _classify_type(val: str) -> str:
        if not isinstance(val, str):
            return "solo"
        val = val.lower().strip()
        if "корп" in val or "corporate" in val or "бизнес" in val:
            return "corporate"
        # Ищем числа взрослых/детей
        import re
        adults = re.findall(r"(\d+)\s*взр", val)
        children = re.findall(r"(\d+)\s*дет|реб", val)
        n_adults = int(adults[0]) if adults else (2 if "двое" in val else 1)
        n_children = int(children[0]) if children else 0
        if n_children > 0:
            return "family"
        if n_adults >= 2:
            return "couple"
        return "solo"

    g["segment_type"] = g.get("guests_count", pd.Series("", index=g.index)).apply(_classify_type)

    type_summary = (
        g.groupby(["client_id", "segment_type"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .drop_duplicates("client_id")
        [["client_id", "segment_type"]]
    )

    return type_summary
