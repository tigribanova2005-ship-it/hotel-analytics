"""
strategic.py
Безопасная версия стратегического отчёта.
Не падает на малом количестве клиентов.
"""

import pandas as pd


# ─────────────────────────────────────────────
# LTV DISTRIBUTION
# ─────────────────────────────────────────────

def table_ltv_distribution(client_metrics: pd.DataFrame) -> pd.DataFrame:

    if client_metrics.empty:
        return pd.DataFrame({"info": ["Нет клиентских данных"]})

    if "ltv_all_time" not in client_metrics.columns:
        return pd.DataFrame({"info": ["Колонка ltv_all_time отсутствует"]})

    ltv = client_metrics["ltv_all_time"].dropna()

    if len(ltv) < 10 or ltv.nunique() < 2:
        return pd.DataFrame({
            "info": [f"Недостаточно клиентов для децильной сегментации (n={len(ltv)})"]
        })

    deciles = pd.qcut(
        ltv,
        q=10,
        labels=[f"D{i}" for i in range(1, 11)],
        duplicates="drop"
    )

    result = (
        client_metrics.assign(decile=deciles)
        .groupby("decile")
        .agg(
            clients=("client_id", "count"),
            avg_ltv=("ltv_all_time", "mean"),
        )
        .reset_index()
    )

    return result


# ─────────────────────────────────────────────
# MAIN REPORT
# ─────────────────────────────────────────────

def print_full_report(
    channel_kpi_city: pd.DataFrame,
    channel_kpi_country: pd.DataFrame,
    client_metrics: pd.DataFrame,
    cohort_retention: pd.DataFrame,
    migration_data,
    ltv_confidence: pd.DataFrame,
):

    print("\n==============================")
    print("СТРАТЕГИЧЕСКИЙ ОТЧЁТ")
    print("==============================\n")

    print("---- KPI (City) ----")
    if channel_kpi_city is not None and not channel_kpi_city.empty:
        print(channel_kpi_city.to_string(index=False))
    else:
        print("Нет данных")

    print("\n---- KPI (Country) ----")
    if channel_kpi_country is not None and not channel_kpi_country.empty:
        print(channel_kpi_country.to_string(index=False))
    else:
        print("Нет данных")

    print("\n---- LTV распределение ----")
    ltv_table = table_ltv_distribution(client_metrics)
    print(ltv_table.to_string(index=False))

    print("\n---- Cohort retention ----")
    if cohort_retention is not None and not cohort_retention.empty:
        print(cohort_retention.to_string(index=False))
    else:
        print("Нет данных")

    print("\n---- LTV по confidence ----")
    if ltv_confidence is not None and not ltv_confidence.empty:
        print(ltv_confidence.to_string(index=False))
    else:
        print("Нет данных")

    print("\n---- Миграция гостей ----")
    if isinstance(migration_data, dict) and migration_data.get("detail") is not None:
        if not migration_data["detail"].empty:
            print(migration_data["detail"].to_string(index=False))
        else:
            print("Нет данных")
    else:
        print("Нет данных")

    print("\n==============================\n")