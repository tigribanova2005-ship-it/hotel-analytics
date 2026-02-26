"""
transform.py
Стабильная версия с channel.
"""

import hashlib
import logging
import numpy as np
import pandas as pd

from ingest import normalize_dates_in_df, normalize_name
from identify import build_client_ids, remap_columns

logger = logging.getLogger(__name__)


def _make_surrogate_id(*parts) -> str:
    composite = "|".join(str(p) for p in parts)
    return hashlib.md5(composite.encode()).hexdigest()[:20]


# ─────────────────────────────────────────────
# BOOKINGS
# ─────────────────────────────────────────────

BOOKING_FIELD_ALIASES = {
    "amount": ["сумма", "amount", "стоимость", "выручка"],
    "checkin_date": ["дата_заезда", "checkin", "arrival"],
    "checkout_date": ["дата_выезда", "checkout", "departure"],
    "source": ["источник", "source"],
    "channel": ["канал", "channel", "источник_бронирования"],
    "name": ["гость", "фио", "name"],
}


def transform_bookings(raw_bookings: pd.DataFrame) -> pd.DataFrame:

    df = raw_bookings.copy()
    df = remap_columns(df, BOOKING_FIELD_ALIASES)

    df = normalize_dates_in_df(df, ["checkin_date", "checkout_date"])

    if "amount" in df.columns:
        df["amount"] = (
            df["amount"]
            .astype(str)
            .str.replace(r"[^\d.,]", "", regex=True)
            .str.replace(",", ".")
        )
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    if "name" in df.columns:
        df["normalized_name"] = df["name"].apply(normalize_name)

    # Если channel нет — берём source
    if "channel" not in df.columns:
        if "source" in df.columns:
            df["channel"] = df["source"]
        else:
            df["channel"] = "unknown"

    df["channel"] = df["channel"].fillna("unknown").astype(str)

    df = df.reset_index(drop=True)

    df["booking_id"] = df.apply(
        lambda r: _make_surrogate_id(
            r.get("hotel_name", ""),
            r.get("source_file", ""),
            r.name,
        ),
        axis=1,
    )

    allowed_columns = [
        "booking_id",
        "client_id",
        "hotel_name",
        "hotel_type",
        "amount",
        "checkin_date",
        "channel",
        "source",
        "confidence_level",
        "report_year",
        "report_month",
        "source_file",
    ]

    df = df[[c for c in df.columns if c in allowed_columns]]

    return df


# ─────────────────────────────────────────────
# GUESTS
# ─────────────────────────────────────────────

def transform_guests(raw_guests: pd.DataFrame) -> pd.DataFrame:

    df = raw_guests.copy()
    df = build_client_ids(df)

    df = normalize_dates_in_df(df, ["checkin_date", "birth_date"])

    df = df.reset_index(drop=True)

    df["guest_id"] = df.apply(
        lambda r: _make_surrogate_id(
            r.get("hotel_name", ""),
            r.get("source_file", ""),
            r.name,
        ),
        axis=1,
    )

    df["booking_id"] = df.apply(
        lambda r: _make_surrogate_id(
            r.get("hotel_name", ""),
            r.get("source_file", ""),
            r.name,
        ),
        axis=1,
    )

    allowed_columns = [
        "guest_id",
        "booking_id",
        "client_id",
        "hotel_name",
        "normalized_name",
        "citizenship",
        "phone",
        "email",
        "birth_date",
        "checkin_date",
        "confidence_level",
        "report_year",
        "report_month",
        "source_file",
    ]

    df = df[[c for c in df.columns if c in allowed_columns]]

    return df


# ─────────────────────────────────────────────
# CLIENTS
# ─────────────────────────────────────────────

def build_clients_table(guests_df: pd.DataFrame) -> pd.DataFrame:

    if guests_df.empty:
        return pd.DataFrame()

    df = guests_df.copy()

    def first_nonempty(series):
        non_empty = series.dropna().replace("", np.nan).dropna()
        return non_empty.iloc[0] if len(non_empty) > 0 else None

    agg_funcs = {
        "normalized_name": first_nonempty,
        "confidence_level": "first",
    }

    for field in ["phone", "email", "birth_date", "citizenship"]:
        if field in df.columns:
            agg_funcs[field] = first_nonempty

    clients = df.groupby("client_id").agg(agg_funcs).reset_index()

    if "phone" in clients.columns:
        clients = clients.rename(columns={"phone": "best_phone"})
    if "email" in clients.columns:
        clients = clients.rename(columns={"email": "best_email"})

    return clients
