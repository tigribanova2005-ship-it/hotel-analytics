"""
identify.py
Устойчивая идентификация клиентов и линковка бронирований.
"""

import hashlib
import logging
import pandas as pd

from ingest import normalize_name, normalize_phone, normalize_email, parse_date

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# ALIASES
# ─────────────────────────────────────────────

FIELD_ALIASES = {
    "name": ["фио", "имя", "гость", "name", "full_name"],
    "phone": ["телефон", "phone"],
    "email": ["email", "почта"],
    "birth_date": ["дата_рождения", "birth_date"],
    "checkin_date": ["дата_заезда", "checkin", "arrival"],
}


# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────

def remap_columns(df: pd.DataFrame, aliases: dict) -> pd.DataFrame:
    rename_map = {}
    for standard, possible in aliases.items():
        for p in possible:
            if p in df.columns:
                rename_map[p] = standard
                break
    return df.rename(columns=rename_map)


def _make_key(*parts) -> str:
    composite = "|".join(str(p) for p in parts if p)
    return hashlib.md5(composite.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────
# CLIENT IDENTIFICATION
# ─────────────────────────────────────────────

def assign_client_id(row: pd.Series):

    name = normalize_name(row.get("name", ""))

    phone = normalize_phone(row.get("phone", ""))
    email = normalize_email(row.get("email", ""))

    birth_raw = row.get("birth_date", "")
    birth = str(parse_date(birth_raw).date()) if birth_raw and parse_date(birth_raw) else ""

    checkin_raw = row.get("checkin_date", "")
    checkin = str(parse_date(checkin_raw).date()) if checkin_raw and parse_date(checkin_raw) else ""

    if name and phone:
        return _make_key(name, phone), "high"
    if name and email:
        return _make_key(name, email), "medium"
    if name and birth:
        return _make_key(name, birth), "low_dob"
    if name and checkin:
        return _make_key(name, checkin), "low"

    return _make_key(name), "low"


def build_client_ids(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    df = remap_columns(df, FIELD_ALIASES)

    if "name" in df.columns:
        df["normalized_name"] = df["name"].apply(normalize_name)
    else:
        df["normalized_name"] = ""

    results = df.apply(assign_client_id, axis=1, result_type="expand")
    df["client_id"] = results[0]
    df["confidence_level"] = results[1]

    return df


# ─────────────────────────────────────────────
# BOOKINGS ENRICHMENT
# ─────────────────────────────────────────────

def enrich_bookings_with_client_id(
    bookings: pd.DataFrame,
    guests: pd.DataFrame
) -> pd.DataFrame:

    if guests.empty:
        bookings["client_id"] = None
        bookings["confidence_level"] = None
        return bookings

    bookings = remap_columns(bookings, FIELD_ALIASES)

    # normalized_name
    if "name" in bookings.columns:
        bookings["normalized_name"] = bookings["name"].apply(normalize_name)
    else:
        bookings["normalized_name"] = ""

    # normalize checkin_date если есть
    if "checkin_date" in bookings.columns:
        bookings["checkin_date"] = bookings["checkin_date"].apply(parse_date)

    # безопасное формирование guests_small
    required_cols = ["normalized_name", "client_id", "confidence_level"]

    if "checkin_date" in guests.columns:
        required_cols.append("checkin_date")

    guests_small = guests[required_cols].copy()

    if "checkin_date" not in guests_small.columns:
        guests_small["checkin_date"] = None

    # merge keys
    merge_keys = ["normalized_name"]

    if "checkin_date" in bookings.columns:
        merge_keys.append("checkin_date")

    bookings = bookings.merge(
        guests_small,
        on=merge_keys,
        how="left"
    )

    # fallback если не нашли client_id
    mask = bookings["client_id"].isna()
    if mask.any():
        fallback = bookings.loc[mask].apply(
            assign_client_id,
            axis=1,
            result_type="expand"
        )
        bookings.loc[mask, "client_id"] = fallback[0].values
        bookings.loc[mask, "confidence_level"] = fallback[1].values

    # удаляем техническую колонку
    if "normalized_name" in bookings.columns:
        bookings = bookings.drop(columns=["normalized_name"])

    return bookings
