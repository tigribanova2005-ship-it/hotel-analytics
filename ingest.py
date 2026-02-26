"""
ingest.py
─────────
Шаг 1: Сканирование папки с xlsx-файлами Travelline.
Шаг 2: Извлечение hotel_name и типа отчёта из имени файла.
Шаг 3: Определение hotel_type по hotel_name.
Шаг 4: Загрузка файла, нормализация колонок и системных полей.
Шаг 5: Фильтрация ранних данных загородного отеля (LR).

Формат имени файла:
  {Prefix}_{ReportType}_{DD.MM.YYYY}-{DD.MM.YYYY}.xlsx

  Prefix:       Gold | Italy | Nevsky | Rubik | Central → hotel_type = "city"
                LR                                       → hotel_type = "country"
  ReportType:   "GuestsListReport" → report_type = "guests"
                "Report"           → report_type = "bookings"

Примеры:
  Gold_Report_01.04.2024-30.04.2024.xlsx           → hotel="Gold",  type="bookings"
  Gold_GuestsListReport_01.04.2024-30.04.2024.xlsx  → hotel="Gold",  type="guests"
  LR_Report_01.06.2024-30.06.2024.xlsx              → hotel="LR",    type="bookings"
  LR_GuestsListReport_01.06.2024-30.06.2024.xlsx    → hotel="LR",    type="guests"
"""

import logging
import os
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from settings import COUNTRY_HOTEL_CUTOFF

logger = logging.getLogger(__name__)

# Префикс, однозначно идентифицирующий загородный отель
COUNTRY_HOTEL_PREFIX = "LR"

# Порядок важен: "GuestsListReport" проверяется первым,
# иначе подстрока "Report" перехватит его раньше
_REPORT_TYPE_PATTERNS: list[tuple[str, str]] = [
    ("GuestsListReport", "guests"),
    ("Report",           "bookings"),
]


# ─── 1. Парсинг имени файла ───────────────────────────────────────────────────

def parse_filename(filename: str) -> dict:
    """
    Извлекает из имени xlsx-файла hotel_name, hotel_type, report_type.

    hotel_name  — всё до первого «_» (например "Gold", "LR")
    hotel_type  — "country" если hotel_name == COUNTRY_HOTEL_PREFIX, иначе "city"
    report_type — "bookings" | "guests" | "unknown"
    """
    stem = Path(filename).stem

    hotel_name  = stem.split("_")[0]
    hotel_type  = "country" if hotel_name == COUNTRY_HOTEL_PREFIX else "city"

    report_type = "unknown"
    for pattern, rtype in _REPORT_TYPE_PATTERNS:
        if pattern in stem:
            report_type = rtype
            break

    return {
        "hotel_name":  hotel_name,
        "hotel_type":  hotel_type,
        "report_type": report_type,
    }


# ─── 2. Сканирование папки ────────────────────────────────────────────────────

def scan_data_directory(base_path: str) -> list[dict]:
    """
    Сканирует base_path в поисках xlsx-файлов Travelline.

    Поддерживает два режима хранения:
      A) Плоская папка: base_path/*.xlsx
      B) Вложенная структура (legacy): base_path/{hotel}/{year}/{month}/*.xlsx

    Год и месяц извлекаются из даты начала периода в имени файла (режим A)
    или из имён родительских директорий (режим B, fallback).
    """
    base = Path(base_path)
    records = []

    if not base.exists():
        logger.error(f"Папка не найдена: {base_path}")
        return records

    all_files = sorted(base.rglob("*.xlsx"))
    if not all_files:
        logger.warning(f"xlsx-файлы не найдены в {base_path}")
        return records

    for fpath in all_files:
        parsed = parse_filename(fpath.name)

        if parsed["report_type"] == "unknown":
            logger.warning(f"Неизвестный тип отчёта, файл пропущен: {fpath.name}")
            continue

        year, month = _extract_period_from_filename(fpath.name)
        if year is None:
            year, month = _extract_period_from_path(fpath)
        if year is None:
            logger.warning(f"Не удалось определить период файла, пропущен: {fpath.name}")
            continue

        records.append({
            "file_path":   str(fpath),
            "hotel_name":  parsed["hotel_name"],
            "hotel_type":  parsed["hotel_type"],
            "report_type": parsed["report_type"],
            "year":        year,
            "month":       month,
        })

    logger.info(f"Найдено файлов: {len(records)}")
    return records


def _extract_period_from_filename(filename: str) -> tuple[Optional[int], Optional[int]]:
    """
    Ищет первую дату DD.MM.YYYY в имени файла.
    Возвращает (year, month) или (None, None).
    """
    match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", filename)
    if match:
        return int(match.group(3)), int(match.group(2))
    return None, None


def _extract_period_from_path(fpath: Path) -> tuple[Optional[int], Optional[int]]:
    """
    Fallback: извлекает год и месяц из структуры .../year/month/file.xlsx
    """
    parts = fpath.parts
    try:
        return int(parts[-3]), int(parts[-2])
    except (IndexError, ValueError):
        return None, None


# ─── 3. Загрузка и нормализация одного файла ─────────────────────────────────

def load_and_normalize(meta: dict) -> Optional[pd.DataFrame]:
    """
    Загружает один xlsx-файл, добавляет системные колонки, нормализует заголовки.

    Системные колонки: hotel_name, hotel_type, report_year, report_month,
                       report_type, source_file.

    Для загородного отеля (hotel_type == "country") применяется фильтр:
    строки с датой заезда < COUNTRY_HOTEL_CUTOFF отбрасываются.

    Возвращает None если файл пустой, нечитаемый или все строки отфильтрованы.
    """
    fpath = meta["file_path"]

    try:
        df = pd.read_excel(fpath, dtype=str, engine="openpyxl")
    except Exception as exc:
        logger.warning(f"Не удалось прочитать {fpath}: {exc}")
        return None

    if df.empty:
        logger.debug(f"Пустой файл: {fpath}")
        return None

    df.columns = [_normalize_column(c) for c in df.columns]

    df["hotel_name"]   = meta["hotel_name"]
    df["hotel_type"]   = meta["hotel_type"]
    df["report_year"]  = meta["year"]
    df["report_month"] = meta["month"]
    df["report_type"]  = meta["report_type"]
    df["source_file"]  = os.path.basename(fpath)

    if meta["hotel_type"] == "country":
        df = _filter_country_hotel_rows(df)
        if df is None or df.empty:
            logger.info(
                f"Файл загородного отеля полностью отфильтрован "
                f"(все записи до {COUNTRY_HOTEL_CUTOFF}): {os.path.basename(fpath)}"
            )
            return None

    logger.debug(f"Загружено: {os.path.basename(fpath)} ({len(df)} строк)")
    return df


def _normalize_column(col: str) -> str:
    """
    Нормализует имя колонки: strip → lower → пробелы/дефисы → «_» → убрать не-word.
    """
    col = col.strip().lower()
    col = re.sub(r"[\s\-/\\]+", "_", col)
    col = re.sub(r"[^\w]", "", col)
    return col


# ─── 4. Фильтрация загородного отеля ─────────────────────────────────────────

def _filter_country_hotel_rows(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Удаляет строки загородного отеля с датой заезда < COUNTRY_HOTEL_CUTOFF.

    Фильтрация построчная — корректно обрабатывает пограничные месяцы.
    Если колонка с датой заезда не найдена — применяется fallback-фильтр
    по периоду файла (report_year / report_month).
    """
    checkin_col = _find_checkin_column(df)

    if checkin_col:
        checkin_dates = df[checkin_col].apply(parse_date)
        cutoff = pd.Timestamp(COUNTRY_HOTEL_CUTOFF)
        mask = checkin_dates >= cutoff
        skipped = (~mask).sum()
        if skipped:
            logger.info(
                f"Загородный отель: отброшено {skipped} строк "
                f"с датой заезда < {COUNTRY_HOTEL_CUTOFF}"
            )
        filtered = df[mask].copy()
        return filtered if not filtered.empty else None

    else:
        import datetime
        try:
            period_start = datetime.date(int(df["report_year"].iloc[0]), int(df["report_month"].iloc[0]), 1)
        except (KeyError, ValueError, IndexError):
            logger.warning(
                "Загородный отель: не удалось определить период, файл отброшен целиком"
            )
            return None

        if period_start < COUNTRY_HOTEL_CUTOFF:
            logger.info(
                f"Загородный отель: файл за {period_start:%Y-%m} "
                f"отброшен (период до {COUNTRY_HOTEL_CUTOFF})"
            )
            return None

        return df


def _find_checkin_column(df: pd.DataFrame) -> Optional[str]:
    """Ищет колонку с датой заезда по известным нормализованным именам."""
    candidates = [
        "дата_заезда", "checkin_date", "checkin", "check_in",
        "arrival_date", "дата_прибытия", "заезд",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


# ─── 5. Нормализация полей ────────────────────────────────────────────────────

def normalize_name(raw: str) -> str:
    """
    Нормализует ФИО: strip, collapse spaces, Title Case,
    удаляет всё кроме букв, пробелов и дефиса.
    """
    if not isinstance(raw, str) or not raw.strip():
        return ""
    name = re.sub(r"\s+", " ", raw.strip())
    name = re.sub(r"[^а-яёА-ЯЁa-zA-Z\s\-]", "", name)
    return " ".join(w.capitalize() for w in name.split())


DATE_FORMATS = [
    "%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y",
    "%d-%m-%Y", "%Y.%m.%d", "%d.%m.%y",
]


def parse_date(raw) -> Optional[pd.Timestamp]:
    """
    Разбирает дату из строки. Перебирает DATE_FORMATS,
    fallback — pandas smart parse (dayfirst=True).
    """
    if pd.isna(raw) or str(raw).strip() == "":
        return None
    raw_str = str(raw).strip()
    for fmt in DATE_FORMATS:
        try:
            return pd.Timestamp(pd.to_datetime(raw_str, format=fmt))
        except Exception:
            continue
    try:
        return pd.Timestamp(pd.to_datetime(raw_str, dayfirst=True))
    except Exception:
        return None


def normalize_dates_in_df(df: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame:
    """Применяет parse_date ко всем указанным колонкам DataFrame."""
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(parse_date)
    return df


def normalize_phone(raw) -> str:
    """
    Нормализует телефон: только цифры, 8→7, 10 цифр → добавить 7.
    Возвращает строку из 11 цифр или "".
    """
    if not isinstance(raw, str) or not raw.strip():
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    return digits if len(digits) >= 10 else ""


def normalize_email(raw) -> str:
    """Приводит email к нижнему регистру и валидирует формат."""
    if not isinstance(raw, str):
        return ""
    email = raw.strip().lower()
    return email if re.match(r"[^@]+@[^@]+\.[^@]+", email) else ""


# ─── 6. Master loader ────────────────────────────────────────────────────────

def load_all_data(base_path: str) -> dict[str, pd.DataFrame]:
    """
    Загружает все xlsx-файлы из base_path, группирует по типу отчёта.

    Возвращает словарь {"bookings": df, "guests": df}.
    Ключ отсутствует если соответствующих файлов не найдено.
    """
    file_metas = scan_data_directory(base_path)
    buckets: dict[str, list[pd.DataFrame]] = {
        "bookings": [],
        "guests":   [],
        "unknown":  [],
    }

    for meta in file_metas:
        df = load_and_normalize(meta)
        if df is not None:
            bucket_key = meta["report_type"] if meta["report_type"] in buckets else "unknown"
            buckets[bucket_key].append(df)

    result: dict[str, pd.DataFrame] = {}
    for rtype, dfs in buckets.items():
        if dfs:
            result[rtype] = pd.concat(dfs, ignore_index=True)
            logger.info(f"Тип '{rtype}': {len(result[rtype])} строк из {len(dfs)} файлов")

    return result
