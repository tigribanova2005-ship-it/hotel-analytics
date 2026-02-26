import pandas as pd
import numpy as np
import os
from pathlib import Path
import hashlib

# ----------------------------
# 1. Чтение всех файлов
# ----------------------------

DATA_PATH = Path("data/raw/2025")

all_files = list(DATA_PATH.glob("*/*.xlsx"))

dfs = []
for file in all_files:
    df = pd.read_excel(file)
    dfs.append(df)

df = pd.concat(dfs, ignore_index=True)

print(f"Загружено строк: {len(df)}")

# ----------------------------
# 2. Нормализация колонок
# ----------------------------

df.columns = [c.strip() for c in df.columns]

# Переименуем для удобства
rename_map = {
    "№ брони": "booking_id",
    "Гость": "guest_name",
    "Стоимость": "amount",
    "Дата бронирования": "booking_date",
    "Заезд": "checkin_date",
    "Выезд": "checkout_date",
    "Источник": "channel",
    "Источник перехода": "traffic_source",
    "Статус брони": "status",
    "Объект размещения": "hotel_name",
    "Номер телефона": "phone",
    "Email": "email",
    "Дата рождения": "birth_date",
    "Тип устройства": "device_type"
}

df = df.rename(columns=rename_map)

# ----------------------------
# 3. Определяем тип отеля
# ----------------------------

df["hotel_type"] = np.where(
    df["hotel_name"].str.contains("Лесная Ривьера", na=False),
    "country",
    "city"
)

# ----------------------------
# 4. Очистка данных
# ----------------------------

def clean_phone(x):
    if pd.isna(x):
        return None
    x = str(x)
    return "".join(filter(str.isdigit, x))

def clean_email(x):
    if pd.isna(x):
        return None
    return str(x).strip().lower()

df["phone"] = df["phone"].apply(clean_phone)
df["email"] = df["email"].apply(clean_email)

df["checkin_date"] = pd.to_datetime(df["checkin_date"], errors="coerce")
df["checkout_date"] = pd.to_datetime(df["checkout_date"], errors="coerce")
df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

# ----------------------------
# 5. Создание client_id
# ----------------------------

def make_hash(*args):
    raw = "|".join([str(a) for a in args if pd.notna(a)])
    return hashlib.md5(raw.encode()).hexdigest()[:16]

def generate_client_id(row):
    if row["phone"]:
        return make_hash(row["phone"])
    if row["email"]:
        return make_hash(row["email"])
    if pd.notna(row["birth_date"]):
        return make_hash(row["guest_name"], row["birth_date"])
    return make_hash(row["guest_name"], row["checkin_date"])

df["client_id"] = df.apply(generate_client_id, axis=1)

# ----------------------------
# 6. Разделение статусов
# ----------------------------

df["is_confirmed"] = df["status"].str.contains("актив", case=False, na=False)
df["is_cancelled"] = df["status"].str.contains("отмен", case=False, na=False)

confirmed = df[df["is_confirmed"]].copy()

print(f"Подтвержденных броней: {len(confirmed)}")
print(f"Отмен: {df['is_cancelled'].sum()}")

# ----------------------------
# 7. Базовые метрики
# ----------------------------

clients_total = confirmed["client_id"].nunique()

visits_per_client = confirmed.groupby("client_id")["booking_id"].count()
repeat_clients = (visits_per_client > 1).sum()

ltv = confirmed.groupby("client_id")["amount"].sum()

print("\n===== РЕЗУЛЬТАТ =====")
print(f"Уникальных клиентов: {clients_total}")
print(f"Повторных клиентов: {repeat_clients}")
print(f"Средний LTV: {ltv.mean():.2f}")

# Миграция city -> country
client_hotels = confirmed.groupby("client_id")["hotel_type"].nunique()
multi_hotel_clients = (client_hotels > 1).sum()

print(f"Клиентов, посетивших и city и country: {multi_hotel_clients}")