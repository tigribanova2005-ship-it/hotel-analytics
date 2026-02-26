import pandas as pd
import numpy as np
from pathlib import Path
import hashlib

# =====================================================
# 1. Загрузка
# =====================================================

DATA_PATH = Path("data/raw")
all_files = list(DATA_PATH.glob("*/*/*.xlsx"))

dfs = []
for file in all_files:
    df = pd.read_excel(file)
    dfs.append(df)

df = pd.concat(dfs, ignore_index=True)

df.columns = [c.strip() for c in df.columns]

rename_map = {
    "№ брони": "booking_id",
    "Гость": "guest_name",
    "Стоимость": "amount",
    "Дата бронирования": "booking_date",
    "Заезд": "checkin_date",
    "Выезд": "checkout_date",
    "Статус брони": "status",
    "Объект размещения": "hotel_name",
    "Номер телефона": "phone",
    "Email": "email",
    "Дата рождения": "birth_date",
    "Наименование юр. лица (агент)": "agent_company",
    "Наименование юр. лица (гость)": "guest_company"
}

df = df.rename(columns=rename_map)

df["hotel_type"] = np.where(
    df["hotel_name"].str.contains("Лесная Ривьера", na=False),
    "country",
    "city"
)

df["checkin_date"] = pd.to_datetime(df["checkin_date"], errors="coerce")
df["checkout_date"] = pd.to_datetime(df["checkout_date"], errors="coerce")
df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

# =====================================================
# 2. Физлица
# =====================================================

df["is_corporate"] = (
    df["agent_company"].notna() |
    df["guest_company"].notna()
)

df_phys = df[~df["is_corporate"]].copy()

df_phys["is_confirmed"] = df_phys["status"].str.contains("актив", case=False, na=False)
confirmed = df_phys[df_phys["is_confirmed"]].copy()

confirmed["nights"] = (
    confirmed["checkout_date"].dt.normalize() -
    confirmed["checkin_date"].dt.normalize()
).dt.days

confirmed["has_contact"] = (
    confirmed["phone"].notna() |
    confirmed["email"].notna()
)

# =====================================================
# 3. Клиенты
# =====================================================

def make_hash(*args):
    raw = "|".join([str(a) for a in args if pd.notna(a)])
    return hashlib.md5(raw.encode()).hexdigest()[:16]

confirmed_contact = confirmed[confirmed["has_contact"]].copy()

def generate_client_id(row):
    if pd.notna(row["phone"]):
        return make_hash("phone", row["phone"])
    if pd.notna(row["email"]):
        return make_hash("email", row["email"])
    return make_hash("anon", row["booking_id"])

confirmed_contact["client_id"] = confirmed_contact.apply(generate_client_id, axis=1)

client_ltv = confirmed_contact.groupby("client_id").agg(
    total_revenue=("amount", "sum"),
    visits=("booking_id", "count")
)

client_ltv["avg_check"] = client_ltv["total_revenue"] / client_ltv["visits"]

client_ltv["ltv_segment"] = pd.qcut(
    client_ltv["total_revenue"],
    q=4,
    labels=["Low", "Medium", "High", "VIP"],
    duplicates="drop"
)

# =====================================================
# 4. ВЫВОД ДЛЯ ДЕМОНСТРАЦИИ
# =====================================================

print("\n" + "="*70)
print("АНАЛИТИКА СЕТИ ОТЕЛЕЙ | 2025")
print("="*70)

print("\nОБЩАЯ КАРТИНА")
print("-"*40)
print(f"Всего подтверждённых броней: {len(confirmed)}")
print(f"Общая выручка: {confirmed['amount'].sum():,.0f} ₽")
print(f"Уникальных клиентов (с контактами): {client_ltv.shape[0]}")
print(f"Средний чек: {confirmed['amount'].mean():,.0f} ₽")
print(f"Средний LTV: {client_ltv['total_revenue'].mean():,.0f} ₽")
print(f"Повторность: {(client_ltv['visits'] > 1).mean()*100:.2f}%")

print("\nБРОНИ БЕЗ КОНТАКТА")
print("-"*40)
print(f"Количество: {len(confirmed[~confirmed['has_contact']])}")
print(f"Доля: {len(confirmed[~confirmed['has_contact']])/len(confirmed)*100:.2f}%")
print(f"Выручка: {confirmed[~confirmed['has_contact']]['amount'].sum():,.0f} ₽")

print("\nПО ОТЕЛЯМ")
print("-"*40)

hotel_summary = confirmed.groupby("hotel_name").agg(
    bookings=("booking_id", "count"),
    revenue=("amount", "sum"),
    avg_check=("amount", "mean"),
    avg_nights=("nights", "mean")
)

print(hotel_summary.sort_values("revenue", ascending=False))

print("\nСЕГМЕНТАЦИЯ LTV")
print("-"*40)

segment_summary = client_ltv.groupby("ltv_segment").agg(
    clients=("total_revenue", "count"),
    avg_ltv=("total_revenue", "mean"),
    total_revenue=("total_revenue", "sum")
)

print(segment_summary)

print("\n" + "="*70)
print("КЛЮЧЕВОЙ ВЫВОД:")
print("Повторность клиентов низкая — основной потенциал роста в возврате гостей.")
print("="*70)