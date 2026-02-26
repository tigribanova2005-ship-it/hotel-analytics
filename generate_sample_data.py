"""
generate_sample_data.py
───────────────────────
Генерирует тестовые CSV-файлы для проверки pipeline.
Запуск: python generate_sample_data.py

Создаёт структуру data/raw/{hotel}/{year}/{month}/
"""

import os
import random
import hashlib
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import numpy as np

HOTELS = {
    "Отель Центральный":  ("city",    date(2023, 1, 1)),
    "Отель Северный":     ("city",    date(2023, 1, 1)),
    "Отель Южный":        ("city",    date(2023, 1, 1)),
    "Отель Восточный":    ("city",    date(2023, 1, 1)),
    "Отель Западный":     ("city",    date(2023, 1, 1)),
    "Загородный клуб":    ("country", date(2024, 6, 1)),
}

CHANNELS = ["booking", "direct_site", "direct_phone", "ostrovok", "yandex_travel",
            "corporate", "airbnb", "ota_other"]
TARIFFS  = ["Стандарт", "Комфорт", "Бизнес", "Люкс", "Завтрак включён", "Без питания"]
CITIZENSHIPS = ["РФ", "РФ", "РФ", "РФ", "Беларусь", "Казахстан", "Германия", "Китай"]
FIRST_NAMES = ["Александр", "Мария", "Дмитрий", "Елена", "Сергей", "Анна",
               "Иван", "Ольга", "Алексей", "Наталья", "Виктор", "Светлана"]
LAST_NAMES  = ["Иванов", "Петров", "Сидоров", "Козлов", "Новиков", "Морозов",
               "Волков", "Соколов", "Лебедев", "Попов", "Кузнецов", "Смирнов"]
GUEST_TYPES = ["1 взрослый", "2 взрослых", "2 взрослых + 1 ребёнок", "Корпоративный"]
SERVICES    = ["", "Завтрак", "Парковка", "Трансфер", "СПА", "Ранний заезд"]

random.seed(42)
np.random.seed(42)

BASE_PATH = Path("data/raw")


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def make_phone() -> str:
    return f"79{random.randint(100000000, 999999999)}"


def make_email(name: str) -> str:
    domains = ["gmail.com", "mail.ru", "yandex.ru", ""]
    d = random.choice(domains)
    if not d:
        return ""
    slug = name.lower().replace(" ", ".").replace("ё", "е")
    return f"{slug}{random.randint(1, 99)}@{d}"


def generate_guest_pool(n=200):
    guests = []
    for i in range(n):
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        name = f"{ln} {fn}"
        guests.append({
            "name": name,
            "phone": make_phone() if random.random() > 0.3 else "",
            "email": make_email(fn + " " + ln) if random.random() > 0.4 else "",
            "citizenship": random.choice(CITIZENSHIPS),
        })
    return guests


def generate_month(hotel: str, hotel_type: str, year: int, month: int, guests: list):
    out_dir = BASE_PATH / hotel / str(year) / f"{month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)

    n = random.randint(15, 60)
    selected = [random.choice(guests) for _ in range(n)]

    # ─── Отчёт бронирований ───────────────────────────────────────────────
    booking_rows = []
    guest_rows   = []

    for g in selected:
        checkin = random_date(date(year, month, 1), date(year, month, 28))
        nights  = random.randint(1, 7)
        checkout = checkin + timedelta(days=nights)
        channel = random.choice(CHANNELS)
        tariff  = random.choice(TARIFFS)
        base_price = {"Стандарт": 4500, "Комфорт": 6500, "Бизнес": 9000,
                      "Люкс": 14000, "Завтрак включён": 5500, "Без питания": 4000}
        amount = base_price.get(tariff, 5000) * nights * random.uniform(0.8, 1.3)

        booking_rows.append({
            "Гость":               g["name"],
            "Канал":               channel,
            "Тариф":               tariff,
            "Сумма":               round(amount, 2),
            "Дата заезда":         checkin.strftime("%d.%m.%Y"),
            "Дата выезда":         checkout.strftime("%d.%m.%Y"),
            "Источник":            channel,
            "Тип бронирования":    random.choice(["OTA", "Прямое", "Корпоративное"]),
        })

        guest_rows.append({
            "ФИО":                 g["name"],
            "Гражданство":         g["citizenship"],
            "Состав гостей":       random.choice(GUEST_TYPES),
            "Доп. услуги":         random.choice(SERVICES),
            "Телефон":             g["phone"],
            "Email":               g["email"],
            "Дата заезда":         checkin.strftime("%d.%m.%Y"),
        })

    pd.DataFrame(booking_rows).to_csv(
        out_dir / f"bookings_{year}_{month:02d}.csv", index=False, encoding="utf-8-sig"
    )
    pd.DataFrame(guest_rows).to_csv(
        out_dir / f"guests_{year}_{month:02d}.csv", index=False, encoding="utf-8-sig"
    )


def main():
    guests = generate_guest_pool(200)
    total_files = 0

    for hotel, (htype, launch) in HOTELS.items():
        for year in [2023, 2024, 2025]:
            for month in range(1, 13):
                period_start = date(year, month, 1)
                if period_start < launch:
                    continue
                # Не генерируем будущее
                if period_start > date(2025, 12, 1):
                    continue
                generate_month(hotel, htype, year, month, guests)
                total_files += 2

    # Маркетинговые расходы
    cac_rows = []
    for channel in CHANNELS:
        for year in [2023, 2024, 2025]:
            for month in range(1, 13):
                for hotel_type in ["city", "country"]:
                    if hotel_type == "country" and year == 2023:
                        continue
                    cac_rows.append({
                        "hotel_name":   "",
                        "hotel_type":   hotel_type,
                        "channel":      channel,
                        "period_year":  year,
                        "period_month": month,
                        "cac_amount":   round(random.uniform(10000, 150000), 2),
                    })

    pd.DataFrame(cac_rows).to_csv("data/marketing_costs.csv", index=False, encoding="utf-8-sig")
    print(f"✅ Сгенерировано: {total_files} файлов данных + marketing_costs.csv")


if __name__ == "__main__":
    main()
