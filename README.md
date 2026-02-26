# Hotel Analytics System
## Система управления маркетингом через LTV и возвратность

Аналитическая платформа для сети из 6 отелей (5 городских + 1 загородный).  
Данные: 2023–2025. Источник: Travelline.

---

## Архитектура

```
hotel_analytics/
├── config/
│   └── settings.py          # Конфиг отелей, пороги сегментации, даты
│
├── pipeline/
│   ├── ingest.py            # Загрузка CSV, автоопределение типа, нормализация
│   ├── transform.py         # Трансформация в стандартную схему, surrogate IDs
│   └── identify.py          # Многоуровневая идентификация клиентов (client_id)
│
├── warehouse/
│   └── db.py                # SQLite mini-DWH: DDL, загрузка, upsert, запросы
│
├── analytics/
│   └── metrics.py           # LTV, retention, когорты, KPI каналов, миграция
│
├── reports/
│   └── strategic.py         # Стратегические таблицы и рекомендации
│
├── main.py                  # Точка входа (CLI)
├── generate_sample_data.py  # Генератор тестовых данных
└── requirements.txt
```

---

## Структура входных данных

### Формат имени файла

Файлы выгружаются из Travelline и именуются по шаблону:

```
{Prefix}_{ReportType}_{DD.MM.YYYY}-{DD.MM.YYYY}.xlsx
```

| Часть имени  | Описание                                         | Пример                    |
|--------------|--------------------------------------------------|---------------------------|
| `Prefix`     | Короткое имя отеля — **всё до первого `_`**      | `Gold`, `LR`              |
| `ReportType` | Тип отчёта                                       | `Report`, `GuestsListReport` |
| Даты         | Период в формате `DD.MM.YYYY-DD.MM.YYYY`         | `01.04.2024-30.04.2024`   |

**Актуальные префиксы отелей:**

| Prefix    | Отель           | hotel_type  |
|-----------|-----------------|-------------|
| `Gold`    | Отель Gold      | `city`      |
| `Italy`   | Отель Italy     | `city`      |
| `Nevsky`  | Отель Nevsky    | `city`      |
| `Rubik`   | Отель Rubik     | `city`      |
| `Central` | Отель Central   | `city`      |
| `LR`      | Загородный клуб | `country`   |

> **Правило:** любой префикс, кроме `LR`, автоматически получает `hotel_type = "city"`.
> Добавление нового городского отеля не требует изменений в коде.

**Примеры имён файлов:**

```
Gold_Report_01.04.2024-30.04.2024.xlsx            → hotel="Gold",  report_type="bookings"
Gold_GuestsListReport_01.04.2024-30.04.2024.xlsx   → hotel="Gold",  report_type="guests"
Italy_Report_01.03.2023-31.03.2023.xlsx            → hotel="Italy", report_type="bookings"
LR_Report_01.06.2024-30.06.2024.xlsx               → hotel="LR",    report_type="bookings"
LR_GuestsListReport_01.06.2024-30.06.2024.xlsx     → hotel="LR",    report_type="guests"
```

### Определение типа отчёта

Pipeline ищет подстроку в имени файла (порядок важен — специфичный паттерн первым):

| Подстрока в имени файла      | report_type                                         |
|------------------------------|-----------------------------------------------------|
| `GuestsListReport`           | `guests`                                            |
| `Report`                     | `bookings`                                          |
| ничего из вышеперечисленного | `unknown` — файл пропускается с предупреждением     |

### Размещение файлов

Поддерживаются оба варианта:

**A) Плоская папка (рекомендуется):**
```
data/raw/
├── Gold_Report_01.04.2024-30.04.2024.xlsx
├── Gold_GuestsListReport_01.04.2024-30.04.2024.xlsx
├── LR_Report_01.06.2024-30.06.2024.xlsx
└── ...
```

**B) Вложенная структура (legacy):**
```
data/raw/
└── {hotel}/{year}/{month}/*.xlsx
```

### Колонки отчётов (гибкий маппинг)

**Отчёт по бронированиям** (`*_Report_*`):

| Стандартное поле | Варианты в файле Travelline                      |
|------------------|--------------------------------------------------|
| `channel`        | Канал, channel, канал_привлечения                |
| `tariff`         | Тариф, tariff, rate_plan                         |
| `amount`         | Сумма, amount, стоимость, revenue                |
| `checkin_date`   | Дата заезда, check_in, arrival                   |
| `checkout_date`  | Дата выезда, check_out, departure                |
| `booking_type`   | Тип бронирования, booking_type                   |
| `source`         | Источник, source                                 |

**Отчёт по гостям** (`*_GuestsListReport_*`):

| Стандартное поле | Варианты в файле Travelline                      |
|------------------|--------------------------------------------------|
| `name`           | ФИО, Гость, full_name, guest_name                |
| `phone`          | Телефон, phone, мобильный                        |
| `email`          | Email, почта, e_mail                             |
| `citizenship`    | Гражданство, citizenship                         |
| `guests_count`   | Состав гостей, количество_гостей                 |
| `services`       | Доп. услуги, services, extras                    |
| `checkin_date`   | Дата заезда, check_in, arrival_date              |

---

## Запуск

### Установка зависимостей
```bash
pip install -r requirements.txt
```

### Генерация тестовых данных
```bash
python generate_sample_data.py
```

### Полный pipeline
```bash
python main.py \
  --data-path ./data/raw \
  --db ./data/hotel_analytics.db \
  --cac ./data/marketing_costs.csv
```

### Только пересчёт отчётов (без перезагрузки)
```bash
python main.py --db ./data/hotel_analytics.db --report-only
```

---

## Идентификация клиентов

| Уровень     | Ключ                     | Вес  |
|-------------|--------------------------|------|
| `high`      | ФИО + телефон            | 1.00 |
| `medium`    | ФИО + email              | 0.85 |
| `low_dob`   | ФИО + дата рождения      | 0.70 |
| `low`       | ФИО + дата заезда        | 0.50 |
| `unknown`   | только ФИО               | —    |

- `client_id`: детерминированный MD5-хеш от composite key (16 символов)
- `potential_duplicate`: флаг если одно ФИО → несколько client_id

---

## Схема DWH (SQLite → масштабируется в PostgreSQL)

```
dim_hotels          ─── Справочник отелей
clients             ─── Уникальные клиенты (дедуплицированы)
bookings            ─── Все бронирования (FK → clients, dim_hotels)
guests              ─── Гости при заезде (FK → bookings, clients)
marketing_costs     ─── CAC по каналам и периодам
client_metrics      ─── Витрина: LTV, retention, сегменты (пересчитывается)
```

**Аналитические таблицы** (сохраняются после каждого запуска):
```
analytics_channel_kpi_city
analytics_channel_kpi_country
analytics_cohort_retention
analytics_ltv_confidence
analytics_guest_migration
```

---

## Рассчитываемые метрики

### Клиентские
| Метрика                  | Описание                                        |
|--------------------------|-------------------------------------------------|
| `ltv_all_time`           | Суммарная выручка за всё время                  |
| `ltv_12m`                | LTV за последние 12 месяцев                     |
| `ltv_24m`                | LTV за последние 24 месяца                      |
| `city_revenue`           | Выручка только в городских                      |
| `country_revenue`        | Выручка только в загородном (с июня 2024)       |
| `avg_check`              | Средний чек                                     |
| `visit_frequency_days`   | Средний интервал между визитами (дней)          |
| `relationship_days`      | Дней от первого до последнего визита            |
| `migrated_to_country`    | Был в городских И загородном                    |
| `segment_ltv`            | high / mid / low (по квантилям)                 |
| `segment_frequency`      | frequent (3+) / rare                            |
| `segment_type`           | solo / couple / family / corporate              |

### Маркетинговые KPI по каналам
| KPI                    | Формула                                    |
|------------------------|--------------------------------------------|
| `avg_ltv`              | Среднее LTV клиентов канала                |
| `retention_share`      | Доля возвратных гостей                     |
| `roi`                  | (Total LTV − CAC) / CAC                    |
| `cac_ltv_ratio`        | CAC / Total LTV                            |

---

## Стратегические вопросы → ответы

| Вопрос                                          | Источник данных                         |
|--------------------------------------------------|-----------------------------------------|
| Какие каналы → самые прибыльные гости?           | `channel_kpi` → `avg_ltv`, `total_ltv` |
| Какие каналы → только разовые визиты?            | `retention_share` < 5%                  |
| Есть переток city → country?                     | `client_metrics.migrated_to_country`   |
| Где самый высокий retention?                     | `cohort_retention` pivot                |
| Какие сегменты дают max LTV?                     | `segment_ltv` × `segment_type`         |

---

## Переход на Travelline ID

При получении нативных ID из Travelline:

1. Добавить колонку `travelline_guest_id` в таблицы `bookings`, `guests`
2. Изменить логику `assign_client_id`: если `travelline_id` есть → использовать его
3. Провести дедупликацию: JOIN текущих hash-client_id с Travelline ID
4. Обновить `confidence_level = 'travelline'` для таких записей

---

## Правила загородного отеля (LR)

- Идентифицируется **исключительно по префиксу `LR`** в имени файла
- Тестовые бронирования: апрель 2024 — **исключены автоматически**
- Официальное открытие: 15 июня 2024
- **Фильтр в pipeline:** строки с датой заезда < `2024-06-01` отбрасываются построчно в `_filter_country_hotel_rows()`
- Если колонка с датой заезда не найдена — файл фильтруется целиком по периоду
- В когортном анализе загородный участвует только начиная с когорты `2024`
- LTV городских и загородного рассчитывается **раздельно**
