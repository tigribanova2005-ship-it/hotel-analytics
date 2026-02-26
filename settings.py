from datetime import date

HOTELS = {
    "Gold":    {"type": "city",    "launch_date": date(2023, 1, 1)},
    "Italy":   {"type": "city",    "launch_date": date(2023, 1, 1)},
    "Nevsky":  {"type": "city",    "launch_date": date(2023, 1, 1)},
    "Rubik":   {"type": "city",    "launch_date": date(2023, 1, 1)},
    "Central": {"type": "city",    "launch_date": date(2023, 1, 1)},
    "LR":      {"type": "country", "launch_date": date(2024, 6, 15)},
}

COUNTRY_HOTEL_CUTOFF = date(2024, 6, 1)

CONFIDENCE_LEVELS = {
    "high":    {"weight": 1.0,  "keys": ["normalized_name", "phone"]},
    "medium":  {"weight": 0.85, "keys": ["normalized_name", "email"]},
    "low_dob": {"weight": 0.7,  "keys": ["normalized_name", "birth_date"]},
    "low":     {"weight": 0.5,  "keys": ["normalized_name", "checkin_date"]},
}

SEGMENT_LTV_THRESHOLDS = {
    "high": 0.75,
    "mid":  0.40,
    "low":  0.0,
}

LTV_WINDOWS = {
    "all_time": None,
    "12m":      365,
    "24m":      730,
}

ANALYSIS_YEARS = [2023, 2024, 2025]
