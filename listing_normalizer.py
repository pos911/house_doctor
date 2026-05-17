import math
import re
from datetime import datetime


EOK_UNIT = 10000
PYEONG_M2 = 3.3058
ORIENTATION_PATTERNS = [
    "남향",
    "남동향",
    "남서향",
    "동향",
    "서향",
    "북향",
    "북동향",
    "북서향",
]


def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def area_to_pyeong(area_m2):
    if not area_m2:
        return 0.0
    return round(float(area_m2) / PYEONG_M2, 1)


def classify_age_bucket(approval_date: str, now=None):
    now = now or datetime.now()
    text = str(approval_date or "").strip()
    if len(text) < 4 or not text[:4].isdigit():
        return "unknown"
    approved_year = int(text[:4])
    years = now.year - approved_year
    return "10년 이내" if years <= 10 else "10년 초과"


def normalize_price_range(price_value):
    price = safe_int(price_value, default=0)
    return {
        "price_min_manwon": price,
        "price_max_manwon": price,
        "price_display": f"{price:,}" if price else "",
        "price_band": f"{price // EOK_UNIT}억대" if price else "",
    }


def normalize_floor_range(floor, total_floor=None, floor_raw=""):
    floor_text = str(floor or "").strip()
    total_floor_val = safe_int(total_floor, default=0)

    if floor_raw and "/" in str(floor_raw):
        left, _, right = str(floor_raw).partition("/")
        floor_text = floor_text or left.strip()
        total_floor_val = total_floor_val or safe_int(right.strip(), default=0)

    floor_min = None
    floor_max = None
    floor_category = ""

    if floor_text in {"저", "저층"}:
        floor_category = "저층"
        if total_floor_val:
            floor_min = 1
            floor_max = max(1, math.floor(total_floor_val * 0.3))
    elif floor_text in {"중", "중층"}:
        floor_category = "중층"
        if total_floor_val:
            floor_min = max(1, math.floor(total_floor_val * 0.3) + 1)
            floor_max = max(floor_min, math.floor(total_floor_val * 0.7))
    elif floor_text in {"고", "고층"}:
        floor_category = "고층"
        if total_floor_val:
            floor_min = max(1, math.floor(total_floor_val * 0.7) + 1)
            floor_max = total_floor_val
    else:
        numeric_floor = safe_int(floor_text, default=0)
        if numeric_floor:
            floor_min = numeric_floor
            floor_max = numeric_floor
        if total_floor_val and numeric_floor:
            ratio = numeric_floor / total_floor_val
            if ratio <= 0.3:
                floor_category = "저층"
            elif ratio <= 0.7:
                floor_category = "중층"
            else:
                floor_category = "고층"

    return {
        "floor_text": floor_text,
        "total_floor": total_floor_val,
        "floor_min": floor_min,
        "floor_max": floor_max,
        "floor_category": floor_category,
    }


def extract_orientation(*texts):
    combined = " ".join([str(text or "") for text in texts])
    for pattern in ORIENTATION_PATTERNS:
        if pattern in combined:
            return pattern
    return ""


def extract_type(*texts):
    combined = " ".join([str(text or "") for text in texts]).strip()
    if not combined:
        return ""

    patterns = [
        r"(\d+\s*[A-Z]타입)",
        r"(\d+\s*타입)",
        r"([A-Z]\s*타입)",
        r"(\d+\s*[A-Z])",
    ]
    for pattern in patterns:
        match = re.search(pattern, combined, re.IGNORECASE)
        if match:
            return match.group(1).replace(" ", "")
    return ""


def normalize_confirm_date(value):
    text = str(value or "").strip()
    digits = re.sub(r"[^\d]", "", text)
    if len(digits) >= 8:
        return digits[:8]
    return ""


def is_target_area(area_m2):
    area = float(area_m2 or 0)
    pyeong = area_to_pyeong(area)
    return (55 <= area <= 85) or (20 <= pyeong < 30)


def is_target_price(price_min_manwon, price_max_manwon, min_price=140000, max_price=185000):
    if not price_min_manwon and not price_max_manwon:
        return False
    low = price_min_manwon or price_max_manwon
    high = price_max_manwon or price_min_manwon
    return not (high < min_price or low > max_price)
