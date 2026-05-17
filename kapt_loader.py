import re
from typing import Dict, List, Tuple

import pandas as pd


REQUIRED_COLUMNS = [
    "시도",
    "시군구",
    "동리",
    "단지코드",
    "단지명",
    "법정동주소",
    "도로명주소",
    "사용승인일",
    "동수",
    "세대수",
]

COLUMN_ALIASES = {
    "시도": ["시도", "sido"],
    "시군구": ["시군구", "sigungu"],
    "동리": ["동리", "dongri"],
    "단지코드": ["단지코드", "kapt_code"],
    "단지명": ["단지명", "apt_name"],
    "법정동주소": ["법정동주소", "legal_address"],
    "도로명주소": ["도로명주소", "road_address"],
    "사용승인일": ["사용승인일", "approval_date"],
    "동수": ["동수", "building_count"],
    "세대수": ["세대수", "household_count"],
}


def clean_value(value):
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def normalize_name_for_compare(name: str) -> str:
    text = clean_value(name)
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"\((.*?)\)", r"\1", text)
    text = re.sub(r"(?i)apt", "", text)
    text = text.replace("아파트", "")
    text = text.replace("단지", "")
    text = text.replace("-", "")
    return text.lower()


def build_name_aliases(name: str) -> List[str]:
    raw_name = clean_value(name)
    aliases = []
    if raw_name:
        aliases.append(raw_name)

    parenthetical = re.findall(r"\((.*?)\)", raw_name)
    base_name = re.sub(r"\s*\(.*?\)\s*", "", raw_name).strip()
    if base_name and base_name not in aliases:
        aliases.append(base_name)

    for alias in parenthetical:
        alias = alias.strip()
        if alias and alias not in aliases:
            aliases.append(alias)

    if parenthetical:
        combined = base_name + "".join(parenthetical)
        combined = combined.strip()
        if combined and combined not in aliases:
            aliases.append(combined)

    return aliases


def normalize_kapt_record(row: Dict[str, object]) -> Dict[str, object]:
    apt_name = clean_value(row.get("단지명"))
    aliases = build_name_aliases(apt_name)
    normalized_aliases = [normalize_name_for_compare(alias) for alias in aliases if alias]
    legal_dong_code8 = clean_value(
        row.get("legal_dong_code8")
        or row.get("법정동코드8")
        or row.get("법정동코드")
        or row.get("code8")
    )

    return {
        "kapt_code": clean_value(row.get("단지코드")),
        "apt_name": apt_name,
        "normalized_apt_name": normalize_name_for_compare(apt_name),
        "name_aliases": aliases,
        "normalized_aliases": list(dict.fromkeys([name for name in normalized_aliases if name])),
        "sido": clean_value(row.get("시도")),
        "sigungu": clean_value(row.get("시군구")),
        "dongri": clean_value(row.get("동리")),
        "legal_address": clean_value(row.get("법정동주소")),
        "road_address": clean_value(row.get("도로명주소")),
        "approval_date": clean_value(row.get("사용승인일")),
        "building_count": _parse_int(row.get("동수")),
        "household_count": _parse_int(row.get("세대수")),
        "legal_dong_code8": legal_dong_code8 or None,
    }


def _parse_int(value) -> int:
    text = clean_value(value)
    if not text:
        return 0
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0


def detect_header_row(excel_path: str) -> Tuple[int, List[str]]:
    probe = pd.read_excel(excel_path, header=None, nrows=10, engine="openpyxl")
    for idx, row in probe.iterrows():
        row_values = [clean_value(v) for v in row.tolist()]
        if "단지코드" in row_values and "단지명" in row_values:
            return idx, row_values
    raise ValueError("K-apt 엑셀 헤더 행을 찾을 수 없습니다.")


def _rename_alias_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.copy()
    for canonical, aliases in COLUMN_ALIASES.items():
        if canonical in renamed.columns:
            continue
        for alias in aliases:
            if alias in renamed.columns:
                renamed = renamed.rename(columns={alias: canonical})
                break
    return renamed


def load_kapt_complexes(excel_path: str) -> List[Dict[str, object]]:
    header_idx, _ = detect_header_row(excel_path)
    df = pd.read_excel(excel_path, header=header_idx, engine="openpyxl")
    df = _rename_alias_columns(df)
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"필수 컬럼이 없습니다: {missing}")

    records = []
    for row in df.to_dict(orient="records"):
        normalized = normalize_kapt_record(row)
        if normalized["kapt_code"] and normalized["apt_name"]:
            records.append(normalized)

    return records
