import requests
import os
import json

# Common headers matching the prompt
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.neonet.co.kr/",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "X-Requested-With": "XMLHttpRequest",
}

# Mapping of Dong names to their 8-digit codes
DONG_MAPPING = {
    "용강동": "11440105",
    "토정동": "11440106",
    "현석동": "11440111",
    "신수동": "11440114",
    "구수동": "11440112",
    "대흥동": "11440108",
    "마포동": "11440104",
    "상수동": "11440115",
}

NORMALIZED_DONG_MAPPING = {key.replace(" ", ""): value for key, value in DONG_MAPPING.items()}

def get_session():
    """Returns a requests Session with predefined headers for cookie maintenance."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session

def resolve_legal_dong_code8(dongri="", legal_address="", road_address=""):
    """
    Resolve code8 using the limited in-repo dong mapping.
    Returns (code8, warnings, matched_name).
    """
    warnings = []
    candidates = []

    if dongri:
        candidates.append(str(dongri).strip())

    for address in [legal_address, road_address]:
        if address:
            candidates.extend(str(address).replace(",", " ").split())

    normalized_candidates = []
    for item in candidates:
        cleaned = item.replace(" ", "").strip()
        if cleaned:
            normalized_candidates.append(cleaned)

    seen = set()
    deduped = []
    for candidate in normalized_candidates:
        if candidate not in seen:
            seen.add(candidate)
            deduped.append(candidate)

    for candidate in deduped:
        if candidate in NORMALIZED_DONG_MAPPING:
            return NORMALIZED_DONG_MAPPING[candidate], warnings, candidate

    for candidate in deduped:
        for mapped_name, code8 in NORMALIZED_DONG_MAPPING.items():
            if mapped_name in candidate:
                return code8, warnings, mapped_name

    warnings.append("legal_dong_code8_missing")
    return None, warnings, None

# Naver API Credentials - Environment Variables (GitHub) first, then secrets.json (Local)
NAVER_CLIENT_ID = os.environ.get("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET")

if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
    try:
        secrets_path = os.path.join(os.path.dirname(__file__), "secrets.json")
        if os.path.exists(secrets_path):
            with open(secrets_path, "r", encoding="utf-8") as f:
                secrets = json.load(f)
                NAVER_CLIENT_ID = NAVER_CLIENT_ID or secrets.get("NAVER_CLIENT_ID")
                NAVER_CLIENT_SECRET = NAVER_CLIENT_SECRET or secrets.get("NAVER_CLIENT_SECRET")
    except Exception as e:
        print(f"(!) secrets.json 로딩 중 오류 발생: {e}")
