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

def get_session():
    """Returns a requests Session with predefined headers for cookie maintenance."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session

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

