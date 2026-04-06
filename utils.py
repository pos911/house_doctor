import Levenshtein
import re
import requests
from config import NAVER_CLIENT_ID, NAVER_CLIENT_SECRET

def clean_text(text: str) -> str:
    if not text:
        return ""
    # Normalize spaces
    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    return text

def is_match_name(name1: str, name2: str, threshold: float = 0.8) -> bool:
    name1 = clean_text(name1).replace(" ", "").lower()
    name2 = clean_text(name2).replace(" ", "").lower()
    if not name1 or not name2:
        return False
    
    # Calculate Levenshtein ratio
    ratio = Levenshtein.ratio(name1, name2)
    return ratio >= threshold
    
def parse_price(price_str: str) -> int:
    """Removes commas and returns integer price (in 만원)."""
    if not price_str:
        return 0
    clean_p = re.sub(r'[^\d]', '', price_str)
    return int(clean_p) if clean_p else 0

def search_official_names_naver(keyword: str) -> list:
    if not NAVER_CLIENT_ID or NAVER_CLIENT_ID == "YOUR_NAVER_CLIENT_ID":
        print("(!) 네이버 API 키가 설정되지 않았습니다.")
        return []
        
    url = "https://openapi.naver.com/v1/search/local.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET
    }
    params = {
        "query": keyword,
        "display": 5
    }
    
    try:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", [])
            official_names = []
            for item in items:
                # Remove HTML tags like <b> from the title
                title = clean_text(re.sub(r'<[^>]+>', '', item.get("title", "")))
                official_names.append(title)
            return official_names
        else:
            print(f"(!) 네이버 API 에러: {resp.status_code}")
    except Exception as e:
        print(f"Error fetching from Naver API: {e}")
    return []
