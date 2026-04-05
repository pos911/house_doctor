import requests

# Common headers matching the prompt
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.neonet.co.kr/",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "X-Requested-With": "XMLHttpRequest",
}

# Hardcoded Regions for Phase 1
REGIONS = [
    {"name": "용강동", "code8": "11440105"},
    {"name": "토정동", "code8": "11440106"}
]

def get_session():
    """Returns a requests Session with predefined headers for cookie maintenance."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session
