import Levenshtein
import re

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
