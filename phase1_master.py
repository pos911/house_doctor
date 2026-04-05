import json
import re
from bs4 import BeautifulSoup
from config import REGIONS, get_session
from utils import is_match_name

def fetch_rter_danji(session, code8):
    url = f"https://rter2.com/search/danjiList?legalDongCode={code8}"
    # Send empty payload just in case it requires a POST body format
    try:
        resp = session.post(url, json={})
        if resp.status_code == 200:
            data = resp.json()
            return data.get("result", {}).get("list", [])
    except Exception as e:
        print(f"Error fetching Rter for {code8}: {e}")
    return []

def fetch_bank_danji(session, code8):
    url = f"https://www.neonet.co.kr/novo-rebank/view/offerings/inc_OfferingsList.neo?offerings_gbn=AT&offer_gbn=P&region_cd={code8}00"
    try:
        resp = session.get(url)
        # Decode using 'replace' per requirement to minimize hangul loss
        html = resp.content.decode('euc-kr', errors='replace')
        soup = BeautifulSoup(html, 'html.parser')
        
        bank_danjis = []
        target_links = soup.find_all('a', class_='link_blue')
        for link in target_links:
            apt_name = link.get_text(strip=True)
            href = link.get('href', '')
            match = re.search(r"onClickDetail\(\s*'[^']*'\s*,\s*'([^']+)'\s*\)", href)
            if match:
                bank_id = match.group(1)
                bank_danjis.append({
                    "aptName": apt_name,
                    "bank_id": bank_id
                })
        return bank_danjis
    except Exception as e:
        print(f"Error fetching Bank for {code8}: {e}")
    return []

def build_master_table():
    session = get_session()
    master_table = []
    
    for region in REGIONS:
        code8 = region["code8"]
        rter_list = fetch_rter_danji(session, code8)
        bank_list = fetch_bank_danji(session, code8)
        
        for rter_item in rter_list:
            rter_name = rter_item.get("aptName", "")
            matched_bank_id = None
            matched_bank_name = None
            
            for bank_item in bank_list:
                bank_name = bank_item.get("aptName", "")
                if is_match_name(rter_name, bank_name, 0.8):
                    matched_bank_id = bank_item.get("bank_id")
                    matched_bank_name = bank_name
                    break
            
            if matched_bank_id:
                master_table.append({
                    "region": region["name"],
                    "code8": code8,
                    "rter_aptName": rter_name,
                    "bank_aptName": matched_bank_name,
                    "naverAptNo": rter_item.get("naverAptNo"),
                    "totalHouseCount": rter_item.get("totalHouseCount"),
                    "bank_id": matched_bank_id
                })
                
    with open("Danji_Master.json", "w", encoding="utf-8") as f:
        json.dump(master_table, f, ensure_ascii=False, indent=2)
    
    print(f"Successfully generated Danji_Master.json with {len(master_table)} matching records.")
    return master_table

if __name__ == "__main__":
    build_master_table()
