import json
import re
from bs4 import BeautifulSoup
from config import DONG_MAPPING, get_session
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

def update_master_table(regions=None):
    session = get_session()
    
    # Load existing master data if it exists
    master_table = []
    try:
        with open("Danji_Master.json", "r", encoding="utf-8") as f:
            master_table = json.load(f)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Error loading Danji_Master.json: {e}")

    # Track unique combinations to avoid duplicates
    existing_keys = { (str(m.get("naverAptNo")), str(m.get("bank_id"))) for m in master_table }
    
    if regions is None:
        # Default to all regions in mapping if none provided
        regions = [{"name": name, "code8": code} for name, code in DONG_MAPPING.items()]
    
    # Identify dongs that have already been indexed in the master file
    indexed_dongs = { str(m.get("code8")) for m in master_table }
        
    new_count = 0
    for region in regions:
        code8 = str(region["code8"])
        
        # Skip if this dong is already indexed (Incremental Update)
        if code8 in indexed_dongs:
            # print(f"[{region.get('name', 'Region')}] 이미 인덱싱되어 있습니다. 건너뜁니다.")
            continue
            
        print(f"[{region.get('name', 'New Region')}] 신규 지역 스캔 중 (코드: {code8})...")
        
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
                naver_no = str(rter_item.get("naverAptNo"))
                bank_id = str(matched_bank_id)
                
                if (naver_no, bank_id) not in existing_keys:
                    master_table.append({
                        "region": region.get("name", "New Region"),
                        "code8": code8,
                        "rter_aptName": rter_name,
                        "bank_aptName": matched_bank_name,
                        "naverAptNo": rter_item.get("naverAptNo"),
                        "totalHouseCount": rter_item.get("totalHouseCount"),
                        "bank_id": matched_bank_id
                    })
                    existing_keys.add((naver_no, bank_id))
                    new_count += 1
                
    if new_count > 0:
        with open("Danji_Master.json", "w", encoding="utf-8") as f:
            json.dump(master_table, f, ensure_ascii=False, indent=2)
        print(f"Danji_Master.json 업데이트 완료. 새롭게 {new_count}개의 단지가 추가되었습니다.")
    else:
        print("새로운 단지가 발견되지 않았습니다. 기존 마스터 리스트를 유지합니다.")
        
    return master_table

if __name__ == "__main__":
    update_master_table()
