import json
import pandas as pd
import re
from config import get_session
from utils import parse_price, clean_text
from bs4 import BeautifulSoup

def process_rter_item(item):
    try:
        space2 = round(float(item.get("space2", 0)))
    except:
        space2 = 0
        
    price1 = parse_price(str(item.get("price1", "0")))
    feature = clean_text(item.get("atclFetrDesc", "")) or clean_text(item.get("feature", ""))
    
    dong_raw = clean_text(str(item.get("dong", "")))
    if not dong_raw:
        dong_raw = clean_text(str(item.get("danji", {}).get("danjiDongName", "")))
    dong = "".join(filter(str.isdigit, dong_raw))
    floor = clean_text(str(item.get("floor", "")))
    total_floor = clean_text(str(item.get("floorTotal", "")))
    
    floor_raw = f"{floor}/{total_floor}" if total_floor else floor
    
    return {
        "platform": "알터",
        "dong": dong,
        "floor": floor,
        "total_floor": total_floor,
        "floor_raw": floor_raw,
        "space": space2,
        "price": price1,
        "feature": feature
    }

def scrape_rter_listings(session, naver_apt_no):
    url = "https://rter2.com/hompyArticle/list"
    
    payload = {
        "pageNo": 1,
        "pageSize": 50,
        "searchMore": {
            "method": "30051A1",
            "type": "30000C01",
            "naverAptNo": str(naver_apt_no),
            "order": "show_start_date,desc"
        },
        "temporary": {"aptNo": str(naver_apt_no)},
        "northLatitude": "37.5446",
        "eastLongitude": "126.9525"
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    print(f"[알터 호출 전] Final URL: {url}")
    print(f"[알터 호출 전] Payload (Dict): {payload}")
    
    listings = []
    try:
        resp = session.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            print(f"[알터 호출 후] Raw Data (First 100 chars): {resp.text[:100]}...")
            data = resp.json()
            if data.get("status", {}).get("code") != 0:
                print(f"[알터 에러 코드] {data.get('status', {}).get('code')}: {data.get('status', {}).get('message')}")
                return []
            
            items = data.get("result", {}).get("list", [])
            
            # [Task 3] 데이터 유실 방지 로그
            missing_dong = sum(1 for item in items if not item.get("dong") and not (item.get("danji") and item.get("danji").get("danjiDongName")))
            missing_floor = sum(1 for item in items if not item.get("floor"))
            print(f"[알터 데이터 품질] 총 {len(items)}개 중 동 추출불가: {missing_dong}개, 층 부재: {missing_floor}개")
            
            for item in items:
                listings.append(process_rter_item(item))
    except Exception as e:
        print(f"Rter Scrape Error: {e}")
    return listings

def process_bank_row(tds):
    if len(tds) != 10:
        return None
        
    area_text = clean_text(tds[5].get_text())
    match = re.search(r"전용\s*([\d.]+)", area_text)
    if match:
        space_f = float(match.group(1))
    else:
        space_f = 0.0
        
    dong_raw = clean_text(tds[6].get_text())
    dong = "".join(filter(str.isdigit, dong_raw))
    
    floor_raw = clean_text(tds[7].get_text())
    
    price_str = clean_text(tds[8].get_text())
    
    # 방어 로직: 02-로 시작하거나 숫자가 6자리 미만이면 패스
    if price_str.startswith("02-"):
        return None
        
    digits_only = "".join(filter(str.isdigit, price_str))
    if len(digits_only) < 6:
        return None
        
    try:
        price = int(digits_only)
    except:
        return None
        
    floor = ""
    total_floor = ""
    if "/" in floor_raw:
        parts = floor_raw.split("/")
        floor = parts[0].strip()
        if len(parts) > 1:
            total_floor = parts[1].strip()
    else:
        floor = floor_raw

    return {
        "platform": "뱅크",
        "dong": dong,
        "floor": floor,
        "total_floor": total_floor,
        "floor_raw": floor_raw,
        "space": space_f,
        "price": price,
        "feature": ""
    }

def scrape_bank_listings(session, bank_id="A0001062", region_cd="1144010600"):
    url = f"https://www.neonet.co.kr/novo-rebank/view/offerings/inc_OfferingsList.neo?offerings_gbn=AT&sub_offerings_gbn=&complex_cd={bank_id}&offer_gbn=P&region_cd={region_cd}"
    print(f"[뱅크 호출 전] Final URL: {url}")
    listings = []
    try:
        resp = session.get(url)
        html = resp.content.decode('euc-kr', errors='replace')
        print(f"[뱅크 호출 후] Raw Data (First 300 chars): {html[:300]}...")
        
        soup = BeautifulSoup(html, 'html.parser')
        trs = soup.find_all('tr')
        print(f"[DEBUG] Found {len(trs)} <tr> elements.")
        if len(trs) < 10:
             print(f"[DEBUG] Bank HTML snippet: {html[500:1500]}")
        
        i = 0
        while i < len(trs):
            tr1 = trs[i]
            tds1 = tr1.find_all('td')
            # print(f"[DEBUG] Row {i}: {len(tds1)} tds") # Internal debug
            
            if len(tds1) == 10:
                item = process_bank_row(tds1)
                
                # Check for the next row for feature
                if i + 1 < len(trs):
                    tr2 = trs[i+1]
                    tds2 = tr2.find_all('td')
                    if tds2:
                        feature_text = clean_text(tds2[0].get_text())
                        if item:
                            item["feature"] = feature_text
                            
                if item:
                    listings.append(item)
                    
                i += 2
            else:
                i += 1
                
    except Exception as e:
        print(f"Bank Scrape Error: {e}")
    return listings

def run_phase2(target_name):
    session = get_session()
    
    try:
        with open("Danji_Master.json", "r", encoding="utf-8") as f:
            master_data = json.load(f)
    except Exception as e:
        print(f"Error loading Danji_Master.json: {e}")
        return [], []
        
    target_entry = None
    for entry in master_data:
        if entry.get("bank_aptName") == target_name or entry.get("rter_aptName") == target_name or entry.get("target_name") == target_name:
            target_entry = entry
            break
            
    if not target_entry:
        print(f"Target '{target_name}' not found in Danji_Master.json")
        return [], []
        
    naver_apt_no = str(target_entry.get("naverAptNo", ""))
    bank_id = str(target_entry.get("bank_id", ""))
    # code8 falls back to default if unavailable
    region_cd = str(target_entry.get("code8", "11440106")) + "00"
    
    print(f"[{target_name}] Direct API Scraping (Naver ID: {naver_apt_no}, Bank ID: {bank_id}, Region: {region_cd})...")
    
    rter_data = scrape_rter_listings(session, naver_apt_no)
    bank_data = scrape_bank_listings(session, bank_id, region_cd)
    
    return rter_data, bank_data
