import json
import pandas as pd
from config import get_session
from utils import parse_price, clean_text
from bs4 import BeautifulSoup

def process_rter_item(item):
    # Altor API 실시간 필드명 (상태 코드 0 확인됨)
    try:
        space2 = round(float(item.get("space2", 0)))
    except:
        space2 = 0
        
    price1 = parse_price(str(item.get("price1", "0")))
    # atclFetrDesc가 특징 필드임
    feature = clean_text(item.get("atclFetrDesc", "")) or clean_text(item.get("feature", ""))
    
    # confirmed keys: dong, floor, floorTotal
    dong = clean_text(str(item.get("dong", "")))
    floor = clean_text(str(item.get("floor", "")))
    total_floor = clean_text(str(item.get("floorTotal", "")))
    
    return {
        "platform": "알터",
        "dong": dong,
        "floor": floor,
        "total_floor": total_floor,
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
        "eastLongitude": "126.9525",
        "southLatitude": "37.5348",
        "westLongitude": "126.9294",
        "centerLatitude": "37.5397",
        "centerLongitude": "126.9410",
        "zoomLevel": 17
    }
    
    headers = session.headers.copy()
    headers.update({
        "X-Ajax-Call": "true",
        "X-Requested-With": "XMLHttpRequest"
    })
    
    print(f"[알터 호출 전] Final URL: {url}")
    print(f"[알터 호출 전] JSON Payload: {json.dumps(payload, ensure_ascii=False)}")
    
    listings = []
    try:
        # Use json=payload to send as application/json
        resp = session.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            print(f"[알터 호출 후] Raw Data (First 100 chars): {resp.text[:100]}...")
            data = resp.json()
            items = data.get("result", {}).get("list", [])
            for item in items:
                listings.append(process_rter_item(item))
    except Exception as e:
        print(f"Rter Scrape Error: {e}")
    return listings

def process_bank_row(tds):
    # Strict index mapping based on instructions:
    # ids[5]: space_f, ids[6]: dong, ids[7]: floor_raw, ids[8]: price
    if len(tds) < 9:
        return None
        
    area_text = clean_text(tds[5].get_text())
    # Extract only decimal if '전용' is present
    if "전용" in area_text:
        try:
            space_f = float("".join(filter(lambda x: x.isdigit() or x == '.', area_text)))
        except:
            space_f = 0.0
    else:
        # If '전용' is not clearly labelled, try to extract anyway but keep in mind user instruction
        space_f = 0.0
        
    # dong (동): tds[6] -> "101" 등 숫자만 추출.
    dong_raw = clean_text(tds[6].get_text())
    dong = "".join(filter(str.isdigit, dong_raw))
    
    # floor_raw (층): tds[7] -> "3/28" 등 전체를 가져올 것.
    floor_raw = clean_text(tds[7].get_text())
    
    # price (매매가): tds[8] -> "185,000"에서 숫자만 정수로 변환.
    price_str = clean_text(tds[8].get_text())
    
    # Skip if price contains phone number (02-) or is clearly invalid
    if "매물이 없습니다" in dong_raw or "-" in price_str or price_str.startswith("02"):
        return None
        
    price = parse_price(price_str)
    if price == 0:
        return None
        
    floor = ""
    total_floor = ""
    if "/" in floor_raw:
        parts = floor_raw.split("/")
        floor = parts[0].strip()
        total_floor = parts[1].strip()
    else:
        floor = floor_raw
        
    return {
        "platform": "뱅크",
        "dong": dong,
        "floor": floor,
        "total_floor": total_floor,
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
        print(f"[뱅크 호출 후] Raw Data (First 100 chars): {html[:100]}...")
        
        soup = BeautifulSoup(html, 'html.parser')
        trs = soup.find_all('tr')
        for tr in trs:
            tds = tr.find_all('td')
            item = process_bank_row(tds)
            if item:
                listings.append(item)
    except Exception as e:
        print(f"Bank Scrape Error: {e}")
    return listings

def run_phase2(target_name):
    session = get_session()
    
    # Load Danji_Master.json for mapping
    try:
        with open("Danji_Master.json", "r", encoding="utf-8") as f:
            master_data = json.load(f)
    except Exception as e:
        print(f"Error loading Danji_Master.json: {e}")
        return [], []
        
    target_entry = None
    for entry in master_data:
        if entry.get("bank_aptName") == target_name or entry.get("rter_aptName") == target_name:
            target_entry = entry
            break
            
    if not target_entry:
        print(f"Target '{target_name}' not found in Danji_Master.json")
        return [], []
        
    naver_apt_no = str(target_entry.get("naverAptNo", ""))
    bank_id = str(target_entry.get("bank_id", ""))
    region_cd = str(target_entry.get("code8", "")) + "00"
    
    print(f"[{target_name}] Direct API Scraping (Naver ID: {naver_apt_no}, Bank ID: {bank_id}, Region: {region_cd})...")
    
    rter_data = scrape_rter_listings(session, naver_apt_no)
    bank_data = scrape_bank_listings(session, bank_id, region_cd)
    
    return rter_data, bank_data
