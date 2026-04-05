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

def scrape_rter_listings(session, naver_apt_no="13915"):
    url = "https://rter2.com/hompyArticle/list"
    
    sm_data = {
        "method": "30051A1",
        "type": "30000C01",
        "naverAptNo": str(naver_apt_no),
        "order": "show_start_date,desc"
    }
    
    payload = {
        "pageNo": 1,
        "pageSize": 50,
        "searchMore": json.dumps(sm_data, ensure_ascii=False),
        "temporary": json.dumps({"aptNo": "421"}), 
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
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
    })
    
    print(f"[알터 호출 전] Final URL: {url}")
    print(f"[알터 호출 전] POST Body: {json.dumps(payload, ensure_ascii=False)}")
    
    listings = []
    try:
        resp = session.post(url, data=payload, headers=headers)
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
    if len(tds) < 10:
        return None
        
    area_text = clean_text(tds[5].get_text())
    try:
        if "/" in area_text:
            space_f = float("".join(filter(lambda x: x.isdigit() or x == '.', area_text.split("/")[-1])))
        else:
            space_f = float("".join(filter(lambda x: x.isdigit() or x == '.', area_text)))
    except:
        space_f = 0.0
        
    dong = clean_text(tds[6].get_text())
    floor_raw = clean_text(tds[7].get_text())
    price_str = clean_text(tds[8].get_text())
    
    if "매물이 없습니다" in dong or "-" in price_str or price_str.startswith("02"):
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

def run_phase2():
    session = get_session()
    print("[한강삼성] Direct API Scraping items...")
    rter_data = scrape_rter_listings(session, "13915")
    bank_data = scrape_bank_listings(session, "A0001062", "1144010600")
    
    return rter_data, bank_data
