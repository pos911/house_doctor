import json
import pandas as pd
import re
from config import get_session
from utils import parse_price, clean_text
from bs4 import BeautifulSoup

def build_rter_request(naver_apt_no):
    url = "https://rter2.com/hompyArticle/list"
    payload = {
        "pageNo": 1,
        "pageSize": 50,
        "northLatitude": "37.5446348",
        "eastLongitude": "126.952589",
        "southLatitude": "37.5348174",
        "westLongitude": "126.9294147",
        "centerLatitude": "37.5397263",
        "centerLongitude": "126.9410019",
        "zoomLevel": "17",
        "searchMore": json.dumps({
            "method": "30051A1",
            "type": "30000C01",
            "naverAptNo": int(naver_apt_no),
            "order": "show_start_date,desc"
        }),
        "temporary": json.dumps({"aptNo": int(naver_apt_no)})
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "X-Ajax-Call": "true",
        "Referer": "https://rter2.com/mapview"
    }
    return {
        "url": url,
        "method": "POST",
        "params": payload,
        "headers": headers,
    }

def build_bank_request(bank_id="A0001062", region_cd="1144010600"):
    params = {
        "offerings_gbn": "AT",
        "sub_offerings_gbn": "",
        "complex_cd": str(bank_id),
        "offer_gbn": "P",
        "region_cd": str(region_cd),
    }
    query = "&".join([f"{key}={value}" for key, value in params.items()])
    url = f"https://www.neonet.co.kr/novo-rebank/view/offerings/inc_OfferingsList.neo?{query}"
    return {
        "url": url,
        "method": "GET",
        "params": params,
        "headers": {},
    }

def _flatten_fields(value, prefix=""):
    fields = set()
    if isinstance(value, dict):
        for key, nested in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            fields.add(next_prefix)
            fields.update(_flatten_fields(nested, next_prefix))
    elif isinstance(value, list):
        for item in value:
            fields.update(_flatten_fields(item, prefix))
    return fields

def process_rter_item(item):
    try:
        space2 = round(float(item.get("space2", 0)))
    except:
        space2 = 0
        
    price1 = parse_price(str(item.get("price1", "0")))
    feature = clean_text(item.get("atclFetrDesc", "")) or clean_text(item.get("feature", ""))
    
    atcl_nm = clean_text(str(item.get("atclNm") or ""))
    
    dong_raw = clean_text(str(item.get("dong") or ""))
    if not dong_raw or dong_raw == "None":
        dong_raw = clean_text(str(item.get("danji", {}).get("danjiDongName") or ""))
    
    # [Task 1] If dong is still missing, try extracting from atcl_nm (e.g., "101동")
    found_dong = False
    if not dong_raw or dong_raw == "None":
        match_dong = re.search(r"(\d+)동", atcl_nm)
        if match_dong:
            dong_raw = match_dong.group(1)
            found_dong = True
            
    dong = "".join(filter(str.isdigit, str(dong_raw)))
    
    floor = clean_text(str(item.get("floor") or ""))
    if floor == "None": floor = ""
    total_floor = clean_text(str(item.get("floorTotal") or ""))
    if total_floor == "None": total_floor = ""
    
    # [Task 1] If floor info is sparse, try extracting from atcl_nm (e.g., "5/20층")
    found_floor = False
    if (not floor or floor in ["저", "중", "고"]) and atcl_nm:
        match_floor = re.search(r"(\d+)/(\d+)층", atcl_nm)
        if match_floor:
            floor = match_floor.group(1)
            found_floor = True
            if not total_floor:
                total_floor = match_floor.group(2)
    
    if found_dong or found_floor:
        print(f"  [알터 매칭] 제목에서 추출 성공: {dong or '?'}동, {floor or '?'}층 (제목: {atcl_nm[:30]}...)")
    
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
    debug = scrape_rter_listings_debug(session, naver_apt_no)
    return debug.get("parsed_items", [])

def process_bank_row(tds):
    # Support both 9 (colspan used) and 10 columns
    count = len(tds)
    if count not in [9, 10]:
        return None
        
    # Standard indices if 10 tds: [0:거래, 1:종류, 2:확인일, 3:이미지, 4:매물명, 5:면적, 6:동, 7:층, 8:매물가, 9:연락처]
    # If 9 tds (colspan=2 for image/name): index shifts by -1 after td[3]
    offset = -1 if count == 9 else 0
    
    area_idx = 5 + offset
    dong_idx = 6 + offset
    floor_idx = 7 + offset
    price_idx = 8 + offset
    
    area_text = clean_text(tds[area_idx].get_text())
    # [Fix] Always extract the LAST number from area cell (전용면적).
    # Handles formats like "108A/84.9㎡", "84.9", "전용 84.9" etc.
    all_nums = re.findall(r"[\d.]+", area_text)
    if all_nums:
        space_f = round(float(all_nums[-1]))
    else:
        space_f = 0
        
    dong_raw = clean_text(tds[dong_idx].get_text())
    dong = "".join(filter(str.isdigit, dong_raw))
    
    floor_raw = clean_text(tds[floor_idx].get_text())
    
    price_str = clean_text(tds[price_idx].get_text())
    
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
    debug = scrape_bank_listings_debug(session, bank_id, region_cd)
    return debug.get("parsed_items", [])

def scrape_rter_listings_debug(session, naver_apt_no):
    request_info = build_rter_request(naver_apt_no)
    print(f"[알터 호출 전] URL: {request_info['url']}")
    print(f"[알터 호출 전] Form Data: {request_info['params']}")

    debug = {
        "request": {
            "url": request_info["url"],
            "method": request_info["method"],
            "params": request_info["params"],
        },
        "status_code": None,
        "raw_response": {},
        "raw_items": [],
        "raw_count": 0,
        "parsed_items": [],
        "parsed_count": 0,
        "fields": [],
        "warnings": [],
        "error": None,
    }

    try:
        resp = session.post(
            request_info["url"],
            data=request_info["params"],
            headers=request_info["headers"],
        )
        debug["status_code"] = resp.status_code
        if resp.status_code == 200:
            print("  [알터 응답] 성공 (HTTP 200)")
            data = resp.json()
            debug["raw_response"] = data
            if data.get("status", {}).get("code") != 0:
                warning = f"rter_status_error:{data.get('status', {}).get('code')}"
                debug["warnings"].append(warning)
                print(f"[알터 에러 코드] {data.get('status', {}).get('code')}: {data.get('status', {}).get('message')}")
                return debug

            items = data.get("result", {}).get("list", [])
            debug["raw_items"] = items
            debug["raw_count"] = len(items)
            debug["fields"] = sorted(_flatten_fields(items))
            print(f"[알터 Raw 데이터] 서버 응답 리스트 개수: {len(items)}")

            missing_dong = sum(1 for item in items if not item.get("dong") and not (item.get("danji") and item.get("danji").get("danjiDongName")))
            missing_floor = sum(1 for item in items if not item.get("floor"))
            print(f"[알터 데이터 품질] 총 {len(items)}개 중 동 추출불가: {missing_dong}개, 층 부재: {missing_floor}개")

            parsed = [process_rter_item(item) for item in items]
            debug["parsed_items"] = parsed
            debug["parsed_count"] = len(parsed)
        else:
            debug["warnings"].append(f"rter_http_{resp.status_code}")
    except Exception as e:
        debug["error"] = str(e)
        debug["warnings"].append("rter_request_failed")
        print(f"Rter Scrape Error: {e}")

    return debug

def scrape_bank_listings_debug(session, bank_id="A0001062", region_cd="1144010600"):
    request_info = build_bank_request(bank_id, region_cd)
    print(f"[뱅크 호출 전] Bank ID: {bank_id}, Region: {region_cd}")
    print(f"[뱅크 호출 전] Final URL: {request_info['url']}")

    debug = {
        "request": {
            "url": request_info["url"],
            "method": request_info["method"],
            "params": request_info["params"],
        },
        "status_code": None,
        "raw_response": "",
        "raw_items": [],
        "raw_count": 0,
        "parsed_items": [],
        "parsed_count": 0,
        "fields": [],
        "warnings": [],
        "error": None,
    }

    try:
        resp = session.get(request_info["url"])
        debug["status_code"] = resp.status_code
        if resp.status_code != 200:
            debug["warnings"].append(f"bank_http_{resp.status_code}")
            print(f"  [뱅크 에러] HTTP Status: {resp.status_code}")
            return debug

        html = resp.content.decode('euc-kr', errors='replace')
        debug["raw_response"] = html

        soup = BeautifulSoup(html, 'html.parser')
        trs = soup.find_all('tr')
        debug["raw_count"] = len(trs)
        print(f"[뱅크 Raw 데이터] 서버 응답 행(tr) 개수: {len(trs)}")

        if len(trs) < 10:
            print(f"  [뱅크 진단] HTML 내용 일부: {html[:1000]}")

        listings = []
        i = 0
        while i < len(trs):
            tr1 = trs[i]
            tds1 = tr1.find_all('td')

            if len(tds1) in [9, 10]:
                item = process_bank_row(tds1)

                if i + 1 < len(trs):
                    tr2 = trs[i + 1]
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

        debug["parsed_items"] = listings
        debug["parsed_count"] = len(listings)
        debug["raw_items"] = listings
        debug["fields"] = sorted(_flatten_fields(listings))
    except Exception as e:
        debug["error"] = str(e)
        debug["warnings"].append("bank_request_failed")
        print(f"Bank Scrape Error: {e}")

    return debug

def run_phase2_by_ids(target_entry):
    session = get_session()
    apt_name = str(target_entry.get("apt_name") or target_entry.get("rter_aptName") or target_entry.get("bank_aptName") or "")
    naver_apt_no = str(target_entry.get("rter_naverAptNo") or target_entry.get("naverAptNo") or "")
    bank_id = str(target_entry.get("bank_id") or "")
    legal_dong_code8 = str(target_entry.get("legal_dong_code8") or target_entry.get("code8") or "")
    region_cd = f"{legal_dong_code8}00" if legal_dong_code8 else ""

    print(f"[{apt_name}] Direct API Scraping by IDs (Naver ID: {naver_apt_no}, Bank ID: {bank_id}, Region: {region_cd})...")

    rter_data = scrape_rter_listings(session, naver_apt_no) if naver_apt_no else []
    bank_data = scrape_bank_listings(session, bank_id, region_cd) if bank_id and region_cd else []
    return rter_data, bank_data

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
    region_cd = str(target_entry.get("code8", "11440106")) + "00"

    print(f"[{target_name}] Direct API Scraping (Naver ID: {naver_apt_no}, Bank ID: {bank_id}, Region: {region_cd})...")

    rter_data = scrape_rter_listings(session, naver_apt_no)
    bank_data = scrape_bank_listings(session, bank_id, region_cd)

    return rter_data, bank_data
