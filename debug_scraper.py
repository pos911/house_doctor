import json
import requests
from config import get_session

def debug_rter():
    session = get_session()
    url = "https://rter2.com/hompyArticle/list"
    naver_apt_no = "421" # 한강삼성
    
    # [Fix] Altor expects application/x-www-form-urlencoded, not raw JSON body.
    # We also include more precise coordinates found in the browser capture.
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
    
    resp = session.post(url, data=payload, headers=headers)
    print(f"Rter Status: {resp.status_code}")
    data = resp.json()
    items = data.get("result", {}).get("list", [])
    print(f"Rter Raw Items: {len(items)}")
    if items:
        # Show first item's atclNm and dong info
        it = items[0]
        print(f"Sample Item: Title='{it.get('atclNm')}', Dong='{it.get('dong')}', Floor='{it.get('floor')}'")
        with open("debug_rter.json", "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)

def debug_bank():
    session = get_session()
    bank_id = "A0001062" # 한강삼성 (Corrected)
    region_cd = "1144010600"
    url = f"https://www.neonet.co.kr/novo-rebank/view/offerings/inc_OfferingsList.neo?offerings_gbn=AT&sub_offerings_gbn=&complex_cd={bank_id}&offer_gbn=P&region_cd={region_cd}"
    
    resp = session.get(url)
    print(f"Bank Status: {resp.status_code}")
    html = resp.content.decode('euc-kr', errors='replace')
    print(f"Bank HTML Length: {len(html)}")
    with open("debug_bank.html", "w", encoding="utf-8") as f:
        f.write(html)

if __name__ == "__main__":
    debug_rter()
    debug_bank()
