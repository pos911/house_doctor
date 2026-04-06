import pandas as pd
import datetime
import json
from phase2_scraper import run_phase2
from phase3_dedup import deduplicate_and_merge
from phase1_master import update_master_table
from config import DONG_MAPPING
from utils import search_official_names_naver

def run_pipeline(target_name="한강삼성"):
    print(f"\n=== Starting Direct API Pipeline for {target_name} ===")
    rter_data, bank_data = run_phase2(target_name)
    print(f"Scraped {len(rter_data)} items from Rter, {len(bank_data)} items from Bank.")
    
    if not rter_data and not bank_data:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"[{current_time}] {target_name}: 실시간 매물 없음 (HTTP 200 정상)")
        return
        
    final_results = deduplicate_and_merge(rter_data, bank_data)
    
    # Format: [상태] [동/층] [전용면적] [매매가(만원)] [플랫폼] [매물특징]
    rows = []
    for r in final_results:
        # Format: {동}동 {현재층}/{전체층}
        dong_val = r.get('dong', '')
        if '동' not in str(dong_val) and dong_val:
            dong_label = f"{dong_val}동"
        else:
            dong_label = str(dong_val)
            
        floor_raw = r.get('floor_raw', '')
        if not floor_raw:
            floor_val = r.get('floor', '')
            total_f = r.get('total_floor', '')
            if floor_val and total_f:
                floor_raw = f"{floor_val}/{total_f}"
            else:
                floor_raw = floor_val

        dong_floor = f"{dong_label} {floor_raw}".strip()
            
        rows.append({
            "상태": "매매",
            "동/층": dong_floor,
            "전용면적": r.get('space'),
            "매매가(만원)": r.get('price'),
            "플랫폼": r.get('플랫폼'),
            "매물특징": r.get('feature')
        })
        
    df = pd.DataFrame(rows)
    
    # Sort: 면적(ASC) -> 가격(ASC)
    if not df.empty:
        df.sort_values(by=["전용면적", "매매가(만원)"], ascending=[True, True], inplace=True)
    
    print("\n--- Final Pipeline Result ---")
    print(df.to_string(index=False))
    
    # Save CSV
    csv_file = f"{target_name}_최종_통합매물.csv"
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")
    print(f"\nSaved to {csv_file}")
    
    # Save JSON
    json_file = f"{target_name}_최종_통합매물.json"
    with open(json_file, "w", encoding="utf-8") as f:
        # Save as a list of dicts for JSON
        json_data = df.to_dict(orient='records')
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    print(f"Saved to {json_file}")

def main():
    while True:
        print("\n" + "="*55)
        print(" [House Doctor] 실시간 매물 통합 검색 프로그램")
        print("="*55)
        
        # 1. Multi-dong Input
        dong_input = input("조회할 동네 이름을 입력하세요 (예: 용강동, 토정동): ").strip()
        if not dong_input:
            print("(!) 동네 이름은 필수입니다.\n")
            continue
            
        dong_names = [name.strip() for name in dong_input.split(",")]
        requested_regions = []
        invalid_names = []
        
        for name in dong_names:
            if name in DONG_MAPPING:
                requested_regions.append({"name": name, "code8": DONG_MAPPING[name]})
            else:
                invalid_names.append(name)
        
        if invalid_names:
            print(f"(!) '{', '.join(invalid_names)}' 코드를 찾을 수 없습니다. (매핑 데이터 확인 필요)")
            if not requested_regions:
                continue
                
        # 2. Sequential/Incremental Indexing
        print(f"\n[*] 요청 지역({', '.join([r['name'] for r in requested_regions])})의 마스터 정보를 확인합니다...")
        update_master_table(requested_regions)
        
        # Load updated master data
        master_data = []
        try:
            with open("Danji_Master.json", "r", encoding="utf-8") as f:
                master_data = json.load(f)
        except:
            pass

        # 3. Naver API Official Name Search
        query_name = input("\n조회할 아파트명을 입력하세요 (예: 삼성): ").strip()
        if not query_name:
            print("(!) 아파트명은 필수 입력 사항입니다.")
            continue
            
        print(f"\n[*] 네이버 검색 API로 '{query_name}'의 공식 명칭을 검색합니다...")
        official_names = search_official_names_naver(query_name)
        
        if not official_names:
            print("(!) 네이버 검색 결과가 없습니다. 기존 방식(로컬 퍼지 검색)으로 진행합니다.")
            official_names = [query_name]
        else:
            print(f"\n--- 네이버 검색 결과 (공식 명칭 선택) ---")
            for i, name in enumerate(official_names):
                print(f"[{i+1}] {name}")
            print(f"[{len(official_names)+1}] 직접 입력 (검색 결과에 없음)")
            
            nav_sel = input("\n해당하는 아파트의 번호를 선택하세요: ").strip()
            if nav_sel.isdigit() and 1 <= int(nav_sel) <= len(official_names):
                query_name = official_names[int(nav_sel)-1]
                print(f"[*] 선택된 공식 명칭: {query_name}")
            elif nav_sel == str(len(official_names)+1):
                query_name = input("직접 검색할 명칭을 입력하세요: ").strip()
            else:
                print("(!) 잘못된 선택입니다. 처음부터 다시 진행합니다.")
                continue

        # Find all matches in local master data using the selected query_name
        matches = []
        for m in master_data:
            if str(m.get("code8")) in [r["code8"] for r in requested_regions]:
                if query_name in m.get("rter_aptName", "") or query_name in m.get("bank_aptName", ""):
                    matches.append(m)

        if not matches:
            print(f"(!) 요청하신 동네에서 '{query_name}'을(를) 포함하는 단지를 찾을 수 없습니다.")
            continue

        # 4. Selection Interface for Local Data
        print(f"\n--- '{query_name}' 관련 로컬 마스터 검색 결과 ({len(matches)}개) ---")
        for i, m in enumerate(matches):
            print(f"[{i+1}] {m.get('region')} {m.get('rter_aptName')} (매물수: {m.get('totalHouseCount', 'N/A')})")
        print(f"[{len(matches)+1}] 위 리스트 전체 조회")
        
        selection = input("\n조회할 번호를 입력하세요 (취소: Enter): ").strip()
        if not selection:
            continue
            
        selected_targets = []
        if selection == str(len(matches)+1):
            selected_targets = matches
        elif selection.isdigit() and 1 <= int(selection) <= len(matches):
            selected_targets = [matches[int(selection)-1]]
        else:
            print("(!) 잘못된 입력입니다.")
            continue

        # 5. Execute Pipeline for each selected target
        for target in selected_targets:
            target_name = target.get("rter_aptName")
            print(f"\n>>> [{target_name}] 매핑 정보 확인 완료. 데이터를 수집합니다.")
            run_pipeline(target_name)
            
        choice = input("\n다른 지역/단지를 추가로 조회하시겠습니까? (y/n, 기본 n): ").lower()
        if choice != 'y':
            print("\n프로그램을 종료합니다. 감사합니다.")
            break

if __name__ == "__main__":
    main()
