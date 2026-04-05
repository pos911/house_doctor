import pandas as pd
import datetime
import json
from phase2_scraper import run_phase2
from phase3_dedup import deduplicate_and_merge

def run_pipeline(target_name="한강삼성"):
    print(f"\n=== Starting Direct API Pipeline for {target_name} ===")
    rter_data, bank_data = run_phase2()
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
            
        floor_val = r.get('floor', '')
        total_f = r.get('total_floor', '')
        
        if floor_val and total_f:
            dong_floor = f"{dong_label} {floor_val}/{total_f}"
        else:
            dong_floor = f"{dong_label} {floor_val}"
            
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

if __name__ == "__main__":
    run_pipeline("한강삼성")
