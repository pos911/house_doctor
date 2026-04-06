import json
import pandas as pd
from config import get_session
from phase2_scraper import run_phase2
from phase3_dedup import deduplicate_and_merge

def final_verify():
    target_name = "한강삼성"
    print(f"--- Final Verification for {target_name} ---")
    
    rter_data, bank_data = run_phase2(target_name)
    
    print(f"Scraped Altor: {len(rter_data)} items")
    print(f"Scraped Bank: {len(bank_data)} items")
    
    # Check if Bank has items
    if len(bank_data) > 0:
        print(f"Sample Bank Item 1: {bank_data[0]}")
    
    final_results = deduplicate_and_merge(rter_data, bank_data)
    print(f"Final Deduplicated Results: {len(final_results)} items")
    
    # Save for user review
    df = pd.DataFrame(final_results)
    # Re-order columns to match user expectation: 상태, 동/층, 전용면적, 매매가(만원), 플랫폼, 매물특징
    # Note: final_results fields are 'platform', 'dong', 'floor', 'space', 'price', 'feature' etc.
    # In main.py, they are mapped to Korean headers.
    
    # Quick mapping for manual verification
    records = []
    for r in final_results:
        dong = r.get("dong", "")
        floor = r.get("floor", "")
        dong_floor = f"{dong}동 {floor}층".strip() if dong else floor
        records.append({
            "상태": "매매",
            "동/층": dong_floor,
            "전용면적": r.get("space"),
            "매매가(만원)": r.get("price"),
            "플랫폼": r.get("플랫폼"),
            "매물특징": r.get("feature")
        })
    
    df_mapped = pd.DataFrame(records)
    df_mapped.to_csv("한강삼성_검증결과.csv", index=False, encoding="utf-8-sig")
    print("Saved results to 한강삼성_검증결과.csv")

if __name__ == "__main__":
    final_verify()
