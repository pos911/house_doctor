import re
from phase2_scraper import process_rter_item
from phase3_dedup import deduplicate_and_merge

def test_extraction():
    print("--- Testing Extraction ---")
    # Mock Altor item missing dong/floor info but having it in title
    mock_item = {
        "atclNm": "[급매] 한강삼성 101동 15/20층 한강뷰",
        "dong": None,
        "danji": {"danjiDongName": ""},
        "floor": None,
        "floorTotal": "",
        "space2": 84.5,
        "price1": "195,000",
        "atclFetrDesc": "한강뷰 로얄층"
    }
    
    result = process_rter_item(mock_item)
    print(f"Original Dong: {mock_item.get('dong')}, DanjiDongName: {mock_item.get('danji', {}).get('danjiDongName')}")
    print(f"Extraction result - Dong: '{result['dong']}', Floor: '{result['floor']}'")
    
    # Assertions for internal verification
    if result['dong'] == '101' and result['floor'] == '15':
        print(">> [SUCCESS] Extraction test passed.")
    else:
        print(">> [FAILED] Extraction test failed.")

def test_dedup_no_dong():
    print("\n--- Testing Dedup for Missing/Invalid Dong ---")
    # Three items:
    # 1. No dong
    # 2. Dong="0"
    # 3. Same space/price as #1
    # They should ALL remain separate.
    rter_data = [
        {"dong": "", "floor": "저", "total_floor": "10", "space": 84, "price": 150000, "feature": "No Dong Items"}
    ]
    bank_data = [
        {"dong": "0", "floor": "중", "total_floor": "10", "space": 84, "price": 150000, "feature": "Dong=0 Item"},
        {"dong": "", "floor": "고", "total_floor": "10", "space": 84, "price": 150000, "feature": "No Dong Items 2"}
    ]
    
    final = deduplicate_and_merge(rter_data, bank_data)
    print(f"Final items: {len(final)} (Expected: 3, they should NOT merge)")
    for i, item in enumerate(final):
        tags_str = " ".join(item['tags'])
        print(f"Item {i+1}: {item['feature']} | Platform: {tags_str}")
    
    if len(final) == 3:
        print(">> [SUCCESS] Dedup isolate test passed.")
    else:
        print(">> [FAILED] Dedup isolate test failed (Items merged).")

if __name__ == "__main__":
    test_extraction()
    test_dedup_no_dong()
