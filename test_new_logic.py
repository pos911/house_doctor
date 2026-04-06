import json
import sys
import os

# Add current dir to path
sys.path.append(os.getcwd())

from phase2_scraper import process_rter_item
from phase3_dedup import deduplicate_and_merge

def test_extraction():
    print("--- Testing Extraction ---")
    mock_item = {
        'dong': '',
        'danji': {'danjiDongName': '101동'},
        'floor': '15',
        'floorTotal': '20',
        'space2': 84.99,
        'price1': '150,000',
        'atclFetrDesc': 'Good view'
    }
    processed = process_rter_item(mock_item)
    print(f"Original Dong: {mock_item['dong']}, DanjiDongName: {mock_item['danji']['danjiDongName']}")
    print(f"Processed Dong: '{processed['dong']}' (Expected: '101')")
    print(f"Processed Floor: '{processed['floor']}' (Expected: '15')")

def test_dedup_no_dong():
    print("\n--- Testing Dedup for Missing Dong ---")
    # Two identical items with NO dong info
    item1 = {
        'dong': '',
        'floor': '중',
        'total_floor': '20',
        'space': 84,
        'price': 150000,
        'feature': 'Feature A'
    }
    item2 = {
        'dong': '',
        'floor': '중',
        'total_floor': '20',
        'space': 84,
        'price': 150000,
        'feature': 'Feature B'
    }
    
    rter_data = [item1]
    bank_data = [item2]
    
    # Run dedup
    final = deduplicate_and_merge(rter_data, bank_data)
    
    print(f"Final items: {len(final)} (Expected: 2, because no-dong items should NOT merge)")
    for i, r in enumerate(final):
        print(f"Item {i+1}: {r.get('feature')} (Tags: {r.get('tags')})")

if __name__ == '__main__':
    test_extraction()
    test_dedup_no_dong()
