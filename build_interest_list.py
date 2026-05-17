import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

from config import get_session, resolve_legal_dong_code8
from inspect_calls import sanitize_filename, write_json
from kapt_loader import load_kapt_complexes, normalize_name_for_compare
from listing_normalizer import (
    area_to_pyeong,
    classify_age_bucket,
    extract_orientation,
    extract_type,
    is_target_area,
    is_target_price,
    normalize_confirm_date,
    normalize_floor_range,
    normalize_price_range,
    safe_int,
)
from phase2_scraper import scrape_bank_listings_debug, scrape_rter_listings_debug
from site_resolver import resolve_site_candidates


def filter_complexes(complexes, only_seoul=True, min_households=295, sigungu=None, apt_queries=None):
    filtered = []
    normalized_queries = [normalize_name_for_compare(item) for item in (apt_queries or []) if item]
    for item in complexes:
        if only_seoul and item.get("sido") != "서울특별시":
            continue
        if sigungu and item.get("sigungu") != sigungu:
            continue
        if safe_int(item.get("household_count"), 0) < min_households:
            continue
        if normalized_queries:
            candidates = set(item.get("normalized_aliases") or [])
            candidates.add(item.get("normalized_apt_name", ""))
            if not any(
                query and any(query == candidate or query in candidate for candidate in candidates)
                for query in normalized_queries
            ):
                continue
        item = item.copy()
        item["age_bucket"] = classify_age_bucket(item.get("approval_date"))
        filtered.append(item)
    return filtered


def collect_complex_debug(complex_info):
    code8 = complex_info.get("legal_dong_code8")
    warnings = []
    if not code8:
        code8, code_warnings, _ = resolve_legal_dong_code8(
            dongri=complex_info.get("dongri", ""),
            legal_address=complex_info.get("legal_address", ""),
            road_address=complex_info.get("road_address", ""),
        )
        warnings.extend(code_warnings)

    working = complex_info.copy()
    if code8:
        working["legal_dong_code8"] = code8

    resolver_result = resolve_site_candidates(working, legal_dong_code8=code8)
    warnings = list(dict.fromkeys(warnings + resolver_result.get("warnings", [])))

    session = get_session()
    selected = resolver_result.get("selected", {})
    rter_selected = selected.get("rter", {})
    bank_selected = selected.get("bank", {})

    rter_debug = {
        "raw_items": [],
        "parsed_items": [],
        "warnings": ["rter_selected_id_missing_or_low_confidence"],
        "status_code": None,
        "fields": [],
        "raw_count": 0,
        "parsed_count": 0,
        "request": {},
        "raw_response": {},
    }
    bank_debug = {
        "raw_items": [],
        "parsed_items": [],
        "warnings": ["bank_selected_id_missing_or_low_confidence"],
        "status_code": None,
        "fields": [],
        "raw_count": 0,
        "parsed_count": 0,
        "request": {},
        "raw_response": "",
    }

    if rter_selected.get("match_status") in {"confirmed", "candidate"} and rter_selected.get("naverAptNo"):
        rter_debug = scrape_rter_listings_debug(session, rter_selected.get("naverAptNo"))

    if (
        bank_selected.get("match_status") in {"confirmed", "candidate"}
        and bank_selected.get("bank_id")
        and resolver_result.get("legal_dong_code8")
    ):
        bank_debug = scrape_bank_listings_debug(
            session,
            bank_selected.get("bank_id"),
            f"{resolver_result['legal_dong_code8']}00",
        )

    return {
        "complex_info": working,
        "resolver_result": resolver_result,
        "rter_debug": rter_debug,
        "bank_debug": bank_debug,
        "warnings": list(dict.fromkeys(warnings + rter_debug.get("warnings", []) + bank_debug.get("warnings", []))),
    }


def normalize_rter_listings(bundle):
    complex_info = bundle["complex_info"]
    raw_items = bundle["rter_debug"].get("raw_items", [])
    parsed_items = bundle["rter_debug"].get("parsed_items", [])
    normalized = []

    for raw_item, parsed_item in zip(raw_items, parsed_items):
        price_info = normalize_price_range(parsed_item.get("price"))
        floor_info = normalize_floor_range(
            parsed_item.get("floor"),
            parsed_item.get("total_floor"),
            parsed_item.get("floor_raw"),
        )
        area_m2 = float(parsed_item.get("space") or 0)
        normalized.append({
            "kapt_code": complex_info.get("kapt_code"),
            "apt_name": complex_info.get("apt_name"),
            "age_bucket": complex_info.get("age_bucket"),
            "legal_dong_code8": complex_info.get("legal_dong_code8"),
            "source": "rter",
            "source_listing_id": str(raw_item.get("seq") or raw_item.get("naverUid") or ""),
            "price_min_manwon": price_info["price_min_manwon"],
            "price_max_manwon": price_info["price_max_manwon"],
            "price_display": price_info["price_display"],
            "exclusive_area_m2": area_m2,
            "exclusive_area_pyeong": area_to_pyeong(area_m2),
            "type_name": extract_type(
                raw_item.get("atclNm"),
                raw_item.get("danji", {}).get("danjiPlanTypeName"),
                raw_item.get("danji", {}).get("naverAptTypeName"),
            ),
            "orientation": extract_orientation(raw_item.get("atclNm"), parsed_item.get("feature")),
            "confirm_date": normalize_confirm_date(raw_item.get("showStartDate")),
            "dong": parsed_item.get("dong", ""),
            "floor_text": floor_info["floor_text"],
            "floor_min": floor_info["floor_min"],
            "floor_max": floor_info["floor_max"],
            "floor_category": floor_info["floor_category"],
            "total_floor": floor_info["total_floor"],
            "feature": parsed_item.get("feature", ""),
            "raw_ref": f"{sanitize_filename(complex_info.get('apt_name'))}_rter_raw.json",
        })
    return normalized


def normalize_bank_listings(bundle):
    complex_info = bundle["complex_info"]
    parsed_items = bundle["bank_debug"].get("parsed_items", [])
    normalized = []

    for index, parsed_item in enumerate(parsed_items, start=1):
        price_info = normalize_price_range(parsed_item.get("price"))
        floor_info = normalize_floor_range(
            parsed_item.get("floor"),
            parsed_item.get("total_floor"),
            parsed_item.get("floor_raw"),
        )
        area_m2 = float(parsed_item.get("space") or 0)
        normalized.append({
            "kapt_code": complex_info.get("kapt_code"),
            "apt_name": complex_info.get("apt_name"),
            "age_bucket": complex_info.get("age_bucket"),
            "legal_dong_code8": complex_info.get("legal_dong_code8"),
            "source": "bank",
            "source_listing_id": f"{sanitize_filename(complex_info.get('apt_name'))}_bank_{index}",
            "price_min_manwon": price_info["price_min_manwon"],
            "price_max_manwon": price_info["price_max_manwon"],
            "price_display": price_info["price_display"],
            "exclusive_area_m2": area_m2,
            "exclusive_area_pyeong": area_to_pyeong(area_m2),
            "type_name": extract_type(parsed_item.get("feature")),
            "orientation": extract_orientation(parsed_item.get("feature")),
            "confirm_date": "",
            "dong": parsed_item.get("dong", ""),
            "floor_text": floor_info["floor_text"],
            "floor_min": floor_info["floor_min"],
            "floor_max": floor_info["floor_max"],
            "floor_category": floor_info["floor_category"],
            "total_floor": floor_info["total_floor"],
            "feature": parsed_item.get("feature", ""),
            "raw_ref": f"{sanitize_filename(complex_info.get('apt_name'))}_bank_raw.html",
        })
    return normalized


def group_listings(listings):
    groups = {}
    for item in listings:
        key = (
            item.get("kapt_code"),
            round(float(item.get("exclusive_area_m2") or 0)),
            item.get("price_min_manwon"),
            item.get("price_max_manwon"),
            item.get("dong") or "",
            item.get("floor_min"),
            item.get("floor_max"),
        )
        if key not in groups:
            groups[key] = {
                "group_key": "|".join([str(part) for part in key]),
                "kapt_code": item.get("kapt_code"),
                "apt_name": item.get("apt_name"),
                "exclusive_area_m2": item.get("exclusive_area_m2"),
                "price_min_manwon": item.get("price_min_manwon"),
                "price_max_manwon": item.get("price_max_manwon"),
                "dong": item.get("dong"),
                "floor_min": item.get("floor_min"),
                "floor_max": item.get("floor_max"),
                "sources": [],
                "items": [],
            }
        groups[key]["sources"].append(item.get("source"))
        groups[key]["items"].append(item)

    result = []
    for group in groups.values():
        group["sources"] = sorted(list(set(group["sources"])))
        group["group_status"] = "cross_source" if len(group["sources"]) > 1 else "single_source"
        group["item_count"] = len(group["items"])
        result.append(group)

    result.sort(key=lambda x: (x["apt_name"], x["price_min_manwon"], x["exclusive_area_m2"]))
    return result


def build_interest_list(groups):
    interest = []
    for group in groups:
        if not is_target_area(group.get("exclusive_area_m2")):
            continue
        if not is_target_price(group.get("price_min_manwon"), group.get("price_max_manwon")):
            continue
        interest.append(group)
    return interest


def write_interest_csv(path: Path, groups):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for group in groups:
        rows.append({
            "단지코드": group.get("kapt_code"),
            "단지명": group.get("apt_name"),
            "전용면적": group.get("exclusive_area_m2"),
            "평형": area_to_pyeong(group.get("exclusive_area_m2")),
            "가격최소(만원)": group.get("price_min_manwon"),
            "가격최대(만원)": group.get("price_max_manwon"),
            "동": group.get("dong"),
            "층최소": group.get("floor_min"),
            "층최대": group.get("floor_max"),
            "출처": ",".join(group.get("sources", [])),
            "그룹상태": group.get("group_status"),
            "매물수": group.get("item_count"),
        })

    if not rows:
        rows.append({
            "단지코드": "",
            "단지명": "",
            "전용면적": "",
            "평형": "",
            "가격최소(만원)": "",
            "가격최대(만원)": "",
            "동": "",
            "층최소": "",
            "층최대": "",
            "출처": "",
            "그룹상태": "",
            "매물수": 0,
        })

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="서울 관심 매물 리스트 생성 파이프라인")
    parser.add_argument("--excel", required=True, help="base_complex_master.xlsx 또는 K-apt 엑셀 경로")
    parser.add_argument("--output", default="outputs", help="출력 디렉터리")
    parser.add_argument("--limit", type=int, default=0, help="테스트용 상위 N개 단지만 처리")
    parser.add_argument("--sigungu", help="시군구 필터")
    parser.add_argument("--apt", help="단지명 필터")
    args = parser.parse_args()

    complexes = load_kapt_complexes(args.excel)
    apt_queries = [item.strip() for item in str(args.apt or "").split(",") if item.strip()]
    filtered_complexes = filter_complexes(
        complexes,
        sigungu=args.sigungu,
        apt_queries=apt_queries,
    )
    if args.limit > 0:
        filtered_complexes = filtered_complexes[:args.limit]

    date_root = Path(args.output) / datetime.now().strftime("%Y-%m-%d")
    interest_root = date_root / "interest_pipeline"
    interest_root.mkdir(parents=True, exist_ok=True)

    bundles = []
    normalized_listings = []
    mapping_items = []

    for complex_info in filtered_complexes:
        bundle = collect_complex_debug(complex_info)
        bundles.append(bundle)
        normalized_listings.extend(normalize_rter_listings(bundle))
        normalized_listings.extend(normalize_bank_listings(bundle))

        resolver = bundle["resolver_result"]
        selected = resolver.get("selected", {})
        mapping_items.append({
            "kapt_code": complex_info.get("kapt_code"),
            "apt_name": complex_info.get("apt_name"),
            "sido": complex_info.get("sido"),
            "sigungu": complex_info.get("sigungu"),
            "dongri": complex_info.get("dongri"),
            "approval_date": complex_info.get("approval_date"),
            "age_bucket": complex_info.get("age_bucket"),
            "household_count": complex_info.get("household_count"),
            "legal_dong_code8": resolver.get("legal_dong_code8"),
            "rter_selected": selected.get("rter", {}),
            "bank_selected": selected.get("bank", {}),
            "warnings": bundle.get("warnings", []),
        })

    grouped = group_listings(normalized_listings)
    interest_groups = build_interest_list(grouped)

    write_json(interest_root / "filtered_complexes.json", filtered_complexes)
    write_json(interest_root / "site_mapping_summary.json", mapping_items)
    write_json(interest_root / "normalized_listings.json", normalized_listings)
    write_json(interest_root / "listing_groups.json", grouped)
    write_json(interest_root / "interest_list.json", interest_groups)
    write_interest_csv(interest_root / "interest_list.csv", interest_groups)

    print("\n[파이프라인 결과]")
    print(f"- 대상 단지 수: {len(filtered_complexes)}")
    print(f"- 정규화 매물 수: {len(normalized_listings)}")
    print(f"- 그룹 수: {len(grouped)}")
    print(f"- 관심 매물 그룹 수: {len(interest_groups)}")
    print(f"- 출력 폴더: {interest_root}")


if __name__ == "__main__":
    main()
