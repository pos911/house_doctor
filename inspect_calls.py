import argparse
import json
from datetime import datetime
from pathlib import Path

from config import get_session, resolve_legal_dong_code8
from kapt_loader import load_kapt_complexes, normalize_name_for_compare
from phase2_scraper import (
    build_bank_request,
    build_rter_request,
    scrape_bank_listings_debug,
    scrape_rter_listings_debug,
)
from site_resolver import resolve_site_candidates


def sanitize_filename(name: str) -> str:
    sanitized = "".join(ch for ch in name if ch.isalnum() or ch in ("_", "-", " ")).strip()
    return sanitized.replace(" ", "_") or "unknown"


def select_targets(complexes, apt=None, apt_list=None, select_all=False, sigungu=None):
    if select_all:
        selected = complexes
    else:
        names = []
        if apt:
            names.append(apt)
        if apt_list:
            names.extend([item.strip() for item in apt_list.split(",") if item.strip()])

        normalized_names = [normalize_name_for_compare(name) for name in names]
        selected = []
        for item in complexes:
            candidates = set(item.get("normalized_aliases") or [])
            candidates.add(item.get("normalized_apt_name", ""))
            if any(
                query and any(query == candidate or query in candidate for candidate in candidates)
                for query in normalized_names
            ):
                selected.append(item)

    if sigungu:
        selected = [item for item in selected if item.get("sigungu") == sigungu]

    deduped = []
    seen = set()
    for item in selected:
        key = item.get("kapt_code")
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def build_call_debug(target, resolver_result, rter_debug, bank_debug):
    selected = resolver_result.get("selected", {})
    rter_selected = selected.get("rter", {})
    bank_selected = selected.get("bank", {})
    warnings = list(dict.fromkeys(
        resolver_result.get("warnings", [])
        + (rter_debug.get("warnings", []) if rter_debug else [])
        + (bank_debug.get("warnings", []) if bank_debug else [])
    ))

    return {
        "target": {
            "kapt_code": target.get("kapt_code"),
            "apt_name": target.get("apt_name"),
            "sido": target.get("sido"),
            "sigungu": target.get("sigungu"),
            "dongri": target.get("dongri"),
            "legal_address": target.get("legal_address"),
            "road_address": target.get("road_address"),
            "approval_date": target.get("approval_date"),
            "household_count": target.get("household_count"),
            "building_count": target.get("building_count"),
        },
        "resolved_ids": {
            "legal_dong_code8": resolver_result.get("legal_dong_code8"),
            "rter_naverAptNo": rter_selected.get("naverAptNo", ""),
            "bank_complex_cd": bank_selected.get("bank_id", ""),
        },
        "resolver_candidates": {
            "rter": resolver_result.get("rter_candidates", []),
            "bank": resolver_result.get("bank_candidates", []),
        },
        "requests": {
            "rter": rter_debug.get("request", {}) if rter_debug else {},
            "bank": bank_debug.get("request", {}) if bank_debug else {},
        },
        "response_summary": {
            "rter_status_code": rter_debug.get("status_code") if rter_debug else None,
            "bank_status_code": bank_debug.get("status_code") if bank_debug else None,
            "rter_count": rter_debug.get("raw_count", 0) if rter_debug else 0,
            "bank_count": bank_debug.get("parsed_count", 0) if bank_debug else 0,
            "rter_parsed_count": rter_debug.get("parsed_count", 0) if rter_debug else 0,
            "bank_raw_count": bank_debug.get("raw_count", 0) if bank_debug else 0,
            "bank_parsed_count": bank_debug.get("parsed_count", 0) if bank_debug else 0,
            "rter_fields": rter_debug.get("fields", []) if rter_debug else [],
            "bank_fields": bank_debug.get("fields", []) if bank_debug else [],
        },
        "sample_records": {
            "rter": (rter_debug.get("parsed_items", []) if rter_debug else [])[:5],
            "bank": (bank_debug.get("parsed_items", []) if bank_debug else [])[:5],
        },
        "warnings": warnings,
    }


def write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def print_target_summary(target, resolver_result, call_debug):
    selected = resolver_result.get("selected", {})
    print("\n" + "=" * 72)
    print(f"[대상 단지] {target.get('apt_name')} ({target.get('kapt_code')})")
    print(f"- 시도/시군구/동리: {target.get('sido')} / {target.get('sigungu')} / {target.get('dongri')}")
    print(f"- 법정동주소: {target.get('legal_address')}")
    print(f"- 도로명주소: {target.get('road_address')}")
    print(f"- 사용승인일: {target.get('approval_date')}, 동수: {target.get('building_count')}, 세대수: {target.get('household_count')}")
    print(f"- legal_dong_code8: {resolver_result.get('legal_dong_code8') or 'N/A'}")

    print("\n[알터 후보 TOP 5]")
    for item in resolver_result.get("rter_candidates", [])[:5]:
        print(f"- {item.get('rter_aptName')} | naverAptNo={item.get('naverAptNo')} | score={item.get('match_score')} | reason={item.get('match_reason')}")
    if not resolver_result.get("rter_candidates"):
        print("- 없음")

    print("\n[뱅크 후보 TOP 5]")
    for item in resolver_result.get("bank_candidates", [])[:5]:
        print(f"- {item.get('bank_aptName')} | bank_id={item.get('bank_id')} | score={item.get('match_score')} | reason={item.get('match_reason')}")
    if not resolver_result.get("bank_candidates"):
        print("- 없음")

    print("\n[선택된 식별값]")
    print(f"- rter_naverAptNo: {selected.get('rter', {}).get('naverAptNo', '')}")
    print(f"- bank_id: {selected.get('bank', {}).get('bank_id', '')}")

    print("\n[실제 호출]")
    print(f"- rter: {call_debug['requests']['rter'].get('method')} {call_debug['requests']['rter'].get('url')}")
    print(f"  params={call_debug['requests']['rter'].get('params', {})}")
    print(f"- bank: {call_debug['requests']['bank'].get('method')} {call_debug['requests']['bank'].get('url')}")
    print(f"  params={call_debug['requests']['bank'].get('params', {})}")

    print("\n[응답 요약]")
    summary = call_debug["response_summary"]
    print(f"- rter status/raw/parsed: {summary.get('rter_status_code')} / {summary.get('rter_count')} / {summary.get('rter_parsed_count')}")
    print(f"- bank status/raw/parsed: {summary.get('bank_status_code')} / {summary.get('bank_raw_count')} / {summary.get('bank_parsed_count')}")
    print(f"- rter fields: {summary.get('rter_fields', [])}")
    print(f"- bank fields: {summary.get('bank_fields', [])}")
    print(f"- warnings: {call_debug.get('warnings', [])}")


def process_target(target, output_root: Path):
    code8, code_warnings, _ = resolve_legal_dong_code8(
        dongri=target.get("dongri", ""),
        legal_address=target.get("legal_address", ""),
        road_address=target.get("road_address", ""),
    )
    resolver_result = resolve_site_candidates(target, legal_dong_code8=code8)
    resolver_result["warnings"] = list(dict.fromkeys(code_warnings + resolver_result.get("warnings", [])))

    session = get_session()
    selected = resolver_result.get("selected", {})
    rter_selected = selected.get("rter", {})
    bank_selected = selected.get("bank", {})

    rter_debug = None
    bank_debug = None

    should_call_rter = rter_selected.get("match_status") in {"confirmed", "candidate"} and rter_selected.get("naverAptNo")
    should_call_bank = (
        bank_selected.get("match_status") in {"confirmed", "candidate"}
        and bank_selected.get("bank_id")
        and resolver_result.get("legal_dong_code8")
    )

    if should_call_rter:
        rter_debug = scrape_rter_listings_debug(session, rter_selected.get("naverAptNo"))
    else:
        rter_debug = {
            "request": {},
            "warnings": ["rter_selected_id_missing_or_low_confidence"],
            "parsed_items": [],
            "fields": [],
            "raw_count": 0,
            "parsed_count": 0,
            "raw_response": {},
            "status_code": None,
        }

    if should_call_bank:
        region_cd = f"{resolver_result['legal_dong_code8']}00"
        bank_debug = scrape_bank_listings_debug(session, bank_selected.get("bank_id"), region_cd)
    else:
        bank_debug = {
            "request": {},
            "warnings": ["bank_selected_id_missing_or_low_confidence"],
            "parsed_items": [],
            "fields": [],
            "raw_count": 0,
            "parsed_count": 0,
            "raw_response": "",
            "status_code": None,
        }

    if should_call_rter:
        rter_request = build_rter_request(rter_selected["naverAptNo"])
        rter_debug["request"] = {
            "url": rter_request["url"],
            "method": rter_request["method"],
            "params": rter_request["params"],
        }

    if should_call_bank:
        region_cd = f"{resolver_result['legal_dong_code8']}00"
        bank_request = build_bank_request(bank_selected["bank_id"], region_cd)
        bank_debug["request"] = {
            "url": bank_request["url"],
            "method": bank_request["method"],
            "params": bank_request["params"],
        }

    call_debug = build_call_debug(target, resolver_result, rter_debug, bank_debug)

    name = sanitize_filename(target.get("apt_name", "unknown"))
    write_json(output_root / "call_debug" / f"{name}_call_debug.json", call_debug)
    write_json(output_root / "raw" / f"{name}_rter_raw.json", rter_debug.get("raw_response", {}))
    raw_bank_path = output_root / "raw" / f"{name}_bank_raw.html"
    raw_bank_path.parent.mkdir(parents=True, exist_ok=True)
    with open(raw_bank_path, "w", encoding="utf-8") as f:
        f.write(bank_debug.get("raw_response", ""))
    write_json(output_root / "parsed" / f"{name}_rter_parsed.json", {
        "count": rter_debug.get("parsed_count", 0),
        "fields": rter_debug.get("fields", []),
        "items": rter_debug.get("parsed_items", []),
    })
    write_json(output_root / "parsed" / f"{name}_bank_parsed.json", {
        "count": bank_debug.get("parsed_count", 0),
        "fields": bank_debug.get("fields", []),
        "items": bank_debug.get("parsed_items", []),
    })

    print_target_summary(target, resolver_result, call_debug)
    return resolver_result, call_debug


def main():
    parser = argparse.ArgumentParser(description="K-apt 기반 사이트 호출값 진단 도구")
    parser.add_argument("--excel", required=True, help="K-apt 단지 기본정보 엑셀 경로")
    parser.add_argument("--apt", help="대상 단지명 1개")
    parser.add_argument("--apt-list", help="쉼표로 구분한 대상 단지명 목록")
    parser.add_argument("--all", action="store_true", help="전체 단지 대상")
    parser.add_argument("--sigungu", help="시군구 필터")
    parser.add_argument("--output", default="outputs", help="출력 디렉터리")
    args = parser.parse_args()

    complexes = load_kapt_complexes(args.excel)
    targets = select_targets(
        complexes,
        apt=args.apt,
        apt_list=args.apt_list,
        select_all=args.all,
        sigungu=args.sigungu,
    )
    if not targets:
        raise SystemExit("조건에 맞는 단지를 찾지 못했습니다.")

    date_root = Path(args.output) / datetime.now().strftime("%Y-%m-%d")
    mapping_items = []

    for target in targets:
        resolver_result, call_debug = process_target(target, date_root)
        selected = resolver_result.get("selected", {})
        mapping_items.append({
            "kapt_code": target.get("kapt_code"),
            "apt_name": target.get("apt_name"),
            "normalized_apt_name": target.get("normalized_apt_name"),
            "sido": target.get("sido"),
            "sigungu": target.get("sigungu"),
            "dongri": target.get("dongri"),
            "legal_address": target.get("legal_address"),
            "road_address": target.get("road_address"),
            "approval_date": target.get("approval_date"),
            "household_count": target.get("household_count"),
            "building_count": target.get("building_count"),
            "legal_dong_code8": resolver_result.get("legal_dong_code8"),
            "source_ids": {
                "rter": {
                    "apt_name": selected.get("rter", {}).get("rter_aptName", ""),
                    "naverAptNo": selected.get("rter", {}).get("naverAptNo", ""),
                    "match_score": selected.get("rter", {}).get("match_score", 0.0),
                    "match_status": selected.get("rter", {}).get("match_status", "failed"),
                    "match_reason": selected.get("rter", {}).get("match_reason", []),
                },
                "bank": {
                    "apt_name": selected.get("bank", {}).get("bank_aptName", ""),
                    "bank_id": selected.get("bank", {}).get("bank_id", ""),
                    "match_score": selected.get("bank", {}).get("match_score", 0.0),
                    "match_status": selected.get("bank", {}).get("match_status", "failed"),
                    "match_reason": selected.get("bank", {}).get("match_reason", []),
                },
            },
            "warnings": call_debug.get("warnings", []),
        })

    write_json(date_root / "site_mapping.json", {
        "generated_at": datetime.now().isoformat(),
        "source_excel": Path(args.excel).name,
        "items": mapping_items,
    })


if __name__ == "__main__":
    main()
