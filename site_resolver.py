from typing import Dict, List, Optional

from config import get_session, resolve_legal_dong_code8
from kapt_loader import normalize_name_for_compare
from phase1_master import fetch_bank_danji_debug, fetch_rter_danji_debug
from utils import clean_text


def _ratio(name1: str, name2: str) -> float:
    if not name1 or not name2:
        return 0.0
    try:
        import Levenshtein

        return Levenshtein.ratio(name1, name2)
    except Exception:
        return 1.0 if name1 == name2 else 0.0


def _score_candidate(
    target: Dict[str, object],
    candidate_name: str,
    candidate_household_count: Optional[int] = None,
    legal_dong_code8: Optional[str] = None,
) -> Dict[str, object]:
    target_name = str(target.get("normalized_apt_name") or "")
    target_aliases = target.get("normalized_aliases") or [target_name]
    candidate_normalized = normalize_name_for_compare(candidate_name)

    best_ratio = 0.0
    exact_match = False
    alias_included = False

    for alias in target_aliases:
        ratio = _ratio(alias, candidate_normalized)
        if ratio > best_ratio:
            best_ratio = ratio
        if alias and alias == candidate_normalized:
            exact_match = True
        if alias and candidate_normalized and (alias in candidate_normalized or candidate_normalized in alias):
            alias_included = True

    score = best_ratio * 70.0
    reasons = [f"name_similarity:{best_ratio:.3f}"]

    if exact_match:
        score += 20
        reasons.append("exact_normalized_name")

    if alias_included:
        score += 10
        reasons.append("alias_contains")

    target_households = int(target.get("household_count") or 0)
    if target_households > 0 and candidate_household_count:
        diff_ratio = abs(target_households - candidate_household_count) / target_households
        if diff_ratio <= 0.05:
            score += 10
            reasons.append("household_count_close_5pct")
        elif diff_ratio <= 0.10:
            score += 5
            reasons.append("household_count_close_10pct")

    if legal_dong_code8:
        score += 10
        reasons.append("legal_dong_context_match")

    score = max(0.0, min(100.0, round(score, 2)))
    status = "confirmed" if score >= 90 else "candidate" if score >= 75 else "failed"
    return {
        "match_score": score,
        "match_reason": reasons,
        "match_status": status,
    }


def _select_best(candidates: List[Dict[str, object]], site_key: str) -> Dict[str, object]:
    if not candidates:
        return {
            "site": site_key,
            "match_status": "failed",
            "match_score": 0.0,
            "match_reason": ["no_candidates"],
        }

    best = candidates[0]
    return {
        **best,
        "match_status": best.get("match_status", "failed"),
    }


def resolve_site_candidates(complex_info: Dict[str, object], legal_dong_code8: Optional[str] = None) -> Dict[str, object]:
    code8 = legal_dong_code8
    warnings = []
    matched_name = None
    if not code8:
        code8, code_warnings, matched_name = resolve_legal_dong_code8(
            dongri=complex_info.get("dongri", ""),
            legal_address=complex_info.get("legal_address", ""),
            road_address=complex_info.get("road_address", ""),
        )
        warnings.extend(code_warnings)

    result = {
        "kapt_code": complex_info.get("kapt_code"),
        "apt_name": complex_info.get("apt_name"),
        "legal_dong_code8": code8,
        "resolved_dong_name": matched_name or complex_info.get("dongri", ""),
        "rter_candidates": [],
        "bank_candidates": [],
        "selected": {},
        "warnings": warnings,
        "resolver_debug": {},
    }

    if not code8:
        result["selected"] = {
            "rter": _select_best([], "rter"),
            "bank": _select_best([], "bank"),
            "mapping_status": "failed",
        }
        return result

    session = get_session()
    rter_debug = fetch_rter_danji_debug(session, code8)
    bank_debug = fetch_bank_danji_debug(session, code8)
    result["resolver_debug"] = {
        "rter": {
            "status_code": rter_debug.get("status_code"),
            "url": rter_debug.get("request", {}).get("url"),
            "raw_count": rter_debug.get("raw_count"),
            "error": rter_debug.get("error"),
        },
        "bank": {
            "status_code": bank_debug.get("status_code"),
            "url": bank_debug.get("request", {}).get("url"),
            "raw_count": bank_debug.get("raw_count"),
            "error": bank_debug.get("error"),
        },
    }

    if rter_debug.get("error"):
        warnings.append("rter_candidate_fetch_failed")
    if bank_debug.get("error"):
        warnings.append("bank_candidate_fetch_failed")

    rter_candidates = []
    for item in rter_debug.get("items", []):
        candidate_name = clean_text(item.get("aptName", ""))
        scored = _score_candidate(
            complex_info,
            candidate_name,
            candidate_household_count=int(item.get("totalHouseCount") or 0),
            legal_dong_code8=code8,
        )
        rter_candidates.append({
            "rter_aptName": candidate_name,
            "naverAptNo": str(item.get("naverAptNo", "")),
            "totalHouseCount": int(item.get("totalHouseCount") or 0),
            **scored,
        })

    bank_candidates = []
    for item in bank_debug.get("items", []):
        candidate_name = clean_text(item.get("aptName", ""))
        scored = _score_candidate(
            complex_info,
            candidate_name,
            candidate_household_count=None,
            legal_dong_code8=code8,
        )
        bank_candidates.append({
            "bank_aptName": candidate_name,
            "bank_id": str(item.get("bank_id", "")),
            **scored,
        })

    rter_candidates.sort(key=lambda x: (-x["match_score"], x["rter_aptName"]))
    bank_candidates.sort(key=lambda x: (-x["match_score"], x["bank_aptName"]))
    result["rter_candidates"] = rter_candidates
    result["bank_candidates"] = bank_candidates

    selected_rter = _select_best(rter_candidates, "rter")
    selected_bank = _select_best(bank_candidates, "bank")

    statuses = [selected_rter.get("match_status"), selected_bank.get("match_status")]
    if all(status == "confirmed" for status in statuses):
        mapping_status = "confirmed"
    elif any(status in {"confirmed", "candidate"} for status in statuses):
        mapping_status = "candidate"
    else:
        mapping_status = "failed"

    result["selected"] = {
        "rter": selected_rter,
        "bank": selected_bank,
        "mapping_status": mapping_status,
    }
    return result
