import json
from utils import is_match_name

# ─────────────────────────────────────────────
# 1. 층 카테고리 (저/중/고) 분류
# ─────────────────────────────────────────────
def get_floor_category(floor_val, total_floor_val):
    if floor_val in ["저", "중", "고", "저층", "중층", "고층"]:
        return floor_val.replace("층", "")
    try:
        f = float(floor_val)
        tf = float(total_floor_val)
        if tf == 0:
            return "중"
        ratio = f / tf
        if ratio <= 0.3:
            return "저"
        elif ratio <= 0.7:
            return "중"
        else:
            return "고"
    except:
        return "중"

# ─────────────────────────────────────────────
# 2. 메인 중복제거 & 병합 함수
# ─────────────────────────────────────────────
def deduplicate_and_merge(rter_data, bank_data):
    """
    2-Step 버킷링 + 텍스트 유사도 교차 병합 알고리즘
    Step 1: (space_int, price) Key로 버킷 생성
    Step 2: 버킷 내 알터-뱅크 간 feature 텍스트 유사도로 1:1 병합
    Step 3: 버킷에 알터 1개 + 뱅크 1개만 남으면 강제 병합
    """

    # ── 플랫폼 태깅 & 기본 필드 정규화 ──────────────────
    all_recs = []
    for r in rter_data:
        r["_platform"] = "알터"
        r["_space_int"] = round(float(r.get("space", 0)))
        r["_price"] = int(r.get("price", 0))
        r["_cat"] = get_floor_category(r.get("floor", ""), r.get("total_floor", ""))
        r["tags"] = {"알터"}
        all_recs.append(r)
    for b in bank_data:
        b["_platform"] = "뱅크"
        b["_space_int"] = round(float(b.get("space", 0)))
        b["_price"] = int(b.get("price", 0))
        b["_cat"] = get_floor_category(b.get("floor", ""), b.get("total_floor", ""))
        b["tags"] = {"뱅크"}
        all_recs.append(b)

    # ── Step 1: (space_int, price) 버킷 생성 ────────────
    buckets = {}
    for r in all_recs:
        k = (r["_space_int"], r["_price"])
        if k not in buckets:
            buckets[k] = {"알터": [], "뱅크": []}
        buckets[k][r["_platform"]].append(r)

    merged_results = []
    standalone_results = []

    for key, group in buckets.items():
        rter_items = group["알터"]
        bank_items = group["뱅크"]

        # 버킷에 한 플랫폼만 있으면 → 단독 매물
        if not rter_items:
            standalone_results.extend(bank_items)
            continue
        if not bank_items:
            standalone_results.extend(rter_items)
            continue

        # ── Step 2: Feature 텍스트 유사도로 1:1 매칭 ──────
        used_bank = set()
        used_rter = set()

        # 먼저 고유사도(threshold 0.5 이상) 쌍을 찾아 병합
        pairs = []
        for ri, r in enumerate(rter_items):
            for bi, b in enumerate(bank_items):
                score = _feature_similarity(r.get("feature", ""), b.get("feature", ""))
                if score >= 0.5:
                    pairs.append((score, ri, bi))

        # 점수 내림차순 정렬 → 높은 것부터 1:1 병합
        pairs.sort(key=lambda x: -x[0])
        for score, ri, bi in pairs:
            if ri in used_rter or bi in used_bank:
                continue
            merged = _do_merge(rter_items[ri], bank_items[bi])
            merged_results.append(merged)
            used_rter.add(ri)
            used_bank.add(bi)

        # ── Step 3: 텍스트 매칭 후 각 1개씩 남으면 강제 병합 ──
        remaining_rter = [r for i, r in enumerate(rter_items) if i not in used_rter]
        remaining_bank = [b for i, b in enumerate(bank_items) if i not in used_bank]

        if len(remaining_rter) == 1 and len(remaining_bank) == 1:
            merged = _do_merge(remaining_rter[0], remaining_bank[0])
            merged_results.append(merged)
        else:
            # 나머지는 단독 처리
            standalone_results.extend(remaining_rter)
            standalone_results.extend(remaining_bank)

    # ── 단독 매물 태그 처리 ──────────────────────────────
    final = []
    for r in merged_results:
        _apply_tags(r)
        final.append(r)

    for r in standalone_results:
        _apply_tags(r)
        final.append(r)

    return final


# ─────────────────────────────────────────────
# 내부 헬퍼 함수들
# ─────────────────────────────────────────────
def _feature_similarity(f1: str, f2: str) -> float:
    """feature 텍스트 두 문자열 간 Levenshtein 유사도 반환 (0.0 ~ 1.0)"""
    try:
        import Levenshtein
        t1 = (f1 or "").replace(" ", "").lower()
        t2 = (f2 or "").replace(" ", "").lower()
        if not t1 or not t2:
            return 0.0
        return Levenshtein.ratio(t1, t2)
    except:
        return 0.0


def _do_merge(rter_item: dict, bank_item: dict) -> dict:
    """
    알터 + 뱅크 매물을 1:1 병합.
    - 동/층 정보: 뱅크 우선 (더 정확함)
    - feature: 알터 우선 (더 풍부), 없으면 뱅크 것 사용
    - 가격: 알터 우선
    """
    merged = rter_item.copy()
    merged["tags"] = {"알터", "뱅크"}

    # 동/층: 알터가 비어있으면 뱅크 값으로 채움
    if not merged.get("dong") or merged["dong"] == "":
        merged["dong"] = bank_item.get("dong", "")
    if not merged.get("floor") or merged["floor"] == "":
        merged["floor"] = bank_item.get("floor", "")
    if not merged.get("total_floor") or merged["total_floor"] == "":
        merged["total_floor"] = bank_item.get("total_floor", "")

    # floor_raw 재구성
    f = merged.get("floor", "")
    tf = merged.get("total_floor", "")
    merged["floor_raw"] = f"{f}/{tf}" if tf else f

    # feature: 알터가 비어있으면 뱅크 것 사용
    if not merged.get("feature"):
        merged["feature"] = bank_item.get("feature", "")

    return merged


def _apply_tags(r: dict):
    """플랫폼 태그 및 [확인필요] 접두어 적용"""
    tags = set(r.get("tags", {r.get("_platform", "")}))
    r["tags"] = tags

    has_rter = "알터" in tags
    has_bank = "뱅크" in tags

    feature = str(r.get("feature", ""))

    if has_bank and not has_rter:
        r["feature"] = "[확인필요: 뱅크전용] " + feature
    elif has_rter and not has_bank:
        dong = r.get("dong", "")
        if not dong:
            r["dong"] = "미확인"
        r["feature"] = "[확인필요: 알터전용] " + feature

    tag_str = " ".join([f"#{t}" for t in sorted(list(tags))])
    r["플랫폼"] = tag_str
