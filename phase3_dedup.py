import json

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

def are_cats_adjacent(c1, c2):
    if c1 == c2: return True
    return {c1, c2} in [{"저", "중"}, {"중", "고"}]

def deduplicate_and_merge(rter_data, bank_data):
    all_recs = []
    for r in rter_data:
        r["_platform"] = "알터"
        all_recs.append(r)
    for b in bank_data:
        b["_platform"] = "뱅크"
        all_recs.append(b)
        
    for i, r in enumerate(all_recs):
        r["_cat"] = get_floor_category(r["floor"], r["total_floor"])
        # If dong is missing, assign a unique ID for grouping to prevent any merging
        if r.get("dong"):
            r["_dong"] = r["dong"]
        else:
            r["_dong"] = f"__UNIQUE_{i}__"
            
        r["_space"] = r["space"]
        r["_price"] = r["price"]
        r["tags"] = {r["_platform"]}

    # Phase 3 & Fuzzy Floor Matching: Group by (dong, space, price)
    exact_price_groups = {}
    for r in all_recs:
        k = (r["_dong"], r["_space"], r["_price"])
        if k not in exact_price_groups:
            exact_price_groups[k] = []
        exact_price_groups[k].append(r)
        
    merged_phase3 = []
    for k, group in exact_price_groups.items():
        # merge items inside the group if their cats are adjacent
        merged_sub = []
        for item in group:
            merged = False
            for m in merged_sub:
                if are_cats_adjacent(m["_cat"], item["_cat"]):
                    m["tags"].update(item["tags"])
                    if item["_platform"] == "알터" and item.get("feature"):
                        if not m.get("feature"):
                            m["feature"] = item["feature"]
                    merged = True
                    break
            if not merged:
                merged_sub.append(item.copy())
        merged_phase3.extend(merged_sub)

    # Phase 4 Price Conflict: Group by (dong, cat, space)
    base_map = {}
    for m in merged_phase3:
        bk = (m["_dong"], m["_cat"], m["_space"])
        if bk not in base_map:
            base_map[bk] = [m]
        else:
            base_map[bk].append(m)
            
    final_results = []
    
    for bk, group in base_map.items():
        if len(group) == 1:
            final_results.append(group[0])
            continue
            
        all_tags = set()
        min_price_item = group[0]
        for g in group:
            all_tags.update(g["tags"])
            if g["_price"] < min_price_item["_price"]:
                min_price_item = g
                
        conflict_logs = []
        for g in group:
            for t in g["tags"]:
                conflict_logs.append(f"{t}:{g['_price']}")
        
        feature_notes = f"[가격 불일치({', '.join(conflict_logs)})] "
        
        merged_item = min_price_item.copy()
        merged_item["tags"] = all_tags
        merged_item["feature"] = feature_notes + str(merged_item.get("feature", ""))
        
        final_results.append(merged_item)
        
    for r in final_results:
        tags = set(r["tags"])
        if "뱅크" in tags and "알터" not in tags:
            r["feature"] = "[확인필요: 뱅크전용] " + str(r.get("feature", ""))
            
        tag_str = " ".join([f"#{t}" for t in sorted(list(tags))])
        r["플랫폼"] = tag_str
        
    return final_results
