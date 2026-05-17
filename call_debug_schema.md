# call_debug.json Schema

`call_debug.json`은 단지별 호출 진단 결과를 저장한다.

```json
{
  "target": {
    "kapt_code": "...",
    "apt_name": "...",
    "sido": "...",
    "sigungu": "...",
    "dongri": "...",
    "legal_address": "...",
    "road_address": "...",
    "approval_date": "...",
    "household_count": 0,
    "building_count": 0
  },
  "resolved_ids": {
    "legal_dong_code8": "...",
    "rter_naverAptNo": "...",
    "bank_complex_cd": "..."
  },
  "resolver_candidates": {
    "rter": [],
    "bank": []
  },
  "requests": {
    "rter": {
      "url": "...",
      "method": "POST",
      "params": {}
    },
    "bank": {
      "url": "...",
      "method": "GET",
      "params": {}
    }
  },
  "response_summary": {
    "rter_status_code": 200,
    "bank_status_code": 200,
    "rter_count": 0,
    "bank_count": 0,
    "rter_parsed_count": 0,
    "bank_raw_count": 0,
    "bank_parsed_count": 0,
    "rter_fields": [],
    "bank_fields": []
  },
  "sample_records": {
    "rter": [],
    "bank": []
  },
  "warnings": []
}
```

## Notes

- `target`: K-apt 엑셀에서 정규화한 기준 단지 정보
- `resolved_ids`: Resolver가 선택한 사이트별 식별값
- `resolver_candidates`: 알터/부동산뱅크 후보 목록과 `match_score`, `match_reason`
- `requests`: 민감한 헤더 없이 실제 호출 URL, 메서드, 파라미터만 저장
- `response_summary`: 상태코드, raw/parsed 건수, 응답 필드 목록 요약
- `sample_records`: 파싱된 샘플 5건 이하
- `warnings`: 법정동 코드 누락, 호출 실패, 낮은 신뢰도 등 운영 경고
