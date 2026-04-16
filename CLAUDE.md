# GPT Codex — DART 수주잔고 프로젝트

DART(전자공시시스템) OpenAPI를 통해 KOSPI/KOSDAQ 상장기업의 수주잔고를 파싱·집계하고, GitHub Pages에 대시보드를 퍼블리시하는 프로젝트입니다.

## 프로젝트 구조

```
trade_tracker/dart.py          # 핵심 파서 — HTML 추출·단위 감지·합계 선택
dart_orders_timeseries.py      # 단일 기업 시계열 빌드 + CLI
dart_orders_timeseries_batch.py# 전체 기업 배치 실행 (resume 지원)
dart_orders_dashboard.py       # 대시보드 MD 생성 + mkdocs 빌드 + GitHub 푸시
build_mkdocs_site.py           # mkdocs export·commit·push 헬퍼
dart_orders.py                 # 단일 기업 수주잔고 추출 CLI
dart_classify_listed_companies.py  # 기업 분류 (has_backlog_total 여부)

outputs/수주잔고/              # 기업별 MD 파일 + 전체시계열.csv + 대시보드.md
outputs/.dart_cache/html/      # 공시 HTML 캐시 (rcept_no 기준 디렉토리)
outputs/.dart_cache/filings/   # 기업별 공시 목록 캐시 (corp_code 기준)
outputs/.timeseries_cache/     # 기업별 시계열 CSV 캐시 (corp_code.csv)
outputs/.filings_cache/        # 배치용 공시 목록 캐시
docs/                          # mkdocs 소스 (build_mkdocs_site.py가 생성)
site/                          # mkdocs 빌드 결과
```

## 환경 설정

```
DART_API_KEY=<키>   # .env 파일에 저장
```

의존성 설치: `pip install -r requirements.txt`

## 자주 쓰는 명령어

### 전체 배치 (처음 실행)
```bash
python dart_orders_timeseries_batch.py --html-request-interval 2.0
```

### 전체 배치 (재실행 — 성공 기업 건너뜀)
```bash
python dart_orders_timeseries_batch.py --html-request-interval 0.1 --resume
```

### 대시보드 업데이트 + GitHub 푸시
```bash
python dart_orders_dashboard.py
```

### 단일 기업 시계열 확인
```bash
python dart_orders_timeseries.py --company 294630
```

## 핵심 파서 로직 (`trade_tracker/dart.py`)

### 단위 감지 — `_detect_explicit_backlog_unit`
- HTML 전체 텍스트에서 `단위` 키워드를 **rfind** (마지막 위치)로 찾음
- `단위` 이후 120자 윈도우에서 `백만원 / 천원 / 억원 / 만원 / 원` 탐색
- 비교 전 **공백 제거** (`천 원` → `천원` 처리)
- **주의**: `find` 대신 `rfind`를 쓰는 이유 — 매출실적 테이블의 단위와 수주현황 테이블의 단위가 공존할 때 수주현황(후반부) 단위를 우선해야 하기 때문

### 합계 행 선택 — `build_total_summary`
- `total_terms` 키워드 (`합계`, `수주잔고 금액`, `기말수주잔고` 등) 를 **공백 제거 후** 비교
- `합 계` (공백 포함) 도 `합계`로 인식됨
- `수주총액` 컬럼 값은 기본 제외 (stock_code `094280` 제외)
- fallback 순서: ① `total_mask` 행 선택 → ② 단일 행 공시 → ③ `_aggregate_business_segment_totals` (세그먼트 합산)

### 공시별 독립 처리 — `_build_timeseries_total_summary`
- match_df를 `(filing_date, report_name)` 단위로 **groupby**해서 각 공시에 독립적으로 `build_total_summary` 호출
- 이전 일괄 처리 방식에서는 다른 공시의 합계 행이 fallback 로직을 막는 문제가 있었음

### 특수 기업 처리
| stock_code | 처리 |
|---|---|
| `046940` | `잔여기성` 컬럼 사용 |
| `094280` | `수주총액` 컬럼 사용 |
| `011930` | 클린환경 / 재생에너지 사업부문 분리 (`_build_manual_segmented_series`) |

## 배치 스크립트 주요 옵션

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--classification-csv` | `outputs/dart_listed_backlog_classification_latest1y_html_rerun.csv` | 타겟 기업 목록 |
| `--start-date` | `20220101` | 공시 검색 시작일 |
| `--html-request-interval` | `2.0` | 요청 간격 (캐시 히트 시 `0.1`도 가능) |
| `--resume` | off | 성공 기업 건너뜀 |
| `--finalize-only` | off | DART 미호출, 캐시에서만 CSV 재생성 |
| `--limit` | 없음 | 테스트용 기업 수 제한 |

## 수동 제외 기업 (`MANUAL_NO_BACKLOG_STOCK_CODES`)
대선조선(031990), 디지아이(043360), 케이이엠텍(106080), 인산가(277410), SAMG엔터(419530), 윙입푸드(900340), 한국공항(005430), 시알홀딩스(000480), 조선내화(462520), 디아이씨(092200), 대동기어(008830)

- 시알홀딩스·조선내화: 공시의 "수주총액" 컬럼이 잔고가 아닌 매출 개념이므로 제외
- 디아이씨·대동기어: 수주잔고 없음 (원문 확인)

## 대시보드 GitHub Pages
- URL: `https://agent-jaden.github.io/order-backlog-dashboard/`
- `dart_orders_dashboard.py` 실행 시 자동으로 `git commit + push`
- 표시 분기: 2025.12 / 2025.09 / 2025.06 / 2025.03

## 알려진 엣지 케이스
- **값이 모두 `-`인 수주잔고 테이블**: 파서가 숫자를 추출하지 못해 해당 공시 제외 (정상 동작). 서남(294630) 2023.12~2024.06이 해당.
- **수주잔고 테이블 없이 텍스트만 있는 공시**: 동일하게 제외. 사업보고서에서 "대규모 계약만 공시" 문구로 테이블 생략하는 경우.
- **정정공시 중복**: 동일 기간에 정정공시가 여러 건이면 최신 공시(`filing_date` 기준 최후) 우선.
