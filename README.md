# Korea Trade Tracker MVP

관세청 OpenAPI와 DART 공시를 이용해 국내 상장사의 수주잔고를 추적하는 Python 기반 프로젝트입니다.

## 포함된 기능

- 국가별 수출입 실적 조회
- 품목별 수출입 실적 조회
- SQLite 저장
- 월별 증감 계산
- DART 정기공시 기반 수주잔고 추출
- 기업별 수주잔고 시계열 Markdown 생성
- 분기별 수주잔고 대시보드 생성
- MkDocs 기반 GitHub Pages 게시

## 사용 API

- 관세청 국가별 수출입실적
  - `http://apis.data.go.kr/1220000/nationtrade/getNationtradeList`
- 관세청 품목별 수출입실적
  - `http://apis.data.go.kr/1220000/Itemtrade/getItemtradeList`
- OpenDART
  - `https://opendart.fss.or.kr`

공식 문서:

- https://www.data.go.kr/en/data/15101612/openapi.do
- https://www.data.go.kr/en/data/15101609/openapi.do
- https://opendart.fss.or.kr

## 설치

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

필수 환경변수:

- `DART_API_KEY`

## 무역 데이터 실행 예시

국가별 데이터 조회:

```bash
python test.py country --start 202401 --end 202406 --country US
```

품목별 데이터 조회:

```bash
python test.py item --start 202401 --end 202406 --hs-code 854231
```

DB 저장 없이 조회만 하려면:

```bash
python test.py country --start 202401 --end 202406 --country CN --no-save
```

## DART 수주잔고 추출

정기공시의 분기보고서, 반기보고서, 사업보고서를 읽어 수주잔고 관련 표와 총액을 추출합니다.

단일 기업:

```bash
python dart_orders.py --company 삼성중공업 --start-date 20240101 --end-date 20241231
```

여러 기업 비교:

```bash
python dart_orders_batch.py --companies 삼성중공업 HD현대중공업 현대미포조선 한화오션 --start-date 20240101 --end-date 20241231
```

전체 상장사 분류:

```bash
python dart_classify_listed_companies.py --start-date 20240101 --end-date 20241231 --resume
```

기업별 시계열 생성:

```bash
python dart_orders_timeseries.py --company 삼성중공업 --start-date 20220101 --end-date 20260410 --document-source html --cache-dir outputs\.dart_cache
```

배치 시계열 생성:

```bash
python dart_orders_timeseries_batch.py --classification-csv outputs\dart_listed_backlog_classification_latest1y_html_rerun.csv --start-date 20220101 --end-date 20260410 --document-source html --html-request-interval 2 --cache-dir outputs\.dart_cache --timeseries-cache-dir outputs\.timeseries_cache --filings-cache-dir outputs\.filings_cache --output-dir outputs\수주잔고 --output-csv outputs\수주잔고\수주잔고_전체시계열.csv --manifest-csv outputs\수주잔고\수주잔고_생성현황.csv
```

대시보드 생성:

```bash
python dart_orders_dashboard.py
```

대시보드에는 분기별로 아래 항목이 포함됩니다.

- 수주잔고 규모 Top 순위
- QoQ 증감률 Top 15
- YoY 증감률 Top 15
- `YoY 3분기 이상 연속 증가 기업`
- `QoQ 3분기 이상 연속 증가 기업`

## 출력 구조

주요 출력물:

- `outputs/수주잔고/기업명_수주잔고(종목코드).md`
- `outputs/수주잔고/수주잔고_전체시계열.csv`
- `outputs/수주잔고/수주잔고_대시보드.md`
- `outputs/수주잔고/수주잔고_이상치점검표.md`
- `outputs/수주잔고/수주잔고_이상치점검표.csv`

캐시:

- `outputs/.dart_cache`
- `outputs/.filings_cache`
- `outputs/.timeseries_cache`

운영 메모:

- `dart_orders_timeseries.py`로 단일 기업을 다시 생성하면 기업별 Markdown뿐 아니라 `docs/companies/...` 게시본과 `outputs/.timeseries_cache/<corp_code>.csv`도 함께 갱신됩니다.
- `outputs/.timeseries_cache/*.csv`는 대시보드 합산 CSV와 랭킹 계산의 직접 입력입니다.
- `outputs/dart_listed_backlog_classification_latest1y_html_rerun.csv`에서 `has_backlog_total=False`로 표시된 기업은 배치 시계열 생성과 대시보드 집계 대상에서 제외됩니다.
- 일부 기업은 수동 예외 규칙을 사용합니다. 예: `KD` 시계열 보정, `원익홀딩스` 법인명 필터, `신성이엔지` 사업부문 분리.

## 게시용 문서 흐름

이 프로젝트는 작업용 원본 문서와 GitHub Pages 게시용 문서를 분리해서 관리합니다.

- `outputs/수주잔고/*.md`
  - 로컬 작업용 원본 문서입니다.
  - 기업별 시계열, 대시보드, 점검표를 먼저 이 경로에 생성합니다.
- `docs/*.md`
  - GitHub Pages 게시용 export 문서입니다.
  - MkDocs가 이 폴더를 기준으로 사이트를 생성합니다.

중요한 원칙:

- 원본 링크는 로컬 기준이어도 됩니다.
- 게시용 상대경로와 URL 보정은 `build_mkdocs_site.py`에서만 처리합니다.
- 수동으로 `docs/` 안 파일을 직접 고치기보다, 원본을 다시 만들고 export를 다시 수행하는 방식이 안전합니다.

게시용 export:

```bash
python build_mkdocs_site.py
```

대시보드 생성과 게시까지 한 번에 하려면:

```bash
python dart_orders_dashboard.py
```

즉 문서 갱신 순서는 아래처럼 유지하는 것이 안전합니다.

1. `outputs/수주잔고`의 원본 문서를 생성하거나 갱신합니다.
2. `python build_mkdocs_site.py`를 실행해 `docs/`를 다시 만듭니다.
3. 필요하면 `python dart_orders_dashboard.py`로 원본 대시보드와 `docs/`를 함께 재생성하고 GitHub에 자동 반영합니다.

## GitHub Pages

MkDocs 기반으로 GitHub Pages에 게시합니다.

핵심 파일:

- `mkdocs.yml`
- `build_mkdocs_site.py`
- `.github/workflows/deploy.yml`

배포 흐름:

```bash
python dart_orders_dashboard.py
```

GitHub 저장소에서 `Settings > Pages > Source`를 `GitHub Actions`로 설정하면 자동 배포됩니다.

## giscus 댓글 추가

GitHub Pages 정적 사이트에 `giscus` 댓글 영역을 붙여두었습니다.

현재 저장소에서 해야 할 설정:

1. GitHub 저장소의 `Settings > General > Features`에서 `Discussions`를 활성화합니다.
2. Discussions 안에 댓글용 카테고리 하나를 만듭니다. 현재 설정값은 `Announcements` 카테고리를 사용합니다.
3. `https://giscus.app`에서 아래 정보를 기준으로 설정값을 생성합니다.
   - Repository: `agent-jaden/order-backlog-dashboard`
   - Mapping: `pathname`
   - Category: `Announcements`
4. 발급된 `repoId`, `categoryId`를 `docs/javascripts/giscus.js`에 입력합니다.
5. 변경사항을 push 하면 GitHub Pages에 댓글이 표시됩니다.

설정 파일:

- `docs/javascripts/giscus.js`
- `docs/stylesheets/giscus.css`
- `mkdocs.yml`

주의:

- 댓글 작성자는 GitHub 로그인이 필요합니다.
- 현재 매핑은 URL 경로별(`pathname`)로 분리되어 각 페이지마다 댓글 스레드가 따로 생성됩니다.

## 주의

- DART 공시 본문 구조가 기업마다 달라 자동 추출 결과는 항상 재검토가 필요합니다.
- 단위(`원`, `천원`, `백만원`)와 합계 행 선택 문제로 이상치가 생길 수 있습니다.
- 극단적인 증감률은 분모가 매우 작은 경우 과대하게 보일 수 있습니다.
- 보고서에 자회사 표, 사업부문 표, 외화 표가 섞여 있으면 자동 추출이 오탐할 수 있어 예외 처리나 수동 제외가 필요할 수 있습니다.

## 다음 확장 아이디어

- 대시보드 섹터별 분리
- 이상치 자동 재분류
- CSV 외 Parquet 저장
- Streamlit 또는 별도 웹 UI 추가
