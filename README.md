# Korea Trade Tracker MVP

관세청 수출입 OpenAPI를 사용해 국내 수출입 통계를 수집하고, 월별 변화율을 추적하는 Python 기반 MVP입니다.

## 포함된 기능

- 국가별 수출입 실적 조회
- 품목별 수출입 실적 조회
- SQLite 저장
- 전월 대비 변화율 계산
- 콘솔 기반 요약 리포트 출력

## 사용 API

- 관세청 국가별 수출입실적(GW)
  - 요청 URL: `http://apis.data.go.kr/1220000/nationtrade/getNationtradeList`
- 관세청 품목별 수출입실적(GW)
  - 요청 URL: `http://apis.data.go.kr/1220000/Itemtrade/getItemtradeList`

공식 문서:

- https://www.data.go.kr/en/data/15101612/openapi.do
- https://www.data.go.kr/en/data/15101609/openapi.do

## 설치

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

`.env`에 공공데이터포털 서비스키를 넣어주세요.

## 실행 예시

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

전자공시 정기보고서(분기/반기/사업보고서) 원문 ZIP을 내려받아 `수주잔고` 관련 문맥과 금액 후보를 추출하고, 결과를 마크다운으로 저장할 수 있습니다.

필수 환경변수:

- `DART_API_KEY`

실행 예시:

```bash
python dart_orders.py --company 삼성중공업 --start-date 20240101 --end-date 20241231
```

여러 기업 합계 비교표:

```bash
python dart_orders_batch.py --companies 삼성중공업 HD현대중공업 현대미포조선 한화오션 --start-date 20240101 --end-date 20241231
```

전체 상장사 수주잔고 분류:

```bash
python dart_classify_listed_companies.py --start-date 20240101 --end-date 20241231 --resume
```

출력:

- `outputs/<회사명>_order_backlog.md`
- `outputs/order_backlog_batch.md`
- `outputs/dart_listed_backlog_classification.csv`
- `outputs/dart_listed_backlog_classification.md`
- 요약 표와 원문 문맥 포함

주의:

- DART 원문 표 구조가 기업마다 달라서, 자동 추출 결과는 후보값 중심입니다.
- 단위(`억원`, `백만원` 등)는 원문 주변 문맥에서 추정합니다.
- 실제 운영 전에는 자주 조회하는 기업 몇 곳으로 패턴 보정이 필요합니다.

## 다음 확장 아이디어

- FastAPI로 API 서버 노출
- Streamlit 대시보드 추가
- 품목/국가별 알림 조건 설정
- 스케줄러로 월별 자동 수집
